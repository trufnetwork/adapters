# --- PostgreSQL Test Fixture ---
from collections.abc import Generator
import logging  # Import logging
import time
from typing import Any

from _pytest.config.argparsing import Parser
import docker
from docker.client import DockerClient
import docker.errors  # Import docker.errors
from docker.models.containers import Container
import psycopg
from psycopg import sql  # Import sql submodule
from psycopg.rows import dict_row  # Import for type hinting cursors and the factory function
import pytest

# from pytest_postgresql.janitor import DatabaseJanitor # No longer needed
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.base import Transaction  # Import for type hinting
from sqlalchemy.orm import Session as SqlaSession, sessionmaker

from tsn_adapters.blocks.models.sql_models import metadata

logger = logging.getLogger(__name__)

# We assume the default 'postgres' database is available
DEFAULT_DATABASE_NAME = "postgres"

# Define DB_CONFIG with explicit types
DB_CONFIG: dict[str, Any] = {
    "dbname": "testdb",
    "user": "postgres",
    "password": "mysecretpassword",
    "host": "localhost",
    # Use str for port initially if psycopg complains, but int is usually fine for host port mapping
    "port": 52853,
}


def is_postgres_available(db_name: str | None = None) -> bool:
    """Check if the PostgreSQL server is responding."""
    conn_info = DB_CONFIG.copy()
    conn_info["dbname"] = db_name or DEFAULT_DATABASE_NAME
    # Ensure keys match psycopg.connect parameters (e.g., dbname, user, password, host, port)
    conn_str = (
        f"dbname={conn_info['dbname']} "
        f"user={conn_info['user']} "
        f"password={conn_info['password']} "
        f"host={conn_info['host']} "
        f"port={conn_info['port']}"
    )
    try:
        # Connect using connection string for clarity
        with psycopg.connect(conn_str, connect_timeout=5) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            return True
    except psycopg.OperationalError as e:
        logger.debug(f"Postgres check failed: {e}")
        return False
    except Exception as e:
        logger.warning(f"Unexpected error during Postgres check: {e}")
        return False


@pytest.fixture(scope="session")
def postgresql_container() -> Generator[Container, Any, None]:
    """Starts and stops a PostgreSQL container for the test session."""
    client: DockerClient = docker.from_env()
    container: Container | None = None
    image = "postgres:16"

    logger.info(f"Attempting to start PostgreSQL container (image: {image})...")
    try:
        # Pull the image if it doesn't exist locally
        try:
            client.images.get(image)
            logger.info(f"Docker image {image} found locally.")
        except docker.errors.ImageNotFound:
            logger.info(f"Docker image {image} not found, pulling...")
            client.images.pull(image)
            logger.info(f"Docker image {image} pulled successfully.")

        container = client.containers.run(
            image=image,
            auto_remove=True,
            name="pytest-postgres-db",  # Give it a specific name
            ports={"5432/tcp": DB_CONFIG["port"]},  # Map container 5432 to host port
            environment={"POSTGRES_PASSWORD": str(DB_CONFIG["password"])},  # Ensure password is str
            detach=True,
            # remove=True, # remove=True conflicts with retrieving logs/stopping gracefully
        )
        logger.info(f"Container {container.short_id} started.")

        timeout = 60  # Increased timeout

        # Wait for the container to be running
        start_time = time.time()
        while time.time() - start_time < timeout:
            container.reload()  # type: ignore[no-untyped-call]
            if container.status == "running":  # type: ignore[comparison-overlap]
                logger.info("Container status is 'running'.")
                break
            time.sleep(1)
        else:
            container_logs = container.logs().decode()  # type: ignore[no-untyped-call]
            raise TimeoutError(
                f"Postgres container did not reach 'running' status in {timeout}s. Logs:\n{container_logs}"
            )

        # Wait for the database server to accept connections
        logger.info("Waiting for PostgreSQL server to accept connections...")
        start_time = time.time()
        while time.time() - start_time < timeout:
            # Check against the *default* postgres database first
            if is_postgres_available(db_name=DEFAULT_DATABASE_NAME): 
                logger.info("PostgreSQL server is available.")
                break
            time.sleep(1)
        else:
            container_logs = container.logs().decode() # type: ignore[no-untyped-call]
            raise TimeoutError(f"Postgres server did not become available in {timeout}s. Logs:\n{container_logs}")

        # --- Create the test database --- 
        logger.info(f"Creating test database '{DB_CONFIG['dbname']}'...")
        try:
            # Connect to the default 'postgres' db to create the test db
            conn_info_default = DB_CONFIG.copy()
            conn_info_default["dbname"] = DEFAULT_DATABASE_NAME
            default_conn_str = (
                f"dbname={conn_info_default['dbname']} "
                f"user={conn_info_default['user']} "
                f"password={conn_info_default['password']} "
                f"host={conn_info_default['host']} "
                f"port={conn_info_default['port']}"
            )
            # Type hint the connection and cursor
            with psycopg.connect(default_conn_str, autocommit=True) as conn: # type: ignore
                # Use autocommit=True for CREATE DATABASE
                with conn.cursor(row_factory=dict_row) as cur:
                    db_name_identifier = sql.Identifier(DB_CONFIG['dbname'])
                    # Check if database exists before creating
                    # Use sql module for safe query construction
                    query_exists = sql.SQL("SELECT 1 FROM pg_database WHERE datname = {db_name}").format(
                        db_name=sql.Literal(DB_CONFIG['dbname']) # Use Literal for values
                    )
                    cur.execute(query_exists)
                    exists = cur.fetchone()
                    if not exists:
                        # Use sql module for safe query construction with identifiers
                        query_create = sql.SQL("CREATE DATABASE {db_name}").format(
                            db_name=db_name_identifier
                        )
                        cur.execute(query_create)
                        logger.info(f"Database '{DB_CONFIG['dbname']}' created.")
                    else:
                        logger.info(f"Database '{DB_CONFIG['dbname']}' already exists.")
        except Exception as db_create_err:
            logger.error(f"Failed to create test database '{DB_CONFIG['dbname']}': {db_create_err}", exc_info=True)
            pytest.fail(f"Failed to create test database '{DB_CONFIG['dbname']}': {db_create_err}")
        # -----------------------------------

        yield container

    except Exception as e:
        logger.error(f"Error setting up PostgreSQL container: {e}", exc_info=True)
        if container:
            try:
                container_logs_bytes = container.logs()  # type: ignore[no-untyped-call]
                logger.error(f"Container logs:\n{container_logs_bytes.decode()}")
            except Exception as log_err:
                logger.error(f"Could not retrieve container logs: {log_err}")
        pytest.fail(f"Failed to setup PostgreSQL container: {e}")

    finally:
        if container:
            logger.info(f"Stopping PostgreSQL container {container.short_id}...")
            try:
                container.stop()  # type: ignore[no-untyped-call]
                logger.info("Container stopped.")
            except Exception as e:
                logger.error(f"Error stopping container {container.short_id}: {e}")


# --- Fixtures dependent on the container ---


@pytest.fixture(scope="function")
def db_engine(postgresql_container: Container) -> Engine:  # Remove postgresql_proc dependency
    """Creates a SQLAlchemy engine connected to the test database managed by the container."""
    # Construct connection URL using details from DB_CONFIG, matching the container
    conn_url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    engine = create_engine(conn_url)
    # Optional: Verify connection works before yielding
    try:
        # Type hint the connection
        with engine.connect() as conn: # conn is type SqlaConnection
            conn.execute(text("SELECT 1")) # execute returns CursorResult
    except Exception as e:
        pytest.fail(f"Failed to connect SQLAlchemy engine to test DB: {e}")
    return engine


@pytest.fixture(scope="function")
def db_session(db_engine: Engine) -> Generator[SqlaSession, None, None]:
    """Function-scoped fixture using the engine connected to the test DB.

    Creates tables and provides a session with rollback.
    """
    connection = db_engine.connect()
    # Begin a non-ORM transaction
    trans: Transaction = connection.begin()

    # Bind an individual Session to the connection
    SessionLocal = sessionmaker(bind=connection) # sessionmaker returns a session factory
    session: SqlaSession = SessionLocal() # Calling the factory creates a Session

    # Start the session transaction (optional if logic doesn't require nested)
    # nested_trans: SessionTransaction = session.begin_nested()

    # Setup: Create tables
    try:
        metadata.create_all(db_engine)
    except Exception as e:
        pytest.fail(f"Failed to create tables: {e}")

    yield session

    # Teardown: Rollback and close
    session.rollback() # rollback() is a method of SqlaSession
    session.close() # Close the session
    connection.close() # Close the connection
    # Roll back the broader transaction
    # trans.rollback() # This might be redundant if connection.close() handles it, depends on engine/dialect
    # Drop tables after each test function for isolation
    metadata.drop_all(db_engine)


def pytest_addoption(parser: Parser):
    """Adds command-line options for PostgreSQL connection parameters."""
    # These are kept but not used by the current Docker fixture setup
    parser.addoption("--pguser", action="store", default="testuser", help="PostgreSQL user for Janitor (unused)")
    parser.addoption("--pghost", action="store", default="localhost", help="PostgreSQL host for Janitor (unused)")
    parser.addoption("--pgport", action="store", default="5433", help="PostgreSQL port for Janitor (unused)")
    parser.addoption(
        "--pgpassword", action="store", default="testpassword", help="PostgreSQL password for Janitor (unused)"
    )
    parser.addoption(
        "--pgdbname", action="store", default="test_db", help="PostgreSQL database name for Janitor (unused)"
    )


# --- Basic Test for the Fixture ---

def test_db_session_fixture(db_session: SqlaSession):
    """A very basic test to check if the db_session fixture works."""
    # Check if the session is active
    assert db_session.is_active

    # Execute a simple query to confirm connectivity
    result = db_session.execute(text("SELECT 1")).scalar_one()
    assert result == 1
