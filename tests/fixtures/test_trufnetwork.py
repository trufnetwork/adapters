from collections.abc import Generator
from dataclasses import dataclass, field
import json
import logging
import os
import signal
import subprocess
import time
from typing import Any, Optional
from unittest.mock import Mock

from prefect.client.orchestration import get_client
from prefect.client.schemas.actions import GlobalConcurrencyLimitCreate
from prefect.exceptions import ObjectNotFound
from prefect.testing.utilities import prefect_test_harness
from pydantic import SecretStr
import pytest

from tsn_adapters.blocks.tn_access import TNAccessBlock

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define the network name to use
NETWORK_NAME = "tsn_network"
# Define the default provider URL
KWIL_PROVIDER_URL = "http://localhost:8484"
# Define the private key for database operations, matching server_fixture.go
DB_PRIVATE_KEY = "0000000000000000000000000000000000000000000000000000000000000001"


@dataclass
class ContainerSpec:
    """Configuration for a docker container"""

    name: str
    image: str
    tmpfs_path: Optional[str] = None
    env_vars: list[str] = field(default_factory=list)
    ports: dict[str, str] = field(default_factory=dict)
    entrypoint: Optional[str] = None
    args: list[str] = field(default_factory=list)


# Container specifications
POSTGRES_CONTAINER = ContainerSpec(
    name="test-kwil-postgres",
    image="kwildb/postgres:latest",
    tmpfs_path="/var/lib/postgresql/data",
    env_vars=["POSTGRES_HOST_AUTH_METHOD=trust"],
    ports={"5432": "5432"},
)

# Renamed from TSN_DB_CONTAINER and updated spec
TN_DB_CONTAINER = ContainerSpec(
    name="test-tn-db",
    image="tn-db:local",
    tmpfs_path="/root/.kwild",
    entrypoint="/app/kwild",
    args=[
        "start",
        "--autogen",
        "--root", # Added arg
        "/root/.kwild", # Added arg value
        "--db-owner",
        "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf", # Updated address
        "--db.host",
        "test-kwil-postgres",
        "--consensus.propose-timeout",
        "500ms",
        "--consensus.empty-block-timeout",
        "30s",
    ],
    env_vars=[
        "CONFIG_PATH=/root/.kwild",
        "KWILD_APP_HOSTNAME=test-tn-db",
        "KWILD_APP_PG_DB_HOST=test-kwil-postgres",
        "KWILD_APP_PG_DB_PORT=5432",
        "KWILD_APP_PG_DB_USER=postgres",
        "KWILD_APP_PG_DB_PASSWORD=",
        "KWILD_APP_PG_DB_NAME=postgres",
        "KWILD_CHAIN_P2P_EXTERNAL_ADDRESS=http://test-tn-db:26656",
    ],
    ports={"8080": "8080", "8484": "8484", "26656": "26656"},
)


def run_docker_command(args: list[str], check: bool = False) -> subprocess.CompletedProcess[str]:
    """
    Executes a docker command with the given list of arguments.

    Args:
        args: List of command arguments to pass to docker
        check: If True, raises CalledProcessError on non-zero exit status

    Returns:
        CompletedProcess instance with command output

    Raises:
        subprocess.CalledProcessError: If check=True and command returns non-zero exit status
    """
    command = ["docker", *args]
    logger.debug(f"Running docker command: {' '.join(command)}")
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=check)
        if result.stderr:
            logger.debug(f"Docker command stderr: {result.stderr}")
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"Docker command failed: {e.stderr}")
        raise


def wait_for_postgres_health(max_attempts: int = 30) -> bool:
    """
    Wait for postgres container to be healthy

    Args:
        max_attempts: Maximum number of health check attempts

    Returns:
        bool: True if postgres becomes healthy, False otherwise
    """
    for i in range(max_attempts):
        try:
            result = run_docker_command(["exec", POSTGRES_CONTAINER.name, "pg_isready", "-U", "postgres"])
            if result.returncode == 0:
                logger.info(f"Postgres is healthy after {i+1} attempts")
                return True
            logger.debug(f"Postgres not ready (attempt {i+1}/{max_attempts}): {result.stderr}")
        except Exception as e:
            logger.error(f"Error checking postgres health: {e!s}")
        time.sleep(1)
    return False


def wait_for_tn_health(max_attempts: int = 10) -> bool:
    """
    Wait for TN-DB node to be healthy and produce first block

    Args:
        max_attempts: Maximum number of health check attempts

    Returns:
        bool: True if TN-DB becomes healthy, False otherwise
    """
    import requests

    for i in range(max_attempts):
        try:
            logger.info(f"Checking TN-DB health (attempt {i+1}/{max_attempts})")
            response = requests.get(f"{KWIL_PROVIDER_URL}/api/v1/health")
            if response.status_code == 200:
                data = response.json()
                if data.get("healthy") and data.get("services").get("user").get("block_height") >= 1:
                    logger.info(f"TN-DB is healthy after {i+1} attempts")
                    logger.debug(f"Health check response: {json.dumps(data, indent=2)}")
                    return True
            logger.debug(f"TN-DB not healthy yet (attempt {i+1}/{max_attempts}): {response.text}")
        except Exception as e:
            logger.debug(f"Error checking TN-DB health (attempt {i+1}/{max_attempts}): {e!s}")
        time.sleep(1)
    return False


def start_container(spec: ContainerSpec, network: str) -> bool:
    """
    Start a docker container with the given specification

    Args:
        spec: Container specification
        network: Docker network name

    Returns:
        bool: True if container starts successfully, False otherwise
    """
    # First ensure container doesn't exist
    run_docker_command(["rm", "-f", spec.name])

    args = ["run", "--rm", "--name", spec.name, "--network", network, "-d"]

    if spec.tmpfs_path:
        args.extend(["--tmpfs", spec.tmpfs_path])

    for env in spec.env_vars:
        args.extend(["-e", env])

    for host_port, container_port in spec.ports.items():
        args.extend(["-p", f"{host_port}:{container_port}"])

    if spec.entrypoint:
        args.extend(["--entrypoint", spec.entrypoint])

    args.append(spec.image)

    if spec.args:
        args.extend(spec.args)

    try:
        run_docker_command(args, check=True)
        logger.info(f"Successfully started container {spec.name}")

        # Get container logs
        time.sleep(2)  # Wait a bit for container to initialize
        logs = run_docker_command(["logs", spec.name])
        logger.debug(f"Container logs for {spec.name}:")
        logger.debug(logs.stdout)
        if logs.stderr:
            logger.debug(f"Container stderr: {logs.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to start container {spec.name}: {e.stderr}")
        return False


def stop_container(name: str) -> bool:
    """
    Stop a docker container

    Args:
        name: Name of the container to stop

    Returns:
        bool: True if container stops successfully, False otherwise
    """
    logger.info(f"Stopping container {name}...")
    try:
        run_docker_command(["stop", name], check=True)
        logger.info(f"Successfully stopped container {name}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to stop container {name}: {e.stderr}")
        return False


@pytest.fixture(scope="session")
def docker_network():
    """
    Pytest fixture to set up and tear down a Docker network.

    Setup:
      - Attempts to remove any existing network with the same name
      - Creates a new network using: docker network create <NETWORK_NAME>

    Teardown:
      - Removes the network using: docker network rm <NETWORK_NAME>

    Returns:
        str: The name of the created network

    Raises:
        pytest.FixureError: If network creation fails
    """
    logger.info("Setting up docker network...")
    # Remove existing network (ignore errors)
    run_docker_command(["network", "rm", NETWORK_NAME])

    # Create the new network
    try:
        run_docker_command(["network", "create", NETWORK_NAME], check=True)
        logger.info(f"Docker network '{NETWORK_NAME}' created.")
    except subprocess.CalledProcessError as e:
        logger.error(f"Failed to create docker network '{NETWORK_NAME}': {e.stderr}")
        pytest.fail(f"Failed to create docker network '{NETWORK_NAME}': {e.stderr}")

    try:
        yield NETWORK_NAME
    finally:
        logger.info("Tearing down docker network...")
        run_docker_command(["network", "rm", NETWORK_NAME])
        logger.info(f"Docker network '{NETWORK_NAME}' removed.")


@pytest.fixture(scope="session")
def tn_node(docker_network: str) -> Generator[str, None, None]:
    """
    Pytest fixture that sets up a TN-DB node with Postgres for testing.

    This fixture:
    1. Starts a Postgres container
    2. Waits for Postgres to be healthy
    3. Starts the TN-DB node
    4. Waits for the node to be healthy and produce its first block
    5. Cleans up both containers after tests

    Args:
        docker_network: The docker network fixture

    Returns:
        str: The API endpoint URL for the TN-DB node

    Raises:
        pytest.FixureError: If container setup fails
    """
    logger.info("Starting Postgres container...")
    if not start_container(POSTGRES_CONTAINER, docker_network):
        pytest.fail("Failed to start Postgres container")

    logger.info("Waiting for Postgres to be healthy...")
    if not wait_for_postgres_health():
        stop_container(POSTGRES_CONTAINER.name)
        pytest.fail("Postgres failed to become healthy")

    logger.info("Starting TN-DB container...")
    if not start_container(TN_DB_CONTAINER, docker_network):
        stop_container(POSTGRES_CONTAINER.name)
        pytest.fail("Failed to start TN-DB container")

    logger.info("Waiting for TN-DB node to be healthy...")
    if not wait_for_tn_health():
        stop_container(TN_DB_CONTAINER.name)
        stop_container(POSTGRES_CONTAINER.name)
        pytest.fail("TN-DB node failed to become healthy")

    logger.info("Running migration task after TN-DB is healthy...")
    if not run_migration_task():
        stop_container(TN_DB_CONTAINER.name)
        stop_container(POSTGRES_CONTAINER.name)
        pytest.fail("Migration task failed")

    try:
        yield KWIL_PROVIDER_URL
    finally:
        logger.info("Cleaning up containers...")
        # Use updated container name TN_DB_CONTAINER
        stop_container(TN_DB_CONTAINER.name)
        stop_container(POSTGRES_CONTAINER.name)


class TrufNetworkProvider:
    """Provider class for interacting with the TrufNetwork node"""

    def __init__(self, api_endpoint: str = "http://localhost:8484"):
        """
        Initialize the provider

        Args:
            api_endpoint: The API endpoint URL for the TN node
        """
        self.api_endpoint = api_endpoint
        # Ensure the provider points to the correct URL constant if defaulting
        if self.api_endpoint == "http://localhost:8484":
            self.api_endpoint = KWIL_PROVIDER_URL
        self.provider = self

    def get_provider(self):
        """Get the provider instance"""
        return self.provider


@pytest.fixture(scope="session")
def tn_provider(tn_node: str) -> TrufNetworkProvider:
    """
    Returns a TrufNetworkProvider instance configured to use the test TN node.

    Args:
        tn_node: The TN node fixture providing the API endpoint

    Returns:
        TrufNetworkProvider: Configured provider instance
    """
    return TrufNetworkProvider(api_endpoint=tn_node)


# Skip these tests on CI environment
class TestTrufNetworkFixtures:
    """
    Test suite for TrufNetwork fixtures.

    These tests verify the fixture setup/teardown behavior.
    Only run locally, skipped in CI.
    """

    def test_docker_network_fixture(self, docker_network: str):
        """Test docker network creation and cleanup"""
        # Check network exists
        result = run_docker_command(["network", "inspect", docker_network])
        assert result.returncode == 0, "Docker network should exist during test"

    def test_tn_node_fixture(self, tn_node: str):
        """Test TN node setup and health"""
        import requests

        # Verify endpoint is accessible
        response = requests.get(f"{tn_node}/api/v1/health")
        assert response.status_code == 200

        data = response.json()
        assert data.get("healthy") is True
        assert data.get("services").get("user").get("block_height") >= 1

        # Verify containers are running
        for container in [POSTGRES_CONTAINER.name, TN_DB_CONTAINER.name]:
            result = run_docker_command(["container", "inspect", container])
            assert result.returncode == 0, f"Container {container} should be running"

    def test_tn_provider_fixture(self, tn_provider: TrufNetworkProvider):
        """Test TrufNetworkProvider configuration"""
        assert isinstance(tn_provider, TrufNetworkProvider)
        assert tn_provider.api_endpoint.startswith("http://")
        assert tn_provider.get_provider() is tn_provider


@pytest.fixture(scope="session", autouse=True)
def term_handler():
    """
    Fixture to transform SIGTERM into SIGINT. This permit us to gracefully stop the suite uppon SIGTERM.
    """
    orig = signal.signal(signal.SIGTERM, signal.getsignal(signal.SIGINT))
    yield
    signal.signal(signal.SIGTERM, orig)


@pytest.fixture(scope="session", autouse=False)
def prefect_test_fixture():
    """Prefect test harness fixture that also manages the 'tn-write' GCL."""
    limit_name = "tn-write"
    with prefect_test_harness(server_startup_timeout=120):
        with get_client(sync_client=True) as client:
            # Attempt to create the global concurrency limit
            try:
                logger.info(f"Creating '{limit_name}' global concurrency limit...")
                client.create_global_concurrency_limit(
                    concurrency_limit=GlobalConcurrencyLimitCreate(name=limit_name, limit=1)
                )
                logger.info(f"'{limit_name}' global concurrency limit created.")
            except Exception as e:
                # Check if it already exists by trying to read it
                try:
                    client.read_global_concurrency_limit_by_name(name=limit_name)
                    logger.warning(f"Global concurrency limit '{limit_name}' already exists. Skipping creation.")
                except ObjectNotFound:
                    logger.error(f"Failed to create global concurrency limit '{limit_name}': {e!s}")
                    raise  # Re-raise the original error if it wasn't an 'already exists' situation
                except Exception as read_e:
                    logger.error(
                        f"Failed to create or check global concurrency limit '{limit_name}': {e!s} / Check failed: {read_e!s}"
                    )
                    raise read_e  # Raise the checking error if reading failed

        try:
            yield
        finally:
            # Clean up the concurrency limit
            with get_client(sync_client=True) as client:
                try:
                    logger.info(f"Deleting '{limit_name}' global concurrency limit...")
                    client.delete_global_concurrency_limit_by_name(name=limit_name)
                    logger.info(f"'{limit_name}' global concurrency limit deleted.")
                except ObjectNotFound:
                    logger.warning(
                        f"Global concurrency limit '{limit_name}' not found during cleanup. Skipping deletion."
                    )
                except Exception as e:
                    # Log error but don't raise to avoid masking test failures
                    logger.error(f"Failed to delete global concurrency limit '{limit_name}': {e!s}")


@pytest.fixture(scope="function")
def show_prefect_logs_fixture(monkeypatch: Any):
    monkeypatch.setattr("tsn_adapters.utils.logging.get_logger_safe", Mock(return_value=logging.getLogger()))


DEFAULT_TN_PRIVATE_KEY = "0" * 63 + "1"  # 64 zeros ending with 1


@pytest.fixture(scope="session")
def tn_block(
    tn_provider: TrufNetworkProvider, prefect_test_fixture: Any
) -> TNAccessBlock:
    """Create a TNAccessBlock with test node and default credentials."""
    tn_block = TNAccessBlock(
        tn_provider=tn_provider.api_endpoint,
        tn_private_key=SecretStr(os.environ.get("TN_PRIVATE_KEY", DEFAULT_TN_PRIVATE_KEY)),
    )
    return tn_block


# Added from SDK fixture
def run_migration_task() -> bool:
    """
    Run the migration task using the command from server_fixture.go.

    Returns:
        bool: True if migration task is successful, False otherwise.
    """
    logger.info("Running migration task...")
    node_repo_dir = os.environ.get("NODE_REPO_DIR")
    if not node_repo_dir:
        logger.error("NODE_REPO_DIR environment variable not set. Migration task cannot run.")
        return False

    provider_arg = f"PROVIDER={KWIL_PROVIDER_URL}"
    private_key_arg = f"PRIVATE_KEY={DB_PRIVATE_KEY}"
    command = ["task", "action:migrate", provider_arg, private_key_arg]

    logger.info(f"Executing command in {node_repo_dir}: {' '.join(command)}")
    try:
        result = subprocess.run(
            command,
            cwd=node_repo_dir,
            capture_output=True,
            text=True,
            check=True  # Raise CalledProcessError on non-zero exit
        )
        logger.info(f"Migration task successful. Output:\n{result.stdout}")
        if result.stderr:
            logger.warning(f"Migration task stderr:\n{result.stderr}")
        return True
    except FileNotFoundError:
        logger.error(
            f"Migration task command 'task' not found. Ensure it's in PATH or NODE_REPO_DIR ({node_repo_dir}) is correct and contains the executable."
        )
        return False
    except subprocess.CalledProcessError as e:
        logger.error(f"Migration task failed in {node_repo_dir}. Error: {e}")
        logger.error(f"Stdout: {e.stdout}")
        logger.error(f"Stderr: {e.stderr}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during migration task: {e!s}")
        return False
