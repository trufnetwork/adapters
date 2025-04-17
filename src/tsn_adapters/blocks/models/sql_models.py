"""
SQLAlchemy models for blocks.
"""

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    MetaData,
    Table,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import TEXT

# Define metadata for the tables
metadata = MetaData()

primitive_sources_table = Table(
    "primitive_sources",
    metadata,
    Column("stream_id", TEXT, primary_key=True, nullable=False),
    Column("source_id", TEXT, nullable=False),
    Column("source_type", TEXT, nullable=False),
    Column("source_display_name", TEXT, nullable=True),
    Column("is_deployed", Boolean, nullable=False, server_default=text('false')),
    Column("deployed_at", DateTime(timezone=True), nullable=True),
    Column(
        "created_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    ),
    Column(
        "updated_at",
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    ),
    # Optional: Add specific database schema name if required
    # schema="your_schema_name",
)
