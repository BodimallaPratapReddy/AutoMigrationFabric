"""Connection helper for the Fabric SQL configuration database."""

from __future__ import annotations

import os
from collections.abc import Sequence
from contextlib import nullcontext
from datetime import datetime
from typing import TYPE_CHECKING, Annotated
from uuid import UUID, uuid4

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

if TYPE_CHECKING:
    from mssql_python.connection import Connection


NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]


class FabricWorkspace(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: int = Field(alias="Id")
    workspace_name: str = Field(alias="WorkspaceName")
    workspace_id: str = Field(alias="WorkspaceId")
    created_timestamp: datetime | None = Field(alias="CreatedTimestamp")
    updated_timestamp: datetime | None = Field(alias="UpdatedTimestamp")


class SourceTableCreate(BaseModel):
    connection_name: NonEmptyString
    source_table_name: NonEmptyString
    data_source_type: str | None = None
    application_component: str | None = None
    delta: str | None = None
    is_active: bool = Field(default=True, strict=True)
    fabric_workspace_name: str | None = None
    fabric_workspace_id: str | None = None
    fabric_lakehouse_name: str | None = None
    fabric_lakehouse_schema: str | None = None
    fabric_table_name: str | None = None


class SourceTableColumnCreate(BaseModel):
    sno: int = Field(gt=0, strict=True)
    column_name: NonEmptyString
    source_data_type: NonEmptyString
    fabric_data_type: NonEmptyString
    description: str | None = None


class WatermarkControlCreate(BaseModel):
    source_table_full_name: NonEmptyString
    destination_table_full_name: NonEmptyString
    watermark_column: NonEmptyString
    watermark_column_data_type: NonEmptyString
    last_watermark_value: str | None = None
    max_row_fetch: int = Field(default=0, ge=0, strict=True)
    ingestion_flag: bool = Field(default=True, strict=True)
    load_strategy: NonEmptyString = "INCREMENTAL"
    merge_key_column: str | None = None


class ConfigDB:
    """Access the Fabric SQL configuration database using CONFIG_DB_URL."""

    def __init__(self, connection_string: str | None = None) -> None:
        load_dotenv()
        self.connection_string = connection_string or os.getenv("CONFIG_DB_URL")
        if not self.connection_string:
            raise ValueError("Missing Fabric configuration: CONFIG_DB_URL")

    def connect(self) -> Connection:
        """Open a connection that the caller must close after use."""
        import mssql_python

        return mssql_python.connect(
            self.connection_string,
            timeout=int(os.getenv("CONFIG_DB_CONNECT_TIMEOUT", "30")),
        )

    def list_fabric_workspaces(self) -> list[FabricWorkspace]:
        """Return every configured Fabric workspace, ordered by database Id."""
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT Id, WorkspaceName, WorkspaceId, "
                    "CreatedTimestamp, UpdatedTimestamp "
                    "FROM bronze_replication.FabricWorkspaces ORDER BY Id"
                )
                rows = cursor.fetchall()

        return [
            FabricWorkspace(
                id=row[0],
                workspace_name=row[1],
                workspace_id=row[2],
                created_timestamp=row[3],
                updated_timestamp=row[4],
            )
            for row in rows
        ]

    def insert_source_table(
        self,
        table: SourceTableCreate,
        *,
        connection: Connection | None = None,
    ) -> str:
        """Insert a source table and return its GUID as a string.

        Pass an open connection to group this insert with column inserts in
        one transaction. Otherwise this method opens and commits its own.
        """
        guid = str(uuid4())
        manager = nullcontext(connection) if connection is not None else self.connect()
        with manager as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO bronze_replication.SourceTables ("
                    "GUID, ConnectionName, SourceTableName, CreatedTimestamp, "
                    "UpdatedTimestamp, DataSourceType, ApplicationComponent, "
                    "FabricWorkspaceName, FabricWorkspaceId, FabricLakehouseName, "
                    "FabricLakehouseSchema, FabricTableName, IsActive, Delta"
                    ") VALUES (?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME(), "
                    "?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        guid,
                        table.connection_name,
                        table.source_table_name,
                        table.data_source_type,
                        table.application_component,
                        table.fabric_workspace_name,
                        table.fabric_workspace_id,
                        table.fabric_lakehouse_name,
                        table.fabric_lakehouse_schema,
                        table.fabric_table_name,
                        table.is_active,
                        table.delta,
                    ),
                )
        return guid

    def insert_source_table_columns(
        self,
        source_table_guid: str | UUID,
        columns: Sequence[SourceTableColumnCreate],
        *,
        connection: Connection | None = None,
    ) -> None:
        """Insert the metadata columns for an existing source table."""
        try:
            guid = str(UUID(str(source_table_guid)))
        except (ValueError, AttributeError) as exc:
            raise ValueError("source_table_guid must be a valid UUID") from exc
        if not columns:
            raise ValueError("columns must be nonempty")

        rows: list[tuple[str, int, str, str | None, str, str]] = []
        seen_positions: set[int] = set()
        for column in columns:
            if column.sno in seen_positions:
                raise ValueError("each column sno must be unique")
            seen_positions.add(column.sno)
            rows.append(
                (
                    guid,
                    column.sno,
                    column.column_name,
                    column.description,
                    column.source_data_type,
                    column.fabric_data_type,
                )
            )

        manager = nullcontext(connection) if connection is not None else self.connect()
        with manager as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute(
                    "SELECT 1 FROM bronze_replication.SourceTables WHERE GUID = ?",
                    (guid,),
                )
                if cursor.fetchone() is None:
                    raise ValueError(f"Source table {guid} does not exist")
                cursor.executemany(
                    "INSERT INTO bronze_replication.SourceTableColumns "
                    "(GUID, Sno, ColumnName, Description, SourceDataType, FabricDataType) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    rows,
                )

    def insert_watermark_control(
        self,
        watermark: WatermarkControlCreate,
        *,
        connection: Connection | None = None,
    ) -> None:
        """Insert a watermark record with the current UTC modification time."""
        manager = nullcontext(connection) if connection is not None else self.connect()
        with manager as db_connection:
            with db_connection.cursor() as cursor:
                cursor.execute(
                    "INSERT INTO bronze_replication.watermark_control ("
                    "source_table_full_name, destination_table_full_name, "
                    "watermark_column, watermark_column_data_type, last_watermark_value, "
                    "max_row_fetch, ingestion_flag, load_strategy, merge_key_column, "
                    "last_modified_timestamp"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETUTCDATE())",
                    (
                        watermark.source_table_full_name,
                        watermark.destination_table_full_name,
                        watermark.watermark_column,
                        watermark.watermark_column_data_type,
                        watermark.last_watermark_value,
                        watermark.max_row_fetch,
                        watermark.ingestion_flag,
                        watermark.load_strategy,
                        watermark.merge_key_column,
                    ),
                )


# Preserve the lowercase spelling used in callers of this helper.
configdb = ConfigDB
