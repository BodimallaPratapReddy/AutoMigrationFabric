"""Oracle connection factory using platform-specific Instant Client libraries."""

from __future__ import annotations

import os
import platform
import re
import threading
from base64 import b64encode
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import TYPE_CHECKING, Annotated

from dotenv import load_dotenv
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    SecretStr,
    StringConstraints,
    model_validator,
)

if TYPE_CHECKING:
    import oracledb


NonEmptyString = Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)]
_NATIVE_DEPENDENCIES = Path(__file__).resolve().parent / "native_dependencies"
_READ_QUERY = re.compile(r"^\s*(?:SELECT|WITH)\b", re.IGNORECASE)
_ORACLE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
_CLIENT_INIT_LOCK = threading.Lock()
_CLIENT_INIT_CONFIG: tuple[object, str, str] | None = None


class OracleQueryResult(BaseModel):
    columns: list[str]
    rows: list[list[JsonValue]]
    row_count: int


class OracleIndex(BaseModel):
    index_name: str = Field(validation_alias="INDEX_NAME")
    column_name: str = Field(validation_alias="COLUMN_NAME")
    column_position: int = Field(validation_alias="COLUMN_POSITION")


class OracleTableColumn(BaseModel):
    id: int = Field(validation_alias="ID")
    column_name: str = Field(validation_alias="COLUMN_NAME")
    datatype: str = Field(validation_alias="DATATYPE")
    description: str | None = Field(validation_alias="DESC")
    data_type: str | None = Field(default=None, validation_alias="DATA_TYPE")
    data_length: int | None = Field(default=None, validation_alias="DATA_LENGTH")
    char_length: int | None = Field(default=None, validation_alias="CHAR_LENGTH")
    precision: int | None = Field(default=None, validation_alias="DATA_PRECISION")
    scale: int | None = Field(default=None, validation_alias="DATA_SCALE")
    nullable: bool | None = Field(default=None, validation_alias="NULLABLE")


class OracleConnectionStatus(BaseModel):
    success: bool
    message: str | None = None


class OracleKeyColumn(BaseModel):
    name: str
    position: int


class OracleKey(BaseModel):
    constraint_name: str
    columns: list[OracleKeyColumn]


class OracleIndexDefinition(BaseModel):
    index_name: str
    uniqueness: str
    index_type: str
    columns: list[OracleKeyColumn]
    status: str | None = None


class OracleWatermarkCandidate(BaseModel):
    column_name: str
    data_type: str
    indexed: bool
    nullable: bool
    reason: str
    priority: int = 0
    recommended: bool = False
    leading_index_column: bool = False
    index_details: list[OracleWatermarkIndexInfo] = Field(default_factory=list)


class OracleWatermarkIndexInfo(BaseModel):
    index_name: str
    column_position: int
    uniqueness: str


class OracleTableStatistics(BaseModel):
    schema_name: str
    table_name: str
    estimated_rows: int | None = None
    blocks: int | None = None
    avg_row_len: int | None = None
    last_analyzed: datetime | None = None
    statistics_available: bool = False


class OraclePartition(BaseModel):
    partition_name: str
    position: int
    estimated_rows: int | None = None


class OraclePartitionInfo(BaseModel):
    partitioned: bool
    partitioning_type: str | None = None
    subpartitioning_type: str | None = None
    partitions: list[OraclePartition] = Field(default_factory=list)


class OracleTableInfo(BaseModel):
    schema_name: str
    table_name: str
    exists: bool
    temporary: bool | None = None
    partitioned: bool | None = None
    estimated_rows: int | None = None
    last_analyzed: datetime | None = None
    primary_key: OracleKey | None = None
    unique_keys: list[OracleKey] = Field(default_factory=list)


class OracleTableInspection(BaseModel):
    schema_name: str
    table_name: str
    table_info: OracleTableInfo
    columns: list[OracleTableColumn]
    primary_key: OracleKey | None = None
    unique_keys: list[OracleKey] = Field(default_factory=list)
    indexes: list[OracleIndexDefinition] = Field(default_factory=list)
    watermark_candidates: list[OracleWatermarkCandidate] = Field(default_factory=list)
    statistics: OracleTableStatistics | None = None
    partition_info: OraclePartitionInfo | None = None
    warnings: list[str] = Field(default_factory=list)


def _identifier(value: str, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be nonempty")
    identifier = value.strip()
    if not _ORACLE_IDENTIFIER.fullmatch(identifier):
        raise ValueError(f"{name} must be an ordinary Oracle identifier")
    return identifier.upper()


def _records(result_json: str) -> list[dict[str, JsonValue]]:
    result = OracleQueryResult.model_validate_json(result_json)
    return [dict(zip(result.columns, row, strict=True)) for row in result.rows]


def _to_json_value(value: object) -> JsonValue:
    """Convert common Oracle result values without losing number precision."""
    import oracledb

    if isinstance(value, oracledb.LOB):
        return _to_json_value(value.read())
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return str(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"base64": b64encode(value).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [_to_json_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _to_json_value(item) for key, item in value.items()}
    raise TypeError(f"Oracle value of type {type(value).__name__} is not JSON serializable")


class OracleConnectionSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    username: NonEmptyString
    password: SecretStr
    dsn: NonEmptyString | None = None
    host: NonEmptyString | None = None
    service_name: NonEmptyString | None = Field(default=None, alias="serviceName")
    port: int = Field(default=1521, ge=1, le=65535, strict=True)
    client_path: Path | None = Field(default=None, alias="clientPath")

    @model_validator(mode="after")
    def validate_address(self) -> OracleConnectionSettings:
        if self.dsn is None and (self.host is None or self.service_name is None):
            raise ValueError("provide dsn or both host and service_name")
        if self.dsn is not None and (self.host is not None or self.service_name is not None):
            raise ValueError("provide either dsn or host and service_name")
        if not self.password.get_secret_value():
            raise ValueError("password must be nonempty")
        return self

    @classmethod
    def from_env(cls) -> OracleConnectionSettings:
        """Read the default Oracle connection from environment or .env."""
        load_dotenv()
        names = ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN")
        values = {name: os.getenv(name) for name in names}
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise ValueError(f"Missing Oracle configuration: {', '.join(missing)}")
        return cls(
            username=values["ORACLE_USER"],
            password=values["ORACLE_PASSWORD"],
            dsn=values["ORACLE_DSN"],
            client_path=os.getenv("ORACLE_CLIENT_PATH") or None,
        )


class OracleClient:
    """Create Oracle connections in Thick mode with the native client."""

    def __init__(self, settings: OracleConnectionSettings | None = None) -> None:
        self.settings = settings or OracleConnectionSettings.from_env()

    def connect(self) -> oracledb.Connection:
        """Open a connection; the caller owns and must close it."""
        import oracledb

        self._initialize_native_client(oracledb)
        dsn = self.settings.dsn or oracledb.makedsn(
            self.settings.host,
            self.settings.port,
            service_name=self.settings.service_name,
        )
        return oracledb.connect(
            user=self.settings.username,
            password=self.settings.password.get_secret_value(),
            dsn=dsn,
        )

    def test_connection(self) -> OracleConnectionStatus:
        result = _records(self.execute_sql("SELECT 1 AS HEALTH_CHECK FROM DUAL"))
        if len(result) != 1 or result[0].get("HEALTH_CHECK") != 1:
            raise RuntimeError("Oracle health check returned an unexpected result")
        return OracleConnectionStatus(success=True)

    def table_exists(self, schema_name: str, table_name: str) -> bool:
        rows = _records(self.execute_sql(
            "SELECT 1 AS FOUND FROM all_tables "
            "WHERE owner = :schema_name AND table_name = :table_name",
            {"schema_name": _identifier(schema_name, "schema_name"),
             "table_name": _identifier(table_name, "table_name")},
        ))
        return bool(rows)

    def schema_exists(self, schema_name: str) -> bool:
        rows = _records(self.execute_sql(
            "SELECT 1 AS FOUND FROM all_users WHERE username = :schema_name",
            {"schema_name": _identifier(schema_name, "schema_name")},
        ))
        return bool(rows)

    def get_table_statistics(
        self, schema_name: str, table_name: str
    ) -> OracleTableStatistics:
        schema = _identifier(schema_name, "schema_name")
        table = _identifier(table_name, "table_name")
        rows = _records(self.execute_sql(
            "SELECT t.num_rows, t.blocks, t.avg_row_len, t.last_analyzed, "
            "s.stale_stats FROM all_tables t LEFT JOIN all_tab_statistics s "
            "ON s.owner = t.owner AND s.table_name = t.table_name "
            "AND s.object_type = 'TABLE' AND s.partition_name IS NULL "
            "WHERE t.owner = :schema_name AND t.table_name = :table_name",
            {"schema_name": schema, "table_name": table},
        ))
        if not rows:
            raise ValueError(f"Oracle table {schema}.{table} was not found or is not visible")
        row = rows[0]
        analyzed = row.get("LAST_ANALYZED")
        usable = bool(analyzed and row.get("STALE_STATS") != "YES")
        return OracleTableStatistics(
            schema_name=schema, table_name=table,
            estimated_rows=row.get("NUM_ROWS") if usable else None,
            blocks=row.get("BLOCKS") if usable else None,
            avg_row_len=row.get("AVG_ROW_LEN") if usable else None,
            last_analyzed=analyzed,
            statistics_available=bool(usable and row.get("NUM_ROWS") is not None),
        )

    def get_partition_info(self, schema_name: str, table_name: str) -> OraclePartitionInfo:
        schema = _identifier(schema_name, "schema_name")
        table = _identifier(table_name, "table_name")
        rows = _records(self.execute_sql(
            "SELECT partitioning_type, subpartitioning_type FROM all_part_tables "
            "WHERE owner = :schema_name AND table_name = :table_name",
            {"schema_name": schema, "table_name": table},
        ))
        if not rows:
            return OraclePartitionInfo(partitioned=False)
        partitions = _records(self.execute_sql(
            "SELECT partition_name, partition_position, num_rows FROM all_tab_partitions "
            "WHERE table_owner = :schema_name AND table_name = :table_name "
            "ORDER BY partition_position",
            {"schema_name": schema, "table_name": table},
        ))
        return OraclePartitionInfo(
            partitioned=True,
            partitioning_type=str(rows[0]["PARTITIONING_TYPE"]),
            subpartitioning_type=rows[0].get("SUBPARTITIONING_TYPE"),
            partitions=[OraclePartition(
                partition_name=str(row["PARTITION_NAME"]),
                position=int(row["PARTITION_POSITION"]),
                estimated_rows=row.get("NUM_ROWS"),
            ) for row in partitions],
        )

    def get_table_info(self, schema_name: str, table_name: str) -> OracleTableInfo:
        schema = _identifier(schema_name, "schema_name")
        table = _identifier(table_name, "table_name")
        rows = _records(self.execute_sql(
            "SELECT t.temporary, t.partitioned, t.num_rows, t.last_analyzed, "
            "s.stale_stats FROM all_tables t LEFT JOIN all_tab_statistics s "
            "ON s.owner = t.owner AND s.table_name = t.table_name "
            "AND s.object_type = 'TABLE' AND s.partition_name IS NULL "
            "WHERE t.owner = :schema_name AND t.table_name = :table_name",
            {"schema_name": schema, "table_name": table},
        ))
        if not rows:
            return OracleTableInfo(schema_name=schema, table_name=table, exists=False)
        row = rows[0]
        return OracleTableInfo(
            schema_name=schema, table_name=table, exists=True,
            temporary=row.get("TEMPORARY") == "Y",
            partitioned=row.get("PARTITIONED") == "YES",
            estimated_rows=row.get("NUM_ROWS") if row.get("LAST_ANALYZED") and row.get("STALE_STATS") != "YES" else None,
            last_analyzed=row.get("LAST_ANALYZED"),
            primary_key=self.get_primary_key(schema, table),
            unique_keys=self.get_unique_keys(schema, table),
        )

    def inspect_table(self, schema_name: str, table_name: str) -> OracleTableInspection:
        schema = _identifier(schema_name, "schema_name")
        table = _identifier(table_name, "table_name")
        info = self.get_table_info(schema, table)
        if not info.exists:
            raise ValueError(f"Oracle table {schema}.{table} was not found or is not visible")
        columns = self.get_table_schema(schema, table)
        indexes = self.get_indexes(schema, table)
        candidates = self._watermark_candidates(columns, indexes)
        statistics = self.get_table_statistics(schema, table)
        partitions = self.get_partition_info(schema, table) if info.partitioned else OraclePartitionInfo(partitioned=False)
        warnings = []
        if not info.primary_key:
            warnings.append("No primary key found")
        if not statistics.statistics_available:
            warnings.append("Optimizer row statistics are unavailable")
        return OracleTableInspection(
            schema_name=schema, table_name=table, table_info=info,
            columns=columns, primary_key=info.primary_key, unique_keys=info.unique_keys,
            indexes=indexes, watermark_candidates=candidates,
            statistics=statistics, partition_info=partitions, warnings=warnings,
        )

    def _keys(self, schema_name: str, table_name: str, kind: str) -> list[OracleKey]:
        rows = _records(self.execute_sql(
            "SELECT c.constraint_name, cc.column_name, cc.position "
            "FROM all_constraints c JOIN all_cons_columns cc "
            "ON c.owner = cc.owner AND c.constraint_name = cc.constraint_name "
            "AND c.table_name = cc.table_name "
            "WHERE c.owner = :schema_name AND c.table_name = :table_name "
            "AND c.constraint_type = :constraint_type AND c.status = 'ENABLED' "
            "ORDER BY c.constraint_name, cc.position",
            {"schema_name": _identifier(schema_name, "schema_name"),
             "table_name": _identifier(table_name, "table_name"),
             "constraint_type": kind},
        ))
        grouped: dict[str, list[OracleKeyColumn]] = {}
        for row in rows:
            grouped.setdefault(str(row["CONSTRAINT_NAME"]), []).append(
                OracleKeyColumn(name=str(row["COLUMN_NAME"]), position=int(row["POSITION"]))
            )
        return [OracleKey(constraint_name=name, columns=columns)
                for name, columns in grouped.items()]

    def get_primary_key(self, schema_name: str, table_name: str) -> OracleKey | None:
        keys = self._keys(schema_name, table_name, "P")
        return keys[0] if keys else None

    def validate_replication_key(self, schema_name: str, table_name: str,
                                 columns: list[str]) -> dict:
        """Exact existence checks in one read-only snapshot; no source values returned."""
        schema = _identifier(schema_name, "schema_name")
        table = _identifier(table_name, "table_name")
        if not columns or len(set(columns)) != len(columns):
            raise ValueError("Select distinct key fields")
        names = [_identifier(name, "key column") for name in columns]
        metadata = {column.column_name: column for column in self.get_table_schema(schema, table)}
        if any(name not in metadata for name in names):
            raise ValueError("Key fields must exist in the source table")
        if any((metadata[name].data_type or metadata[name].datatype).upper().startswith(
                ("CLOB", "NCLOB", "BLOB", "LONG", "XMLTYPE", "BFILE")) for name in names):
            raise ValueError("LOB and complex fields cannot be replication keys")
        quoted = [f'"{name}"' for name in names]
        source = f'"{schema}"."{table}"'
        connection = self.connect()
        try:
            connection.call_timeout = 120000
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(f"SELECT 1 FROM {source} WHERE (" +
                               " OR ".join(f"{name} IS NULL" for name in quoted) + ") AND ROWNUM = 1")
                has_nulls = cursor.fetchone() is not None
                cursor.execute("SELECT 1 FROM (SELECT " + ", ".join(quoted) +
                               f" FROM {source} GROUP BY " + ", ".join(quoted) +
                               " HAVING COUNT(*) > 1) WHERE ROWNUM = 1")
                has_duplicates = cursor.fetchone() is not None
            return {"valid": not has_nulls and not has_duplicates, "status": "CHECKED",
                    "columns": names, "has_nulls": has_nulls, "has_duplicates": has_duplicates,
                    "checked_at": datetime.now().astimezone().isoformat(), "scope": "FULL_TABLE"}
        finally:
            try:
                connection.rollback()
            finally:
                connection.close()

    def get_unique_keys(self, schema_name: str, table_name: str) -> list[OracleKey]:
        return self._keys(schema_name, table_name, "U")

    def get_indexes(self, schema_name: str, table_name: str) -> list[OracleIndexDefinition]:
        rows = _records(self.execute_sql(
            "SELECT i.index_name, i.uniqueness, i.index_type, i.status, "
            "ic.column_name, ic.column_position "
            "FROM all_indexes i JOIN all_ind_columns ic "
            "ON i.owner = ic.index_owner AND i.index_name = ic.index_name "
            "AND i.table_owner = ic.table_owner AND i.table_name = ic.table_name "
            "WHERE i.table_owner = :schema_name AND i.table_name = :table_name "
            "ORDER BY i.index_name, ic.column_position",
            {"schema_name": _identifier(schema_name, "schema_name"),
             "table_name": _identifier(table_name, "table_name")},
        ))
        grouped: dict[str, OracleIndexDefinition] = {}
        for row in rows:
            name = str(row["INDEX_NAME"])
            if name not in grouped:
                grouped[name] = OracleIndexDefinition(
                    index_name=name, uniqueness=str(row["UNIQUENESS"]),
                    index_type=str(row["INDEX_TYPE"]), status=row.get("STATUS"), columns=[])
            grouped[name].columns.append(OracleKeyColumn(
                name=str(row["COLUMN_NAME"]), position=int(row["COLUMN_POSITION"])))
        return list(grouped.values())

    def is_column_indexed(self, schema_name: str, table_name: str, column_name: str) -> bool:
        name = _identifier(column_name, "column_name")
        return any(column.name == name for index in self.get_indexes(schema_name, table_name)
                   for column in index.columns)

    def get_watermark_candidates(
        self, schema_name: str, table_name: str
    ) -> list[OracleWatermarkCandidate]:
        return self._watermark_candidates(
            self.get_table_schema(schema_name, table_name),
            self.get_indexes(schema_name, table_name),
        )

    @staticmethod
    def _watermark_candidates(
        columns: list[OracleTableColumn], indexes: list[OracleIndexDefinition]
    ) -> list[OracleWatermarkCandidate]:
        indexed = {column.name for index in indexes for column in index.columns}
        leading = {column.name for index in indexes for column in index.columns
                   if column.position == 1}
        candidates = []
        for column in columns:
            data_type = (column.data_type or column.datatype).upper()
            if data_type != "DATE" and not data_type.startswith("TIMESTAMP"):
                continue
            name = column.column_name.upper()
            creation_only = name in {"CREATION_DATE", "CREATED_AT"} or name.startswith("CREAT")
            update_named = any(part in name for part in ("UPDATE", "MODIF", "CHANGE"))
            is_indexed = name in indexed
            index_details = [OracleWatermarkIndexInfo(
                index_name=index.index_name,
                column_position=index_column.position,
                uniqueness=index.uniqueness,
            ) for index in indexes for index_column in index.columns
                if index_column.name == name]
            non_null = column.nullable is False
            priority = (100 if update_named else 10) + (20 if name in {
                "LAST_UPDATE_DATE", "LAST_UPDATED_DATE", "UPDATE_DATE", "UPDATED_AT",
                "MODIFIED_DATE", "MODIFIED_AT", "LAST_MODIFIED_DATE",
            } else 0) + (10 if name in leading else 5 if is_indexed else 0) + (5 if non_null else 0)
            if creation_only:
                priority = min(priority, 5)
            candidates.append(OracleWatermarkCandidate(
                column_name=column.column_name, data_type=column.datatype.upper(),
                indexed=is_indexed, leading_index_column=name in leading,
                index_details=index_details,
                nullable=column.nullable if column.nullable is not None else True,
                reason=("insert-only candidate" if creation_only else
                        "update timestamp name" if update_named else "date or timestamp type"),
                priority=priority,
            ))
        candidates.sort(key=lambda item: (-item.priority, item.column_name))
        # Scoring orders candidates for review; it does not choose a watermark.
        return candidates

    def execute_sql(
        self,
        sql: str,
        parameters: Mapping[str, object] | Sequence[object] | None = None,
        *,
        max_rows: int | None = 10000,
    ) -> str:
        """Execute a read query and return JSON with columns, rows, and count.

        Bind values through ``parameters``. A dedicated connection and Oracle
        read-only transaction are used for each call.
        """
        if not isinstance(sql, str) or not _READ_QUERY.match(sql):
            raise ValueError("sql must be a SELECT or WITH query")
        if max_rows is not None and (not isinstance(max_rows, int) or isinstance(max_rows, bool) or max_rows < 1):
            raise ValueError("max_rows must be a positive integer or None")

        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION READ ONLY")
                cursor.execute(sql, parameters)
                if cursor.description is None:
                    raise ValueError("sql did not return a result set")
                columns = [column.name for column in cursor.description]
                fetched = cursor.fetchmany(max_rows + 1) if max_rows is not None else cursor.fetchall()
                if max_rows is not None and len(fetched) > max_rows:
                    raise ValueError(f"Oracle query exceeds max_rows={max_rows}")
                rows = [[_to_json_value(value) for value in row] for row in fetched]
                return OracleQueryResult(
                    columns=columns, rows=rows, row_count=len(rows)
                ).model_dump_json()
        finally:
            try:
                connection.rollback()
            finally:
                connection.close()

    def execute_query(
        self,
        sql: str,
        parameters: Mapping[str, object] | Sequence[object] | None = None,
        *,
        max_rows: int | None = 10000,
    ) -> OracleQueryResult:
        """Return a typed result while preserving execute_sql's JSON contract."""
        return OracleQueryResult.model_validate_json(
            self.execute_sql(sql, parameters, max_rows=max_rows)
        )

    def get_index(
        self, schema_name: str, table_name: str, column_name: str = "LAST_UPDATED_DATE"
    ) -> list[OracleIndex]:
        """Deprecated compatibility helper; prefer get_indexes for full definitions."""
        schema_name = _identifier(schema_name, "schema_name")
        table_name = _identifier(table_name, "table_name")
        column_name = _identifier(column_name, "column_name")
        return [
            OracleIndex(INDEX_NAME=index.index_name, COLUMN_NAME=column.name,
                        COLUMN_POSITION=column.position)
            for index in self.get_indexes(schema_name, table_name)
            for column in index.columns if column.name == column_name
        ]

    def get_table_schema(
        self, schema_name: str, table_name: str
    ) -> list[OracleTableColumn]:
        """Return column positions, names, formatted types, and comments."""
        schema_name = _identifier(schema_name, "schema_name")
        table_name = _identifier(table_name, "table_name")

        query_json = self.execute_sql(
            "SELECT c.column_id AS id, c.column_name, "
            "CASE "
            "WHEN c.data_type IN ('VARCHAR2', 'CHAR', 'NVARCHAR2', 'NCHAR') "
            "THEN c.data_type || '(' || c.char_length || ')' "
            "WHEN c.data_type = 'NUMBER' "
            "AND c.data_precision IS NOT NULL AND c.data_scale IS NOT NULL "
            "THEN c.data_type || '(' || c.data_precision || ',' || c.data_scale || ')' "
            "WHEN c.data_type = 'NUMBER' AND c.data_precision IS NOT NULL "
            "THEN c.data_type || '(' || c.data_precision || ')' "
            "WHEN c.data_type LIKE 'TIMESTAMP%' THEN 'TIMESTAMP' "
            "ELSE c.data_type END AS datatype, cc.comments AS \"DESC\", "
            "c.data_type, c.data_length, c.char_length, c.data_precision, "
            "c.data_scale, CASE WHEN c.nullable = 'Y' THEN 1 ELSE 0 END AS nullable "
            "FROM all_tab_columns c "
            "LEFT JOIN all_col_comments cc "
            "ON c.owner = cc.owner AND c.table_name = cc.table_name "
            "AND c.column_name = cc.column_name "
            "WHERE c.owner = :schema_name AND c.table_name = :table_name "
            "ORDER BY c.column_id",
            {
                "schema_name": schema_name,
                "table_name": table_name,
            },
        )
        result = OracleQueryResult.model_validate_json(query_json)
        return [
            OracleTableColumn.model_validate(dict(zip(result.columns, row, strict=True)))
            for row in result.rows
        ]

    def _initialize_native_client(self, driver: object) -> None:
        global _CLIENT_INIT_CONFIG
        system = platform.system()
        if system not in ("Windows", "Linux"):
            raise RuntimeError(f"Unsupported Oracle client platform: {system}")

        client_path = self.settings.client_path or (
            _NATIVE_DEPENDENCIES / system.lower() / "instantclient_23_26"
        )
        client_path = Path(client_path).expanduser().resolve()
        with _CLIENT_INIT_LOCK:
            if _CLIENT_INIT_CONFIG is not None and _CLIENT_INIT_CONFIG[0] is driver:
                if _CLIENT_INIT_CONFIG[1:] != (system, str(client_path)):
                    raise RuntimeError("Oracle client already initialized with a different client path")
                return
            if system == "Windows":
                if not (client_path / "oci.dll").is_file():
                    raise FileNotFoundError(f"Oracle Instant Client oci.dll not found in {client_path}")
                driver.init_oracle_client(lib_dir=str(client_path))
            else:
                if not any(path.is_file() for path in client_path.glob("libclntsh.so*")):
                    raise FileNotFoundError(
                        f"Oracle Instant Client libclntsh.so not found in {client_path}"
                    )
                # Linux resolves shared libraries from the loader path configured
                # before Python starts; passing lib_dir is generally unsupported.
                driver.init_oracle_client()
            _CLIENT_INIT_CONFIG = (driver, system, str(client_path))
