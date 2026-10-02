# Fabric provisioning script. In Fabric, mark the first code cell as parameters.
# Install python-oracledb in the Fabric environment for Oracle data movement.
# Apply configdb/migrations/003_target_change_runs.sql before ALTER_BACKFILL or
# REPLACE_FULL, attach the approved Lakehouse, and schedule an Oracle write pause.
# %%
plan_guid = ""  # Required Text parameter supplied by the app
CREATIONREQUEST = "BOTH"  # Optional manual scope: TABLE, VIEW, BOTH
RUN_MODE = "PROVISION"  # Interactive modes: PROVISION, TEST, DIAGNOSE

# %%


# %% [markdown]
# Part 1 - Config DB connection
# Token of the running identity (same workspace, no secrets); server / catalog found through the Fabric REST API.
#

# %%
import re
import struct
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple

import pyodbc  # pre-installed in the Fabric Spark runtime (with ODBC Driver 18)

# ---------------------------------------------------------------------------
# Configuration (non-secret)
# ---------------------------------------------------------------------------
CONFIG_DB_ITEM_NAME = "AutoMigrationConfigDB"   # display name of the SQL database item
CONFIG_DB_SERVER_OVERRIDE = ""                  # optional: "<host>,1433" - skips REST discovery
CONFIG_DB_NAME_OVERRIDE = ""                    # optional: exact catalog name - skips REST discovery
CONFIG_DB_SCHEMA = "bronze_replication"         # schema that holds the configuration tables

ODBC_DRIVER = "ODBC Driver 18 for SQL Server"
SQL_TOKEN_AUDIENCES = ("https://database.windows.net/", "pbi")  # tried in this order
SQL_COPT_SS_ACCESS_TOKEN = 1256                 # ODBC attribute id for an access token


class ConfigDbError(Exception):
    """Raised when the Config DB cannot be reached or a statement fails."""

# %%
def _normalise_server(server: str) -> str:
    """Return the server as ``host,port`` (default port 1433), without a ``tcp:`` prefix."""
    server = server.strip()
    if server.lower().startswith("tcp:"):
        server = server[4:]
    return server if "," in server else f"{server},1433"


def discover_config_db_endpoint(item_name: str = CONFIG_DB_ITEM_NAME) -> Tuple[str, str]:
    """
    Find ``(server, database)`` of the SQL database item in the *current* workspace.

    Uses the Fabric REST API (``GET /v1/workspaces/{id}/sqlDatabases``) through the
    pre-installed ``sempy`` client, authenticated as the running identity.
    """
    if CONFIG_DB_SERVER_OVERRIDE and CONFIG_DB_NAME_OVERRIDE:
        return _normalise_server(CONFIG_DB_SERVER_OVERRIDE), CONFIG_DB_NAME_OVERRIDE

    try:
        import sempy.fabric as fabric
        workspace_id = notebookutils.runtime.context["currentWorkspaceId"]
        response = fabric.FabricRestClient().get(f"/v1/workspaces/{workspace_id}/sqlDatabases")
        response.raise_for_status()
        items = response.json().get("value", [])
    except Exception as exc:  # noqa: BLE001 - surfaced with guidance below
        raise ConfigDbError(
            "Could not list SQL databases in this workspace "
            f"({type(exc).__name__}). Set CONFIG_DB_SERVER_OVERRIDE and "
            "CONFIG_DB_NAME_OVERRIDE from the item's connection string."
        ) from exc

    for item in items:
        if item.get("displayName") == item_name:
            props = item.get("properties", {}) or {}
            server, database = props.get("serverFqdn"), props.get("databaseName")
            if server and database:
                return _normalise_server(server), database
            raise ConfigDbError(f"SQL database item '{item_name}' has no server/database properties.")
    raise ConfigDbError(f"SQL database item '{item_name}' was not found in the current workspace.")

# %%
class ConfigDbConnection:
    """
    Thin, safe wrapper around a pyodbc connection to the Config DB.

    * Authenticates with an Entra access token of the running identity.
    * Always uses bound ``?`` parameters - never string-format values into SQL.
    * ``fetch_all`` / ``fetch_one`` return rows as plain dictionaries.
    * ``transaction()`` groups several statements into one commit/rollback.
    * Never logs tokens, connection strings or credentials.

    Usage::

        with ConfigDbConnection() as db:
            rows = db.fetch_all("SELECT ... WHERE PlanGUID = ?", (plan_guid,))
    """

    def __init__(self, server: Optional[str] = None, database: Optional[str] = None,
                 login_timeout: int = 30) -> None:
        self._server = server
        self._database = database
        self._login_timeout = login_timeout
        self._conn: Optional[Any] = None

    # -- lifecycle ---------------------------------------------------------
    def connect(self) -> "ConfigDbConnection":
        """Open the connection (idempotent)."""
        if self._conn is not None:
            return self
        if not (self._server and self._database):
            self._server, self._database = discover_config_db_endpoint()

        conn_str = (
            f"Driver={{{ODBC_DRIVER}}};Server={self._server};Database={self._database};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        last_error: Optional[Exception] = None
        for audience in SQL_TOKEN_AUDIENCES:
            try:
                token = notebookutils.credentials.getToken(audience)
                raw = token.encode("utf-16-le")
                token_struct = struct.pack(f"<I{len(raw)}s", len(raw), raw)
                self._conn = pyodbc.connect(
                    conn_str,
                    attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
                    timeout=self._login_timeout,
                    autocommit=True,
                )
                return self
            except Exception as exc:  # noqa: BLE001 - try next audience
                last_error = exc
        raise ConfigDbError(
            f"Unable to connect to the Config DB ({type(last_error).__name__}). "
            "Check that the running identity has access to the SQL database item."
        )

    def close(self) -> None:
        """Close the connection (safe to call repeatedly)."""
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None

    def __enter__(self) -> "ConfigDbConnection":
        return self.connect()

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    def _require(self) -> Any:
        if self._conn is None:
            raise ConfigDbError("Config DB connection is not open.")
        return self._conn

    # -- statements --------------------------------------------------------
    def fetch_all(self, sql: str, params: Sequence[Any] = ()) -> List[Dict[str, Any]]:
        """Run a SELECT and return all rows as dictionaries."""
        cursor = self._require().cursor()
        try:
            cursor.execute(sql, tuple(params))
            if cursor.description is None:
                return []
            columns = [c[0] for c in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
        finally:
            cursor.close()

    def fetch_one(self, sql: str, params: Sequence[Any] = ()) -> Optional[Dict[str, Any]]:
        """Run a SELECT and return the first row (or ``None``)."""
        rows = self.fetch_all(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Sequence[Any] = ()) -> int:
        """Run INSERT/UPDATE/DELETE and return the affected row count."""
        cursor = self._require().cursor()
        try:
            cursor.execute(sql, tuple(params))
            return cursor.rowcount
        finally:
            cursor.close()

    @contextmanager
    def transaction(self) -> Iterator["ConfigDbConnection"]:
        """Group statements into one transaction (commit on success, rollback on error)."""
        conn = self._require()
        previous = conn.autocommit
        conn.autocommit = False
        try:
            yield self
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.autocommit = previous

# %% [markdown]
# Part 2 - Shared library
# Validation, type mapping, domain models, Config DB repository, change-plan helpers,
# Delta table and view operations and the task entry points.
#

# %% [markdown]
# ## 1. Constants and tunables

# %%
import json
import logging
import re
import sys
import time
import uuid
from decimal import Decimal, InvalidOperation, localcontext
from concurrent.futures import ThreadPoolExecutor, wait
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Set, Tuple


class CreationRequest:
    """CREATIONREQUEST values - which kinds of object are in scope."""
    TABLE = "TABLE"
    VIEW = "VIEW"
    BOTH = "BOTH"
    VALUES = (TABLE, VIEW, BOTH)
    ALIASES = {"ALL": BOTH}


class PlanAction:
    """What the notebook does. Decided ONLY from MigrationPlans (Status + Notes), never from parameters."""
    CREATE_NEW = "CREATE_NEW"              # plan Status PROVISIONING / READY_TO_PROVISION
    REVISE_PLANNED = "REVISE_PLANNED"      # Status CHANGE_REQUESTED + Notes.target_change.decision
    ALTER_FUTURE = "ALTER_FUTURE"
    ALTER_BACKFILL = "ALTER_BACKFILL"
    REPLACE_FULL = "REPLACE_FULL"
    CHANGE_ACTIONS = (REVISE_PLANNED, ALTER_FUTURE, ALTER_BACKFILL, REPLACE_FULL)


class ObjectType:
    TABLE = "TABLE"
    VIEW = "VIEW"


class Status:
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


# Task names executed by the orchestrator (one task = one table / view / change).
TASK_TABLE_PROVISION = "TABLE_PROVISION"   # CREATE_NEW for one table
TASK_VIEW_PROVISION = "VIEW_PROVISION"     # CREATE_NEW for one view
TASK_TABLE_CHANGE = "TABLE_CHANGE"         # change-plan decision for the existing table

# MigrationPlans.Status -> action.
NEW_PLAN_STATUSES = frozenset({"PROVISIONING", "READY_TO_PROVISION"})
CHANGE_PLAN_STATUS = "CHANGE_REQUESTED"

# SourceTables / FabricViews row statuses that CREATE_NEW will provision (retry-friendly).
NEW_ROW_STATUSES = ("PENDING", "IN_PROGRESS", "FAILED")

# CREATE_NEW fails when the physical table already exists, even if its schema matches.
# A matching schema alone cannot prove this plan created it in a previous attempt.
CREATE_NEW_ALLOWS_IDENTICAL_EXISTING = False

MAX_PARALLEL_TASKS = 8            # tasks (threads) running in parallel inside one dependency wave
WAVE_TIMEOUT_SECONDS = 3600       # a wave that takes longer is reported FAILED
MAX_MESSAGE_LENGTH = 300          # length of messages returned to the API
ORACLE_FETCH_ROWS = 5000           # bounded driver memory; tune for row width

# %% [markdown]
# ## 2. Exceptions

# %%
class MigrationError(Exception):
    """Base class of all expected, user-presentable errors."""


class ValidationError(MigrationError):
    """Invalid input parameter, configuration row or unsupported value."""


class UnsupportedTypeError(ValidationError):
    """A FabricDataType could not be mapped to a Delta type."""


class UnsafeChangeError(MigrationError):
    """The requested change would lose data or is not a safe in-place alteration."""


class TargetStateError(MigrationError):
    """The physical Lakehouse state does not allow the requested operation."""


# %% [markdown]
# ## 3. Utilities

# %%
def get_logger(name: str = "migration") -> logging.Logger:
    """Return a stdout logger (configured once). Never log secrets with it."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


LOG = get_logger()


def utc_now_iso() -> str:
    """Current UTC time as ISO-8601 with milliseconds, e.g. 2026-10-01T12:00:00.123Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


_BLANK_TOKENS = {"", "NONE", "NULL", "<NA>", "N/A", "NAN"}


def normalize_text(value: Any) -> Optional[str]:
    """Strip a notebook parameter; return ``None`` when it is blank/null-like."""
    if value is None:
        return None
    text = str(value).strip()
    return None if text.upper() in _BLANK_TOKENS else text


def normalize_enum(value: Any) -> Optional[str]:
    """Like :func:`normalize_text` but upper-cased, for enumerated parameters."""
    text = normalize_text(value)
    return text.upper() if text is not None else None


_GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def is_valid_uuid(value: Any) -> bool:
    """True only for the canonical 8-4-4-4-12 form."""
    return isinstance(value, str) and bool(_GUID_RE.match(value.strip()))


def norm_guid(value: Any) -> str:
    """Lower-case canonical form of a GUID coming from SQL Server or a parameter."""
    return str(value).strip().lower()


_SECRET_RE = re.compile(r"(?i)(password|pwd|secret|token|accountkey|sig)\s*[=:]\s*[^\s;,'\"]+")


def safe_message(error: Any, limit: int = MAX_MESSAGE_LENGTH) -> str:
    """
    Build a short, single-line, secret-free message for the API report.

    Expected (MigrationError) messages are used as written; other exceptions are
    prefixed with their type name. Whitespace is collapsed, secret-looking
    ``key=value`` fragments are redacted and the text is truncated.
    """
    if isinstance(error, MigrationError):
        text = str(error)
    elif isinstance(error, BaseException):
        text = f"{type(error).__name__}: {error}"
    else:
        text = str(error)
    text = _SECRET_RE.sub(r"\1=***", " ".join(text.split()))
    return text if len(text) <= limit else text[: limit - 3] + "..."


_IDENT_RE = re.compile(r"^[A-Za-z0-9_$#]{1,128}$")


def validate_identifier(name: Any, what: str) -> str:
    """Allow only simple identifiers; this is what keeps DDL free of injection."""
    if not isinstance(name, str) or not _IDENT_RE.match(name):
        raise ValidationError(f"Invalid {what}: {str(name)[:60]!r} (allowed: letters, digits, _, $, #)")
    return name


def quote_ident(name: str) -> str:
    """Validate and back-tick quote an identifier for Spark SQL."""
    return f"`{validate_identifier(name, 'identifier')}`"


def sql_literal(text: str) -> str:
    """Quote free text (e.g. a column description) as a Spark SQL string literal."""
    cleaned = " ".join(str(text).split())
    return "'" + cleaned.replace("\\", "\\\\").replace("'", "\\'") + "'"


def get_spark() -> Any:
    """Return the active Spark session (the predefined ``spark`` in Fabric notebooks)."""
    existing = globals().get("spark")
    if existing is not None:
        return existing
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def first_line(exc: BaseException, limit: int = 110) -> str:
    """First line of an exception text (Spark errors are very long); used to keep reports readable."""
    text = str(exc).strip()
    return (text.splitlines()[0] if text else type(exc).__name__)[:limit]


def _runtime_context() -> Dict[str, Any]:
    """Fabric runtime context (workspace / default lakehouse ids); empty if unavailable."""
    try:
        return dict(notebookutils.runtime.context)
    except Exception:  # noqa: BLE001
        return {}

# %% [markdown]
# ## 4. Data types
# `FabricDataType` text from the config table is normalised to one canonical Spark
# type. `VARCHAR/CHAR/NVARCHAR/TEXT(n)` all become `STRING` (Delta does not enforce
# lengths and Spark reports them back as `string`, which keeps schema comparison exact).

# %%
_SIMPLE_TYPES = {
    "STRING", "BOOLEAN", "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
    "DATE", "TIMESTAMP", "TIMESTAMP_NTZ", "BINARY",
}
_TYPE_ALIASES = {
    "INTEGER": "INT", "LONG": "BIGINT", "SHORT": "SMALLINT", "BYTE": "TINYINT",
    "REAL": "FLOAT", "BOOL": "BOOLEAN", "DATETIME": "TIMESTAMP", "TEXT": "STRING",
}
_INTEGRAL_ORDER = ("TINYINT", "SMALLINT", "INT", "BIGINT")
_DECIMAL_RE = re.compile(r"^(?:DECIMAL|NUMERIC|DEC)(?:\((\d+)(?:,(\d+))?\))?$")
_STRING_RE = re.compile(r"^(?:VARCHAR|NVARCHAR|CHAR|NCHAR|STRING)(?:\((?:\d+|MAX)\))?$")


def normalize_fabric_type(raw: Any) -> str:
    """Return the canonical Delta type for a configured FabricDataType, e.g. ``DECIMAL(18,0)``."""
    if not isinstance(raw, str) or not raw.strip():
        raise UnsupportedTypeError("FabricDataType is empty")
    text = re.sub(r"\s+", "", raw).upper()
    match = _DECIMAL_RE.match(text)
    if match:
        precision = int(match.group(1) or 10)
        scale = int(match.group(2) or 0)
        if not (1 <= precision <= 38 and 0 <= scale <= precision):
            raise UnsupportedTypeError(f"Invalid decimal precision/scale: {raw[:40]!r}")
        return f"DECIMAL({precision},{scale})"
    if _STRING_RE.match(text):
        return "STRING"
    text = _TYPE_ALIASES.get(text, text)
    if text in _SIMPLE_TYPES:
        return text
    raise UnsupportedTypeError(f"Unsupported FabricDataType: {raw[:40]!r}")


def normalize_physical_type(spark_simple_string: str) -> str:
    """Normalise ``DataType.simpleString()`` from Spark for comparison (e.g. ``decimal(18,0)``)."""
    return re.sub(r"\s+", "", spark_simple_string).upper()


def _decimal_parts(type_name: str) -> Optional[Tuple[int, int]]:
    match = re.match(r"^DECIMAL\((\d+),(\d+)\)$", type_name)
    return (int(match.group(1)), int(match.group(2))) if match else None


def is_safe_widening(old: str, new: str) -> bool:
    """
    True if changing a column from ``old`` to ``new`` can never lose data.

    Supported: TINYINT -> SMALLINT -> INT -> BIGINT, FLOAT -> DOUBLE and DECIMAL
    widening (scale and integer digits must not shrink). Everything else is rejected.
    """
    if old in _INTEGRAL_ORDER and new in _INTEGRAL_ORDER:
        return _INTEGRAL_ORDER.index(new) > _INTEGRAL_ORDER.index(old)
    if old == "FLOAT" and new == "DOUBLE":
        return True
    old_dec, new_dec = _decimal_parts(old), _decimal_parts(new)
    if old_dec and new_dec:
        (p1, s1), (p2, s2) = old_dec, new_dec
        return (p2, s2) != (p1, s1) and s2 >= s1 and (p2 - s2) >= (p1 - s1)
    return False

# %% [markdown]
# ## 5. Domain models

# %%
@dataclass(frozen=True)
class ColumnDef:
    """One desired target column (from ``SourceTableColumns`` where IsSelected = 1)."""
    sno: int
    source_name: str
    name: str                     # TargetColumnName, or ColumnName when not set
    fabric_type: str              # canonical Delta type
    nullable: bool                # effective nullability used for CREATE
    nullable_known: bool          # False when IsNullable is NULL in the config (unknown)
    is_primary_key: bool
    description: Optional[str]
    source_type: Optional[str] = None             # original SourceDataType (kept when metadata is rewritten)
    raw_fabric_type: Optional[str] = None         # FabricDataType exactly as configured
    is_watermark_candidate: Optional[bool] = None
    source_expression: Optional[str] = None
    transformation_notes: Optional[str] = None


# How tables of a lakehouse are addressed, decided by LakehouseTarget.preflight and then cached:
#   WITH_LAKEHOUSE -> `lakehouse`.`schema`.`table`   SCHEMA_ONLY -> `schema`.`table` (default lakehouse implied)
_NAMESPACE_MODE: Dict[str, str] = {}


@dataclass(frozen=True)
class LakehouseTarget:
    """Address of one Delta table / view in a schema-enabled Lakehouse."""
    workspace_id: str
    lakehouse_id: Optional[str]
    lakehouse: str
    schema: str
    name: str

    @property
    def namespace(self) -> str:
        """Back-tick quoted schema namespace; shape depends on the mode found by ``preflight``."""
        if _NAMESPACE_MODE.get(self.lakehouse.lower()) == "SCHEMA_ONLY":
            return quote_ident(self.schema)
        return f"{quote_ident(self.lakehouse)}.{quote_ident(self.schema)}"

    @property
    def fq(self) -> str:
        """Back-tick quoted object name, e.g. `lakehouse`.`schema`.`object` (or `schema`.`object`)."""
        return f"{self.namespace}.{quote_ident(self.name)}"

    @property
    def display(self) -> str:
        """Name used in the API report, e.g. LOCAL_ORACLE.SALES_ORDER_HEADER."""
        return f"{self.schema}.{self.name}"

    def preflight(self, spark: Any, ensure_schema: bool = False) -> bool:
        """
        Verify that this notebook can address the target and return whether the schema exists.

        1. Same workspace; the notebook's default lakehouse is the approved target lakehouse.
        2. Probe the fully qualified target namespace with ``SHOW TABLES``. Fabric can
           resolve that namespace even when ``SHOW SCHEMAS IN <lakehouse>`` fails.
           If the probe fails, list schemas using the available catalog form.
        3. With ``ensure_schema`` the schema is created when it is missing.
        """
        ctx = _runtime_context()
        current_ws, default_id, default_name = (ctx.get("currentWorkspaceId"), ctx.get("defaultLakehouseId"),
                                                ctx.get("defaultLakehouseName"))
        if current_ws and self.workspace_id and str(current_ws).lower() != self.workspace_id.lower():
            raise TargetStateError("Target workspace differs from the notebook workspace "
                                   "(cross-workspace provisioning is not supported).")
        if default_id and self.lakehouse_id and str(default_id).lower() != self.lakehouse_id.lower():
            raise TargetStateError("The attached default lakehouse is not the approved target lakehouse.")
        if default_name and str(default_name).lower() != self.lakehouse.lower():
            raise TargetStateError(f"The notebook's default lakehouse is '{default_name}' but the plan "
                                   f"targets '{self.lakehouse}'.")

        qualified_namespace = f"{quote_ident(self.lakehouse)}.{quote_ident(self.schema)}"
        try:
            spark.sql(f"SHOW TABLES IN {qualified_namespace}").collect()
        except Exception:  # noqa: BLE001 - a missing schema is handled by the fallback below
            pass
        else:
            _NAMESPACE_MODE[self.lakehouse.lower()] = "WITH_LAKEHOUSE"
            return True

        mode: Optional[str] = None
        schemas: List[str] = []
        errors: List[str] = []
        for candidate, statement in (("WITH_LAKEHOUSE", f"SHOW SCHEMAS IN {quote_ident(self.lakehouse)}"),
                                     ("SCHEMA_ONLY", "SHOW SCHEMAS")):
            try:
                schemas = [str(r[0]).lower() for r in spark.sql(statement).collect()]
                mode = candidate
                break
            except Exception as exc:  # noqa: BLE001 - try the next form, keep the reason
                errors.append(f"{candidate}: {first_line(exc)}")
                LOG.error("%s failed: %s", statement, str(exc)[:1000])
        if mode is None:
            raise TargetStateError(f"Cannot list schemas of lakehouse '{self.lakehouse}'. " + " | ".join(errors))
        if mode == "SCHEMA_ONLY" and self.lakehouse.lower() in schemas and self.schema.lower() not in schemas:
            raise TargetStateError(f"Lakehouse '{self.lakehouse}' does not look schema-enabled (SHOW SCHEMAS lists "
                                   "lakehouses, not schemas). A schema-enabled lakehouse is required.")
        _NAMESPACE_MODE[self.lakehouse.lower()] = mode

        exists = self.schema.lower() in schemas
        if not exists and ensure_schema:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {self.namespace}")
            exists = True
        return exists


@dataclass(frozen=True)
class TableSpec:
    """Validated definition of a target table (``SourceTables`` + ``SourceTableColumns``)."""
    guid: str
    plan_guid: str
    status: str
    is_active: bool
    target: LakehouseTarget
    columns: Tuple[ColumnDef, ...]


@dataclass(frozen=True)
class ViewSpec:
    """Validated definition of a derived object (``FabricViews``)."""
    guid: str
    plan_guid: str
    status: str
    is_active: bool
    target: LakehouseTarget
    view_type: str                # SQL_VIEW | MATERIALIZED_TABLE | SPARK_DERIVED_TABLE
    view_sql: str
    description: Optional[str]
    creation_order: int


@dataclass(frozen=True)
class ObjectRef:
    """Lightweight reference used by the master to plan the run."""
    object_type: str
    guid: str
    name: str
    creation_order: int = 0


@dataclass(frozen=True)
class PlanInfo:
    plan_guid: str
    status: str
    plan_version: Optional[int]
    notes: Optional[str]


@dataclass
class ObjectResult:
    """Outcome of one table/view. ``to_report`` yields exactly the fields the API parser accepts."""
    object_type: str
    name: str
    status: str
    duration_seconds: float = 0.0
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    rows_written: Optional[int] = None
    message: Optional[str] = None

    def to_report(self) -> Dict[str, Any]:
        report: Dict[str, Any] = {
            "object_type": self.object_type,
            "name": self.name,
            "status": self.status,
            "duration_seconds": round(max(float(self.duration_seconds), 0.0), 2),
        }
        for key in ("started_at", "completed_at", "rows_written", "message"):
            value = getattr(self, key)
            if value is not None:
                report[key] = value
        return report


def run_timed(object_type: str, name_ref: Dict[str, str],
              action: Callable[[], Tuple[str, Optional[int]]]) -> ObjectResult:
    """
    Execute ``action`` and wrap the outcome into an :class:`ObjectResult`.

    ``action`` returns ``(message, rows_written)``. It may update ``name_ref["name"]``
    once the real object name is known. Exceptions never escape: they become a
    FAILED result with a short, secret-free message.
    """
    started_at, t0 = utc_now_iso(), time.monotonic()
    try:
        message, rows = action()
        status = Status.SUCCESS
    except Exception as exc:  # noqa: BLE001 - converted into a FAILED result
        message, rows, status = safe_message(exc), None, Status.FAILED
        LOG.error("%s %s failed: %s", object_type, name_ref.get("name"), message)
    return ObjectResult(object_type, name_ref["name"], status,
                        time.monotonic() - t0, started_at, utc_now_iso(), rows, message)

# %% [markdown]
# ## 6. ConfigRepository - read access to the configuration tables
# Pure SQL access plus row-to-model conversion. Row conversion is split into the pure
# functions `build_table_spec` / `build_view_spec` so it can be unit-tested without a database.

# %%
def _clean_text(value: Any) -> Optional[str]:
    return value.strip() if isinstance(value, str) and value.strip() else None


def build_column(row: Dict[str, Any]) -> ColumnDef:
    """Convert one ``SourceTableColumns`` row into a validated :class:`ColumnDef`."""
    name = validate_identifier(row.get("TargetColumnName") or row["ColumnName"], "column name")
    raw_nullable = row.get("IsNullable")
    known = raw_nullable is not None
    is_pk = bool(row.get("IsPrimaryKey"))
    return ColumnDef(
        sno=int(row["Sno"]), source_name=str(row["ColumnName"]), name=name,
        fabric_type=normalize_fabric_type(row["FabricDataType"]),
        nullable=bool(raw_nullable) if known else (not is_pk),
        nullable_known=known, is_primary_key=is_pk, description=_clean_text(row.get("Description")),
        source_type=_clean_text(row.get("SourceDataType")), raw_fabric_type=_clean_text(row.get("FabricDataType")),
        is_watermark_candidate=(bool(row["IsWatermarkCandidate"]) if row.get("IsWatermarkCandidate") is not None else None),
        source_expression=row.get("SourceExpression"), transformation_notes=row.get("TransformationNotes"),
    )


def build_column_from_plan(item: Dict[str, Any]) -> ColumnDef:
    """Convert one ``proposed_plan.columns[]`` entry (snake_case, from Notes) into a :class:`ColumnDef`."""
    if not isinstance(item, dict) or not item.get("column_name") or not item.get("fabric_data_type"):
        raise ValidationError("Each proposed column needs column_name and fabric_data_type.")
    nullable = item.get("is_nullable")
    is_pk = bool(item.get("is_primary_key"))
    return build_column({
        "Sno": item.get("sno"), "ColumnName": item["column_name"], "TargetColumnName": item.get("target_column_name"),
        "Description": item.get("description"), "SourceDataType": item.get("source_data_type"),
        "FabricDataType": item["fabric_data_type"], "IsPrimaryKey": is_pk, "IsNullable": nullable,
        "IsWatermarkCandidate": item.get("is_watermark_candidate"),
        "SourceExpression": item.get("source_expression"), "TransformationNotes": item.get("transformation_notes"),
    })


def validate_columns(columns: Sequence[ColumnDef]) -> Tuple[ColumnDef, ...]:
    """At least one column; target names (case-insensitive) and ordinals must be unique."""
    if not columns:
        raise ValidationError("No selected columns are configured for this table.")
    names: Set[str] = set()
    ordinals: Set[int] = set()
    for col in columns:
        if col.name.lower() in names:
            raise ValidationError(f"Duplicate target column name: {col.name}")
        if col.sno in ordinals:
            raise ValidationError(f"Duplicate column ordinal (Sno): {col.sno}")
        names.add(col.name.lower())
        ordinals.add(col.sno)
    return tuple(columns)


def _build_target(row: Dict[str, Any], schema_key: str, name_key: str) -> LakehouseTarget:
    return LakehouseTarget(
        workspace_id=str(row.get("FabricWorkspaceId") or ""),
        lakehouse_id=str(row["FabricLakehouseId"]) if row.get("FabricLakehouseId") else None,
        lakehouse=validate_identifier(row.get("FabricLakehouseName"), "lakehouse name"),
        schema=validate_identifier(row.get(schema_key), "schema name"),
        name=validate_identifier(row.get(name_key), "object name"),
    )


def build_table_spec(row: Dict[str, Any], column_rows: Sequence[Dict[str, Any]]) -> TableSpec:
    """Validate a ``SourceTables`` row and its columns and return a :class:`TableSpec`."""
    columns = validate_columns([build_column(r) for r in column_rows])
    return TableSpec(
        guid=norm_guid(row["GUID"]), plan_guid=norm_guid(row["PlanGUID"]),
        status=str(row["ProvisioningStatus"]), is_active=bool(row["IsActive"]),
        target=_build_target(row, "FabricLakehouseSchema", "FabricTableName"), columns=columns,
    )


def build_view_spec(row: Dict[str, Any]) -> ViewSpec:
    """Validate a ``FabricViews`` row and return a :class:`ViewSpec`."""
    view_type = str(row.get("ViewType") or "SQL_VIEW").upper()
    if view_type not in ("SQL_VIEW", "MATERIALIZED_TABLE", "SPARK_DERIVED_TABLE"):
        raise ValidationError(f"Unsupported ViewType: {view_type[:40]}")
    description = row.get("LogicDescription")
    return ViewSpec(
        guid=norm_guid(row["ViewGUID"]), plan_guid=norm_guid(row["PlanGUID"]),
        status=str(row["ProvisioningStatus"]), is_active=bool(row["IsActive"]),
        target=_build_target(row, "FabricSchemaName", "ViewName"),
        view_type=view_type, view_sql=str(row.get("ViewSQL") or ""),
        description=description.strip() if isinstance(description, str) and description.strip() else None,
        creation_order=int(row.get("CreationOrder") or 1),
    )


_STATUS_SQL = ", ".join(f"'{x}'" for x in NEW_ROW_STATUSES)   # constants only - never user input


class ConfigRepository:
    """Read access (plus the single BACKFILL write) to the ``bronze_replication`` tables."""

    def __init__(self, db: "ConfigDbConnection") -> None:
        self._db = db
        self._s = CONFIG_DB_SCHEMA

    # -- plan ---------------------------------------------------------------
    def get_plan(self, plan_guid: str) -> Optional[PlanInfo]:
        row = self._db.fetch_one(
            f"SELECT PlanGUID, Status, PlanVersion, Notes FROM {self._s}.MigrationPlans WHERE PlanGUID = ?",
            (plan_guid,))
        if row is None:
            return None
        return PlanInfo(norm_guid(row["PlanGUID"]), str(row["Status"]), row.get("PlanVersion"), row.get("Notes"))

    # -- work discovery (master) -------------------------------------------
    def list_pending_tables(self, plan_guid: str, include_inactive: bool = False) -> List[ObjectRef]:
        """Tables of the plan waiting to be provisioned (statuses in NEW_ROW_STATUSES)."""
        active = "" if include_inactive else "AND IsActive = 1"
        rows = self._db.fetch_all(
            f"SELECT GUID, FabricLakehouseSchema, FabricTableName FROM {self._s}.SourceTables "
            f"WHERE PlanGUID = ? AND ProvisioningStatus IN ({_STATUS_SQL}) {active} "
            "ORDER BY CreatedTimestamp, FabricTableName", (plan_guid,))
        return [ObjectRef(ObjectType.TABLE, norm_guid(r["GUID"]),
                          f"{r['FabricLakehouseSchema']}.{r['FabricTableName']}") for r in rows]

    def list_pending_views(self, plan_guid: str, include_inactive: bool = False) -> List[ObjectRef]:
        active = "" if include_inactive else "AND IsActive = 1"
        rows = self._db.fetch_all(
            f"SELECT ViewGUID, FabricSchemaName, ViewName, CreationOrder FROM {self._s}.FabricViews "
            f"WHERE PlanGUID = ? AND ProvisioningStatus IN ({_STATUS_SQL}) {active} "
            "ORDER BY CreationOrder, ViewName", (plan_guid,))
        return [ObjectRef(ObjectType.VIEW, norm_guid(r["ViewGUID"]),
                          f"{r['FabricSchemaName']}.{r['ViewName']}", int(r["CreationOrder"] or 1)) for r in rows]

    def get_view_dependencies(self, view_guids: Sequence[str]) -> List[Dict[str, Any]]:
        """Declared dependencies of the given views (``FabricViewDependencies``)."""
        if not view_guids:
            return []
        marks = ",".join("?" for _ in view_guids)
        rows = self._db.fetch_all(
            f"SELECT ViewGUID, DependencyType, SourceTableGUID, DependsOnViewGUID "
            f"FROM {self._s}.FabricViewDependencies WHERE ViewGUID IN ({marks})", tuple(view_guids))
        return [{"view": norm_guid(r["ViewGUID"]), "type": r["DependencyType"],
                 "table": norm_guid(r["SourceTableGUID"]) if r["SourceTableGUID"] else None,
                 "depends_on_view": norm_guid(r["DependsOnViewGUID"]) if r["DependsOnViewGUID"] else None}
                for r in rows]

    # -- full specs (children re-read right before they act) ----------------
    def get_table_spec(self, guid: str) -> TableSpec:
        row = self._db.fetch_one(
            f"SELECT GUID, PlanGUID, FabricWorkspaceId, FabricLakehouseName, FabricLakehouseId, "
            f"FabricLakehouseSchema, FabricTableName, ProvisioningStatus, IsActive "
            f"FROM {self._s}.SourceTables WHERE GUID = ?", (guid,))
        if row is None:
            raise ValidationError("Table configuration row was not found.")
        cols = self._db.fetch_all(
            f"SELECT Sno, ColumnName, TargetColumnName, Description, SourceDataType, FabricDataType, "
            f"IsPrimaryKey, IsNullable FROM {self._s}.SourceTableColumns "
            f"WHERE GUID = ? AND IsSelected = 1 ORDER BY Sno", (guid,))
        return build_table_spec(row, cols)

    def get_view_spec(self, view_guid: str) -> ViewSpec:
        row = self._db.fetch_one(
            f"SELECT ViewGUID, PlanGUID, FabricWorkspaceId, FabricLakehouseName, FabricLakehouseId, "
            f"FabricSchemaName, ViewName, ViewType, ViewSQL, LogicDescription, CreationOrder, "
            f"ProvisioningStatus, IsActive FROM {self._s}.FabricViews WHERE ViewGUID = ?", (view_guid,))
        if row is None:
            raise ValidationError("View configuration row was not found.")
        return build_view_spec(row)

    # -- change plans (existing target) -------------------------------------
    def get_column_rows(self, source_table_guid: str) -> List[Dict[str, Any]]:
        """ALL recorded column rows (selected or not) with every field, to preserve what Notes lacks."""
        return self._db.fetch_all(
            f"SELECT Sno, ColumnName, TargetColumnName, Description, SourceDataType, FabricDataType, "
            f"IsPrimaryKey, IsNullable, IsWatermarkCandidate, IsSelected, SourceExpression, "
            f"TransformationNotes FROM {self._s}.SourceTableColumns WHERE GUID = ? ORDER BY Sno",
            (source_table_guid,))

    def get_replication_config(self, source_table_guid: str) -> Optional[Dict[str, Any]]:
        return self._db.fetch_one(
            f"SELECT IncrementalMethod, WriteStrategy, PrimaryKeyColumns, MergeKeyColumns, "
            f"WatermarkColumn, IngestionFlag "
            f"FROM {self._s}.ReplicationConfig WHERE SourceTableGUID = ?", (source_table_guid,))

    def assert_replication_idle(self, source_table_guid: str) -> None:
        row = self._db.fetch_one(
            f"SELECT Status FROM {self._s}.ReplicationState WHERE SourceTableGUID = ?",
            (source_table_guid,))
        if row and str(row['Status']).upper() == 'RUNNING':
            raise TargetStateError('A replication batch is running; wait for it to finish before changing the target.')

    def get_oracle_source(self, source_table_guid: str) -> Dict[str, Any]:
        row = self._db.fetch_one(
            f"SELECT st.ConnectionName, st.SourceSchemaName, st.SourceTableName, "
            f"st.SourceSystemType, dc.SourceType, dc.ConnectionDetails "
            f"FROM {self._s}.SourceTables st JOIN {self._s}.DBConnections dc "
            f"ON dc.ConnectionName = st.ConnectionName WHERE st.GUID = ? AND dc.IsActive = 1",
            (source_table_guid,))
        if row is None or str(row['SourceSystemType']).upper() != 'ORACLE' or str(row['SourceType']).upper() != 'ORACLE':
            raise ValidationError('An active Oracle source connection is required for data movement.')
        validate_oracle_identifier(row['SourceSchemaName'], 'Oracle schema')
        validate_oracle_identifier(row['SourceTableName'], 'Oracle table')
        return row

    def begin_change_run(self, plan_guid: str, target_guid: str, action: str, stage_name: str) -> None:
        """Reserve the approved plan; a prior physical mutation requires manual reconciliation."""
        s = self._s
        with self._db.transaction():
            row = self._db.fetch_one(
                f"SELECT Phase, SourceTableGUID, Action, StageTableName "
                f"FROM {s}.TargetChangeRuns WITH (UPDLOCK, HOLDLOCK) WHERE PlanGUID = ?",
                (plan_guid,))
            if row is None:
                self._db.execute(
                    f"INSERT INTO {s}.TargetChangeRuns "
                    f"(PlanGUID, SourceTableGUID, Action, Phase, StageTableName) VALUES (?,?,?,?,?)",
                    (plan_guid, target_guid, action, 'STARTED', stage_name))
            elif row['Phase'] in ('STARTED', 'STAGED'):
                if (norm_guid(row['SourceTableGUID']) != norm_guid(target_guid)
                        or row['Action'] != action or row['StageTableName'] != stage_name):
                    raise TargetStateError('Existing change run does not match the approved target and action.')
                self._db.execute(
                    f"UPDATE {s}.TargetChangeRuns SET Phase = 'STARTED', SnapshotSCN = NULL, "
                    f"SourceRows = NULL, UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ?",
                    (plan_guid,))
            else:
                raise TargetStateError('This plan already changed the target or completed; reconcile its run record.')

    def mark_change_run(self, plan_guid: str, phase: str, *, snapshot_scn: Optional[int] = None,
                        source_rows: Optional[int] = None) -> None:
        if phase not in ('STAGED', 'MUTATION_STARTED'):
            raise ValueError('Invalid change run phase.')
        changed = self._db.execute(
            f"UPDATE {self._s}.TargetChangeRuns SET Phase = ?, SnapshotSCN = COALESCE(?, SnapshotSCN), "
            f"SourceRows = COALESCE(?, SourceRows), UpdatedTimestamp = SYSUTCDATETIME() "
            f"WHERE PlanGUID = ?", (phase, snapshot_scn, source_rows, plan_guid))
        if changed != 1:
            raise TargetStateError('Target change run record was not found.')

    def set_ingestion_flag(self, source_table_guid: str, enabled: bool) -> None:
        """Pause (False) or resume (True) replication of one target; exactly one row must change."""
        changed = self._db.execute(
            f"UPDATE {self._s}.ReplicationConfig SET IngestionFlag = ?, UpdatedTimestamp = SYSUTCDATETIME() "
            f"WHERE SourceTableGUID = ?", (1 if enabled else 0, source_table_guid))
        if changed != 1:
            raise TargetStateError("ReplicationConfig row for the target was not found.")

    def apply_metadata_change(self, source_table_guid: str, expected_status: str,
                              column_rows: Sequence[Tuple[Any, ...]], replication_updates: Dict[str, Any],
                              complete_run_guid: Optional[str] = None,
                              resume_ingestion: Optional[bool] = None) -> None:
        """
        Replace the recorded columns and update ReplicationConfig in ONE transaction.

        The SourceTables row is locked and must still have ``expected_status`` and be active
        (a precondition that protects against concurrent edits). ReplicationState is never touched,
        so the watermark stays unchanged.
        """
        s = self._s
        with self._db.transaction():
            row = self._db.fetch_one(
                f"SELECT ProvisioningStatus, IsActive FROM {s}.SourceTables WITH (UPDLOCK, ROWLOCK) WHERE GUID = ?",
                (source_table_guid,))
            if row is None or str(row["ProvisioningStatus"]) != expected_status or not row["IsActive"]:
                raise TargetStateError("The target record changed while the notebook was running; nothing was saved.")
            self._db.execute(f"DELETE FROM {s}.SourceTableColumns WHERE GUID = ?", (source_table_guid,))
            for values in column_rows:
                self._db.execute(
                    f"INSERT INTO {s}.SourceTableColumns (GUID, Sno, ColumnName, TargetColumnName, Description, "
                    f"SourceDataType, FabricDataType, IsPrimaryKey, IsNullable, IsWatermarkCandidate, IsSelected, "
                    f"SourceExpression, TransformationNotes) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (source_table_guid,) + tuple(values))
            if replication_updates:
                assignments = ", ".join(f"{col} = ?" for col in replication_updates)   # column names come from a whitelist
                changed = self._db.execute(
                    f"UPDATE {s}.ReplicationConfig SET {assignments}, UpdatedTimestamp = SYSUTCDATETIME() "
                    f"WHERE SourceTableGUID = ?", tuple(replication_updates.values()) + (source_table_guid,))
                if changed != 1:
                    raise TargetStateError("ReplicationConfig row for the target was not found; nothing was saved.")
            if self._db.execute(f"UPDATE {s}.SourceTables SET UpdatedTimestamp = SYSUTCDATETIME() WHERE GUID = ?",
                                (source_table_guid,)) != 1:
                raise TargetStateError("Target record disappeared; nothing was saved.")
            if complete_run_guid and self._db.execute(
                    f"UPDATE {s}.TargetChangeRuns SET Phase = 'COMPLETED', "
                    f"UpdatedTimestamp = SYSUTCDATETIME() "
                    f"WHERE PlanGUID = ? AND SourceTableGUID = ? AND Phase = 'MUTATION_STARTED'",
                    (complete_run_guid, source_table_guid)) != 1:
                raise TargetStateError('Change run state changed before metadata commit.')
            if resume_ingestion is not None and self._db.execute(
                    f"UPDATE {s}.ReplicationConfig SET IngestionFlag = ?, "
                    f"UpdatedTimestamp = SYSUTCDATETIME() WHERE SourceTableGUID = ?",
                    (1 if resume_ingestion else 0, source_table_guid)) != 1:
                raise TargetStateError('Replication config disappeared before ingestion could resume.')


@contextmanager
def default_repository() -> Iterator[ConfigRepository]:
    """Open a Config DB connection and yield a repository; always closes the connection."""
    with ConfigDbConnection() as db:
        yield ConfigRepository(db)

# %% [markdown]
# ## 6b. Change plans (CHANGE_REQUESTED) - parse, verify and compare
# The approved instruction is `MigrationPlans.Notes.target_change` (contract section 6).
# Everything below is pure and unit-tested.

# %%
@dataclass(frozen=True)
class TargetChange:
    """Validated ``Notes.target_change`` of a CHANGE_REQUESTED plan."""
    decision: str
    existing_guid: str                        # SourceTables.GUID of the existing target
    review_target: Dict[str, Any]
    proposed_table: Dict[str, Any]
    columns: Tuple[ColumnDef, ...]            # proposed columns
    replication: Dict[str, Any]               # proposed replication settings (may be empty)
    approved_comparison: Optional[Dict[str, Any]]

    @property
    def display(self) -> str:
        return f"{self.proposed_table.get('fabric_lakehouse_schema')}.{self.proposed_table.get('fabric_table_name')}"


def parse_target_change(notes: Optional[str]) -> TargetChange:
    """Parse and validate the approved change instruction stored in ``MigrationPlans.Notes``."""
    try:
        data = json.loads(notes or "")["target_change"]
        if not isinstance(data, dict):
            raise TypeError
    except (ValueError, KeyError, TypeError):
        raise ValidationError("Plan Notes do not contain a valid target_change.") from None
    decision = str(data.get("decision") or "").strip().upper()
    if decision not in PlanAction.CHANGE_ACTIONS:
        raise ValidationError(f"Unknown target-change decision: {decision[:40]!r}")
    guid = data.get("existing_source_table_guid")
    if not is_valid_uuid(guid):
        raise ValidationError("target_change.existing_source_table_guid is missing or invalid.")
    proposed = data.get("proposed_plan")
    if not isinstance(proposed, dict) or not isinstance(proposed.get("table"), dict):
        raise ValidationError("target_change.proposed_plan.table is missing.")
    raw_columns = proposed.get("columns")
    if not isinstance(raw_columns, list):
        raise ValidationError("target_change.proposed_plan.columns is missing.")
    columns = validate_columns([build_column_from_plan(c) for c in raw_columns if c.get("is_selected", True)])
    review = data.get("review") if isinstance(data.get("review"), dict) else {}
    comparison = None
    for definition in review.get("definitions") or []:
        if isinstance(definition, dict) and is_valid_uuid(definition.get("source_table_guid")) \
                and norm_guid(definition["source_table_guid"]) == norm_guid(guid):
            comparison = definition.get("comparison")
    replication = proposed.get("replication") if isinstance(proposed.get("replication"), dict) else {}
    return TargetChange(decision, norm_guid(guid), review.get("target") or {}, proposed["table"], columns,
                        replication, comparison if isinstance(comparison, dict) else None)


def verify_target_agreement(change: TargetChange, live: LakehouseTarget) -> None:
    """The review target, the proposed table and the live SourceTables row must name the same target."""
    rt, pt = change.review_target, change.proposed_table
    fields = {
        "workspace": (rt.get("workspace_id"), pt.get("fabric_workspace_id"), live.workspace_id),
        "lakehouse": (rt.get("lakehouse_id"), pt.get("fabric_lakehouse_id"), live.lakehouse_id),
        "lakehouse name": (pt.get("fabric_lakehouse_name"), live.lakehouse),
        "schema": (rt.get("schema"), pt.get("fabric_lakehouse_schema"), live.schema),
        "table": (rt.get("table"), pt.get("fabric_table_name"), live.name),
    }
    for label, values in fields.items():
        present = {str(v).strip().lower() for v in values if v not in (None, "")}
        if len(present) > 1:
            raise ValidationError(f"Approved {label} differs between the review, the proposal and the live record.")


def compare_definitions(recorded: Sequence[ColumnDef], proposed: Sequence[ColumnDef]) -> Dict[str, Set[str]]:
    """Added / removed / changed column names between two definitions (lower-case)."""
    rec, prop = {c.name.lower(): c for c in recorded}, {c.name.lower(): c for c in proposed}

    def differs(a: ColumnDef, b: ColumnDef) -> bool:
        return (a.fabric_type != b.fabric_type or a.is_primary_key != b.is_primary_key
                or (a.nullable_known and b.nullable_known and a.nullable != b.nullable)
                or (b.description is not None and a.description != b.description)
                or bool(a.source_type and b.source_type and a.source_type.upper() != b.source_type.upper()))

    return {"added": set(prop) - set(rec), "removed": set(rec) - set(prop),
            "changed": {k for k in rec.keys() & prop.keys() if differs(rec[k], prop[k])}}


def _comparison_names(items: Any) -> Optional[Set[str]]:
    """Column names in an approved comparison list; ``None`` if the format is not recognised."""
    if items in (None, []):
        return set()
    if not isinstance(items, list):
        return None
    names: Set[str] = set()
    for item in items:
        if isinstance(item, str):
            names.add(item.lower())
        elif isinstance(item, dict):
            nested = item.get("after") if isinstance(item.get("after"), dict) else {}
            value = next((item[k] for k in ("column_name", "target_column_name", "name", "column") if item.get(k)), None)
            value = value or nested.get("name")
            if value is None:
                return None
            names.add(str(value).lower())
        else:
            return None
    return names


def check_against_approval(diff: Dict[str, Set[str]], comparison: Optional[Dict[str, Any]], required: bool) -> None:
    """
    Stop if the recomputed difference is not what the user approved.

    ``added`` and ``removed`` must match exactly. ``changed`` may only contain columns that
    were approved as changed (the notebook must never apply an unapproved change).
    """
    if comparison is None:
        if required:
            raise ValidationError("The approval review has no comparison for the existing target; request a fresh review.")
        LOG.warning("No approved comparison found for the existing target; skipping the approval check.")
        return
    approved = {k: _comparison_names(comparison.get(k)) for k in ("added", "removed", "changed")}
    if comparison.get("same") is True and any(diff.values()):
        raise ValidationError("The approved review says the definitions are the same, but they differ now.")
    for key in ("added", "removed"):
        if approved[key] is None:
            raise ValidationError(f"Approved '{key}' list has an unrecognised format.")
        if approved[key] != diff[key]:
            raise ValidationError(f"The current '{key}' columns differ from the approved review; request a fresh review.")
    if approved["changed"] is None or diff["changed"] != approved["changed"]:
        raise ValidationError("The current changed columns differ from the approved review; request a fresh review.")


def _json_name_set(value: Any) -> Optional[Set[str]]:
    try:
        parsed = json.loads(value) if isinstance(value, str) else value
        return {str(x).lower() for x in parsed} if isinstance(parsed, list) else None
    except ValueError:
        return None


def check_key_stability(recorded: Sequence[ColumnDef], proposed: Sequence[ColumnDef],
                        current_config: Optional[Dict[str, Any]], replication: Dict[str, Any]) -> None:
    """Keys must not change in place (ALTER_FUTURE / ALTER_BACKFILL)."""
    if {c.name.lower() for c in recorded if c.is_primary_key} != {c.name.lower() for c in proposed if c.is_primary_key}:
        raise UnsafeChangeError("Primary key columns change; not allowed in place (needs REPLACE_FULL).")
    for plan_key, db_key in (("primary_key_columns", "PrimaryKeyColumns"), ("merge_key_columns", "MergeKeyColumns")):
        if current_config and plan_key in replication:
            old, new = _json_name_set(current_config.get(db_key)), _json_name_set(replication[plan_key])
            if (old or set()) != (new or set()):
                raise UnsafeChangeError(f"{plan_key} change; not allowed in place (needs REPLACE_FULL).")


# proposed_plan.replication key -> ReplicationConfig column (whitelist; ingestion_flag is handled separately)
REPLICATION_FIELDS = {
    "incremental_method": "IncrementalMethod", "watermark_column": "WatermarkColumn",
    "watermark_column_data_type": "WatermarkColumnDataType", "watermark_index_name": "WatermarkIndexName",
    "write_strategy": "WriteStrategy", "primary_key_columns": "PrimaryKeyColumns",
    "merge_key_columns": "MergeKeyColumns", "effective_from_column": "EffectiveFromColumn",
    "effective_to_column": "EffectiveToColumn", "current_flag_column": "CurrentFlagColumn",
    "max_row_fetch": "MaxRowFetch", "pipeline_workspace_id": "PipelineWorkspaceId", "pipeline_item_id": "PipelineItemId",
}


def replication_updates(replication: Dict[str, Any]) -> Dict[str, Any]:
    """Map the proposed replication settings to ReplicationConfig columns (JSON arrays serialised)."""
    return {REPLICATION_FIELDS[k]: (json.dumps(v) if isinstance(v, (list, dict)) else v)
            for k, v in replication.items() if k in REPLICATION_FIELDS}


def build_column_insert_rows(proposed: Sequence[ColumnDef], existing_rows: Sequence[Dict[str, Any]]) -> List[Tuple[Any, ...]]:
    """
    Rows for ``SourceTableColumns`` after a change: the proposed columns, with details that the
    plan does not carry (watermark candidate, expression, notes, ...) preserved from the old row,
    plus previously *unselected* columns that are not part of the proposal.
    """
    old = {str(r.get("TargetColumnName") or r["ColumnName"]).lower(): r for r in existing_rows}
    rows: List[Tuple[Any, ...]] = []
    for c in sorted(proposed, key=lambda x: x.sno):
        prev = old.get(c.name.lower(), {})
        pick = lambda new, key: new if new is not None else prev.get(key)   # noqa: E731
        watermark = c.is_watermark_candidate if c.is_watermark_candidate is not None else bool(prev.get("IsWatermarkCandidate"))
        rows.append((c.sno, c.source_name, c.name, pick(c.description, "Description"),
                     c.source_type or prev.get("SourceDataType") or c.fabric_type, c.raw_fabric_type or c.fabric_type,
                     1 if c.is_primary_key else 0, int(c.nullable) if c.nullable_known else None, 1 if watermark else 0, 1,
                     pick(c.source_expression, "SourceExpression"), pick(c.transformation_notes, "TransformationNotes")))
    names = {c.name.lower() for c in proposed}
    next_sno = max([c.sno for c in proposed] or [0]) + 1
    for r in existing_rows:
        if not r.get("IsSelected") and str(r.get("TargetColumnName") or r["ColumnName"]).lower() not in names:
            rows.append((next_sno, r["ColumnName"], r.get("TargetColumnName"), r.get("Description"), r["SourceDataType"],
                         r["FabricDataType"], int(bool(r.get("IsPrimaryKey"))),
                         None if r.get("IsNullable") is None else int(bool(r["IsNullable"])),
                         int(bool(r.get("IsWatermarkCandidate"))), 0, r.get("SourceExpression"), r.get("TransformationNotes")))
            next_sno += 1
    return rows


class TargetLease:
    """
    Exclusive, per-target lease so that only one job changes a target at a time.

    Implemented with a SQL Server *session* application lock (``sp_getapplock``) on a dedicated
    connection: no schema migration is needed and the lock is released automatically if the
    notebook dies. No SQL transaction is held open.
    """

    def __init__(self, resource: str) -> None:
        self.resource = resource.lower()[:255]
        self._db: Optional["ConfigDbConnection"] = None

    def __enter__(self) -> "TargetLease":
        self._db = ConfigDbConnection().connect()
        row = self._db.fetch_one(
            "SET NOCOUNT ON; DECLARE @rc int; EXEC @rc = sys.sp_getapplock @Resource = ?, "
            "@LockMode = 'Exclusive', @LockOwner = 'Session', @LockTimeout = 0; SELECT @rc AS rc;", (self.resource,))
        if row is None or int(row["rc"]) < 0:
            self._db.close()
            raise TargetStateError("Another job currently holds the lease for this target; try again later.")
        return self

    def __exit__(self, *_exc: Any) -> None:
        try:
            if self._db is not None:
                self._db.execute("EXEC sys.sp_releaseapplock @Resource = ?, @LockOwner = 'Session'", (self.resource,))
        except Exception:  # noqa: BLE001 - closing the connection releases the lock anyway
            pass
        finally:
            if self._db is not None:
                self._db.close()

# %% [markdown]
# ## 7. Delta table operations (Spark)

# %%
@dataclass(frozen=True)
class PhysicalColumn:
    """A column as it exists in the Delta table."""
    name: str
    data_type: str
    nullable: bool
    comment: Optional[str]


@dataclass
class SchemaDiff:
    """Difference between the physical table and the desired definition."""
    added: List[ColumnDef] = field(default_factory=list)
    widened: List[Tuple[ColumnDef, str]] = field(default_factory=list)   # (desired, old type)
    relaxed: List[ColumnDef] = field(default_factory=list)               # NOT NULL -> nullable
    comments: List[ColumnDef] = field(default_factory=list)              # description changes
    rejected: List[str] = field(default_factory=list)                    # unsafe changes

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.widened or self.relaxed or self.comments or self.rejected)

    def summary(self) -> str:
        return (f"added={len(self.added)}, widened={len(self.widened)}, "
                f"nullability_relaxed={len(self.relaxed)}, descriptions={len(self.comments)}")


def plan_schema_changes(physical: Sequence[PhysicalColumn], desired: Sequence[ColumnDef]) -> SchemaDiff:
    """
    Compare the physical schema with the desired one (names are case-insensitive).

    Allowed in place: new nullable columns, safe type widening, NOT NULL -> nullable,
    description changes. Everything else is recorded in ``rejected`` (never applied):
    removed/renamed columns, narrowing or incompatible type changes, nullable ->
    NOT NULL, and NOT NULL columns that would be added to an existing table.
    """
    diff = SchemaDiff()
    phys = {c.name.lower(): c for c in physical}
    want = {c.name.lower(): c for c in desired}

    for key, p in phys.items():
        if key not in want:
            diff.rejected.append(f"column {p.name} is not in the configuration (removal/rename is not allowed in place)")

    for key, d in want.items():
        p = phys.get(key)
        if p is None:
            if d.nullable:
                diff.added.append(d)
            else:
                diff.rejected.append(f"new column {d.name} is NOT NULL (only nullable columns can be added)")
            continue
        if p.data_type != d.fabric_type:
            if is_safe_widening(p.data_type, d.fabric_type):
                diff.widened.append((d, p.data_type))
            else:
                diff.rejected.append(f"column {d.name}: {p.data_type} -> {d.fabric_type} is not a safe widening")
        if d.nullable_known:
            if p.nullable and not d.nullable:
                diff.rejected.append(f"column {d.name}: nullable -> NOT NULL is not allowed in place")
            elif not p.nullable and d.nullable:
                diff.relaxed.append(d)
        if d.description is not None and (p.comment or "").strip() != d.description:
            diff.comments.append(d)
    return diff


def structure_matches(physical: Sequence[PhysicalColumn], columns: Sequence[ColumnDef]) -> bool:
    """True if names, types and (known) nullability agree; descriptions are ignored."""
    d = plan_schema_changes(physical, columns)
    return not (d.added or d.widened or d.relaxed or d.rejected)


def column_ddl(col: ColumnDef) -> str:
    """Column fragment ``name TYPE [NOT NULL] [COMMENT '...']`` for CREATE / ADD COLUMNS."""
    parts = [quote_ident(col.name), col.fabric_type]
    if not col.nullable:
        parts.append("NOT NULL")
    if col.description:
        parts.append(f"COMMENT {sql_literal(col.description)}")
    return " ".join(parts)


def build_create_table_sql(spec: TableSpec, or_replace: bool = False) -> str:
    """CREATE [OR REPLACE] TABLE statement for a validated spec (pure function)."""
    head = "CREATE OR REPLACE TABLE" if or_replace else "CREATE TABLE"
    cols = ",\n  ".join(column_ddl(c) for c in sorted(spec.columns, key=lambda c: c.sno))
    return f"{head} {spec.target.fq} (\n  {cols}\n) USING DELTA"


class DeltaTableManager:
    """Create, inspect, alter and drop Delta tables through Spark SQL."""

    def __init__(self, spark: Any = None) -> None:
        self.spark = spark or get_spark()

    def exists(self, target: LakehouseTarget) -> bool:
        if not target.preflight(self.spark):
            return False
        rows = self.spark.sql(f"SHOW TABLES IN {target.namespace}").collect()
        return any(str(r["tableName"]).lower() == target.name.lower() for r in rows)

    def physical_columns(self, target: LakehouseTarget) -> List[PhysicalColumn]:
        fields = self.spark.table(target.fq).schema.fields
        return [PhysicalColumn(f.name, normalize_physical_type(f.dataType.simpleString()), bool(f.nullable),
                               (f.metadata or {}).get("comment")) for f in fields]

    def create(self, spec: TableSpec) -> None:
        spec.target.preflight(self.spark, ensure_schema=True)
        self.spark.sql(build_create_table_sql(spec))

    def apply(self, spec: TableSpec, diff: SchemaDiff) -> None:
        """Apply an already-validated diff (no rejections) in a safe order."""
        fq = spec.target.fq
        if diff.widened:
            self.spark.sql(f"ALTER TABLE {fq} SET TBLPROPERTIES ('delta.enableTypeWidening' = 'true')")
            for col, _old in diff.widened:
                self.spark.sql(f"ALTER TABLE {fq} ALTER COLUMN {quote_ident(col.name)} TYPE {col.fabric_type}")
        if diff.added:
            self.spark.sql(f"ALTER TABLE {fq} ADD COLUMNS ({', '.join(column_ddl(c) for c in diff.added)})")
        for col in diff.relaxed:
            self.spark.sql(f"ALTER TABLE {fq} ALTER COLUMN {quote_ident(col.name)} DROP NOT NULL")
        added_names = {c.name.lower() for c in diff.added}      # their comment is already in ADD COLUMNS
        for col in diff.comments:
            if col.name.lower() not in added_names:
                self.spark.sql(f"ALTER TABLE {fq} ALTER COLUMN {quote_ident(col.name)} "
                               f"COMMENT {sql_literal(col.description or '')}")

    def verify(self, spec: TableSpec) -> None:
        """Re-read the physical schema and require it to match the specification."""
        leftover = plan_schema_changes(self.physical_columns(spec.target), spec.columns)
        if not leftover.is_empty:
            raise TargetStateError("Physical schema does not match the configuration after the change: "
                                   + "; ".join(leftover.rejected or [leftover.summary()]))


# %% [markdown]
# ## 7b. Oracle snapshot and staged data movement
# A SHARE lock holds source DML until the Delta and Config DB changes finish. This
# deliberately requires a maintenance window; without CDC, releasing that lock
# during a full copy would allow missed updates and deletes.

# %%
_ORACLE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")


def validate_oracle_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not _ORACLE_IDENTIFIER.fullmatch(value):
        raise ValidationError(f'{label} must be an ordinary Oracle identifier.')
    return value.upper()


def quote_oracle_identifier(value: str) -> str:
    return '"' + validate_oracle_identifier(value, 'Oracle identifier') + '"'


def oracle_dsn(details: Dict[str, Any]) -> str:
    dsn = details.get('dsn')
    if isinstance(dsn, str) and dsn.strip():
        return dsn.strip()
    host, service = details.get('host'), details.get('serviceName') or details.get('service_name')
    port = details.get('port', 1521)
    if not isinstance(host, str) or not host.strip() or not isinstance(service, str) or not service.strip():
        raise ValidationError('Oracle connection needs a DSN or host and service name.')
    if not isinstance(port, int) or isinstance(port, bool) or not (1 <= port <= 65535):
        raise ValidationError('Oracle connection port is invalid.')
    return f'{host.strip()}:{port}/{service.strip()}'


def parse_oracle_connection(source: Dict[str, Any]) -> Tuple[str, str, str]:
    try:
        details = source['ConnectionDetails']
        if isinstance(details, str):
            details = json.loads(details)
        if not isinstance(details, dict):
            raise ValueError('invalid JSON object')
        username, password = details.get('username'), details.get('password')
        if not isinstance(username, str) or not username or not isinstance(password, str) or not password:
            raise ValueError('missing credentials')
        return username, password, oracle_dsn(details)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValidationError('Oracle connection details are incomplete or invalid.') from exc


class OracleSnapshot:
    """Exclusive-to-this-run Oracle read with a held SHARE table lock and fixed SCN."""

    def __init__(self, source: Dict[str, Any]) -> None:
        self.source = source
        self.connection: Any = None
        self.scn: Optional[int] = None

    def __enter__(self) -> 'OracleSnapshot':
        import oracledb  # installed in the Fabric notebook environment
        username, password, dsn = parse_oracle_connection(self.source)
        try:
            self.connection = oracledb.connect(user=username, password=password, dsn=dsn)
            table = (f"{quote_oracle_identifier(self.source['SourceSchemaName'])}."
                     f"{quote_oracle_identifier(self.source['SourceTableName'])}")
            with self.connection.cursor() as cursor:
                cursor.execute(f'LOCK TABLE {table} IN SHARE MODE NOWAIT')
                cursor.execute('SELECT DBMS_FLASHBACK.GET_SYSTEM_CHANGE_NUMBER FROM DUAL')
                self.scn = int(cursor.fetchone()[0])
            return self
        except Exception as exc:
            self.__exit__(None, None, None)
            raise TargetStateError(f'Oracle snapshot or source lock failed ({type(exc).__name__}). '
                                   'Check Oracle access, flashback privilege and a quiet source table.') from exc

    def __exit__(self, *_exc: Any) -> None:
        if self.connection is not None:
            try:
                self.connection.rollback()  # releases SHARE lock; never commit source changes
            except Exception as exc:
                LOG.error('Oracle source rollback failed (%s).', type(exc).__name__)
            finally:
                try:
                    self.connection.close()
                except Exception as exc:
                    LOG.error('Oracle source connection close failed (%s).', type(exc).__name__)
                finally:
                    self.connection = None

    def batches(self, columns: Sequence[ColumnDef]) -> Iterator[List[Tuple[Any, ...]]]:
        if self.connection is None or self.scn is None:
            raise TargetStateError('Oracle snapshot is not open.')
        names = [quote_oracle_identifier(c.source_name) for c in columns]
        table = (f"{quote_oracle_identifier(self.source['SourceSchemaName'])}."
                 f"{quote_oracle_identifier(self.source['SourceTableName'])}")
        query = f"SELECT {', '.join(names)} FROM {table} AS OF SCN :scn"
        cursor = self.connection.cursor()
        try:
            cursor.arraysize = ORACLE_FETCH_ROWS
            cursor.execute(query, scn=self.scn)
            while True:
                rows = cursor.fetchmany(ORACLE_FETCH_ROWS)
                if not rows:
                    break
                yield [tuple(row) for row in rows]
        except Exception as exc:
            raise TargetStateError(f'Oracle snapshot read failed ({type(exc).__name__}).') from exc
        finally:
            cursor.close()


def spark_type(fabric_type: str) -> Any:
    from pyspark.sql import types as T
    simple = {
        'STRING': T.StringType, 'BOOLEAN': T.BooleanType, 'TINYINT': T.ByteType,
        'SMALLINT': T.ShortType, 'INT': T.IntegerType, 'BIGINT': T.LongType,
        'FLOAT': T.FloatType, 'DOUBLE': T.DoubleType, 'DATE': T.DateType,
        'TIMESTAMP': T.TimestampType, 'TIMESTAMP_NTZ': T.TimestampNTZType,
        'BINARY': T.BinaryType,
    }
    parts = _decimal_parts(fabric_type)
    if parts:
        return T.DecimalType(*parts)
    if fabric_type in simple:
        return simple[fabric_type]()
    raise UnsupportedTypeError(f'No Spark conversion for {fabric_type}.')


def convert_oracle_value(value: Any, col: ColumnDef) -> Any:
    """Convert before createDataFrame; reject overflow and lossy decimal casts."""
    if value is None:
        if not col.nullable:
            raise ValidationError(f'Oracle column {col.source_name} contains NULL but target is NOT NULL.')
        return None
    if hasattr(value, 'read') and callable(value.read):
        value = value.read()  # CLOB/BLOB
    kind = col.fabric_type
    try:
        if kind == 'STRING':
            if isinstance(value, (bytes, bytearray, memoryview)):
                raise ValueError('binary cannot be converted to string implicitly')
            return str(value)
        if kind == 'BINARY':
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise ValueError('not binary')
            return bytes(value)
        if kind == 'BOOLEAN':
            if isinstance(value, bool) or value in (0, 1):
                return bool(value)
            raise ValueError('not boolean')
        if kind in _INTEGRAL_ORDER:
            integer = int(value)
            limits = {'TINYINT': (-128, 127), 'SMALLINT': (-32768, 32767),
                      'INT': (-2147483648, 2147483647),
                      'BIGINT': (-9223372036854775808, 9223372036854775807)}
            if Decimal(str(value)) != integer or not (limits[kind][0] <= integer <= limits[kind][1]):
                raise ValueError('integer overflow or fractional value')
            return integer
        if kind in ('FLOAT', 'DOUBLE'):
            import math
            result = float(value)
            if not math.isfinite(result):
                raise ValueError('nonfinite float')
            return result
        parts = _decimal_parts(kind)
        if parts:
            decimal = Decimal(str(value))
            precision, scale = parts
            quantum = Decimal(1).scaleb(-scale)
            with localcontext() as context:
                context.prec = 80
                if not decimal.is_finite():
                    raise ValueError('decimal scale overflow')
                quantized = decimal.quantize(quantum)
                if decimal != quantized:
                    raise ValueError('decimal scale overflow')
                if len(quantized.as_tuple().digits) > precision or abs(quantized) >= Decimal(10) ** (precision - scale):
                    raise ValueError('decimal precision overflow')
            return quantized
        if kind == 'DATE':
            if isinstance(value, datetime):
                return value.date()
            if isinstance(value, date):
                return value
        if kind in ('TIMESTAMP', 'TIMESTAMP_NTZ') and isinstance(value, datetime):
            if kind == 'TIMESTAMP' and value.tzinfo is not None:
                return value.astimezone(timezone.utc).replace(tzinfo=None)
            if value.tzinfo is None:
                return value
        raise ValueError('unsupported source value')
    except (ValueError, TypeError, ArithmeticError, InvalidOperation) as exc:
        raise ValidationError(f'Oracle value for {col.source_name} cannot be converted to {kind}.') from exc


def staging_target(target: LakehouseTarget, plan_guid: str) -> LakehouseTarget:
    return replace(target, name='MIG_STAGE_' + uuid.UUID(plan_guid).hex.upper())


class DeltaDataMover:
    """Load a bounded Oracle cursor into a Delta stage, validate, then merge or replace."""

    def __init__(self, manager: DeltaTableManager) -> None:
        self.manager = manager
        self.spark = manager.spark

    def stage(self, target: LakehouseTarget, columns: Sequence[ColumnDef], snapshot: OracleSnapshot) -> int:
        from pyspark.sql import types as T
        target.preflight(self.spark, ensure_schema=True)
        self.spark.conf.set('spark.sql.session.timeZone', 'UTC')
        schema = T.StructType([T.StructField(c.name, spark_type(c.fabric_type), c.nullable) for c in columns])
        self.spark.createDataFrame([], schema).write.format('delta').mode('overwrite').option(
            'overwriteSchema', 'true').saveAsTable(target.fq)
        count = 0
        for batch in snapshot.batches(columns):
            converted = [tuple(convert_oracle_value(value, col) for value, col in zip(row, columns, strict=True))
                         for row in batch]
            self.spark.createDataFrame(converted, schema).write.format('delta').mode('append').saveAsTable(target.fq)
            count += len(converted)
        if self.spark.table(target.fq).count() != count:
            raise TargetStateError('Staging row count differs from the Oracle snapshot.')
        return count

    def validate_keys(self, stage: LakehouseTarget, keys: Sequence[str]) -> None:
        from functools import reduce
        from pyspark.sql import functions as F
        frame = self.spark.table(stage.fq)
        cols = [F.col(quote_ident(k)) for k in keys]
        if frame.filter(reduce(lambda a, b: a | b, (c.isNull() for c in cols))).limit(1).count():
            raise ValidationError('Oracle snapshot has NULL merge keys.')
        if frame.groupBy(*cols).count().filter(F.col('count') > 1).limit(1).count():
            raise ValidationError('Oracle snapshot has duplicate merge keys.')

    def validate_target_keys(self, target: LakehouseTarget, keys: Sequence[str]) -> None:
        from functools import reduce
        from pyspark.sql import functions as F
        frame = self.spark.table(target.fq)
        cols = [F.col(quote_ident(k)) for k in keys]
        if frame.filter(reduce(lambda a, b: a | b, (c.isNull() for c in cols))).limit(1).count():
            raise TargetStateError('Existing target contains NULL merge keys.')
        if frame.groupBy(*cols).count().filter(F.col('count') > 1).limit(1).count():
            raise TargetStateError('Existing target contains duplicate merge keys.')

    def validate_target_subset(self, target: LakehouseTarget, stage: LakehouseTarget,
                               keys: Sequence[str]) -> None:
        on = ' AND '.join(f't.{quote_ident(k)} <=> s.{quote_ident(k)}' for k in keys)
        missing = self.spark.sql(
            f'SELECT 1 FROM {target.fq} AS t LEFT ANTI JOIN {stage.fq} AS s ON {on} LIMIT 1').count()
        if missing:
            raise ValidationError('Existing target has keys absent from Oracle; choose REPLACE_FULL to remove them.')

    def merge(self, target: LakehouseTarget, stage: LakehouseTarget,
              columns: Sequence[ColumnDef], keys: Sequence[str]) -> None:
        names = [c.name for c in columns]
        on = ' AND '.join(f't.{quote_ident(k)} <=> s.{quote_ident(k)}' for k in keys)
        assignments = ', '.join(f't.{quote_ident(n)} = s.{quote_ident(n)}' for n in names)
        names_sql = ', '.join(quote_ident(n) for n in names)
        values_sql = ', '.join(f's.{quote_ident(n)}' for n in names)
        self.spark.sql(f'MERGE INTO {target.fq} AS t USING {stage.fq} AS s ON {on} '
                       f'WHEN MATCHED THEN UPDATE SET {assignments} '
                       f'WHEN NOT MATCHED THEN INSERT ({names_sql}) VALUES ({values_sql})')

    def replace(self, target: LakehouseTarget, stage: LakehouseTarget, columns: Sequence[ColumnDef]) -> None:
        projection = ', '.join(quote_ident(c.name) for c in columns)
        self.spark.sql(f'CREATE OR REPLACE TABLE {target.fq} USING DELTA AS '
                       f'SELECT {projection} FROM {stage.fq}')

    def verify_data(self, target: LakehouseTarget, stage: LakehouseTarget,
                    columns: Sequence[ColumnDef], replace_all: bool) -> None:
        projection = ', '.join(quote_ident(c.name) for c in columns)
        missing = self.spark.sql(
            f'SELECT {projection} FROM {stage.fq} EXCEPT ALL '
            f'SELECT {projection} FROM {target.fq}').limit(1).count()
        if missing:
            raise TargetStateError('Target values differ from the staged Oracle snapshot.')
        if replace_all and self.spark.table(target.fq).count() != self.spark.table(stage.fq).count():
            raise TargetStateError('Replaced target row count differs from the Oracle snapshot.')

# %% [markdown]
# ## 8. View operations (Spark)

# %%
_LEADING_NOISE_RE = re.compile(r"^(?:\s+|--[^\n]*(?:\n|$)|/\*.*?\*/)*", re.S)
_SELECT_START_RE = re.compile(r"^(?:SELECT|WITH)\b", re.I)


def prepare_select(view_sql: str) -> str:
    """
    Validate ``FabricViews.ViewSQL``: it must be one SELECT/WITH statement.

    The notebook adds the ``CREATE ... AS`` prefix itself, so a ViewSQL that already
    starts with CREATE is rejected with a clear message. A single trailing ``;`` is
    removed; any other ``;`` is rejected (no statement stacking).
    """
    text = (view_sql or "").strip().rstrip(";").strip()
    if not text:
        raise ValidationError("ViewSQL is empty.")
    if not _SELECT_START_RE.match(_LEADING_NOISE_RE.sub("", text, count=1)):
        raise ValidationError("ViewSQL must be a single SELECT/WITH query (without CREATE VIEW).")
    if ";" in text:
        raise ValidationError("ViewSQL must contain exactly one statement (no ';').")
    return text


def build_create_view_sql(spec: ViewSpec) -> str:
    """CREATE OR REPLACE VIEW (SQL_VIEW) or CREATE OR REPLACE TABLE ... AS (materialised types)."""
    select = prepare_select(spec.view_sql)
    if spec.view_type == "SQL_VIEW":
        comment = f" COMMENT {sql_literal(spec.description[:1000])}" if spec.description else ""
        return f"CREATE OR REPLACE VIEW {spec.target.fq}{comment} AS\n{select}"
    return f"CREATE OR REPLACE TABLE {spec.target.fq} USING DELTA AS\n{select}"


class ViewManager:
    """Create and verify Spark views (and Spark-derived Delta tables)."""

    def __init__(self, spark: Any = None) -> None:
        self.spark = spark or get_spark()

    def create(self, spec: ViewSpec) -> Optional[int]:
        """Create (or replace) the object, verify it resolves, return rows for derived tables."""
        sql_text = build_create_view_sql(spec)                 # validate before touching anything
        spec.target.preflight(self.spark, ensure_schema=True)
        self.spark.sql(sql_text)
        self.spark.sql(f"SELECT * FROM {spec.target.fq} LIMIT 0").columns   # forces analysis
        if spec.view_type != "SQL_VIEW":
            return int(self.spark.table(spec.target.fq).count())
        return None

# %% [markdown]
# ## 9. Task entry points
# The orchestrator calls exactly one of these functions per table / view / change. Each one re-reads the
# configuration just before acting, verifies the plan / row status and never raises: every outcome is an
# `ObjectResult`.

# %%
def _check_new_row(spec: Any, plan_guid: str) -> None:
    """Defence in depth for CREATE_NEW: the row must belong to the plan and still be waiting."""
    if spec.plan_guid != norm_guid(plan_guid):
        raise ValidationError("Object does not belong to the requested plan.")
    if not spec.is_active or spec.status not in NEW_ROW_STATUSES:
        raise ValidationError(f"Object is not waiting for provisioning (status {spec.status}).")


def _require_guids(*guids: str) -> None:
    if not all(is_valid_uuid(g) for g in guids):
        raise ValidationError("plan_guid and object_guid must be valid GUIDs.")


def run_table_provision(plan_guid: str, object_guid: str, run_id: str = "",
                        repo_factory: Callable[[], Any] = default_repository,
                        manager: Optional[DeltaTableManager] = None) -> ObjectResult:
    """
    CREATE_NEW for one table. If the table already exists with the *same* schema this is a
    successful no-op (safe retry; see CREATE_NEW_ALLOWS_IDENTICAL_EXISTING); a different
    physical schema fails without any DDL.
    """
    name_ref = {"name": str(object_guid)}

    def action() -> Tuple[str, Optional[int]]:
        _require_guids(plan_guid, object_guid)
        with repo_factory() as repo:
            spec = repo.get_table_spec(norm_guid(object_guid))
        name_ref["name"] = spec.target.display
        _check_new_row(spec, plan_guid)
        mgr = manager or DeltaTableManager()
        LOG.info("[%s] create table %s", run_id, spec.target.display)
        if mgr.exists(spec.target):
            if CREATE_NEW_ALLOWS_IDENTICAL_EXISTING and plan_schema_changes(
                    mgr.physical_columns(spec.target), spec.columns).is_empty:
                return "Table already exists; schema validated", None
            raise TargetStateError("Target table already exists" +
                                   (" with a different schema." if CREATE_NEW_ALLOWS_IDENTICAL_EXISTING else "."))
        mgr.create(spec)
        mgr.verify(spec)
        return "Table created; schema validated", 0

    return run_timed(ObjectType.TABLE, name_ref, action)


def run_view_provision(plan_guid: str, object_guid: str, run_id: str = "",
                       repo_factory: Callable[[], Any] = default_repository,
                       manager: Optional[ViewManager] = None) -> ObjectResult:
    """CREATE_NEW for one view (CREATE OR REPLACE VIEW, so a retry is safe)."""
    name_ref = {"name": str(object_guid)}

    def action() -> Tuple[str, Optional[int]]:
        _require_guids(plan_guid, object_guid)
        with repo_factory() as repo:
            spec = repo.get_view_spec(norm_guid(object_guid))
        name_ref["name"] = spec.target.display
        _check_new_row(spec, plan_guid)
        LOG.info("[%s] create view %s", run_id, spec.target.display)
        rows = (manager or ViewManager()).create(spec)
        return "View created and resolved" if spec.view_type == "SQL_VIEW" else "Derived table created", rows

    return run_timed(ObjectType.VIEW, name_ref, action)


def _revise_planned(change: TargetChange, repo_factory: Callable[[], Any], mgr: DeltaTableManager) -> Tuple[str, Optional[int]]:
    """REVISE_PLANNED: rewrite the saved configuration of a PENDING record; no physical table may exist."""
    with repo_factory() as repo:
        existing = repo.get_table_spec(change.existing_guid)
        existing_rows = repo.get_column_rows(change.existing_guid)
    if existing.status != "PENDING" or not existing.is_active:
        raise TargetStateError("REVISE_PLANNED requires an active record in PENDING status.")
    if mgr.exists(existing.target):
        raise TargetStateError("REVISE_PLANNED requires that no physical table exists yet.")
    check_against_approval(compare_definitions(existing.columns, change.columns), change.approved_comparison, required=True)
    with repo_factory() as repo:
        repo.apply_metadata_change(existing.guid, "PENDING", build_column_insert_rows(change.columns, existing_rows),
                                   replication_updates(change.replication))
        if "ingestion_flag" in change.replication:
            repo.set_ingestion_flag(existing.guid, bool(change.replication["ingestion_flag"]))
    return "Planned configuration revised; no physical table created", None


def _alter_future(change: TargetChange, repo_factory: Callable[[], Any], mgr: DeltaTableManager) -> Tuple[str, Optional[int]]:
    """
    ALTER_FUTURE: pause ingestion -> re-read state -> verify baseline -> alter Delta -> verify ->
    commit Config DB -> resume ingestion. Historical rows and the watermark stay unchanged.

    * Baseline rule: the physical schema must equal the RECORDED schema. If it already equals the
      PROPOSED schema (Delta committed, Config DB did not) the run resumes at the metadata step.
    * Failure before anything changed restores the original ingestion flag; failure after a change
      leaves ingestion paused for reconciliation. A retry cannot know whether a paused flag was
      originally on (there is no durable run record), so it never re-enables it by itself.
    """
    guid = change.existing_guid
    with repo_factory() as repo:
        config = repo.get_replication_config(guid)
    original_flag = bool(config["IngestionFlag"]) if config else None
    paused = mutated = False
    try:
        if original_flag:
            with repo_factory() as repo:
                repo.set_ingestion_flag(guid, False)
            paused = True

        with repo_factory() as repo:                                  # re-read AFTER the pause
            existing = repo.get_table_spec(guid)
            existing_rows = repo.get_column_rows(guid)
        if existing.status != "PROVISIONED" or not existing.is_active:
            raise TargetStateError("ALTER_FUTURE requires an active PROVISIONED record.")
        if not mgr.exists(existing.target):
            raise TargetStateError("ALTER_FUTURE requires the physical table to exist.")
        physical = mgr.physical_columns(existing.target)
        recorded, proposed = existing.columns, change.columns
        if not (structure_matches(physical, recorded) or structure_matches(physical, proposed)):
            raise TargetStateError("Physical schema differs from the recorded baseline; request a fresh review.")

        difference = compare_definitions(recorded, proposed)
        if any(difference.values()):                                  # metadata not yet updated
            check_against_approval(difference, change.approved_comparison, required=True)
            check_key_stability(recorded, proposed, config, change.replication)
        target_spec = replace(existing, columns=tuple(proposed))
        ddl = plan_schema_changes(physical, proposed)
        if ddl.rejected:
            raise UnsafeChangeError("Unsafe change rejected, nothing applied: " + "; ".join(ddl.rejected))
        if not ddl.is_empty:
            mutated = True
            mgr.apply(target_spec, ddl)
        mgr.verify(target_spec)

        mutated = True                                                # from here ingestion stays paused on failure
        with repo_factory() as repo:
            repo.apply_metadata_change(guid, "PROVISIONED", build_column_insert_rows(proposed, existing_rows),
                                       replication_updates(change.replication))
            final_flag = original_flag  # The proposal uses False as a staging default, not a user pause request.
            if final_flag is not None:
                repo.set_ingestion_flag(guid, final_flag)
        message = (f"Schema altered ({ddl.summary()}); Config DB updated" if not ddl.is_empty
                   else "Schema already up to date; Config DB synchronised")
        if final_flag is False:
            message += "; ingestion remains paused until resumed"
        return message, None
    except Exception as exc:
        if paused and not mutated:
            try:
                with repo_factory() as repo:
                    repo.set_ingestion_flag(guid, True)
            except Exception:  # noqa: BLE001
                LOG.error("Could not restore the ingestion flag after a failed pre-check.")
            raise
        if paused:
            raise TargetStateError(f"{safe_message(exc)}; ingestion left paused for reconciliation") from exc
        raise


def _data_change(change: TargetChange, plan_guid: str, repo_factory: Callable[[], Any],
                 mgr: DeltaTableManager, mover: Optional[DeltaDataMover] = None,
                 snapshot_factory: Callable[[Dict[str, Any]], OracleSnapshot] = OracleSnapshot) -> Tuple[str, Optional[int]]:
    """Execute an approved Oracle backfill or full replacement under a source SHARE lock."""
    guid = change.existing_guid
    with repo_factory() as repo:
        existing = repo.get_table_spec(guid)
        previous_rows = repo.get_column_rows(guid)
        config = repo.get_replication_config(guid)
        source = repo.get_oracle_source(guid)
    if existing.status != 'PROVISIONED' or not existing.is_active or config is None:
        raise TargetStateError('Data movement requires an active PROVISIONED target and replication config.')
    proposed_source = change.proposed_table
    for key, live in (('connection_name', source['ConnectionName']),
                      ('source_schema_name', source['SourceSchemaName']),
                      ('source_table_name', source['SourceTableName'])):
        if str(proposed_source.get(key) or '').upper() != str(live).upper():
            raise ValidationError('The approved Oracle source differs from the existing target source.')
    if not mgr.exists(existing.target):
        raise TargetStateError('Data movement requires the physical target table.')
    physical = mgr.physical_columns(existing.target)
    if not structure_matches(physical, existing.columns):
        raise TargetStateError('Physical target schema differs from the recorded baseline.')
    difference = compare_definitions(existing.columns, change.columns)
    check_against_approval(difference, change.approved_comparison, required=True)
    if not any(difference.values()):
        raise ValidationError('The approved target change contains no column difference.')
    current_mode = (str(config.get('IncrementalMethod') or '').upper(),
                    str(config.get('WriteStrategy') or '').upper())
    proposed_mode = (str(change.replication.get('incremental_method') or '').upper(),
                     str(change.replication.get('write_strategy') or '').upper())
    if current_mode != proposed_mode or current_mode not in {('FULL', 'REPLACE'), ('WATERMARK', 'UPSERT')}:
        raise ValidationError('Data movement requires unchanged FULL/REPLACE or WATERMARK/UPSERT replication settings.')
    keys: List[str] = []
    ddl: Optional[SchemaDiff] = None
    if change.decision == PlanAction.ALTER_BACKFILL:
        check_key_stability(existing.columns, change.columns, config, change.replication)
        raw_keys = change.replication.get('merge_key_columns') or change.replication.get('primary_key_columns')
        keys = sorted(_json_name_set(raw_keys) or [])
        names = {c.name.lower() for c in change.columns}
        if not keys or any(k.lower() not in names for k in keys):
            raise ValidationError('ALTER_BACKFILL needs a stable approved merge or primary key.')
        if current_mode != ('WATERMARK', 'UPSERT'):
            raise ValidationError('ALTER_BACKFILL requires existing WATERMARK/UPSERT replication.')
        ddl = plan_schema_changes(physical, change.columns)
        if ddl.rejected:
            raise UnsafeChangeError('Unsafe in-place change: ' + '; '.join(ddl.rejected))
    else:
        if change.decision != PlanAction.REPLACE_FULL:
            raise ValidationError('Unknown data change decision.')
    for col in change.columns:
        validate_oracle_identifier(col.source_name, 'Oracle column')
        if col.source_expression:
            raise ValidationError('Data movement does not support source expressions; approve plain Oracle columns.')

    stage = staging_target(existing.target, plan_guid)
    mover = mover or DeltaDataMover(mgr)
    original_flag = bool(config['IngestionFlag'])
    paused = mutation_started = False
    try:
        if original_flag:
            with repo_factory() as repo:
                repo.set_ingestion_flag(guid, False)
            paused = True
        with repo_factory() as repo:
            repo.assert_replication_idle(guid)
            repo.begin_change_run(plan_guid, guid, change.decision, stage.name)
        with snapshot_factory(source) as snapshot:
            rows = mover.stage(stage, change.columns, snapshot)
            if keys:
                mover.validate_keys(stage, keys)
                mover.validate_target_keys(existing.target, keys)
                mover.validate_target_subset(existing.target, stage, keys)
            with repo_factory() as repo:
                repo.mark_change_run(plan_guid, 'STAGED', snapshot_scn=snapshot.scn, source_rows=rows)
                # Mark before the first physical mutation. An uncertain retry then stops for reconciliation.
                repo.mark_change_run(plan_guid, 'MUTATION_STARTED')
            mutation_started = True
            target_spec = replace(existing, columns=tuple(change.columns))
            if change.decision == PlanAction.ALTER_BACKFILL:
                if ddl and not ddl.is_empty:
                    mgr.apply(target_spec, ddl)
                mgr.verify(target_spec)
                mover.merge(existing.target, stage, change.columns, keys)
                mgr.verify(target_spec)
                mover.verify_data(existing.target, stage, change.columns, replace_all=False)
            else:
                mover.replace(existing.target, stage, change.columns)
                mgr.verify(target_spec)
                mover.verify_data(existing.target, stage, change.columns, replace_all=True)
            with repo_factory() as repo:
                repo.apply_metadata_change(
                    guid, 'PROVISIONED', build_column_insert_rows(change.columns, previous_rows),
                    replication_updates(change.replication), complete_run_guid=plan_guid,
                    resume_ingestion=original_flag)
        message = (f'{change.decision} completed from a locked Oracle snapshot; {rows} rows staged; '
                   'previous watermark retained for safe replay')
        if not original_flag:
            message += '; ingestion remains paused'
        return message, rows
    except Exception as exc:
        if paused and not mutation_started:
            try:
                with repo_factory() as repo:
                    repo.set_ingestion_flag(guid, True)
            except Exception:
                LOG.error('Could not restore ingestion after a failed data-change preflight.')
        if mutation_started:
            raise TargetStateError(f'{safe_message(exc)}; target may have changed; ingestion left paused '
                                   'for reconciliation') from exc
        raise


def run_table_change(plan_guid: str, decision: str, run_id: str = "",
                     repo_factory: Callable[[], Any] = default_repository,
                     manager: Optional[DeltaTableManager] = None,
                     lease_factory: Callable[[str], Any] = TargetLease) -> ObjectResult:
    """
    Execute the approved decision of a CHANGE_REQUESTED plan against the EXISTING target.

    The decision, the target and the proposed definition are read from the plan's Notes (the
    ``decision`` argument is only cross-checked). The Oracle data-movement branches
    require an active source connection and the TargetChangeRuns audit table.
    """
    name_ref = {"name": str(plan_guid)}

    def action() -> Tuple[str, Optional[int]]:
        if not is_valid_uuid(plan_guid):
            raise ValidationError("plan_guid must be a valid GUID.")
        with repo_factory() as repo:
            plan = repo.get_plan(norm_guid(plan_guid))
            if plan is None or plan.status != CHANGE_PLAN_STATUS:
                raise ValidationError("Plan is not in CHANGE_REQUESTED status.")
            change = parse_target_change(plan.notes)
            existing = repo.get_table_spec(change.existing_guid)
        name_ref["name"] = change.display
        if normalize_enum(decision) != change.decision:
            raise ValidationError("Requested decision differs from the decision in the plan Notes.")
        verify_target_agreement(change, existing.target)
        mgr = manager or DeltaTableManager()
        t = existing.target
        LOG.info("[%s] %s on %s", run_id, change.decision, change.display)
        with lease_factory(f"{t.workspace_id}/{t.lakehouse}/{t.schema}.{t.name}"):
            if change.decision == PlanAction.REVISE_PLANNED:
                return _revise_planned(change, repo_factory, mgr)
            if change.decision in (PlanAction.ALTER_BACKFILL, PlanAction.REPLACE_FULL):
                return _data_change(change, plan_guid, repo_factory, mgr)
            return _alter_future(change, repo_factory, mgr)

    return run_timed(ObjectType.TABLE, name_ref, action)

# %% [markdown]
# Part 3 - Orchestration
# Decides the action from the plan, plans dependency waves and runs them. Nothing runs until the Run cell.
#

# %% [markdown]
# ## Orchestration helpers (pure functions, unit-tested)

# %%
@dataclass(frozen=True)
class RunRequest:
    """The decided action for this run."""
    plan_guid: str
    action: str                              # PlanAction.*
    creation_request: str                    # TABLE | VIEW | BOTH
    change: Optional[TargetChange] = None    # only for change plans

    @property
    def include_tables(self) -> bool:
        return self.creation_request in (CreationRequest.TABLE, CreationRequest.BOTH)

    @property
    def include_views(self) -> bool:
        return self.creation_request in (CreationRequest.VIEW, CreationRequest.BOTH)

    def success_message(self) -> str:
        return "Provisioning completed" if self.action == PlanAction.CREATE_NEW \
            else f"Target change {self.action} completed"


def resolve_run_request(plan_guid: str, creation_request: Any, plan: PlanInfo) -> RunRequest:
    """Decide the action from the plan Status / Notes. Invalid or unapproved plans raise ValidationError."""
    creation = normalize_enum(creation_request) or CreationRequest.BOTH
    creation = CreationRequest.ALIASES.get(creation, creation)
    if creation not in CreationRequest.VALUES:
        raise ValidationError(f"CREATIONREQUEST must be one of {list(CreationRequest.VALUES)}.")

    if plan.status in NEW_PLAN_STATUSES:
        return RunRequest(norm_guid(plan_guid), PlanAction.CREATE_NEW, creation)
    if plan.status == CHANGE_PLAN_STATUS:
        change = parse_target_change(plan.notes)
        if creation == CreationRequest.VIEW:
            raise ValidationError("A target change applies to a table; CREATIONREQUEST must be TABLE or BOTH.")
        return RunRequest(norm_guid(plan_guid), change.decision, creation, change)
    raise ValidationError(f"Plan is not approved for notebook execution (status {plan.status}).")


def build_dependency_graph(tables: Sequence[ObjectRef], views: Sequence[ObjectRef],
                           view_deps: Sequence[Dict[str, Any]]) -> Dict[str, Set[str]]:
    """
    Return ``{object_guid: {guids that must finish first}}`` for the objects of a CREATE_NEW run.

    Every view waits for all tables, for all views with a lower CreationOrder and for the views it
    declares as dependencies (``FabricViewDependencies`` with type FABRIC_VIEW).
    """
    deps: Dict[str, Set[str]] = {o.guid: set() for o in list(tables) + list(views)}
    table_keys = {t.guid for t in tables}
    view_keys = {v.guid for v in views}
    for v in views:
        deps[v.guid] |= table_keys
        deps[v.guid] |= {o.guid for o in views if o.creation_order < v.creation_order}
    for d in view_deps:
        if d["type"] == "FABRIC_VIEW" and d["view"] in view_keys and d["depends_on_view"] in view_keys \
                and d["view"] != d["depends_on_view"]:
            deps[d["view"]].add(d["depends_on_view"])
    return deps


def topological_levels(deps: Dict[str, Set[str]]) -> List[List[str]]:
    """Group objects into waves; objects inside one wave are independent and run in parallel."""
    remaining = {k: set(v) & set(deps) for k, v in deps.items()}
    levels: List[List[str]] = []
    while remaining:
        ready = sorted(k for k, v in remaining.items() if not v)
        if not ready:
            raise ValidationError("Circular dependency between the objects of this plan "
                                  "(check CreationOrder and FabricViewDependencies).")
        levels.append(ready)
        for k in ready:
            del remaining[k]
        for v in remaining.values():
            v.difference_update(ready)
    return levels


@dataclass
class WorkNode:
    """One task planned by the orchestrator (a table, a view or a change)."""
    ref: ObjectRef
    task: str
    args: Dict[str, str]
    prerequisites: Set[str] = field(default_factory=set)


def plan_task(request: RunRequest, ref: ObjectRef) -> Tuple[str, Dict[str, str]]:
    """Return ``(task, extra args)`` for one object."""
    if request.action != PlanAction.CREATE_NEW:
        return TASK_TABLE_CHANGE, {"decision": request.action}
    return (TASK_TABLE_PROVISION if ref.object_type == ObjectType.TABLE else TASK_VIEW_PROVISION), {}


def execute_task(node: WorkNode) -> ObjectResult:
    """Run one planned task in this session. The ``run_*`` functions never raise."""
    a = node.args
    if node.task == TASK_TABLE_PROVISION:
        return run_table_provision(a["plan_guid"], a["object_guid"], a["run_id"])
    if node.task == TASK_VIEW_PROVISION:
        return run_view_provision(a["plan_guid"], a["object_guid"], a["run_id"])
    if node.task == TASK_TABLE_CHANGE:
        return run_table_change(a["plan_guid"], a["decision"], a["run_id"])
    raise ValidationError(f"Unknown task: {node.task}")

# %% [markdown]
# ## MasterOrchestrator

# %%
class MasterOrchestrator:
    """
    Runs one request end to end. ``run()`` never raises and always returns the report
    dictionary that is exited to the API. Dependencies (repository factory, task
    executor) are injected so the class can be tested without Fabric.
    """

    def __init__(self, plan_guid: Any, creation_request: Any,
                 repo_factory: Callable[[], Any] = default_repository,
                 executor: Optional[Callable[[WorkNode], ObjectResult]] = None) -> None:
        self.plan_guid_raw = normalize_text(plan_guid) or ""
        self._creation = creation_request
        self._repo_factory = repo_factory
        self._executor = executor or execute_task
        self.run_id = str(uuid.uuid4())
        self.results: List[ObjectResult] = []
        self.request: Optional[RunRequest] = None
        self.failure: Optional[str] = None
        self._levels: List[List[str]] = []
        self._t0 = time.monotonic()

    # -- public ---------------------------------------------------------------
    def run(self) -> Dict[str, Any]:
        try:
            self._validate_plan_guid()
            nodes = self._prepare()
            self._execute(nodes)
        except MigrationError as exc:
            self.failure = safe_message(exc)
        except Exception as exc:  # noqa: BLE001 - the API must always get a JSON answer
            self.failure = "Unexpected error: " + safe_message(exc)
        if self.failure:
            LOG.error("Run failed: %s", self.failure)
        return self._report()

    # -- steps ----------------------------------------------------------------
    def _validate_plan_guid(self) -> None:
        if not is_valid_uuid(self.plan_guid_raw):
            raise ValidationError("plan_guid is missing or is not a valid GUID.")

    def _prepare(self) -> Dict[str, WorkNode]:
        """Read Config DB: decide the action, collect the work, plan the waves."""
        with self._repo_factory() as repo:
            plan = repo.get_plan(norm_guid(self.plan_guid_raw))
            if plan is None:
                raise ValidationError("Plan was not found in Config DB.")
            self.request = req = resolve_run_request(self.plan_guid_raw, self._creation, plan)
            LOG.info("Run %s | plan status %s -> %s | %s", self.run_id, plan.status, req.action, req.creation_request)
            if req.action == PlanAction.CREATE_NEW:
                tables = repo.list_pending_tables(req.plan_guid) if req.include_tables else []
                views = repo.list_pending_views(req.plan_guid) if req.include_views else []
                view_deps = repo.get_view_dependencies([v.guid for v in views])
            else:
                tables = [ObjectRef(ObjectType.TABLE, req.change.existing_guid, req.change.display)]
                views, view_deps = [], []
        if not tables and not views:
            raise ValidationError("No objects waiting for provisioning were found for this plan and CREATIONREQUEST.")

        deps = build_dependency_graph(tables, views, view_deps)
        self._levels = topological_levels(deps)
        nodes: Dict[str, WorkNode] = {}
        for ref in list(tables) + list(views):
            task, extra = plan_task(req, ref)
            args = {"plan_guid": req.plan_guid, "run_id": self.run_id, **extra}
            if req.action == PlanAction.CREATE_NEW:
                args["object_guid"] = ref.guid
            nodes[ref.guid] = WorkNode(ref, task, args, deps[ref.guid])
        return nodes

    def _execute(self, planned: Dict[str, WorkNode]) -> None:
        """Run the waves in order; skip (and fail) dependants of failed objects."""
        nodes = planned
        failed: Dict[str, str] = {}
        for number, level in enumerate(self._levels, start=1):
            runnable: List[WorkNode] = []
            for key in level:
                node = nodes[key]
                blocked_by = [p for p in node.prerequisites if p in failed]
                if blocked_by:
                    failed[key] = node.ref.name
                    self.results.append(ObjectResult(
                        node.ref.object_type, node.ref.name, Status.FAILED, 0.0,
                        message=safe_message(f"Not run: depends on failed object {failed[blocked_by[0]]}")))
                else:
                    runnable.append(node)
            if not runnable:
                continue
            LOG.info("Wave %d/%d: %d task(s)", number, len(self._levels), len(runnable))
            for node, result in zip(runnable, self._run_wave(runnable)):
                self.results.append(result)
                if result.status != Status.SUCCESS:
                    failed[node.ref.guid] = node.ref.name

    def _safe_execute(self, node: WorkNode) -> ObjectResult:
        """Run one task; whatever happens, return an ObjectResult carrying the planned type and name."""
        try:
            result = self._executor(node)
        except Exception as exc:  # noqa: BLE001 - one task must never break the others
            result = ObjectResult(node.ref.object_type, node.ref.name, Status.FAILED, 0.0, message=safe_message(exc))
        result.object_type, result.name = node.ref.object_type, node.ref.name
        return result

    def _run_wave(self, nodes: List[WorkNode]) -> List[ObjectResult]:
        """
        Run independent tasks in parallel threads (one shared Spark session) and return the results
        in input order. A wave that exceeds WAVE_TIMEOUT_SECONDS marks its unfinished tasks FAILED
        (their threads cannot be killed and may still finish; every operation is idempotent).
        """
        results: Dict[str, ObjectResult] = {}
        pool = ThreadPoolExecutor(max_workers=max(1, min(len(nodes), MAX_PARALLEL_TASKS)), thread_name_prefix="task")
        try:
            futures = {pool.submit(self._safe_execute, n): n for n in nodes}
            done, pending = wait(futures, timeout=WAVE_TIMEOUT_SECONDS)
            for future in done:
                results[futures[future].ref.guid] = future.result()
            for future in pending:
                node = futures[future]
                future.cancel()
                results[node.ref.guid] = ObjectResult(
                    node.ref.object_type, node.ref.name, Status.FAILED, float(WAVE_TIMEOUT_SECONDS),
                    message=f"Timed out after {WAVE_TIMEOUT_SECONDS} seconds; the operation may still be running")
        finally:
            pool.shutdown(wait=False)
        return [results[n.ref.guid] for n in nodes]

    # -- report ---------------------------------------------------------------
    def _report(self) -> Dict[str, Any]:
        ok = [r for r in self.results if r.status == Status.SUCCESS]
        bad = [r for r in self.results if r.status != Status.SUCCESS]
        success = bool(self.results) and not bad and not self.failure
        if success:
            message = self.request.success_message()
        elif self.failure:
            message = self.failure
        else:
            message = safe_message(f"{len(bad)} of {len(self.results)} object(s) failed; "
                                   f"first: {bad[0].name}: {bad[0].message}")
        return {
            "status": Status.SUCCESS if success else Status.FAILED,
            "plan_guid": self.plan_guid_raw,
            "tables_processed": sum(1 for r in ok if r.object_type == ObjectType.TABLE),
            "views_processed": sum(1 for r in ok if r.object_type == ObjectType.VIEW),
            "duration_seconds": round(time.monotonic() - self._t0, 2),
            "objects": [r.to_report() for r in self.results],
            "message": message,
        }

# %% [markdown]
# Part 4 - Self-tests
# `run_self_tests()` - only executed with RUN_MODE = TEST.
#

# %%
def run_self_tests(verbosity: int = 2) -> Dict[str, Any]:
    """
    Offline unit tests of the logic in this notebook (fakes for Spark and Config DB: nothing is created,
    altered or read from the Lakehouse / SQL database). Run with RUN_MODE = "TEST".
    The tests live inside this function so their helper names never leak into the notebook namespace.
    """
    import json
    import logging
    import sys
    import types
    import unittest
    from contextlib import contextmanager

    NS = globals()                                # the live notebook namespace (the code under test)
    g = types.SimpleNamespace(**NS)               # attribute access to everything defined above

    P = "11111111-1111-1111-1111-111111111111"
    T1, T2 = "aaaaaaaa-0000-0000-0000-000000000001", "aaaaaaaa-0000-0000-0000-000000000002"
    V1, V2 = "bbbbbbbb-0000-0000-0000-000000000001", "bbbbbbbb-0000-0000-0000-000000000002"


    def col(name, typ="STRING", nullable=True, known=True, desc=None, sno=1, pk=False):
        return g.ColumnDef(sno, name, name, typ, nullable, known, pk, desc)


    def phys(name, typ="STRING", nullable=True, comment=None):
        return g.PhysicalColumn(name, typ, nullable, comment)


    class TypeTests(unittest.TestCase):
        def test_normalize(self):
            n = g.normalize_fabric_type
            self.assertEqual(n("varchar(100)"), "STRING")
            self.assertEqual(n("DECIMAL(18, 0)"), "DECIMAL(18,0)")
            self.assertEqual(n("numeric"), "DECIMAL(10,0)")
            self.assertEqual(n("integer"), "INT")
            self.assertEqual(n("datetime"), "TIMESTAMP")
            with self.assertRaises(g.UnsupportedTypeError):
                n("GEOGRAPHY")
            with self.assertRaises(g.UnsupportedTypeError):
                n("DECIMAL(40,2)")

        def test_widening(self):
            w = g.is_safe_widening
            self.assertTrue(w("INT", "BIGINT"))
            self.assertFalse(w("BIGINT", "INT"))
            self.assertTrue(w("FLOAT", "DOUBLE"))
            self.assertTrue(w("DECIMAL(10,2)", "DECIMAL(18,4)"))
            self.assertFalse(w("DECIMAL(10,2)", "DECIMAL(12,0)"))   # scale shrinks
            self.assertFalse(w("DECIMAL(18,2)", "DECIMAL(18,4)"))   # integer digits shrink
            self.assertFalse(w("STRING", "INT"))


    class DiffTests(unittest.TestCase):
        def test_safe_changes(self):
            d = g.plan_schema_changes(
                [phys("ID", "INT", False), phys("NAME")],
                [col("id", "BIGINT", False), col("NAME", desc="Customer name"), col("NEWCOL", "DATE")])
            self.assertEqual([c.name for c in d.added], ["NEWCOL"])
            self.assertEqual(len(d.widened), 1)
            self.assertEqual(len(d.comments), 1)
            self.assertFalse(d.rejected)

        def test_rejections(self):
            d = g.plan_schema_changes([phys("A"), phys("B", "BIGINT"), phys("C")],
                                      [col("A"), col("B", "INT"), col("D", nullable=False)])
            self.assertEqual(len(d.rejected), 3)  # C removed, B narrowed, D NOT NULL add

        def test_nullability(self):
            d = g.plan_schema_changes([phys("A", nullable=False), phys("B")],
                                      [col("A", nullable=True), col("B", nullable=False)])
            self.assertEqual(len(d.relaxed), 1)
            self.assertEqual(len(d.rejected), 1)
            unknown = g.plan_schema_changes([phys("A", nullable=False)], [col("A", nullable=True, known=False)])
            self.assertTrue(unknown.is_empty)

        def test_identical_is_empty(self):
            self.assertTrue(g.plan_schema_changes([phys("A")], [col("a")]).is_empty)


    class SqlTests(unittest.TestCase):
        def target(self):
            return g.LakehouseTarget("ws", "lh", "bronze", "LOCAL_ORACLE", "ORDERS")

        def test_create_sql(self):
            spec = g.TableSpec("g", P, "PENDING", True, self.target(), (
                col("ID", "DECIMAL(18,0)", False, sno=1), col("NOTE", desc="it's \"x\"", sno=2)))
            sql = g.build_create_table_sql(spec)
            self.assertIn("`bronze`.`LOCAL_ORACLE`.`ORDERS`", sql)
            self.assertIn("`ID` DECIMAL(18,0) NOT NULL", sql)
            self.assertIn("COMMENT 'it\\'s \"x\"'", sql)
            self.assertTrue(g.build_create_table_sql(spec, True).startswith("CREATE OR REPLACE TABLE"))

        def test_identifier_injection(self):
            for bad in ("a`b", "a b", "a;drop", "", "x.y"):
                with self.assertRaises(g.ValidationError):
                    g.validate_identifier(bad, "x")

        def test_view_sql(self):
            self.assertEqual(g.prepare_select("  SELECT 1;  "), "SELECT 1")
            self.assertEqual(g.prepare_select("-- c\nWITH a AS (SELECT 1) SELECT * FROM a"),
                             "-- c\nWITH a AS (SELECT 1) SELECT * FROM a")
            for bad in ("", "CREATE VIEW v AS SELECT 1", "SELECT 1; DROP TABLE x", "DROP TABLE x"):
                with self.assertRaises(g.ValidationError):
                    g.prepare_select(bad)

        def test_safe_message(self):
            m = g.safe_message(RuntimeError("fail password=abc123 token: xyz\n" + "x" * 500))
            self.assertNotIn("abc123", m); self.assertNotIn("xyz", m)
            self.assertLessEqual(len(m), 300); self.assertNotIn("\n", m)



    def notes(decision="ALTER_FUTURE", cols=None, replication=None, comparison=None, guid=T1, target=None, same=False):
        cols = cols if cols is not None else [
            {"sno": 1, "column_name": "ID", "source_data_type": "NUMBER", "fabric_data_type": "INT", "is_nullable": False, "is_primary_key": True},
            {"sno": 2, "column_name": "NAME", "source_data_type": "VARCHAR2", "fabric_data_type": "STRING", "is_nullable": True},
            {"sno": 3, "column_name": "EXTRA", "source_data_type": "DATE", "fabric_data_type": "DATE", "is_nullable": True}]
        tgt = target or {"workspace_id": "ws", "lakehouse_id": "lh", "schema": "S", "table": "T"}
        comparison = comparison if comparison is not None else {"same": same, "added": ["EXTRA"], "removed": [], "changed": []}
        return json.dumps({"target_change": {
            "decision": decision, "existing_source_table_guid": guid,
            "review": {"target": tgt, "definitions": [{"source_table_guid": guid, "comparison": comparison}]},
            "proposed_plan": {"table": {"fabric_workspace_id": "ws", "fabric_lakehouse_id": "lh",
                                        "fabric_lakehouse_schema": "S", "fabric_table_name": "T"},
                              "columns": cols, "replication": replication if replication is not None else {}}}})


    class RequestTests(unittest.TestCase):
        def plan(self, status, n=None):
            return g.PlanInfo(P, status, 1, n)

        def test_new_plan(self):
            for st in ("PROVISIONING", "READY_TO_PROVISION"):
                r = g.resolve_run_request(P, "", self.plan(st))
                self.assertEqual((r.action, r.creation_request), ("CREATE_NEW", "BOTH"))
            self.assertEqual(g.resolve_run_request(P, "all", self.plan("PROVISIONING")).creation_request, "BOTH")
            self.assertEqual(g.resolve_run_request(P, "table", self.plan("PROVISIONING")).creation_request, "TABLE")

        def test_change_plan_decisions(self):
            for d in ("REVISE_PLANNED", "ALTER_FUTURE", "ALTER_BACKFILL", "REPLACE_FULL"):
                r = g.resolve_run_request(P, "BOTH", self.plan("CHANGE_REQUESTED", notes(d)))
                self.assertEqual(r.action, d)

        def test_rejections(self):
            for st in ("APPROVED", "PROVISIONED", "REJECTED", "DRAFT", "FAILED"):
                with self.assertRaises(g.ValidationError, msg=st):
                    g.resolve_run_request(P, "BOTH", self.plan(st))
            with self.assertRaises(g.ValidationError):
                g.resolve_run_request(P, "OTHER", self.plan("PROVISIONING"))
            with self.assertRaises(g.ValidationError):
                g.resolve_run_request(P, "VIEW", self.plan("CHANGE_REQUESTED", notes()))   # change = table only
            for bad in (None, "", "{}", "not json", notes("DROP_IT"), notes(guid="x")):
                with self.assertRaises(g.ValidationError, msg=str(bad)[:30]):
                    g.resolve_run_request(P, "BOTH", self.plan("CHANGE_REQUESTED", bad))


    class TargetChangeTests(unittest.TestCase):
        def test_parse(self):
            c = g.parse_target_change(notes(replication={"write_strategy": "UPSERT", "ingestion_flag": False}))
            self.assertEqual((c.decision, c.existing_guid, len(c.columns)), ("ALTER_FUTURE", T1, 3))
            self.assertEqual(c.display, "S.T")
            self.assertIsNotNone(c.approved_comparison)
            self.assertEqual(g.replication_updates(c.replication), {"WriteStrategy": "UPSERT"})   # ingestion_flag excluded

        def test_unselected_and_duplicates(self):
            cols = [{"sno": 1, "column_name": "A", "fabric_data_type": "INT"},
                    {"sno": 2, "column_name": "B", "fabric_data_type": "INT", "is_selected": False}]
            self.assertEqual(len(g.parse_target_change(notes(cols=cols)).columns), 1)
            dup = [{"sno": 1, "column_name": "A", "fabric_data_type": "INT"}, {"sno": 2, "column_name": "a", "fabric_data_type": "INT"}]
            with self.assertRaises(g.ValidationError):
                g.parse_target_change(notes(cols=dup))

        def test_target_agreement(self):
            c = g.parse_target_change(notes())
            g.verify_target_agreement(c, g.LakehouseTarget("WS", "LH", "lakehouse", "s", "t"))   # case-insensitive
            with self.assertRaises(g.ValidationError):
                g.verify_target_agreement(c, g.LakehouseTarget("ws", "other", "lakehouse", "S", "T"))
            with self.assertRaises(g.ValidationError):
                g.verify_target_agreement(c, g.LakehouseTarget("ws", "lh", "lakehouse", "S", "OTHER"))

        def test_approval_check(self):
            rec = [col("ID", "INT", False, sno=1, pk=True), col("NAME", sno=2)]
            prop = [col("ID", "INT", False, sno=1, pk=True), col("NAME", sno=2), col("EXTRA", "DATE", sno=3)]
            diff = g.compare_definitions(rec, prop)
            self.assertEqual(diff["added"], {"extra"})
            ok = {"same": False, "added": [{"column_name": "EXTRA"}], "removed": [], "changed": []}
            g.check_against_approval(diff, ok, True)
            for bad in ({"same": False, "added": [], "removed": [], "changed": []},
                        {"same": True, "added": ["EXTRA"], "removed": [], "changed": []},
                        {"same": False, "added": ["EXTRA", "OTHER"], "removed": [], "changed": []}):
                with self.assertRaises(g.ValidationError):
                    g.check_against_approval(diff, bad, True)
            with self.assertRaises(g.ValidationError):
                g.check_against_approval(diff, None, True)
            g.check_against_approval(diff, None, False)             # lenient mode
            with self.assertRaises(g.ValidationError):
                g.check_against_approval(diff, {"same": False, "added": ["EXTRA"], "removed": [], "changed": [{"x": 1}]}, True)

        def test_unapproved_change(self):
            rec = [col("A", "INT")]; prop = [col("A", "BIGINT")]
            diff = g.compare_definitions(rec, prop)
            with self.assertRaises(g.ValidationError):
                g.check_against_approval(diff, {"same": False, "added": [], "removed": [], "changed": []}, True)
            g.check_against_approval(diff, {"same": False, "added": [], "removed": [], "changed": ["A"]}, True)

        def test_key_stability(self):
            rec = [col("ID", pk=True)]
            with self.assertRaises(g.UnsafeChangeError):
                g.check_key_stability(rec, [col("ID", pk=False)], None, {})
            with self.assertRaises(g.UnsafeChangeError):
                g.check_key_stability(rec, rec, {"PrimaryKeyColumns": '["ID"]'}, {"primary_key_columns": ["ID", "X"]})
            g.check_key_stability(rec, rec, {"PrimaryKeyColumns": '["ID"]'}, {"primary_key_columns": ["id"]})

        def test_insert_rows_preserve(self):
            existing = [{"Sno": 1, "ColumnName": "ID", "TargetColumnName": None, "Description": "old", "SourceDataType": "NUMBER",
                         "FabricDataType": "INT", "IsPrimaryKey": 1, "IsNullable": 0, "IsWatermarkCandidate": 1, "IsSelected": 1,
                         "SourceExpression": "x", "TransformationNotes": "n"},
                        {"Sno": 9, "ColumnName": "HIDDEN", "TargetColumnName": None, "Description": None, "SourceDataType": "DATE",
                         "FabricDataType": "DATE", "IsPrimaryKey": 0, "IsNullable": 1, "IsWatermarkCandidate": 0, "IsSelected": 0,
                         "SourceExpression": None, "TransformationNotes": None}]
            c = g.parse_target_change(notes()).columns
            rows = g.build_column_insert_rows(c, existing)
            id_row = rows[0]
            self.assertEqual((id_row[3], id_row[8], id_row[10]), ("old", 1, "x"))      # description / watermark / expression preserved
            self.assertEqual(rows[-1][1], "HIDDEN"); self.assertEqual(rows[-1][9], 0)   # unselected carried over, new Sno
            self.assertEqual(len({r[0] for r in rows}), len(rows))                       # unique Sno


    class GraphTests(unittest.TestCase):
        def refs(self):
            t = [g.ObjectRef("TABLE", T1, "s.t1"), g.ObjectRef("TABLE", T2, "s.t2")]
            v = [g.ObjectRef("VIEW", V1, "s.v1", 1), g.ObjectRef("VIEW", V2, "s.v2", 2)]
            return t, v

        def test_create_order(self):
            t, v = self.refs()
            self.assertEqual(g.topological_levels(g.build_dependency_graph(t, v, [])), [sorted([T1, T2]), [V1], [V2]])

        def test_declared_dependency(self):
            t, _ = self.refs()
            v = [g.ObjectRef("VIEW", V1, "s.v1", 1), g.ObjectRef("VIEW", V2, "s.v2", 1)]
            deps = [{"view": V1, "type": "FABRIC_VIEW", "depends_on_view": V2, "table": None}]
            self.assertEqual(g.topological_levels(g.build_dependency_graph(t, v, deps))[1:], [[V2], [V1]])

        def test_cycle(self):
            t, _ = self.refs()
            v = [g.ObjectRef("VIEW", V1, "s.v1", 1), g.ObjectRef("VIEW", V2, "s.v2", 1)]
            deps = [{"view": V1, "type": "FABRIC_VIEW", "depends_on_view": V2, "table": None},
                    {"view": V2, "type": "FABRIC_VIEW", "depends_on_view": V1, "table": None}]
            with self.assertRaises(g.ValidationError):
                g.topological_levels(g.build_dependency_graph(t, v, deps))


    class FakeRepo:
        def __init__(self, plan, tables=(), views=(), view_deps=()):
            self.plan, self.tables, self.views, self.vd = plan, list(tables), list(views), list(view_deps)
        def get_plan(self, _): return self.plan
        def list_pending_tables(self, *_): return self.tables
        def list_pending_views(self, *_): return self.views
        def get_view_dependencies(self, _): return self.vd


    def factory(repo):
        @contextmanager
        def f():
            yield repo
        return f


    def child_ok(args):
        return json.dumps({"status": "SUCCESS", "duration_seconds": 1.5, "rows_written": 0,
                           "started_at": "2026-01-01T00:00:00.000Z", "completed_at": "2026-01-01T00:00:01.500Z", "message": "ok"})


    class OrchestratorTests(unittest.TestCase):
        def make(self, cr="BOTH", fail=(), status="PROVISIONING", calls=None, plan_guid=P, empty=False, n=None):
            t = [] if empty else [g.ObjectRef("TABLE", T1, "S.T1"), g.ObjectRef("TABLE", T2, "S.T2")]
            v = [] if empty else [g.ObjectRef("VIEW", V1, "S.V1", 1)]
            repo = FakeRepo(g.PlanInfo(P, status, 1, n), t, v)

            def executor(node):
                if calls is not None:
                    calls.append((node.task, node.args))
                failing = node.args.get("object_guid") in fail or fail == "ALL"
                return g.ObjectResult("X", "wrong-name", "FAILED" if failing else "SUCCESS", 1.5,
                                      "2026-01-01T00:00:00.000Z", "2026-01-01T00:00:01.500Z", 0,
                                      "boom" if failing else "ok")
            return g.MasterOrchestrator(plan_guid, cr, factory(repo), executor)

        def test_create_new_success_and_report_shape(self):
            calls = []
            r = self.make(calls=calls).run()
            self.assertEqual((r["status"], r["tables_processed"], r["views_processed"]), ("SUCCESS", 2, 1))
            self.assertEqual(r["message"], "Provisioning completed")
            self.assertEqual(sorted(c[0] for c in calls), ["TABLE_PROVISION"] * 2 + ["VIEW_PROVISION"])
            self.assertEqual(set(r), {"status", "plan_guid", "tables_processed", "views_processed", "duration_seconds", "objects", "message"})
            allowed = {"object_type", "name", "status", "duration_seconds", "started_at", "completed_at", "rows_written", "message"}
            for o in r["objects"]:
                self.assertTrue(set(o) <= allowed)
            for _, args in calls:
                self.assertEqual(set(args), {"plan_guid", "run_id", "object_guid"})

        def test_failure_blocks_dependants(self):
            calls = []
            r = self.make(fail={T1}, calls=calls).run()
            self.assertEqual((r["status"], r["tables_processed"], r["views_processed"]), ("FAILED", 1, 0))
            view = [o for o in r["objects"] if o["object_type"] == "VIEW"][0]
            self.assertIn("Not run", view["message"])
            self.assertNotIn("VIEW_PROVISION", [c[0] for c in calls])

        def test_creation_request_scope(self):
            r = self.make(cr="TABLE").run(); self.assertEqual((r["tables_processed"], r["views_processed"]), (2, 0))
            r = self.make(cr="VIEW").run();  self.assertEqual((r["tables_processed"], r["views_processed"]), (0, 1))

        def test_change_plan_single_notebook(self):
            calls = []
            o = self.make(status="CHANGE_REQUESTED", n=notes("ALTER_FUTURE"), calls=calls)
            r = o.run()
            self.assertEqual(r["status"], "SUCCESS"); self.assertEqual(r["tables_processed"], 1)
            self.assertEqual([(c[0], c[1]["decision"]) for c in calls], [("TABLE_CHANGE", "ALTER_FUTURE")])
            self.assertEqual(r["message"], "Target change ALTER_FUTURE completed")
            self.assertEqual(r["objects"][0]["name"], "S.T")

        def test_failures_return_json(self):
            cases = [{"plan_guid": "not-a-guid"}, {"status": "APPROVED"}, {"status": "CHANGE_REQUESTED", "n": "garbage"},
                     {"empty": True}, {"cr": "BAD"}, {"status": "CHANGE_REQUESTED", "n": notes(), "cr": "VIEW"}]
            for kw in cases:
                r = self.make(**kw).run()
                self.assertEqual(r["status"], "FAILED", kw)
                self.assertEqual((r["tables_processed"], r["views_processed"], r["objects"]), (0, 0, []))
                json.dumps(r)

        def test_runner_crash_is_failed(self):
            o = self.make()
            o._executor = lambda node: (_ for _ in ()).throw(RuntimeError("timeout token=SECRET"))
            r = o.run()
            self.assertEqual(r["status"], "FAILED"); self.assertNotIn("SECRET", json.dumps(r))
            self.assertEqual(len(r["objects"]), 3)

        def test_result_names_come_from_the_plan(self):
            r = self.make().run()
            self.assertEqual(sorted(o["name"] for o in r["objects"]), ["S.T1", "S.T2", "S.V1"])
            self.assertEqual(sorted(o["object_type"] for o in r["objects"]), ["TABLE", "TABLE", "VIEW"])

        def test_wave_runs_in_parallel_threads_and_keeps_order(self):
            import threading, time
            seen, barrier = set(), threading.Barrier(2, timeout=5)
            def executor(node):
                if node.ref.object_type == "TABLE":
                    barrier.wait()                                   # both tables must be running at the same time
                seen.add(threading.current_thread().name)
                return g.ObjectResult(node.ref.object_type, node.ref.name, "SUCCESS", 0.1)
            repo = FakeRepo(g.PlanInfo(P, "PROVISIONING", 1, None),
                            [g.ObjectRef("TABLE", T1, "S.T1"), g.ObjectRef("TABLE", T2, "S.T2")], [g.ObjectRef("VIEW", V1, "S.V1", 1)])
            r = g.MasterOrchestrator(P, "BOTH", factory(repo), executor).run()
            self.assertEqual(r["status"], "SUCCESS", r["message"])
            self.assertEqual([o["name"] for o in r["objects"]], ["S.T1", "S.T2", "S.V1"])   # tables wave, then view wave
            self.assertGreaterEqual(len(seen), 2)

        def test_execute_task_dispatch(self):
            names = ("run_table_provision", "run_view_provision", "run_table_change")
            saved = {k: NS[k] for k in names}
            seen = []
            NS["run_table_provision"] = lambda p, o, r: seen.append(("tp", p, o, r)) or "R1"
            NS["run_view_provision"] = lambda p, o, r: seen.append(("vp", p, o, r)) or "R2"
            NS["run_table_change"] = lambda p, d, r: seen.append(("tc", p, d, r)) or "R3"
            try:
                ref = g.ObjectRef("TABLE", T1, "S.T1")
                args = {"plan_guid": P, "object_guid": T1, "run_id": "r", "decision": "ALTER_FUTURE"}
                self.assertEqual(g.execute_task(g.WorkNode(ref, "TABLE_PROVISION", args)), "R1")
                self.assertEqual(g.execute_task(g.WorkNode(ref, "VIEW_PROVISION", args)), "R2")
                self.assertEqual(g.execute_task(g.WorkNode(ref, "TABLE_CHANGE", args)), "R3")
                with self.assertRaises(g.ValidationError):
                    g.execute_task(g.WorkNode(ref, "NOPE", args))
            finally:
                NS.update(saved)
            self.assertEqual([s[0] for s in seen], ["tp", "vp", "tc"])


    # ---------------------------------------------------------------- task functions (fakes)
    class FakeMgr:
        def __init__(self, physical, exists=True):
            self.physical, self._exists, self.applied, self.created, self.spark = list(physical), exists, None, False, object()
        def exists(self, _): return self._exists
        def physical_columns(self, _): return self.physical
        def create(self, spec): self.created = True; self._exists = True; self.physical = [phys(c.name, c.fabric_type, c.nullable) for c in spec.columns]
        def apply(self, spec, diff):
            self.applied = diff
            keep = {p.name.lower(): p for p in self.physical}
            for c in spec.columns:
                keep[c.name.lower()] = phys(c.name, c.fabric_type, keep[c.name.lower()].nullable if c.name.lower() in keep else c.nullable)
            self.physical = list(keep.values())
        def verify(self, spec):
            if not g.plan_schema_changes(self.physical, spec.columns).is_empty:
                raise g.TargetStateError("mismatch")


    class Lease:
        held = []
        def __init__(self, resource): self.resource = resource
        def __enter__(self): Lease.held.append(self.resource); return self
        def __exit__(self, *a): pass


    class ChangeRepo:
        """Fake of the repository methods used by change plans."""
        def __init__(self, notes_text, existing_cols, status="PROVISIONED", plan_status="CHANGE_REQUESTED", ingestion=True, fail_apply=False, target=None):
            self.notes_text, self.plan_status, self.status = notes_text, plan_status, status
            self.cols, self.ingestion, self.fail_apply = list(existing_cols), ingestion, fail_apply
            self.flag_history, self.applied, self.pk = [], None, '["ID"]'
            self.target = target or g.LakehouseTarget("ws", "lh", "lakehouse", "S", "T")
        def get_plan(self, _): return g.PlanInfo(P, self.plan_status, 1, self.notes_text)
        def get_table_spec(self, _): return g.TableSpec(T1, P, self.status, True, self.target, tuple(self.cols))
        def get_column_rows(self, _): return []
        def get_replication_config(self, _):
            return {"IncrementalMethod": "WATERMARK", "WriteStrategy": "UPSERT", "PrimaryKeyColumns": self.pk,
                    "MergeKeyColumns": None, "IngestionFlag": self.ingestion}
        def set_ingestion_flag(self, _, enabled): self.ingestion = enabled; self.flag_history.append(enabled)
        def apply_metadata_change(self, guid, expected_status, rows, updates):
            if self.fail_apply: raise g.TargetStateError("db down")
            self.applied = (expected_status, rows, updates)
            self.cols = [col(r[2], g.normalize_fabric_type(r[5]), True if r[7] is None else bool(r[7]), r[7] is not None, sno=r[0], pk=bool(r[6])) for r in rows]


    REC = [col("ID", "INT", False, sno=1, pk=True), col("NAME", sno=2)]
    PHYS_REC = [phys("ID", "INT", False), phys("NAME")]


    class ProvisionTaskTests(unittest.TestCase):
        def repo(self, status="PENDING", columns=(col("ID"),)):
            spec = g.TableSpec(T1, P, status, True, g.LakehouseTarget("ws", None, "lakehouse", "S", "T"), tuple(columns))
            class R:
                def get_table_spec(s, _): return spec
            return factory(R())

        def test_create_new(self):
            m = FakeMgr([], exists=False)
            r = g.run_table_provision(P, T1, "", self.repo(), m)
            self.assertEqual((r.status, r.rows_written), ("SUCCESS", 0)); self.assertTrue(m.created)

        def test_exists_identical_or_different_fails(self):
            self.assertEqual(g.run_table_provision(P, T1, "", self.repo(), FakeMgr([phys("ID")])).status, "FAILED")
            m = FakeMgr([phys("ID"), phys("X")])
            r = g.run_table_provision(P, T1, "", self.repo(), m)
            self.assertEqual(r.status, "FAILED"); self.assertFalse(m.created)

        def test_wrong_plan_or_status(self):
            self.assertEqual(g.run_table_provision("22222222-2222-2222-2222-222222222222", T1, "", self.repo(), FakeMgr([], False)).status, "FAILED")
            self.assertEqual(g.run_table_provision(P, T1, "", self.repo("PROVISIONED"), FakeMgr([], False)).status, "FAILED")


    class ChangeTaskTests(unittest.TestCase):
        def setUp(self):
            Lease.held = []

        def run_change(self, repo, mgr, decision="ALTER_FUTURE"):
            return g.run_table_change(P, decision, "", factory(repo), mgr, Lease)

        def test_alter_future_happy_path(self):
            repo = ChangeRepo(notes(replication={"write_strategy": "UPSERT"}), REC)
            m = FakeMgr(PHYS_REC)
            r = self.run_change(repo, m)
            self.assertEqual(r.status, "SUCCESS", r.message)
            self.assertEqual([c.name for c in m.applied.added], ["EXTRA"])
            self.assertEqual(repo.flag_history, [False, True])          # paused, then resumed
            self.assertEqual(repo.applied[0], "PROVISIONED")
            self.assertEqual(repo.applied[2], {"WriteStrategy": "UPSERT"})
            self.assertEqual(len(Lease.held), 1)

        def test_staging_ingestion_flag_does_not_pause_active_table(self):
            repo = ChangeRepo(notes(replication={"ingestion_flag": False}), REC)
            self.assertEqual(self.run_change(repo, FakeMgr(PHYS_REC)).status, "SUCCESS")
            self.assertTrue(repo.ingestion)
            self.assertEqual(repo.flag_history, [False, True])

        def test_stale_physical_baseline_no_ddl_and_ingestion_restored(self):
            repo = ChangeRepo(notes(), REC)
            m = FakeMgr([phys("ID", "BIGINT", False), phys("NAME")])      # table was changed outside the platform
            r = self.run_change(repo, m)
            self.assertEqual(r.status, "FAILED"); self.assertIn("baseline", r.message)
            self.assertIsNone(m.applied); self.assertTrue(repo.ingestion)

        def test_unsafe_and_unapproved(self):
            cols = [{"sno": 1, "column_name": "ID", "fabric_data_type": "BIGINT", "is_nullable": False, "is_primary_key": True}]  # NAME removed
            repo = ChangeRepo(notes(cols=cols, comparison={"same": False, "added": [], "removed": ["NAME"], "changed": ["ID"]}), REC)
            m = FakeMgr(PHYS_REC)
            r = self.run_change(repo, m)
            self.assertEqual(r.status, "FAILED"); self.assertIn("Unsafe", r.message)
            self.assertIsNone(m.applied); self.assertTrue(repo.ingestion)
            repo = ChangeRepo(notes(comparison={"same": False, "added": [], "removed": [], "changed": []}), REC)   # approval says nothing added
            r = self.run_change(repo, FakeMgr(PHYS_REC))
            self.assertEqual(r.status, "FAILED"); self.assertIn("approved", r.message)

        def test_status_preconditions(self):
            self.assertEqual(self.run_change(ChangeRepo(notes(), REC, status="PENDING"), FakeMgr(PHYS_REC)).status, "FAILED")
            self.assertEqual(self.run_change(ChangeRepo(notes(), REC, plan_status="PROVISIONING"), FakeMgr(PHYS_REC)).status, "FAILED")
            self.assertEqual(self.run_change(ChangeRepo(notes(), REC), FakeMgr([], exists=False)).status, "FAILED")
            self.assertEqual(self.run_change(ChangeRepo(notes(), REC), FakeMgr(PHYS_REC), decision="REVISE_PLANNED").status, "FAILED")

        def test_delta_committed_config_failed_leaves_paused_then_resumes(self):
            repo = ChangeRepo(notes(), REC, fail_apply=True)
            m = FakeMgr(PHYS_REC)
            r = self.run_change(repo, m)
            self.assertEqual(r.status, "FAILED"); self.assertIn("paused", r.message)
            self.assertFalse(repo.ingestion)                              # stays paused
            repo.fail_apply = False                                       # retry: physical already == proposed
            m.applied = None
            r = self.run_change(repo, m)
            self.assertEqual(r.status, "SUCCESS", r.message)
            self.assertIsNone(m.applied)                                  # no second DDL
            self.assertFalse(repo.ingestion)                              # never auto-resumes after an interrupted run
            self.assertIn("remains paused", r.message)

        def test_already_applied_is_idempotent(self):
            repo = ChangeRepo(notes(), REC)
            m = FakeMgr(PHYS_REC)
            self.assertEqual(self.run_change(repo, m).status, "SUCCESS")
            m.applied = None
            r = self.run_change(repo, m)
            self.assertEqual(r.status, "SUCCESS", r.message); self.assertIsNone(m.applied)

        def test_key_change_rejected(self):
            cols = [{"sno": 1, "column_name": "ID", "fabric_data_type": "INT", "is_nullable": False, "is_primary_key": False},
                    {"sno": 2, "column_name": "NAME", "fabric_data_type": "STRING"}]
            repo = ChangeRepo(notes(cols=cols, comparison={"same": False, "added": [], "removed": [], "changed": ["ID"]}), REC)
            r = self.run_change(repo, FakeMgr(PHYS_REC))
            self.assertEqual(r.status, "FAILED"); self.assertIn("Primary key", r.message)

        def test_revise_planned(self):
            repo = ChangeRepo(notes("REVISE_PLANNED", replication={"write_strategy": "SCD1"}), REC, status="PENDING")
            m = FakeMgr([], exists=False)
            r = self.run_change(repo, m, "REVISE_PLANNED")
            self.assertEqual(r.status, "SUCCESS", r.message)
            self.assertEqual(repo.applied[0], "PENDING"); self.assertIsNone(m.applied); self.assertFalse(m.created)
            # physical table present -> refuse
            r = self.run_change(ChangeRepo(notes("REVISE_PLANNED"), REC, status="PENDING"), FakeMgr(PHYS_REC), "REVISE_PLANNED")
            self.assertEqual(r.status, "FAILED")
            # record not PENDING -> refuse
            r = self.run_change(ChangeRepo(notes("REVISE_PLANNED"), REC, status="PROVISIONED"), FakeMgr([], exists=False), "REVISE_PLANNED")
            self.assertEqual(r.status, "FAILED")


    class FakeSpark:
        """Answers SQL statements from a dict: value = list of single-column rows, or an Exception to raise."""
        def __init__(self, answers):
            self.answers, self.statements = answers, []
        def sql(self, stmt):
            self.statements.append(stmt)
            res = self.answers.get(stmt, RuntimeError(f"[PARSE] unexpected statement {stmt}"))
            if isinstance(res, Exception):
                raise res
            return types.SimpleNamespace(collect=lambda: [(x,) if not isinstance(x, dict) else x for x in res])


    class PreflightTests(unittest.TestCase):
        def setUp(self):
            g._NAMESPACE_MODE.clear()
            self.tgt = g.LakehouseTarget("ws", None, "lh_bronze", "LOCAL_ORACLE", "SALES_ORDER_HEADER")

        def tearDown(self):
            g._NAMESPACE_MODE.clear()

        def test_with_lakehouse_form(self):
            sp = FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": ["dbo", "LOCAL_ORACLE"]})
            self.assertTrue(self.tgt.preflight(sp))
            self.assertEqual(self.tgt.fq, "`lh_bronze`.`LOCAL_ORACLE`.`SALES_ORDER_HEADER`")

        def test_existing_namespace_even_when_show_schemas_in_fails(self):
            """Fabric may list the table even when SHOW SCHEMAS IN <lakehouse> fails."""
            sp = FakeSpark({
                "SHOW SCHEMAS IN `lh_bronze`": RuntimeError("[SCHEMA_NOT_FOUND] lh_bronze"),
                "SHOW SCHEMAS": ["dbo"],
                "SHOW TABLES IN `lh_bronze`.`LOCAL_ORACLE`": [
                    {"tableName": "sales_order_header"}],
            })
            self.assertTrue(g.DeltaTableManager(sp).exists(self.tgt))
            self.assertEqual(self.tgt.fq, "`lh_bronze`.`LOCAL_ORACLE`.`SALES_ORDER_HEADER`")

        def test_fallback_when_in_lakehouse_form_fails(self):
            """The reported failure: SHOW SCHEMAS IN <lakehouse> raises, plain SHOW SCHEMAS works."""
            sp = FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": RuntimeError("[SCHEMA_NOT_FOUND] lh_bronze"),
                            "SHOW SCHEMAS": ["dbo", "local_oracle"]})
            self.assertTrue(self.tgt.preflight(sp))
            self.assertEqual(self.tgt.fq, "`LOCAL_ORACLE`.`SALES_ORDER_HEADER`")
            self.assertEqual(self.tgt.namespace, "`LOCAL_ORACLE`")

        def test_missing_schema_is_created_with_matching_namespace(self):
            sp = FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": RuntimeError("x"), "SHOW SCHEMAS": ["dbo"],
                            "CREATE SCHEMA IF NOT EXISTS `LOCAL_ORACLE`": []})
            self.assertFalse(self.tgt.preflight(sp))
            self.assertTrue(self.tgt.preflight(sp, ensure_schema=True))
            self.assertIn("CREATE SCHEMA IF NOT EXISTS `LOCAL_ORACLE`", sp.statements)

        def test_both_fail_reports_real_reasons(self):
            sp = FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": RuntimeError("[SCHEMA_NOT_FOUND] first reason\nstack..."),
                            "SHOW SCHEMAS": RuntimeError("second reason")})
            with self.assertRaises(g.TargetStateError) as cm:
                self.tgt.preflight(sp)
            msg = g.safe_message(cm.exception)
            self.assertIn("first reason", msg); self.assertIn("second reason", msg); self.assertNotIn("stack", msg)

        def test_non_schema_enabled_lakehouse_detected(self):
            sp = FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": RuntimeError("x"), "SHOW SCHEMAS": ["lh_bronze", "lh_silver"]})
            with self.assertRaises(g.TargetStateError) as cm:
                self.tgt.preflight(sp)
            self.assertIn("schema-enabled", str(cm.exception))

        def test_default_lakehouse_name_mismatch(self):
            previous = NS["_runtime_context"]
            NS["_runtime_context"] = lambda: {"defaultLakehouseName": "lh_other"}
            try:
                with self.assertRaises(g.TargetStateError) as cm:
                    self.tgt.preflight(FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": ["LOCAL_ORACLE"]}))
                self.assertIn("lh_other", str(cm.exception))
            finally:
                NS["_runtime_context"] = previous

        def test_exists_uses_resolved_namespace(self):
            sp = FakeSpark({"SHOW SCHEMAS IN `lh_bronze`": RuntimeError("x"), "SHOW SCHEMAS": ["LOCAL_ORACLE"],
                            "SHOW TABLES IN `LOCAL_ORACLE`": [{"tableName": "sales_order_header"}]})
            self.assertTrue(g.DeltaTableManager(sp).exists(self.tgt))


    REAL_NOTES = r'''{
      "target_change": {
        "decision": "REVISE_PLANNED",
        "existing_source_table_guid": "ed9644ac-7797-5641-9e4f-bbb84cc8d8be",
        "proposed_plan": {
          "columns": [
            {
              "column_name": "ORDER_ID",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": false,
              "is_primary_key": true,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 1,
              "source_data_type": "NUMBER",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "ORDER_NUMBER",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": false,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 2,
              "source_data_type": "VARCHAR2(50)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "CUSTOMER_ID",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 3,
              "source_data_type": "NUMBER",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "CUSTOMER_NAME",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 4,
              "source_data_type": "VARCHAR2(200)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "ORDER_DATE",
              "description": null,
              "fabric_data_type": "TIMESTAMP",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": true,
              "sno": 5,
              "source_data_type": "DATE",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "ORDER_STATUS",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 6,
              "source_data_type": "VARCHAR2(30)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "CURRENCY_CODE",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 7,
              "source_data_type": "VARCHAR2(10)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "TOTAL_AMOUNT",
              "description": null,
              "fabric_data_type": "DECIMAL(15,2)",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 8,
              "source_data_type": "NUMBER(15,2)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "CREATED_DATE",
              "description": null,
              "fabric_data_type": "TIMESTAMP",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": true,
              "sno": 9,
              "source_data_type": "TIMESTAMP(6)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "LAST_UPDATED_DATE",
              "description": null,
              "fabric_data_type": "TIMESTAMP",
              "is_nullable": false,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": true,
              "sno": 10,
              "source_data_type": "TIMESTAMP(6)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            },
            {
              "column_name": "CUSTOMER_REGION",
              "description": null,
              "fabric_data_type": "STRING",
              "is_nullable": true,
              "is_primary_key": false,
              "is_selected": true,
              "is_watermark_candidate": false,
              "sno": 11,
              "source_data_type": "VARCHAR2(30)",
              "source_expression": null,
              "target_column_name": null,
              "transformation_notes": null
            }
          ],
          "replication": {
            "current_flag_column": null,
            "effective_from_column": null,
            "effective_to_column": null,
            "incremental_method": "WATERMARK",
            "ingestion_flag": false,
            "max_row_fetch": 0,
            "merge_key_columns": null,
            "pipeline_item_id": null,
            "pipeline_workspace_id": "5a590541-0088-465b-b8a3-d7609f270a5f",
            "primary_key_columns": [
              "ORDER_ID"
            ],
            "watermark_column": "LAST_UPDATED_DATE",
            "watermark_column_data_type": "TIMESTAMP",
            "watermark_index_name": "IDX_SOH_LAST_UPDATED_DATE",
            "write_strategy": "UPSERT"
          },
          "table": {
            "application_component": null,
            "connection_name": "Oracle Local",
            "data_source_type": null,
            "delta": null,
            "fabric_lakehouse_id": "64d625a2-5ca0-432e-a6b7-522211c89352",
            "fabric_lakehouse_name": "lh_bronze",
            "fabric_lakehouse_schema": "LOCAL_ORACLE",
            "fabric_table_name": "SALES_ORDER_HEADER",
            "fabric_workspace_id": "5a590541-0088-465b-b8a3-d7609f270a5f",
            "fabric_workspace_name": null,
            "is_active": true,
            "object_role": null,
            "parent_source_object_guid": null,
            "replication_method": null,
            "source_object_type": "TABLE",
            "source_schema_name": "ONT",
            "source_system_type": "ORACLE",
            "source_table_name": "SALES_ORDER_HEADER"
          }
        },
        "review": {
          "basis": "CONFIG_DB_RECORDED_SCHEMA",
          "definitions": [
            {
              "comparison": {
                "added": [
                  {
                    "fabric_type": "STRING",
                    "name": "CUSTOMER_REGION",
                    "nullable": true
                  }
                ],
                "changed": [],
                "removed": [],
                "same": false
              },
              "plan_guid": "41f4ed9a-001f-5c88-876f-1edc9cf43f88",
              "provisioning_status": "PENDING",
              "source_table_guid": "ed9644ac-7797-5641-9e4f-bbb84cc8d8be"
            }
          ],
          "is_provisioned_record": false,
          "physical_schema_verified": false,
          "selected_existing_guid": "ed9644ac-7797-5641-9e4f-bbb84cc8d8be",
          "target": {
            "lakehouse_id": "64d625a2-5ca0-432e-a6b7-522211c89352",
            "schema": "LOCAL_ORACLE",
            "table": "SALES_ORDER_HEADER",
            "workspace_id": "5a590541-0088-465b-b8a3-d7609f270a5f"
          }
        }
      }
    }'''


    class RealPayloadTests(unittest.TestCase):
        """The Notes JSON exactly as produced by the application."""
        LIVE = g.LakehouseTarget("5a590541-0088-465b-b8a3-d7609f270a5f", "64d625a2-5ca0-432e-a6b7-522211c89352",
                                 "lh_bronze", "LOCAL_ORACLE", "SALES_ORDER_HEADER")

        def setUp(self):
            Lease.held = []
            self.raw = REAL_NOTES
            self.change = g.parse_target_change(self.raw)
            self.recorded = [c for c in self.change.columns if c.name != "CUSTOMER_REGION"]

        def test_parse_and_route(self):
            self.assertEqual((self.change.decision, len(self.change.columns)), ("REVISE_PLANNED", 11))
            plan = g.PlanInfo("41f4ed9a-001f-5c88-876f-1edc9cf43f88", "CHANGE_REQUESTED", 1, self.raw)
            self.assertEqual(g.resolve_run_request(plan.plan_guid, "BOTH", plan).action, "REVISE_PLANNED")
            g.verify_target_agreement(self.change, self.LIVE)
            diff = g.compare_definitions(self.recorded, self.change.columns)
            self.assertEqual(diff, {"added": {"customer_region"}, "removed": set(), "changed": set()})
            g.check_against_approval(diff, self.change.approved_comparison, True)

        def test_revise_planned_end_to_end(self):
            repo = ChangeRepo(self.raw, self.recorded, status="PENDING", ingestion=False, target=self.LIVE)
            m = FakeMgr([], exists=False)
            r = g.run_table_change("41f4ed9a-001f-5c88-876f-1edc9cf43f88", "REVISE_PLANNED", "", factory(repo), m, Lease)
            self.assertEqual(r.status, "SUCCESS", r.message)
            self.assertEqual(r.name, "LOCAL_ORACLE.SALES_ORDER_HEADER")
            status, rows, updates = repo.applied
            self.assertEqual((status, len(rows)), ("PENDING", 11))
            self.assertEqual(updates["PrimaryKeyColumns"], '["ORDER_ID"]')
            self.assertEqual(updates["WriteStrategy"], "UPSERT")
            self.assertNotIn("IngestionFlag", updates)
            self.assertEqual(repo.flag_history, [False])               # ingestion_flag=false from the proposal
            self.assertFalse(m.created); self.assertIsNone(m.applied)  # no Spark DDL for REVISE_PLANNED

        def test_same_payload_as_alter_future(self):
            data = json.loads(self.raw); data["target_change"]["decision"] = "ALTER_FUTURE"
            physical = [phys(c.name, c.fabric_type, c.nullable) for c in self.recorded]
            repo = ChangeRepo(json.dumps(data), self.recorded, status="PROVISIONED", ingestion=True, target=self.LIVE)
            repo.pk = '["ORDER_ID"]'
            m = FakeMgr(physical)
            r = g.run_table_change("41f4ed9a-001f-5c88-876f-1edc9cf43f88", "ALTER_FUTURE", "", factory(repo), m, Lease)
            self.assertEqual(r.status, "SUCCESS", r.message)
            self.assertEqual([c.name for c in m.applied.added], ["CUSTOMER_REGION"])
            self.assertEqual(repo.flag_history, [False, True])         # paused, then restored to its original state
            self.assertNotIn("remains paused", r.message)

        def test_wrong_lakehouse_name_rejected(self):
            other = g.LakehouseTarget(self.LIVE.workspace_id, self.LIVE.lakehouse_id, "other_lh", "LOCAL_ORACLE", "SALES_ORDER_HEADER")
            with self.assertRaises(g.ValidationError):
                g.verify_target_agreement(self.change, other)


    class DataMovementTests(unittest.TestCase):
        def setUp(self):
            data = json.loads(REAL_NOTES)
            self.data = data
            self.live = RealPayloadTests.LIVE
            self.base = g.parse_target_change(REAL_NOTES)
            self.recorded = [c for c in self.base.columns if c.name != 'CUSTOMER_REGION']
            self.physical = [phys(c.name, c.fabric_type, c.nullable) for c in self.recorded]

        def make(self, decision):
            data = json.loads(json.dumps(self.data))
            data['target_change']['decision'] = decision
            change = g.parse_target_change(json.dumps(data))
            parent = self

            class Repo(ChangeRepo):
                def __init__(self):
                    super().__init__(json.dumps(data), parent.recorded, target=parent.live)
                    self.pk = '["ORDER_ID"]'
                    self.phase = None
                def get_oracle_source(self, _):
                    return {'ConnectionName': 'Oracle Local', 'SourceSchemaName': 'ONT',
                            'SourceTableName': 'SALES_ORDER_HEADER'}
                def assert_replication_idle(self, _): pass
                def begin_change_run(self, *_):
                    if self.phase in ('MUTATION_STARTED', 'COMPLETED'):
                        raise g.TargetStateError('Prior mutation requires reconciliation.')
                    self.phase = 'STARTED'
                def mark_change_run(self, _, phase, **kw): self.phase = phase
                def apply_metadata_change(self, guid, status, rows, updates, complete_run_guid=None,
                                          resume_ingestion=None):
                    if self.fail_apply: raise g.TargetStateError('db down')
                    super().apply_metadata_change(guid, status, rows, updates)
                    self.phase = 'COMPLETED'
                    self.ingestion = resume_ingestion

            class Snapshot:
                scn = 42
                def __init__(self, _): pass
                def __enter__(self): return self
                def __exit__(self, *_): pass

            class Mover:
                def __init__(self, mgr):
                    self.mgr, self.calls, self.fail_verify = mgr, [], False
                def stage(self, *_): self.calls.append('stage'); return 12
                def validate_keys(self, *_): self.calls.append('keys')
                def validate_target_keys(self, *_): self.calls.append('target_keys')
                def validate_target_subset(self, *_): self.calls.append('target_subset')
                def merge(self, *_): self.calls.append('merge')
                def replace(self, target, stage, columns):
                    self.calls.append('replace')
                    self.mgr.physical = [phys(c.name, c.fabric_type, c.nullable) for c in columns]
                def verify_data(self, *_ , **__):
                    self.calls.append('verify_data')
                    if self.fail_verify: raise g.TargetStateError('values differ')

            repo = Repo()
            manager = FakeMgr(self.physical)
            mover = Mover(manager)
            return change, repo, manager, mover, Snapshot

        def run_change(self, decision, repo, manager, mover, snapshot):
            return g._data_change(decision, P, factory(repo), manager, mover, snapshot)

        def test_backfill_merges_and_commits_after_verification(self):
            change, repo, manager, mover, snapshot = self.make('ALTER_BACKFILL')
            message, rows = self.run_change(change, repo, manager, mover, snapshot)
            self.assertEqual(rows, 12)
            self.assertEqual(repo.phase, 'COMPLETED')
            self.assertTrue(repo.ingestion)
            self.assertEqual(mover.calls, ['stage', 'keys', 'target_keys', 'target_subset', 'merge', 'verify_data'])
            self.assertIn('previous watermark retained', message)

        def test_replace_uses_staging_and_commits(self):
            change, repo, manager, mover, snapshot = self.make('REPLACE_FULL')
            message, rows = self.run_change(change, repo, manager, mover, snapshot)
            self.assertEqual((rows, repo.phase), (12, 'COMPLETED'))
            self.assertEqual(mover.calls, ['stage', 'replace', 'verify_data'])

        def test_failure_after_target_write_stays_paused_and_blocks_retry(self):
            change, repo, manager, mover, snapshot = self.make('REPLACE_FULL')
            mover.fail_verify = True
            with self.assertRaisesRegex(g.TargetStateError, 'ingestion left paused'):
                self.run_change(change, repo, manager, mover, snapshot)
            self.assertEqual(repo.phase, 'MUTATION_STARTED')
            self.assertFalse(repo.ingestion)
            with self.assertRaises(g.TargetStateError):
                self.run_change(change, repo, manager, mover, snapshot)

        def test_unsafe_backfill_fails_before_pause(self):
            change, repo, manager, mover, snapshot = self.make('ALTER_BACKFILL')
            repo.pk = None
            with self.assertRaises(g.UnsafeChangeError):
                self.run_change(change, repo, manager, mover, snapshot)
            self.assertTrue(repo.ingestion)
            self.assertEqual(mover.calls, [])

        def test_decimal_casts_are_exact(self):
            c = col('AMOUNT', 'DECIMAL(38,2)')
            self.assertEqual(g.convert_oracle_value(Decimal('123.4500'), c), Decimal('123.45'))
            with self.assertRaises(g.ValidationError):
                g.convert_oracle_value(Decimal('123.456'), c)
            with self.assertRaises(g.ValidationError):
                g.convert_oracle_value(Decimal('1E+38'), c)

        def test_oracle_connection_and_identifiers(self):
            source = {'ConnectionDetails': json.dumps({'host': 'db.example', 'port': 1521,
                                                       'serviceName': 'service', 'username': 'u', 'password': 'secret'})}
            self.assertEqual(g.parse_oracle_connection(source), ('u', 'secret', 'db.example:1521/service'))
            with self.assertRaises(g.ValidationError):
                g.quote_oracle_identifier('T; DROP TABLE X')
            with self.assertRaises(g.ValidationError):
                g.parse_oracle_connection({'ConnectionDetails': '{}'})

        def test_data_sql_uses_staging_and_quoted_keys(self):
            statements = []
            spark = types.SimpleNamespace(sql=lambda statement: statements.append(statement))
            mover = g.DeltaDataMover(types.SimpleNamespace(spark=spark))
            stage = g.staging_target(self.live, P)
            mover.merge(self.live, stage, self.base.columns, ['order_id'])
            mover.replace(self.live, stage, self.base.columns)
            self.assertIn('MERGE INTO', statements[0])
            self.assertIn('t.`order_id` <=> s.`order_id`', statements[0])
            self.assertIn(stage.fq, statements[0])
            self.assertTrue(statements[1].startswith('CREATE OR REPLACE TABLE'))
            self.assertNotIn('DROP TABLE', ''.join(statements))

    classes = sorted((c for name, c in list(locals().items())
                      if isinstance(c, type) and issubclass(c, unittest.TestCase) and name.endswith("Tests")),
                     key=lambda c: c.__name__)
    saved_context, saved_level = NS["_runtime_context"], LOG.level
    NS["_runtime_context"] = lambda: {}           # fakes must not be compared with the real workspace
    LOG.setLevel(logging.CRITICAL)
    try:
        suite = unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(c) for c in classes)
        outcome = unittest.TextTestRunner(verbosity=verbosity, stream=sys.stdout).run(suite)
    finally:
        NS["_runtime_context"] = saved_context
        LOG.setLevel(saved_level)
    return {"ok": outcome.wasSuccessful(), "tests_run": outcome.testsRun,
            "failures": len(outcome.failures), "errors": len(outcome.errors)}


# %% [markdown]
# Part 5 - Diagnostics
# `run_diagnostics()` - only executed with RUN_MODE = DIAGNOSE.
#

# %%
# Names used by run_diagnostics (edit if your lakehouse / schema / table differ).
DIAG_LAKEHOUSE_NAME = "lh_bronze"
DIAG_SCHEMA_NAME = "LOCAL_ORACLE"
DIAG_TABLE_NAME = "SALES_ORDER_HEADER"


def run_diagnostics() -> None:
    """
    Read-only troubleshooting (RUN_MODE = "DIAGNOSE"): Config DB connectivity, then how Spark sees the
    default lakehouse and which naming forms work. Creates / alters / drops nothing; prints no secrets.
    """
    print("=== 1. Config DB ===")
    try:
        with ConfigDbConnection() as db:
            who = db.fetch_one("SELECT DB_NAME() AS db_name, SUSER_SNAME() AS login_name")
            print(f"Connected to database : {who['db_name']}\nRunning identity      : {who['login_name']}")
            for table in ("MigrationPlans", "SourceTables", "SourceTableColumns", "FabricViews"):
                n = db.fetch_one(f"SELECT COUNT(*) AS n FROM {CONFIG_DB_SCHEMA}.{table}")["n"]
                print(f"  {CONFIG_DB_SCHEMA}.{table:<20} rows = {n}")
    except Exception as exc:  # noqa: BLE001
        print("Config DB check FAILED:", safe_message(exc))

    print("\n=== 2. Runtime / default lakehouse ===")
    ctx = _runtime_context()
    for key in ("currentWorkspaceId", "currentWorkspaceName", "defaultLakehouseId", "defaultLakehouseName"):
        print(f"{key:24}: {ctx.get(key)}")
    sp = get_spark()
    try:
        print(f"{'current catalog':24}: {sp.catalog.currentCatalog()}\n{'current database':24}: {sp.catalog.currentDatabase()}")
    except Exception as exc:  # noqa: BLE001
        print("catalog info unavailable:", type(exc).__name__)

    print("\n=== 3. Statements the provisioning code may use ===")
    lh, sc, tb = DIAG_LAKEHOUSE_NAME, DIAG_SCHEMA_NAME, DIAG_TABLE_NAME
    for statement in ("SHOW SCHEMAS", f"SHOW SCHEMAS IN `{lh}`", f"SHOW TABLES IN `{sc}`", f"SHOW TABLES IN `{lh}`.`{sc}`",
                      f"SELECT * FROM `{sc}`.`{tb}` LIMIT 0", f"SELECT * FROM `{lh}`.`{sc}`.`{tb}` LIMIT 0"):
        try:
            rows = sp.sql(statement).collect()
            print(f"OK   | {statement}\n       -> {[str(r[0]) if len(r) == 1 else str(tuple(r)) for r in rows][:15]}")
        except Exception as exc:  # noqa: BLE001
            print(f"FAIL | {statement}\n       -> {type(exc).__name__}: {first_line(exc, 200)}")
    print("\nHow to read it: empty/different defaultLakehouseName -> lakehouse not attached to THIS session; SHOW SCHEMAS "
          "listing lakehouse names -> lakehouse is not schema-enabled; everything failing -> re-attach and restart the session.")


# %% [markdown]
# ## Run (never raises) and exit
# `exit()` is deliberately in its **own final top-level cell**, outside any `try/except`, as required by Fabric.

# %%
_mode = (normalize_text(RUN_MODE) or "PROVISION").upper()
if _mode == "PROVISION":
    result = MasterOrchestrator(plan_guid, CREATIONREQUEST).run()
elif _mode == "TEST":
    _t = run_self_tests()
    result = {"status": Status.SUCCESS if _t["ok"] else Status.FAILED, "mode": "TEST", **{k: _t[k] for k in ("tests_run", "failures", "errors")},
              "message": "All tests passed" if _t["ok"] else "Some tests failed - see the output above"}
elif _mode == "DIAGNOSE":
    run_diagnostics()
    result = {"status": Status.SUCCESS, "mode": "DIAGNOSE", "message": "Diagnostics printed in the cell output"}
else:
    result = {"status": Status.FAILED, "plan_guid": normalize_text(plan_guid) or "", "tables_processed": 0,
              "views_processed": 0, "duration_seconds": 0.0, "objects": [], "message": "RUN_MODE must be PROVISION, TEST or DIAGNOSE."}
print(json.dumps(result, indent=2))

# %%
notebookutils.notebook.exit(json.dumps(result))
