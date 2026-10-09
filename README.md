# Fabric API

A minimal FastAPI application with a health endpoint and interactive API documentation.

## Temporal migration control

The API now serves a thin migration control page at `/` and four migration start
routes under `/migrations`. Workflow status, approvals, feedback, cancellation,
and Config DB batch audit are available through the API. The browser never
connects to Temporal directly.

Use **Create connection** on the migration page to register an Oracle or SAP ECC
source in Config DB. Select the source connection first; the migration type
list then comes from `thirdparty/configdb/migration_types.json`. The API serves
that mapping at `GET /migrations/options` and checks it against the selected
connection before starting a workflow. The file can hide implemented migration
types, but cannot add a new workflow route without code. The form saves the
endpoint, username, and password in
`DBConnections.ConnectionDetails`. Protect the Config DB and use HTTPS for
remote access to the API. The connection list and create responses omit the
password. The API exposes `GET /connections` and `POST /connections` for this UI.
The Fabric workspace selector reads active rows from
`bronze_replication.FabricWorkspaces` through `GET /workspaces`; the option
submits `WorkspaceId` while displaying `WorkspaceName`. Selecting a workspace
loads the Lakehouse dropdown from Fabric through
`GET /workspaces/{workspace_id}/lakehouses`; it submits the selected Lakehouse ID.

Start a Temporal development server, then run the worker and API in separate
terminals:

```powershell
temporal server start-dev
.\venv\Scripts\python -m app.migration.worker
.\venv\Scripts\fastapi dev
```

Set `TEMPORAL_ADDRESS` and `TEMPORAL_NAMESPACE` for a nondefault server. The
worker and API share `MIGRATION_TASK_QUEUE` (default `migration-workflows`).
Install the project again after updating to pull in `temporalio`.
Keep the worker running alongside the API; starting the API alone queues workflows
without scheduling any activities. In Command Prompt, run
`venv\Scripts\python.exe -m app.migration.worker` in a separate window.
Already queued workflows resume when that worker connects to the same Temporal
server, namespace, and task queue.

The provisioning notebook ID is optional. With no notebook ID, approval saves
the plan and the workflow ends in `PLANNED` state; Fabric provisioning does not
run. With a notebook ID, the workflow provisions the approved Fabric structure
and makes the table eligible for a separately scheduled replication pipeline.
The migration workflow does not submit a pipeline job or load source rows.

Source-to-Fabric type mappings come from
`thirdparty/oracle/oracle_type_mapping.json` and
`thirdparty/sap/sap_type_mapping.json`. Explicit numeric precision and scale
still map to `DECIMAL(p,s)` when Fabric can represent them. Unbounded Oracle
`NUMBER` uses the Oracle file's `STRING` default and appears with a warning in
the plan review. Restart workers after changing either JSON file.

Oracle review starts with source columns. Review and edit any
proposed Fabric type in the grid. Supported overrides are the basic Lakehouse
types (`STRING`, `BINARY`, `BOOLEAN`, `DATE`, `TIMESTAMP`, integer and floating
point types) and `DECIMAL(p,s)` with precision up to 38. Confirming the grid
does not yet persist the plan. Then choose **Full load** or one of the
discovered timestamp/date watermark columns; the UI can return to the grid
before final approval. A watermark choice requires a source primary key; the
plan stores `IncrementalMethod=WATERMARK`,
`WriteStrategy=UPSERT`, the selected column and type, and `WatermarkIndexName`
when Oracle reports an index on that column. Existing Config DB installations
need `thirdparty/configdb/migrations/002_watermark_index_name.sql` once before
the updated worker persists new plans. A workflow started without a provisioning
notebook only saves this configuration; it does not perform a replication run or
advance `ReplicationState.LastWatermarkValue`.

When Oracle or SAP table discovery reports no primary key, replication setup
now asks for a primary key decision before watermark selection. Select one or
more discovered fields that uniquely identify each row, or explicitly choose
**No primary key — full load only**. User-declared fields are saved in
`ReplicationConfig.PrimaryKeyColumns` and the column `IsPrimaryKey` flags;
the workflow history records who declared them. New Oracle workflows run exact
full-table null and composite-duplicate checks in one read-only transaction for
database-discovered and user-selected keys before watermark selection. Failure
or timeout returns to key selection; it never counts as successful validation.
All Oracle indexes are discovered; ordinary unique indexes provide selectable
key suggestions, with nullable/unknown candidates identified. Users can select
other source fields. SAP and older Oracle histories keep their existing behavior.
This does not create a source constraint or establish future key stability.
Choosing no key keeps watermark upserts unavailable. A previously started workflow waiting for a
watermark can use **Specify primary key fields** to enter the new decision step
after the API and worker are updated.

New Oracle workflows ask for delete handling after replication-method selection.
Choose source soft deletes, scheduled key reconciliation, both, or no incremental
delete propagation. Active policies require validated keys. Soft-delete setup
captures the field and exact deletion values (or a non-null deletion timestamp),
and watermark users must confirm deletion/restoration advance the watermark.
Choose retaining a deletion marker or removing the current-state row; reconciliation
also captures the interval in minutes and requires complete, consistent snapshots.

The approved policy is stored as JSON in `ReplicationConfig.DeletePolicy`, with
key-check evidence in `KeyValidation`. `vw_ReplicationQueue` exposes individual
delete fields for dynamic Fabric pipelines, and `ReplicationState` has the last
successful reconciliation timestamp and SCN. Apply
`thirdparty/configdb/migrations/004_oracle_delete_policy.sql` before deploying the
updated API/worker. The app does not load rows, apply deletes, or schedule data
loading. Your Fabric pipelines must consume and implement the policy. See
[the pipeline contract](thirdparty/fabric/ORACLE_DELETE_PIPELINE_CONTRACT.md).
Delete-policy changes on provisioned targets require a backfill/reset or full
replacement approval, rather than applying only to future rows.

SAP table review also requires a separate watermark decision after datatype
mapping approval. It proposes `DATS` fields and timestamps identified by their
SAP type or standard timestamp data element, excluding standalone `TIMS` fields.
Choose full load or a date/timestamp that tracks modifications. A primary key
is required for `WATERMARK` / `UPSERT`; SAP index metadata is unverified.
The plan stores the source column and datatype in `ReplicationConfig`, while
`ReplicationState.LastWatermarkValue` starts as NULL. The separately scheduled
ingestion pipeline advances it and must handle date granularity, equal values,
and SAP initial dates. Neither approval nor provisioning loads source data.
Existing Temporal histories keep the prior approval sequence through patching.

SAP table workflows also compare existing target columns and replication settings
before saving configuration. They support approved in-place alterations, watermark
resets, empty-table replacement and revision of pending configurations using the
shared provisioning notebook. See [SAP table workflow details](app/migration/workflows-docs/SAP_TABLE_REPLICATION.md).

Runtime configuration persistence batches column inserts within its transaction
and has a 180-second activity timeout with safe retries using the saved plan
fingerprint. The provisioning notebook quotes SAP namespaced column names such
as `/DMBE/DEALNUMBER`, while retaining stricter schema and table name validation.

After Oracle mapping and watermark approval, the worker also checks active saved
configurations for the same Fabric target, including `PENDING` plans. An identical
structure ends as `DUPLICATE_TARGET` without saving another runtime row. A changed
structure opens a separate target review: revise a pending plan, alter for future
data, alter and backfill history, or replace and fully reload. The chosen change
and proposed plan are recorded in `MigrationPlans.Notes` with status
`CHANGE_REQUESTED`. When a provisioning notebook ID is set, the workflow now
submits that approved change to Fabric and records the notebook report. A
previously closed `CHANGE_REQUESTED` workflow remains a saved approval; start
a new Oracle migration workflow to execute a new approved change. Apply
`thirdparty/configdb/migrations/003_target_change_runs.sql` before running
`ALTER_BACKFILL` or `REPLACE_FULL`, and restart the API and worker after this
code update. The current comparison uses
Config DB columns and cannot establish whether the physical Fabric table has
drifted or whether an unregistered table exists; the notebook checks the
physical schema before applying a change. See the
[target change notebook contract](thirdparty/fabric/TARGET_CHANGE_NOTEBOOK_CONTRACT.md)
for Oracle lock and recovery requirements.

Source activities load credentials from the selected active Config DB connection.
Existing connection records without a username and password still use the
worker's Oracle or SAP environment profile; for those records set
`ORACLE_CONNECTION_NAME` or `SAP_CONNECTION_NAME` to the selected name. Passwords
are resolved inside activities and are not put in Temporal workflow inputs.

The `SAP_REBUILD` route requires `MIGRATION_ANALYSIS_MODEL` and model credentials
for Fabric plan proposals. Function module analysis uses the same LLM adapter
and Phoenix tracing, then verifies proposed source objects against SAP before
approval. Class-based and other unsupported extraction routes fail explicitly.
The deterministic TABLE, DB_VIEW, and INFOSET analysis paths reject feedback
revisions; only function module analysis currently supports the analysis
feedback loop. Fabric plan feedback works for all supported rebuild routes.

Before live execution, verify the deployed provisioning notebook exit contract
in a nonproduction Fabric workspace. The repository's adapter tests do not
prove the deployed notebook. The API has no authentication layer yet; keep it on a trusted local
or internal network until authentication and authorization are added.

## Run locally

```powershell
python -m venv venv
.\venv\Scripts\python -m pip install -r requirements.txt
$env:PYTHONIOENCODING = 'utf-8'
.\venv\Scripts\fastapi dev
```

Open http://127.0.0.1:8000/docs for the API documentation, or http://127.0.0.1:8000/health for the health check.

For a production server, run `.\venv\Scripts\fastapi run`.

## Fabric REST API client

Set `FABRIC_TENANT_ID`, `FABRIC_CLIENT_ID`, and `FABRIC_CLIENT_SECRET` in the environment or the local `.env` file. Then use the client as a context manager:

```python
from thirdparty.fabric.utils import connect_to_fabric

with connect_to_fabric() as client:
    response = client.get("workspaces")
    response.raise_for_status()
    print(response.json())
```

The service principal must be allowed to use Fabric APIs in the tenant and have access to the resources it calls. Check each endpoint's documentation for service principal support.

To list lakehouses in a workspace (including paginated results):

```python
from thirdparty.fabric.utils import list_lakehouses

lakehouses = list_lakehouses("cfafbeb1-8037-4d0c-896e-a46fb27ff229")
# [{"id": "...", "name": "Bronze"}, ...]
```

## Configuration database metadata

Use Pydantic models for source table and column metadata. Share one connection
when creating both records so they commit or roll back together:

```python
from thirdparty.configdb.utils import (
    ConfigDB,
    SourceTableColumnCreate,
    SourceTableCreate,
    WatermarkControlCreate,
)

db = ConfigDB()
with db.connect() as connection:
    guid = db.insert_source_table(
        SourceTableCreate(
            connection_name="ORACLE-RPTP",
            source_table_name="PO.PO_HEADERS_ALL",
            data_source_type="Oracle",
        ),
        connection=connection,
    )
    db.insert_source_table_columns(
        guid,
        [
            SourceTableColumnCreate(
                sno=1,
                column_name="PO_HEADER_ID",
                source_data_type="NUMBER",
                fabric_data_type="DOUBLE",
            )
        ],
        connection=connection,
    )
    db.insert_watermark_control(
        WatermarkControlCreate(
            source_table_full_name="PO.PO_HEADERS_ALL",
            destination_table_full_name="bronze.po_headers_all",
            watermark_column="LAST_UPDATE_DATE",
            watermark_column_data_type="DATE",
        ),
        connection=connection,
    )
```

## Oracle database client

`OracleClient` uses `oracledb` Thick mode with the Instant Client for the
current platform. By default it reads `ORACLE_USER`, `ORACLE_PASSWORD`, and
`ORACLE_DSN` from the environment or `.env`:

```python
from thirdparty.oracle.utils import OracleClient

with OracleClient().connect() as connection:
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM DUAL")
        print(cursor.fetchone())
```

Run a read query and receive a JSON string containing `columns`, `rows`, and
`row_count`:

```python
client = OracleClient()
result_json = client.execute_sql(
    "SELECT TABLE_NAME FROM ALL_TABLES WHERE OWNER = :owner",
    {"owner": "PO"},
)

# Find indexes on LAST_UPDATED_DATE (the default) or another column.
indexes = client.get_index("ONT", "SALES_ORDER_HEADER")
order_id_indexes = client.get_index("ONT", "SALES_ORDER_HEADER", "ORDER_ID")

# Get column position, name, formatted data type, and description.
columns = client.get_table_schema("ONT", "SALES_ORDER_HEADER")
```

`execute_sql()` uses an Oracle read-only transaction and closes its connection
after fetching. Decimal values are JSON strings to preserve precision; binary
values use `{"base64": "..."}`. Use a database account with read-only grants
when SQL is provided by an untrusted caller.

For a source-specific connection, construct `OracleConnectionSettings` with
`username`, `password`, and either `dsn` or both `host` and `service_name`.
It also accepts `serviceName` and `clientPath` in JSON connection details.
The optional `client_path` (or `ORACLE_CLIENT_PATH`) overrides the bundled
Instant Client directory.

Install Oracle Instant Client separately; the native client files are excluded
from Git because they exceed repository file-size limits. Place it under
`thirdparty/oracle/native_dependencies/windows/instantclient_23_26` on Windows
or `thirdparty/oracle/native_dependencies/linux/instantclient_23_26` on Linux,
or set `ORACLE_CLIENT_PATH`. On Linux, configure that directory in the system
loader path (`ldconfig` or `LD_LIBRARY_PATH`) **before starting Python**.

## SAP metadata client

Set `SAP_HOST`, `SAP_HTTPS_PORT`, `SAP_USER`, `SAP_PASSWORD`, and `SAP_CLIENT` in `.env`. `SAP_HOST` may be a bare host or an `https://` origin. The client uses HTTPS and HTTP Basic authentication:

```python
from thirdparty.sap.utils import SAPClient

with SAPClient() as sap:
    fields = sap.read_metadata("DTFIGL_4")
    for field in fields:
        print(field.position, field.fieldname, field.datatype)
```

`read_metadata()` accepts a table, view, or structure name and returns a list of `SAPFieldMetadata` objects.

Run a SQL query with the same configured client:

```python
with SAPClient() as sap:
    result = sap.execute_sql_query(
        "SELECT MANDT, BUKRS, BUTXT FROM T001", max_rows=100
    )
    print(result.success, result.row_count)
    for row in result.rows:
        print(row)
```

`execute_sql_query()` returns a Pydantic `SAPSQLQueryResponse`. Each row is a dictionary because column names depend on the SQL query.

Scrape ABAP source and dependencies for an object:

```python
with SAPClient(timeout=120) as sap:
    scrape = sap.abap_code_scraper(
        "BWFID_GET_FIGL_ITEM", object_type="FM", max_depth=10, max_objects=500
    )
    print(scrape.root.name, scrape.stats.objects_returned)
    for item in scrape.objects:
        print(item.name, item.source_code)
```

`abap_code_scraper()` uses the scraper endpoint's query parameters and returns a Pydantic `ABAPScraperResponse` with a nested dependency tree.
