# Fabric API

A minimal FastAPI application with a health endpoint and interactive API documentation.

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
