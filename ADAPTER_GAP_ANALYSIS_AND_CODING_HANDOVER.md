# Coding Agent Handover
## Adapter Readiness for Temporal Migration Workflows

**Project:** Automated Oracle / SAP ECC to Microsoft Fabric Migration  
**Goal of this handover:** Complete and stabilize all integration adapters before implementing the four Temporal workflows.

---

# 1. Project Context

The application is intended to support four durable migration workflows:

1. **Oracle Table -> Microsoft Fabric**
2. **SAP Table -> Microsoft Fabric**
3. **SAP ODP DataSource -> Microsoft Fabric as-is**
4. **SAP DataSource -> Rebuild extractor logic in Microsoft Fabric**

The application uses:

- **Oracle utility** for Oracle metadata / discovery.
- **SAP ECC utility** for SAP metadata, DataSource metadata, DB view analysis, InfoSet analysis, and ABAP scraping.
- **Fabric utility** for Fabric management/execution APIs.
- **ConfigDB utility** for migration configuration, runtime replication state, and execution/audit records.
- **Temporal** for durable workflow orchestration.
- **Arize Phoenix** for LLM/tool tracing and evaluation.
- A **parameterized Fabric notebook** for deterministic Fabric object provisioning.
- Existing **dedicated replication pipelines** for physical table / ODP data replication.

## Core architectural rule

```text
Source adapter
    -> discover deterministic metadata

LLM adapter
    -> interpret only where deterministic inspection is insufficient

User
    -> review / approve

Config DB
    -> persist approved structured runtime configuration

Fabric utility
    -> trigger and monitor notebook / pipeline execution

Fabric notebook
    -> read Config DB by plan_guid and create approved Fabric objects

Replication pipeline
    -> read Config DB and replicate enabled objects

Temporal
    -> coordinate all steps, waits, retries, approvals, and execution state
```

Do **not** put workflow decisions inside the low-level adapters.

---

# 2. Current Utilities Reviewed

The following existing capabilities are present.

## SAP utility

Current methods:

```python
SAPClient.read_metadata(object_name)
SAPClient.execute_sql_query(sql, max_rows=100)
SAPClient.get_datasource_details(datasource_name)
SAPClient.get_infoset_query_details(infoset_query)
SAPClient.get_dbview_query_details(dbview_name)
SAPClient.abap_code_scraper(
    name,
    object_type="FM",
    recursive=True,
    max_depth=10,
    max_objects=500,
)
```

Current typed models include:

- `SAPFieldMetadata`
- `SAPDatasourceDetails`
- `SAPDatasourceType`
- `SAPExtractionMethod`
- `SAPInfoSetQueryDetails`
- `SAPDBViewQueryDetails`
- `ABAPScraperResponse`
- dependency / source object models

## Oracle utility

Current methods:

```python
OracleClient.connect()
OracleClient.execute_sql(sql, parameters=None)
OracleClient.get_index(schema_name, table_name, column_name)
OracleClient.get_table_schema(schema_name, table_name)
```

Current typed models include:

- `OracleConnectionSettings`
- `OracleQueryResult`
- `OracleIndex`
- `OracleTableColumn`

## Fabric utility

Current functionality:

```python
connect_to_fabric()
list_lakehouses(workspace_id)
```

Authentication is via `ClientSecretCredential`.

## ConfigDB utility

Current functionality:

```python
ConfigDB.connect()
ConfigDB.list_fabric_workspaces()
ConfigDB.insert_source_table(...)
ConfigDB.insert_source_table_columns(...)
ConfigDB.insert_watermark_control(...)
```

The current implementation targets the **old Config DB structure** and must be updated to the new schema before workflow development.

---

# 3. Overall Readiness Assessment

| Adapter | Current readiness | Main issue |
|---|---:|---|
| SAP | High for discovery, incomplete for routing/ODP validation | Missing explicit object-resolution, ODP metadata, typed wrapper methods, and unsupported-method handling |
| Oracle | Medium | Missing PK/unique-key/table validation/watermark candidate metadata |
| Fabric | Low | Only authentication + lakehouse listing exists; notebook/pipeline execution APIs are missing |
| ConfigDB | Low against new schema | Current methods target old `SourceTables`/`watermark_control` model |
| LLM / Phoenix | Not implemented in supplied utilities | Required for SAP Function Module rebuild analysis and Fabric plan generation |

**Recommendation:** do not begin Temporal workflow implementation until all P0 adapter items in this handover are completed and covered by tests.

---

# 4. Shared Adapter Design Requirements

All adapters must follow these rules.

## 4.1 Typed contracts

Use Pydantic models for externally consumed request/response contracts.

Avoid returning:

- raw tuples
- arbitrary dictionaries
- unvalidated JSON
- JSON strings when a typed model can be returned

## 4.2 Adapter responsibilities

Adapters answer:

> "How do I communicate with this external system?"

They must not answer:

> "What migration approach should the workflow choose?"

For example:

```python
sap.get_datasource_details(...)
```

is correct.

This is not:

```python
sap.decide_fabric_architecture(...)
```

## 4.3 Temporal compatibility

External I/O belongs in Temporal **activities**, not workflow code.

Adapters must:

- perform bounded calls;
- not sleep indefinitely;
- not contain infinite polling;
- return serializable typed results;
- raise meaningful exceptions.

## 4.4 Validation

Every external identifier passed to an adapter must be validated before constructing URLs / SQL.

## 4.5 Observability

Methods should allow callers to attach correlation identifiers where useful:

```text
plan_guid
temporal_workflow_id
temporal_run_id
batch_run_id
```

Do not couple low-level adapters directly to Temporal SDK types.

---

# 5. SAP Adapter - Detailed Gap Analysis

## 5.1 What already works

### Generic object metadata

```python
read_metadata(object_name)
```

This is useful for:

- SAP table metadata
- DB view metadata
- extract structure metadata

The response already contains:

```text
fieldname
position
keyflag
rollname
datatype
leng
decimals
checktable
reftable
reffield
ddtext
```

This is a strong foundation.

### DataSource details

```python
get_datasource_details(datasource_name)
```

It returns:

```text
DATASOURCE_ID
OBJVERS
TYPE
APPLICATION
EXTRACT_STRUCTURE
EXTRACTOR
EXTRACTION_METHOD
DELTA
DATASOURCE_NAME
```

This is essential for SAP workflows 3 and 4.

### InfoSet support

```python
get_infoset_query_details(...)
```

Already returns:

```text
infoset
sqlTemplate
sqlTemplateComplete
tables
```

### DB view support

```python
get_dbview_query_details(...)
```

Already returns:

```text
view
sqlQuery
sqlQueryComplete
tables
filters
```

### Function Module / ABAP support

```python
abap_code_scraper(...)
```

Already provides:

- root ABAP object
- dependency tree
- all scraped objects
- source code
- warnings
- visited/returned counts

This is the primary deterministic input for the LLM analysis branch.

---

# 6. SAP Adapter - Required Additions

## P0 - Required before workflows

### SAP-01: Connection health check

Add:

```python
def test_connection(self) -> SAPConnectionStatus:
    ...
```

Suggested implementation:

- call a safe lightweight SAP endpoint/query;
- return system/client information when possible;
- do not expose credentials.

Suggested model:

```python
class SAPConnectionStatus(BaseModel):
    success: bool
    sap_client: str
    message: str | None = None
```

---

### SAP-02: Resolve SAP object type

Extraction method `V` means transparent table or DB view.

The workflow must not guess which it is.

Add:

```python
def get_object_type(self, object_name: str) -> SAPObjectTypeInfo:
    ...
```

Expected output:

```python
class SAPObjectTypeInfo(BaseModel):
    object_name: str
    object_type: Literal[
        "TRANSPARENT_TABLE",
        "DB_VIEW",
        "STRUCTURE",
        "UNKNOWN",
    ]
```

Use deterministic SAP dictionary metadata.

This method is critical for:

```text
SAP DataSource rebuild
    extraction method V
        ->
    table?
        -> replicate physical table
    view?
        -> inspect DB view and replicate dependencies
```

---

### SAP-03: Dedicated table metadata wrapper

`read_metadata()` can remain generic, but provide a typed semantic method:

```python
def get_table_schema(self, table_name: str) -> SAPTableSchema:
    ...
```

Return:

```python
class SAPTableSchema(BaseModel):
    table_name: str
    columns: list[SAPFieldMetadata]
    primary_key_columns: list[str]
```

`keyflag` can be used to derive key columns.

The workflow should not repeatedly reconstruct this information itself.

---

### SAP-04: Get DataSource exposed fields

Extract structure fields and **DataSource-exposed fields are not always the same contract**.

Add:

```python
def get_datasource_fields(
    self,
    datasource_name: str,
) -> list[SAPDatasourceField]:
    ...
```

Read from the appropriate SAP DataSource metadata, e.g. `ROOSFIELD` for the currently targeted ECC extractor model.

Suggested output should include at least:

```text
datasource
field_name
position
selection capability if available
hidden / active flags if available
field text
```

The exact attributes should follow what the SAP system exposes.

Workflow usage:

```text
SAP ODP as-is
    -> DataSource exposed columns

SAP rebuild
    -> compare:
       extract structure fields
       exposed DataSource fields
       LLM-derived source mapping
```

---

### SAP-05: Explicit ODP capability lookup

Add a typed method:

```python
def get_odp_capability(
    self,
    datasource_name: str,
) -> SAPODPCapability:
    ...
```

Use the SAP metadata already identified by the project, including the ODP capability source such as `ROOSATTR` where applicable to the ECC system.

Return at minimum:

```python
class SAPODPCapability(BaseModel):
    datasource_name: str
    odp_capable: bool
    context: str | None = None
    raw_attributes: dict[str, JsonValue] = {}
```

This is required for workflow 3.

Do not infer ODP support purely from DataSource name.

---

### SAP-06: Delta metadata wrapper

`SAPDatasourceDetails.DELTA` currently provides only the DataSource delta indicator.

Provide:

```python
def get_datasource_delta_details(
    self,
    datasource_name: str,
) -> SAPDeltaDetails:
    ...
```

Return the deterministic metadata available from SAP.

At minimum:

```text
delta_supported
delta_indicator
delta_mechanism / process when deterministically available
```

If detailed delta semantics are unavailable, explicitly mark them unresolved.

Do not invent delta behavior.

---

### SAP-07: Extraction-method resolver

Add a pure helper/service-level typed resolver:

```python
def resolve_datasource_extraction(
    details: SAPDatasourceDetails,
    object_info: SAPObjectTypeInfo | None = None,
) -> SAPExtractionRoute:
    ...
```

This may live outside `SAPClient` because it is deterministic business mapping rather than I/O.

Routes:

```text
TABLE
DB_VIEW
INFOSET_QUERY
FUNCTION_MODULE
CLASS_BASED
DOMAIN_FIXED_VALUES
ODP_CURSOR
UNSUPPORTED
```

For function modules map:

```text
F1
F2
FS
```

to `FUNCTION_MODULE`.

Do not let Temporal workflows duplicate this mapping.

---

### SAP-08: Class-based extractor support decision

Current enum supports:

```text
CA = Class-Based BI Agent
```

but the handover does not show a guaranteed class scraper contract.

Implement one of:

```python
def scrape_abap_class(...):
    ...
```

or verify that:

```python
abap_code_scraper(name, object_type="CLASS")
```

fully supports class/method traversal.

Add tests.

If unsupported, return a formal:

```text
UNSUPPORTED_EXTRACTION_METHOD
```

instead of allowing the workflow to fail later.

---

### SAP-09: ABAP scraper completeness information

Enhance/wrap scraper results so the workflow can know whether analysis is safe to present.

Add derived flags:

```python
class ABAPScrapeAssessment(BaseModel):
    response: ABAPScraperResponse
    truncated: bool
    unresolved_dependencies: list[ABAPObjectReference]
    warnings: list[str]
```

`max_depth`, `max_objects`, warnings, and failed dependencies must be converted into a clear completeness signal.

The LLM must know if the ABAP context is incomplete.

---

### SAP-10: Batch metadata lookup

When the LLM identifies 10 tables, do not make workflow code call one at a time manually.

Add:

```python
def get_objects_metadata(
    self,
    object_names: Sequence[str],
) -> dict[str, list[SAPFieldMetadata]]:
    ...
```

A sequential implementation is acceptable initially, but provide a single adapter contract.

---

## P1 - Strongly recommended

### SAP-11: Search/list tables

Useful for UI selection:

```python
search_tables(search_text, limit=...)
```

### SAP-12: Search/list DataSources

Useful for UI:

```python
search_datasources(search_text, limit=...)
```

### SAP-13: Validate object existence

```python
object_exists(name)
datasource_exists(name)
```

### SAP-14: Read source sample

Optional debugging / user validation:

```python
preview_table(name, max_rows=20)
```

Keep strict row limits.

---

# 7. Oracle Adapter - Detailed Gap Analysis

## 7.1 What already works

Current adapter supports:

- explicit connection settings;
- thick-mode Oracle Instant Client;
- safe read-only query execution;
- index lookup for a specific column;
- table schema with formatted Oracle datatype and comments.

This is enough to connect, but not enough to fully configure a generic replication workflow.

---

# 8. Oracle Adapter - Required Additions

## P0 - Required before workflows

### ORA-01: Connection health check

Add:

```python
def test_connection(self) -> OracleConnectionStatus:
    ...
```

Use:

```sql
SELECT 1 FROM DUAL
```

Return a typed status.

---

### ORA-02: Table existence / validation

Add:

```python
def table_exists(
    self,
    schema_name: str,
    table_name: str,
) -> bool:
    ...
```

Optionally add:

```python
def get_table_info(...) -> OracleTableInfo:
```

Return owner/table name and relevant metadata.

---

### ORA-03: Primary key metadata

This is a major current gap.

Add:

```python
def get_primary_key(
    self,
    schema_name: str,
    table_name: str,
) -> OracleKey | None:
    ...
```

Suggested model:

```python
class OracleKeyColumn(BaseModel):
    name: str
    position: int

class OracleKey(BaseModel):
    constraint_name: str
    columns: list[OracleKeyColumn]
```

Use Oracle dictionary constraint metadata.

Support **composite keys**.

Do not store a generic single `merge_key_column` assumption.

---

### ORA-04: Unique keys

If no PK exists, users may need a candidate merge key.

Add:

```python
def get_unique_keys(
    self,
    schema_name: str,
    table_name: str,
) -> list[OracleKey]:
    ...
```

The workflow/user can decide whether one is appropriate.

---

### ORA-05: Rich table column metadata

Current `OracleTableColumn` only returns:

```text
id
column_name
datatype
description
```

Extend the model / query to include structured fields:

```text
data_type
data_length
char_length
precision
scale
nullable
default expression where useful
```

Do not rely only on the formatted datatype string for type mapping.

Suggested model:

```python
class OracleTableColumn(BaseModel):
    position: int
    column_name: str
    data_type: str
    data_length: int | None
    char_length: int | None
    precision: int | None
    scale: int | None
    nullable: bool
    description: str | None
```

If backward compatibility is required, expose a formatted property.

---

### ORA-06: Watermark candidate discovery

Do not hard-code only `LAST_UPDATED_DATE`.

Add:

```python
def get_watermark_candidates(
    self,
    schema_name: str,
    table_name: str,
) -> list[OracleWatermarkCandidate]:
    ...
```

Candidates should be deterministic metadata suggestions, not final decisions.

Consider:

- DATE
- TIMESTAMP variants
- numeric sequence columns where explicitly configured
- common update timestamp names

Return:

```text
column
datatype
indexed?
nullable?
reason
```

The workflow/user chooses the actual watermark.

---

### ORA-07: General index metadata

Current:

```python
get_index(..., column_name)
```

is useful but narrow.

Add:

```python
def get_indexes(
    self,
    schema_name: str,
    table_name: str,
) -> list[OracleIndexDefinition]:
    ...
```

Return:

```text
index name
uniqueness
index type
ordered columns
column positions
```

Then implement:

```python
is_column_indexed(...)
```

as a helper.

---

### ORA-08: Type-mapping contract

Do not hard-code Oracle -> Fabric mapping inside workflow code.

Create a deterministic mapper:

```python
def map_oracle_type_to_fabric(
    column: OracleTableColumn,
) -> FabricColumnType:
    ...
```

This can live in a shared `datatype_mapping` module rather than `OracleClient`.

It must handle at least:

```text
VARCHAR2
NVARCHAR2
CHAR
NCHAR
NUMBER(p,s)
FLOAT
BINARY_FLOAT
BINARY_DOUBLE
DATE
TIMESTAMP
TIMESTAMP WITH TIME ZONE
CLOB
NCLOB
BLOB
RAW
```

Return unsupported mappings explicitly.

---

## P1 - Strongly recommended

### ORA-09: List/search schemas

```python
list_schemas()
```

### ORA-10: List/search tables

```python
list_tables(schema_name, search_text=None)
```

### ORA-11: Estimated row count / table statistics

Useful for planning initial loads:

```python
get_table_statistics(...)
```

Do not perform `COUNT(*)` on huge tables merely for UI.

### ORA-12: Preview data

Strictly limited:

```python
preview_table(..., max_rows=20)
```

---

# 9. Shared Data Type Mapping Utility

This should be implemented before workflows.

Create:

```text
integrations/
    datatype_mapping/
        oracle_to_fabric.py
        sap_to_fabric.py
        models.py
```

Suggested contract:

```python
class FabricTypeMapping(BaseModel):
    source_type: str
    fabric_type: str
    supported: bool
    lossy: bool = False
    warning: str | None = None
```

Methods:

```python
map_oracle_column(column)
map_sap_field(field)
```

Do not ask the LLM to perform deterministic datatype mappings.

The approved mapping is persisted into:

```text
SourceTableColumns.SourceDataType
SourceTableColumns.FabricDataType
```

---

# 10. Fabric Adapter - Detailed Gap Analysis

## 10.1 What already works

Current Fabric utility supports:

- service principal authentication;
- token acquisition;
- reusable authenticated `httpx.Client`;
- Lakehouse listing with continuation-token pagination.

This is only a small part of the required execution adapter.

---

# 11. Fabric Adapter - Required Additions

## P0 - Required before workflows

### FAB-01: Refactor into `FabricClient`

Recommended shape:

```python
class FabricClient:
    ...
```

It may internally reuse the existing auth class/context manager.

Avoid a growing collection of unrelated module-level functions.

---

### FAB-02: List workspaces

Add:

```python
def list_workspaces(self) -> list[FabricWorkspace]:
    ...
```

Requirements:

- use normal Fabric Core workspace API;
- support pagination;
- do not use the tenant-admin endpoint for ordinary workflow behavior.

---

### FAB-03: Get/validate workspace

Add:

```python
def get_workspace(
    self,
    workspace_id: str,
) -> FabricWorkspace:
    ...
```

and/or:

```python
def validate_workspace(...) -> bool
```

---

### FAB-04: Keep and move Lakehouse listing into client

Current capability:

```python
list_lakehouses(workspace_id)
```

Preserve pagination behavior.

Add:

```python
get_lakehouse(workspace_id, lakehouse_id)
find_lakehouse_by_name(workspace_id, display_name)
```

Prefer IDs in runtime configuration.

---

### FAB-05: Notebook item validation

Add:

```python
def get_notebook(
    self,
    workspace_id: str,
    notebook_id: str,
) -> FabricNotebook:
    ...
```

Optional UI helper:

```python
list_notebooks(workspace_id)
```

The workflow should validate the provisioning notebook before submission.

---

### FAB-06: Trigger parameterized notebook

This is a critical missing capability.

Add:

```python
def run_notebook(
    self,
    workspace_id: str,
    notebook_id: str,
    *,
    parameters: Mapping[str, FabricJobParameter] | None = None,
) -> FabricJobSubmission:
    ...
```

Primary project usage:

```python
run_notebook(
    workspace_id,
    notebook_id,
    parameters={
        "plan_guid": FabricJobParameter(
            value=str(plan_guid),
            type="Text",
        )
    },
)
```

The adapter must capture:

```text
HTTP 202
Location header
Retry-After header
job instance id
```

Do not wait for job completion in this method.

---

### FAB-07: Notebook job status

Add:

```python
def get_notebook_run(
    self,
    workspace_id: str,
    notebook_id: str,
    job_instance_id: str,
) -> FabricJobInstance:
    ...
```

Return at minimum:

```text
id
item_id
job_type
invoke_type
status
start_time_utc
end_time_utc
failure_reason
exit_value when available
retry_after_seconds
```

Temporal will handle timers between status checks.

---

### FAB-08: Cancel notebook execution

Add:

```python
cancel_notebook_run(...)
```

Useful for user cancellation / Temporal cancellation.

---

### FAB-09: Pipeline item validation

Add:

```python
def get_pipeline(
    self,
    workspace_id: str,
    pipeline_id: str,
) -> FabricPipeline:
    ...
```

Optional:

```python
list_pipelines(...)
```

---

### FAB-10: Trigger existing replication pipeline

Add:

```python
def run_pipeline(
    self,
    workspace_id: str,
    pipeline_id: str,
    *,
    execution_data: Mapping[str, object] | None = None,
) -> FabricJobSubmission:
    ...
```

Do not pass full table configuration if the pipeline already reads Config DB.

If the existing pipeline supports scoping by plan:

```text
plan_guid
```

may be passed as a parameter/config value.

Otherwise trigger it normally and let it query enabled objects.

---

### FAB-11: Pipeline status

Add:

```python
def get_pipeline_run(
    self,
    workspace_id: str,
    pipeline_id: str,
    job_instance_id: str,
) -> FabricJobInstance:
    ...
```

Capture:

```text
status
start/end times
failureReason
rootActivityId when present
Retry-After
```

---

### FAB-12: Standard job status enum

Normalize Fabric status without discarding the raw value.

Example:

```python
class FabricJobStatus(StrEnum):
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"
```

Model:

```python
class FabricJobInstance(BaseModel):
    ...
    status: FabricJobStatus
    raw_status: str
```

---

### FAB-13: Error model

Add:

```python
class FabricError(Exception): ...
class FabricAuthenticationError(FabricError): ...
class FabricPermissionError(FabricError): ...
class FabricNotFoundError(FabricError): ...
class FabricRateLimitError(FabricError): ...
class FabricJobSubmissionError(FabricError): ...
```

Include Fabric correlation/root activity IDs when returned.

Never include access tokens or secrets.

---

### FAB-14: `Retry-After` support

Return retry timing to the caller.

Do not sleep for long periods inside the adapter.

Temporal should use durable timers.

---

## P1 - Recommended

### FAB-15: List/get generic Fabric items

A reusable generic item lookup may simplify notebook/pipeline validation.

### FAB-16: Managed identity authentication

Keep service principal now; design auth so Managed Identity can be added later without changing callers.

### FAB-17: Lakehouse creation

Not required if the workflow assumes approved target Lakehouses already exist.

Only implement if product requirements later allow creating Lakehouses automatically.

---

# 12. ConfigDB Adapter - Detailed Gap Analysis

## 12.1 Major issue

The current `ConfigDB` utility targets the **previous schema**.

Current models/methods are limited to:

```text
FabricWorkspaces
SourceTables
SourceTableColumns
watermark_control
```

The new database design introduced:

```text
DBConnections
FabricWorkspaces

MigrationPlans
SAPAnalysis
SAPAnalysisObjects

SourceTables
SourceTableColumns

ReplicationConfig
ReplicationState

FabricViews
FabricViewDependencies

BatchRuns
BatchObjectRuns
```

Therefore the current ConfigDB utility must be substantially updated before workflows.

---

# 13. ConfigDB Adapter - Required Additions

## P0 - Required before workflows

### CFG-01: DB connection records

Models:

```python
DBConnectionCreate
DBConnection
```

Methods:

```python
list_connections(...)
get_connection(connection_name)
insert_connection(...)
update_connection(...)
```

Important:

- `ConnectionDetails` is JSON.
- avoid persisting plaintext passwords if secrets can be stored elsewhere.
- the utility may return connection metadata needed to construct source adapters.

---

### CFG-02: Migration plan CRUD

Models:

```python
MigrationPlanCreate
MigrationPlan
MigrationPlanUpdate
```

Methods:

```python
create_migration_plan(...)
get_migration_plan(plan_guid)
update_migration_plan_status(...)
update_migration_plan_version(...)
record_plan_approval(...)
set_temporal_ids(...)
```

Recommended plan status enum:

```text
DRAFT
WAITING_ANALYSIS_APPROVAL
ANALYSIS_APPROVED
WAITING_PLAN_APPROVAL
APPROVED
READY_TO_PROVISION
PROVISIONING
PROVISIONED
FAILED
REJECTED
SUPERSEDED
```

---

### CFG-03: SAP analysis persistence

Models:

```python
SAPAnalysisCreate
SAPAnalysisRecord
SAPAnalysisObjectCreate
```

Methods:

```python
create_sap_analysis(...)
insert_sap_analysis_objects(...)
get_sap_analysis(...)
list_sap_analysis_objects(...)
record_analysis_approval(...)
supersede_analysis(...)
```

Must support versioning.

Do not overwrite an already approved analysis version in place.

---

### CFG-04: Source table persistence - update to new model

Current `SourceTableCreate` is outdated.

Replace/extend with fields matching the new schema:

```text
PlanGUID
ConnectionName
SourceSystemType
SourceObjectType
SourceSchemaName
SourceTableName
ParentSourceObjectGUID
DataSourceType
ApplicationComponent
Delta
ReplicationMethod
ObjectRole
FabricWorkspaceName
FabricWorkspaceId
FabricLakehouseName
FabricLakehouseId
FabricLakehouseSchema
FabricTableName
ProvisioningStatus
IsActive
```

Methods:

```python
insert_source_table(...)
get_source_table(...)
list_source_tables_for_plan(...)
update_source_table_provisioning_status(...)
```

---

### CFG-05: Column persistence - update to new model

Current column model only includes:

```text
sno
column_name
source_data_type
fabric_data_type
description
```

Extend to:

```text
TargetColumnName
IsPrimaryKey
IsNullable
IsWatermarkCandidate
IsSelected
SourceExpression
TransformationNotes
```

Methods:

```python
insert_source_table_columns(...)
list_source_table_columns(...)
```

---

### CFG-06: Replication config

Replace use of `insert_watermark_control()` in new workflow code.

Create models:

```python
ReplicationConfigCreate
ReplicationConfigRecord
```

Methods:

```python
create_replication_config(...)
get_replication_config(source_table_guid)
update_replication_config(...)
enable_replication(...)
disable_replication(...)
```

Fields must support:

```text
IncrementalMethod:
    FULL
    WATERMARK
    SAP_ODP_DELTA
    CDC

WriteStrategy:
    APPEND
    UPSERT
    SCD1
    SCD2
    REPLACE

WatermarkColumn
WatermarkColumnDataType
PrimaryKeyColumns
MergeKeyColumns
SCD fields
MaxRowFetch
PipelineWorkspaceId
PipelineItemId
```

Keys are arrays/JSON where appropriate.

---

### CFG-07: Replication state

Methods:

```python
create_replication_state(...)
get_replication_state(...)
mark_replication_started(...)
mark_replication_succeeded(...)
mark_replication_failed(...)
update_watermark(...)
```

The last watermark is mutable **state**, not static replication configuration.

---

### CFG-08: Fabric view definitions

Models:

```python
FabricViewCreate
FabricViewRecord
```

Methods:

```python
insert_fabric_view(...)
get_fabric_view(...)
list_fabric_views_for_plan(...)
update_view_provisioning_status(...)
```

Persist approved SQL only.

Do not store unapproved model suggestions as executable runtime config.

---

### CFG-09: Fabric view dependencies

Methods:

```python
insert_fabric_view_dependencies(...)
list_fabric_view_dependencies(view_guid)
```

Dependency types:

```text
SOURCE_TABLE
FABRIC_VIEW
EXISTING_OBJECT
```

Must support dependency order.

---

### CFG-10: Batch run logging

Models:

```python
BatchRunCreate
BatchRun
BatchObjectRunCreate
BatchObjectRun
```

Methods:

```python
start_batch_run(...)
complete_batch_run(...)
fail_batch_run(...)

start_batch_object_run(...)
complete_batch_object_run(...)
fail_batch_object_run(...)
```

Store correlation IDs:

```text
PlanGUID
TemporalWorkflowId
TemporalRunId
FabricJobInstanceId
FabricPipelineRunId
FabricNotebookRunId
```

---

### CFG-11: Atomic approved-plan persistence

This is extremely important.

Implement a service/repository method:

```python
def persist_approved_runtime_plan(
    plan: ApprovedRuntimePlan,
) -> PersistedRuntimePlan:
    ...
```

It must use **one DB transaction** for:

```text
MigrationPlan final update
SourceTables
SourceTableColumns
ReplicationConfig
initial ReplicationState
FabricViews
FabricViewDependencies
```

If any insert fails:

```text
ROLLBACK
```

Do not leave a partially approved migration plan.

This method may belong in a higher-level repository/service rather than the raw `ConfigDB` class, but implement the transactional capability now.

---

### CFG-12: Query helpers for approved plan

Add:

```python
get_approved_plan_runtime_config(plan_guid)
```

Useful for testing and API responses.

The Fabric notebook may query DB directly and does not need to use this Python method.

---

### CFG-13: Transaction helper

Expose a safe transaction context manager:

```python
@contextmanager
def transaction(self):
    ...
```

Commit only on success.

Rollback on exception.

Current optional-connection approach should be cleaned up and explicitly tested.

---

## P1 - Recommended

### CFG-14: Optimistic version checks

When approving analysis/plan versions, ensure the user is approving the expected current version.

### CFG-15: Idempotency keys

Allow workflow activity retries without duplicate inserts.

Potential keys:

```text
PlanGUID + version
SourceTableGUID
ViewGUID
BatchRunId
```

### CFG-16: Query runtime views

Optional convenience methods for:

```text
vw_ApprovedPlansReadyToProvision
vw_TableProvisioningQueue
vw_ViewProvisioningQueue
vw_ReplicationQueue
```

---

# 14. LLM / AI Analysis Adapter - Required New Utility

The SAP Function Module rebuild path requires an explicit model adapter.

Do not scatter SDK calls throughout Temporal activities.

Create:

```text
integrations/
    llm/
        client.py
        models.py
        prompts.py
        tracing.py
```

Recommended public class:

```python
class MigrationAnalysisClient:
    ...
```

---

# 15. LLM Adapter - Required Methods

## LLM-01: Analyze SAP extractor

```python
def analyze_sap_extractor(
    self,
    request: SAPExtractorAnalysisRequest,
) -> SAPExtractorAnalysis:
    ...
```

Request contains:

```text
DataSource metadata
extract structure metadata
DataSource exposed fields
ABAP dependency tree
all scraped ABAP source objects
scraper warnings/completeness
```

Return structured output.

Suggested model:

```python
class IdentifiedSAPObject(BaseModel):
    name: str
    object_type: str
    role: str
    required: bool
    evidence: list[str]
    confidence: str
    notes: str | None = None

class JoinDefinition(BaseModel):
    left_object: str
    right_object: str
    condition: str
    evidence: list[str]

class FilterDefinition(BaseModel):
    expression: str
    evidence: list[str]

class DerivedFieldDefinition(BaseModel):
    target_field: str
    logic: str
    evidence: list[str]

class SAPExtractorAnalysis(BaseModel):
    source_tables: list[IdentifiedSAPObject]
    source_views: list[IdentifiedSAPObject]
    lookup_tables: list[IdentifiedSAPObject]
    configuration_tables: list[IdentifiedSAPObject]
    joins: list[JoinDefinition]
    filters: list[FilterDefinition]
    derived_fields: list[DerivedFieldDefinition]
    delta_logic: list[str]
    ignored_objects: list[IdentifiedSAPObject]
    unresolved_items: list[str]
    warnings: list[str]
```

The model output must be schema-validated.

---

## LLM-02: Revise analysis from human feedback

```python
def revise_sap_extractor_analysis(
    self,
    *,
    previous_analysis: SAPExtractorAnalysis,
    user_feedback: str,
    source_context: SAPExtractorAnalysisRequest,
) -> SAPExtractorAnalysis:
    ...
```

Never mutate the approved version.

Return a new analysis candidate/version.

---

## LLM-03: Generate Fabric implementation plan

```python
def generate_fabric_plan(
    self,
    request: FabricPlanRequest,
) -> FabricImplementationPlan:
    ...
```

Input:

```text
approved SAP analysis
target Fabric context
known existing Fabric objects if available
datatype mappings
project conventions
```

Output:

```text
tables to replicate
tables to reuse
target table mappings
replication strategy proposals
views to create
view SQL / transformation logic
dependencies
creation order
unresolved decisions
```

---

## LLM-04: Revise Fabric plan

```python
revise_fabric_plan(
    previous_plan,
    user_feedback,
    source_context,
)
```

---

## LLM-05: Phoenix tracing

Instrument model calls with OpenTelemetry / Phoenix-compatible tracing.

At minimum trace:

```text
operation
plan_guid
analysis_version
plan_version
model
prompt/template version
tool calls
latency
token usage when available
result status
```

Do not log:

```text
passwords
Fabric client secret
Oracle password
SAP password
connection secrets
```

Store `PhoenixTraceId` into Config DB analysis records.

---

# 16. Deterministic LLM Output Verification

LLM output must never directly become executable runtime config.

For SAP Function Module analysis:

```text
LLM identifies objects
        ->
SAP adapter validates each object
        ->
metadata is enriched
        ->
user reviews
        ->
approved analysis
```

Required verification helper/service:

```python
verify_llm_identified_sap_objects(...)
```

For every LLM-identified table/view:

1. verify it exists;
2. resolve its object type;
3. fetch metadata;
4. if it is a DB view, fetch DB view query/dependencies;
5. flag hallucinated/unresolved objects.

Only verified objects can be included automatically in the proposed Fabric plan.

---

# 17. Workflow-to-Adapter Capability Matrix

Legend:

```text
EXISTING = already available
ADD-P0   = must implement before workflows
ADD-P1   = recommended
N/A      = not needed
```

| Capability | Oracle Table | SAP Table | SAP ODP | SAP Rebuild |
|---|---:|---:|---:|---:|
| connection test | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| source table schema | EXISTING / enrich | EXISTING via read_metadata | N/A | EXISTING |
| primary key | ADD-P0 | derivable / wrapper ADD-P0 | DataSource-specific | ADD-P0 for identified tables |
| unique keys | ADD-P0 | optional | N/A | optional |
| index metadata | partial -> ADD-P0 | optional | N/A | optional |
| watermark candidates | ADD-P0 | ADD-P0 | N/A | ADD-P0 for base tables |
| DataSource details | N/A | N/A | EXISTING | EXISTING |
| DataSource exposed fields | N/A | N/A | ADD-P0 | ADD-P0 |
| ODP capability | N/A | N/A | ADD-P0 | useful |
| delta details | N/A | N/A | ADD-P0 | ADD-P0 where applicable |
| object table/view resolver | N/A | useful | N/A | ADD-P0 |
| DB view SQL/dependencies | N/A | N/A | N/A | EXISTING |
| InfoSet SQL/dependencies | N/A | N/A | N/A | EXISTING |
| ABAP scraper | N/A | N/A | N/A | EXISTING |
| scraper completeness | N/A | N/A | N/A | ADD-P0 |
| LLM extractor analysis | N/A | N/A | N/A | ADD-P0 |
| Fabric plan LLM | optional | optional | optional | ADD-P0 |
| datatype mapper | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| create plan config | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| persist tables/columns | current but refactor | current but refactor | current but refactor | current but refactor |
| replication config/state | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| view config/deps | N/A | N/A | N/A | ADD-P0 |
| trigger notebook | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| monitor notebook | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| trigger pipeline | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| monitor pipeline | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |
| batch logging | ADD-P0 | ADD-P0 | ADD-P0 | ADD-P0 |

---

# 18. Four Workflow Contracts After Adapter Completion

Do **not** implement the workflows yet as part of this handover. The following contracts define what the adapter layer must be able to support.

---

## Workflow A - Oracle Table -> Fabric

Adapters must support:

```text
Oracle:
    test connection
    validate table
    get rich schema
    get PK / unique keys
    get indexes
    get watermark candidates

Mapping:
    Oracle -> Fabric datatypes

ConfigDB:
    create plan
    persist approved table/columns
    create replication config/state
    log batch execution

Fabric:
    validate target
    trigger provisioning notebook(plan_guid)
    monitor notebook
    trigger replication pipeline
    monitor pipeline
```

---

## Workflow B - SAP Table -> Fabric

Adapters must support:

```text
SAP:
    test connection
    validate table
    get table schema + key fields
    determine watermark candidates

Mapping:
    SAP -> Fabric datatypes

ConfigDB:
    same physical table/runtime APIs

Fabric:
    same notebook/pipeline execution APIs
```

---

## Workflow C - SAP ODP DataSource -> Fabric

Adapters must support:

```text
SAP:
    test connection
    DataSource details
    ODP capability
    exposed DataSource fields
    extract structure metadata
    delta details

Mapping:
    SAP -> Fabric datatype mapping

ConfigDB:
    source object = SAP_DATASOURCE
    replication method = SAP_ODP
    replication config/state

Fabric:
    provision target table
    trigger/monitor ODP replication pipeline
```

---

## Workflow D - SAP DataSource Rebuild -> Fabric

Adapters must support:

```text
SAP:
    DataSource details
    extract structure metadata
    exposed DataSource fields
    extraction-route resolution

Route: table
    -> table metadata

Route: DB view
    -> view SQL
    -> dependency tables

Route: InfoSet
    -> SQL template
    -> tables

Route: Function Module
    -> recursive ABAP scrape
    -> completeness assessment
    -> LLM structured analysis
    -> deterministic verification of LLM objects

Route: Class based
    -> scraper support or explicit unsupported result

LLM:
    extractor analysis
    analysis revision
    Fabric-plan generation
    Fabric-plan revision
    Phoenix tracing

ConfigDB:
    versioned SAP analysis
    analysis objects
    user approval
    migration plan
    physical tables/columns
    replication config/state
    Fabric views
    view dependencies
    batch audit

Fabric:
    notebook execution
    notebook status
    pipeline execution
    pipeline status
```

---

# 19. Recommended Project Layout

Refactor incrementally toward:

```text
backend/
  integrations/
    oracle/
      client.py
      models.py
      exceptions.py

    sap/
      client.py
      models.py
      routing.py
      exceptions.py

    fabric/
      auth.py
      client.py
      models.py
      exceptions.py

    llm/
      client.py
      models.py
      prompts.py
      tracing.py

    datatype_mapping/
      models.py
      oracle.py
      sap.py

  repositories/
    config_db/
      repository.py
      models.py

  services/
    sap_analysis_service.py
    migration_plan_service.py
    fabric_provisioning_service.py

  temporal/
    # Do not implement until adapter readiness is complete.
```

Existing single-file utilities may remain temporarily, but public contracts should move toward these boundaries.

---

# 20. Priority Implementation Order

Implement in this order.

## Phase 1 - Config DB repository

Why first:

Every later adapter test and future workflow needs a stable persistence contract.

Implement:

```text
CFG-01 through CFG-13
```

especially:

```text
MigrationPlans
SourceTables
SourceTableColumns
ReplicationConfig
ReplicationState
FabricViews
FabricViewDependencies
BatchRuns
BatchObjectRuns
atomic plan persistence
```

---

## Phase 2 - Fabric execution client

Implement:

```text
FAB-01 through FAB-14
```

Critical path:

```text
list/get workspace
list/get Lakehouse
get notebook
run notebook
get notebook run
get pipeline
run pipeline
get pipeline run
```

---

## Phase 3 - Oracle metadata completeness

Implement:

```text
ORA-01 through ORA-08
```

---

## Phase 4 - SAP metadata completeness

Implement:

```text
SAP-01 through SAP-10
```

Do not rewrite the already working DB view / InfoSet / scraper methods.

Wrap and extend them.

---

## Phase 5 - datatype mapping

Implement deterministic:

```text
Oracle -> Fabric
SAP -> Fabric
```

with tests.

---

## Phase 6 - LLM/Phoenix adapter

Implement:

```text
LLM-01 through LLM-05
```

and deterministic SAP object verification.

---

## Phase 7 - Adapter integration tests

Only after this phase should Temporal workflow coding start.

---

# 21. Testing Requirements

## SAP tests

Mock the SAP HTTP endpoint and test:

```text
DataSource found/not found
table metadata
key extraction
DataSource fields
ODP capability
delta details
table vs DB-view resolution
DB view response
InfoSet response
ABAP recursive response
scraper truncation
class-based route
unsupported extraction methods
HTTP errors
invalid object names
```

## Oracle tests

Mock Oracle cursor/connection and test:

```text
connection test
table exists
rich schema
composite PK
no PK
multiple unique keys
indexes
watermark candidates
date/timestamp types
NUMBER precision/scale
LOB types
invalid schema/table inputs
```

## Fabric tests

Mock `httpx` and test:

```text
workspace pagination
Lakehouse pagination
get notebook
notebook payload with plan_guid
202 + Location
Retry-After
job ID parsing
running job
completed job
failed job with failureReason
exitValue
cancel
pipeline submission
pipeline status
401
403
404
429
5xx
malformed API response
```

## ConfigDB tests

Use a test DB / transaction rollback pattern and test:

```text
plan creation
plan version/status updates
analysis versioning
source table insert
column insert
composite merge-key JSON
replication state updates
view + dependencies
batch logs
atomic persist approved runtime plan
rollback on child insert failure
idempotent retry behavior
```

## LLM tests

Do not rely on live model calls for normal unit tests.

Test:

```text
request serialization
structured response validation
invalid model output
missing source evidence
revision version behavior
Phoenix trace metadata
secret redaction
```

---

# 22. Microsoft Fabric API Notes to Use During Implementation

Verify against current Microsoft Fabric REST documentation during implementation.

At handover time, the relevant APIs include:

## Workspaces

```http
GET /v1/workspaces
```

Supports pagination and service principal / managed identity.

## Lakehouses

```http
GET /v1/workspaces/{workspaceId}/lakehouses
```

Supports pagination.

## Notebook run-on-demand

```http
POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/jobs/execute/instances?beta=false
```

The release notebook API requires `beta=false`.

It accepts notebook parameters.

Primary project parameter:

```text
plan_guid
```

## Pipeline run-on-demand

```http
POST /v1/workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/jobs/execute/instances
```

A successful request returns `202 Accepted`.

Capture:

```text
Location
Retry-After
```

## Pipeline job status

```http
GET /v1/workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/jobs/execute/instances/{jobInstanceId}
```

Do not implement polling with indefinite sleeps inside the Fabric adapter.

---

# 23. Security Requirements

Do not log or persist:

```text
SAP password
Oracle password
FABRIC_CLIENT_SECRET
access tokens
```

`DBConnections.ConnectionDetails` should preferentially store identifiers / host metadata, not secrets.

If connection credentials must be referenced dynamically, introduce a secret reference:

```text
secret_provider
secret_reference
```

rather than plaintext secret values.

---

# 24. Acceptance Criteria - Adapter Layer Ready for Workflows

Do not mark this handover complete until all of the following are true.

## SAP

- [ ] connection can be tested
- [ ] table metadata has a semantic wrapper
- [ ] DataSource metadata works
- [ ] exposed DataSource fields can be retrieved
- [ ] ODP capability can be determined
- [ ] delta metadata is typed
- [ ] extraction method can be routed deterministically
- [ ] table vs DB view can be resolved
- [ ] DB view details work
- [ ] InfoSet details work
- [ ] Function Module recursive scrape works
- [ ] scraper completeness/truncation is surfaced
- [ ] class-based extractor is supported or explicitly rejected
- [ ] batch object metadata lookup exists

## Oracle

- [ ] connection test works
- [ ] table existence works
- [ ] rich column metadata is returned
- [ ] PK supports composite keys
- [ ] unique keys can be read
- [ ] complete index metadata can be read
- [ ] watermark candidates can be proposed
- [ ] datatype mapping is deterministic

## Fabric

- [ ] workspaces can be listed
- [ ] Lakehouses can be listed/validated
- [ ] provisioning notebook can be validated
- [ ] notebook can be submitted with `plan_guid`
- [ ] notebook job status can be retrieved
- [ ] notebook can be cancelled
- [ ] replication pipeline can be validated
- [ ] pipeline can be submitted
- [ ] pipeline status can be retrieved
- [ ] `Retry-After` is surfaced to Temporal
- [ ] Fabric error responses are normalized

## Config DB

- [ ] utility matches new database schema
- [ ] migration plans can be created/versioned/approved
- [ ] SAP analyses and analysis objects can be versioned
- [ ] source tables/columns can be persisted
- [ ] replication config and state can be persisted separately
- [ ] Fabric views/dependencies can be persisted
- [ ] batch and object run logs can be persisted
- [ ] approved runtime plan can be persisted atomically
- [ ] DB transaction helper is tested

## LLM

- [ ] extractor analysis uses structured output
- [ ] user-feedback revision uses structured output
- [ ] Fabric plan generation uses structured output
- [ ] model-identified SAP objects are deterministically validated
- [ ] Phoenix tracing is enabled
- [ ] sensitive credentials are excluded from traces

---

# 25. Final Instruction to Coding Agent

**Do not build the Temporal workflows in this change.**

The deliverable of this change is a complete, typed, tested adapter/repository layer that makes the four workflows straightforward to compose.

The desired end state is:

```text
Oracle adapter    READY
SAP adapter       READY
Fabric adapter    READY
ConfigDB repo     READY
Type mapper       READY
LLM adapter       READY
        |
        v
NEXT CHANGE:
Temporal workflows
```

Avoid putting workflow orchestration into any adapter.

Do not allow LLM output to directly execute SQL or Fabric changes.

Only **verified + user-approved + persisted structured configuration** may be consumed by the Fabric provisioning notebook and replication pipeline.
