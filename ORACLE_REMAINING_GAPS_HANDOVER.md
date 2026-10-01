# Coding Agent Handover
## Remaining Oracle Adapter Gaps Before Temporal Workflow Development

**Scope:** Only the remaining gaps in the Oracle DB adapter.  
**Baseline:** Current `utils(10).py`.  
**Do not redesign the Oracle client.** Extend and correct the existing implementation minimally.

---

# 1. Current State

The Oracle adapter is already substantially more complete than the original version.

The following capabilities are already implemented and should be preserved:

- Oracle connection settings using DSN or host/service
- Oracle Instant Client initialization for Windows/Linux
- Read-only query execution
- JSON-safe Oracle value conversion
- Connection health check
- Table existence validation
- Primary-key discovery
- Composite primary-key support
- Unique-key discovery
- Full table-index discovery
- Column-index check helper
- Rich table-column metadata
- Column descriptions
- Structured datatype metadata
- Watermark candidate discovery for DATE/TIMESTAMP fields
- Legacy specific-column index lookup

The Oracle adapter is therefore close to workflow-ready.

The remaining gaps are mainly:

1. identifier validation hardening;
2. richer table-level metadata;
3. better watermark candidate ranking/semantics;
4. datatype-mapping integration contract;
5. initial-load planning metadata;
6. result-size safety;
7. batch metadata wrapper for the future workflow/service layer.

---

# 2. Oracle Workflow This Adapter Must Support

The Oracle adapter is required for:

## Oracle Table -> Microsoft Fabric

Target flow:

```text
validate Oracle connection
        ->
validate schema/table
        ->
read table metadata
        ->
read columns
        ->
read primary / unique keys
        ->
read indexes
        ->
identify watermark candidates
        ->
map Oracle datatypes to Fabric
        ->
user selects / confirms replication strategy
        ->
approved runtime configuration
```

The Oracle adapter should only perform deterministic Oracle discovery.

It must not:

```text
select Fabric workspace
write Config DB records
invoke Temporal
trigger Fabric notebook
trigger Fabric pipeline
call an LLM
```

---

# 3. Overall Oracle Readiness

Current assessment:

```text
Connection settings                   READY
Connection test                       READY
Read-only query execution             READY
Table existence                       READY
Rich column metadata                  READY
Column descriptions                   READY
Primary key                           READY
Composite key                         READY
Unique keys                           READY
Index definitions                     READY
Column indexed helper                 READY
Watermark candidates                  PARTIAL
Table-level metadata                  MISSING
Schema existence                      MISSING
Datatype -> Fabric mapper              EXTERNAL DEPENDENCY
Table statistics / estimated rows     MISSING
Partition metadata                    MISSING
Result-size safety                    NEEDS IMPROVEMENT
Identifier validation                 NEEDS HARDENING
Batch inspection wrapper              MISSING
```

The adapter does not need major new discovery logic before workflow development, but the P0 items below should be completed.

---

# 4. Gap ORA-01 - Harden Oracle Identifier Validation

## Current behavior

`_identifier(...)` currently:

```python
strip()
upper()
```

and rejects only empty strings.

That is not enough for identifiers that are interpolated conceptually into metadata lookups or accepted from UI/workflow input.

Although the current SQL uses bind variables for values, stronger validation is still desirable.

## Required change

Validate ordinary Oracle schema/table/column names.

Suggested approach:

```python
_ORACLE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_$#]*$")
```

Then:

```python
def _identifier(value: str, name: str) -> str:
    ...
```

should:

1. verify string;
2. strip whitespace;
3. reject invalid characters;
4. normalize to uppercase.

## Quoted identifiers

Do not attempt to silently support quoted/case-sensitive Oracle identifiers in V1 unless required.

If quoted identifiers are needed later, implement them through a separate explicit identifier type.

## Acceptance criterion

Values such as:

```text
ONT
OE_ORDER_LINES_ALL
LAST_UPDATE_DATE
```

must work.

Unexpected values such as:

```text
ONT; DROP TABLE X
A B
A.B
```

must fail validation.

---

# 5. Gap ORA-02 - Schema Existence Helper

## Problem

The workflow can check whether a table exists, but cannot validate a selected schema independently.

## Required method

Add:

```python
def schema_exists(
    self,
    schema_name: str,
) -> bool:
    ...
```

Possible metadata source:

```text
ALL_USERS
```

or another appropriate Oracle dictionary view available to the application user.

Use bound parameters.

This is useful for:

- UI schema selection
- cleaner validation errors
- future list/search methods

---

# 6. Gap ORA-03 - Table Info Wrapper

## Problem

`table_exists(...)` returns only a boolean.

The workflow/service layer would benefit from one semantic table object containing the key discovery metadata.

## Required model

Add:

```python
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
```

## Required method

Add:

```python
def get_table_info(
    self,
    schema_name: str,
    table_name: str,
) -> OracleTableInfo:
    ...
```

This method may compose existing methods.

Do not duplicate dictionary logic unnecessarily.

---

# 7. Gap ORA-04 - Table Statistics / Estimated Row Count

## Problem

Initial-load strategy may depend on whether a table is:

```text
10,000 rows
10 million rows
500 million rows
```

The adapter currently does not expose table statistics.

## Required method

Add:

```python
def get_table_statistics(
    self,
    schema_name: str,
    table_name: str,
) -> OracleTableStatistics:
    ...
```

Suggested model:

```python
class OracleTableStatistics(BaseModel):
    schema_name: str
    table_name: str

    estimated_rows: int | None
    blocks: int | None
    avg_row_len: int | None
    last_analyzed: datetime | None

    statistics_available: bool
```

Use Oracle optimizer dictionary statistics such as:

```text
ALL_TABLES.NUM_ROWS
BLOCKS
AVG_ROW_LEN
LAST_ANALYZED
```

## Important

Do not automatically execute:

```sql
SELECT COUNT(*) FROM huge_table
```

to obtain a count.

Return `None` when statistics are unavailable/stale rather than forcing an expensive scan.

---

# 8. Gap ORA-05 - Partition Metadata

## Problem

Large Oracle tables may be partitioned.

This can affect initial backfill strategy and future batching.

## Required methods

Add:

```python
def get_partition_info(
    self,
    schema_name: str,
    table_name: str,
) -> OraclePartitionInfo:
    ...
```

Suggested models:

```python
class OraclePartition(BaseModel):
    partition_name: str
    position: int
    high_value: str | None = None
    estimated_rows: int | None = None

class OraclePartitionInfo(BaseModel):
    partitioned: bool
    partitioning_type: str | None = None
    subpartitioning_type: str | None = None
    partitions: list[OraclePartition] = Field(default_factory=list)
```

## Priority

P0 if large/partitioned Oracle tables are part of first-release onboarding.

Otherwise P1.

The workflow does not need to automatically choose partition-based copying yet, but should be able to expose it to planning.

---

# 9. Gap ORA-06 - Improve Watermark Candidate Semantics

## Current behavior

Current method identifies:

```text
DATE
TIMESTAMP*
```

columns and marks a reason based on names containing:

```text
UPDATE
MODIF
CHANGE
```

This is a good baseline.

## Remaining gaps

### A. Ranking

Current output is an unordered candidate list.

Add a deterministic score or priority.

Suggested model extension:

```python
class OracleWatermarkCandidate(BaseModel):
    column_name: str
    data_type: str
    indexed: bool
    nullable: bool
    reason: str

    priority: int
    recommended: bool = False
```

Example ranking:

```text
LAST_UPDATE_DATE indexed/non-null       highest
UPDATED_AT indexed/non-null             high
LAST_UPDATE_DATE non-indexed            medium
generic timestamp                       lower
```

Do not automatically select a watermark solely from the score.

---

### B. Candidate names

Include common enterprise/EBS patterns where appropriate:

```text
LAST_UPDATE_DATE
LAST_UPDATED_DATE
UPDATE_DATE
UPDATED_AT
MODIFIED_DATE
MODIFIED_AT
LAST_MODIFIED_DATE
CREATION_DATE
CREATED_AT
```

However, creation-only columns should be clearly identified as:

```text
insert-only candidate
```

rather than equivalent to an update watermark.

---

### C. Numeric candidate support

Do not automatically treat all numeric columns as watermarks.

If sequence-based incremental replication is later supported, expose it separately.

For V1, DATE/TIMESTAMP support is sufficient.

---

# 10. Gap ORA-07 - Watermark Index Detail

## Current behavior

Watermark candidates have:

```text
indexed: bool
```

This is useful but incomplete.

A column may occur as:

```text
first column in a composite index
later column in a composite index
```

which can matter for filtering performance.

## Suggested model

Add optional:

```python
index_details: list[OracleWatermarkIndexInfo]
```

Example:

```python
class OracleWatermarkIndexInfo(BaseModel):
    index_name: str
    column_position: int
    uniqueness: str
```

At minimum expose whether the candidate is:

```text
leading_index_column
```

This is especially useful for the UI when explaining expected extraction performance.

Priority: P1.

---

# 11. Gap ORA-08 - Column Default Metadata

## Current column metadata

Current model already includes:

```text
position
name
formatted datatype
data_type
data_length
char_length
precision
scale
nullable
description
```

This is strong.

## Optional addition

Add:

```text
data_default
default_on_null
identity information when available
virtual column indicator
```

if the provisioning notebook needs to reproduce these semantics.

For the current Bronze replication use case, these are not mandatory.

Priority: P2 unless business requirements require target-table semantic reproduction beyond datatype/nullability.

---

# 12. Gap ORA-09 - Identity / Generated Column Detection

If Oracle source tables contain:

```text
IDENTITY
VIRTUAL columns
generated columns
```

the migration plan should know.

Add only if required by actual source systems.

Possible model fields:

```python
identity_column: bool | None
virtual_column: bool | None
```

For Bronze ingestion, these may simply be treated as source values rather than recreated generation logic.

Priority: P2.

---

# 13. Gap ORA-10 - Datatype Mapping Contract

## Current state

Oracle metadata now exposes enough structured datatype information for deterministic mapping.

However, `OracleClient` does not map Oracle types to Fabric types.

This is correct separation of responsibility.

A shared datatype-mapping utility must exist before Temporal workflow development.

## Required external utility

Implement:

```python
def map_oracle_column_to_fabric(
    column: OracleTableColumn,
) -> FabricTypeMapping:
    ...
```

Suggested model:

```python
class FabricTypeMapping(BaseModel):
    source_type: str
    target_type: str

    supported: bool
    lossy: bool = False

    warning: str | None = None
```

## Minimum Oracle types

Support explicitly:

```text
VARCHAR2
NVARCHAR2
CHAR
NCHAR

NUMBER
NUMBER(p)
NUMBER(p,s)

FLOAT
BINARY_FLOAT
BINARY_DOUBLE

DATE
TIMESTAMP
TIMESTAMP WITH TIME ZONE
TIMESTAMP WITH LOCAL TIME ZONE

CLOB
NCLOB
BLOB
RAW
LONG
```

Unsupported types must be explicit.

Do not ask an LLM to choose deterministic datatype mappings.

---

# 14. Gap ORA-11 - Number Mapping Needs Precision/Scale Rules

This deserves specific tests because Oracle:

```text
NUMBER
NUMBER(p)
NUMBER(p,s)
```

are common and cannot all be blindly mapped to:

```text
DOUBLE
```

The mapper must define project rules.

Example policy areas:

```text
NUMBER with scale 0 and bounded precision -> INT/BIGINT/DECIMAL
NUMBER with scale > 0 -> DECIMAL(p,s)
unbounded NUMBER -> DECIMAL or DOUBLE based on project policy
```

The exact Fabric mapping policy should be centralized and documented.

Do not implement these rules inside the workflow.

---

# 15. Gap ORA-12 - Batch Table Inspection Wrapper

The Oracle workflow will otherwise call many methods separately.

Add a higher-level helper/service:

```python
def inspect_table(
    self,
    schema_name: str,
    table_name: str,
) -> OracleTableInspection:
    ...
```

Suggested model:

```python
class OracleTableInspection(BaseModel):
    schema_name: str
    table_name: str

    table_info: OracleTableInfo

    columns: list[OracleTableColumn]

    primary_key: OracleKey | None

    unique_keys: list[OracleKey]

    indexes: list[OracleIndexDefinition]

    watermark_candidates: list[OracleWatermarkCandidate]

    statistics: OracleTableStatistics | None = None

    partition_info: OraclePartitionInfo | None = None

    warnings: list[str] = Field(default_factory=list)
```

This is the clean deterministic input object for the future Oracle migration activity.

It may internally compose existing methods.

---

# 16. Gap ORA-13 - Result Size Safety in `execute_sql`

## Current behavior

`execute_sql(...)` performs:

```python
cursor.fetchall()
```

for every read query.

This is acceptable for dictionary metadata queries, but unsafe as a generic adapter method for arbitrary large SELECTs.

## Required change

Choose one of:

### Option A - Restrict method scope

Clearly make it internal:

```python
_execute_sql(...)
```

and document that it is for bounded metadata reads only.

### Option B - Add max row support

Preferred:

```python
def execute_sql(
    self,
    sql: str,
    parameters=None,
    *,
    max_rows: int | None = None,
) -> OracleQueryResult:
    ...
```

Use bounded fetch logic.

Do not rely on `fetchall()` when `max_rows` is supplied.

## Important

Do not expose generic unrestricted SQL execution directly as an LLM tool or user-facing endpoint.

---

# 17. Gap ORA-14 - Return Typed Query Result, Not JSON String

## Current behavior

`execute_sql(...)` returns:

```python
str
```

containing serialized JSON.

Every caller then does:

```python
OracleQueryResult.model_validate_json(...)
```

This is unnecessary inside Python.

## Recommended change

Prefer:

```python
def execute_sql(...) -> OracleQueryResult:
    ...
```

Then `_records(...)` can operate directly on the model.

If backward compatibility is required:

```python
execute_sql(...)
```

may remain temporarily, and add:

```python
execute_query(...)
```

returning `OracleQueryResult`.

Do not force a breaking change if current callers rely on the JSON contract.

Priority: P1.

---

# 18. Gap ORA-15 - Legacy `get_index` Duplication

Current adapter has both:

```python
get_indexes(...)
```

and legacy:

```python
get_index(schema, table, column)
```

The newer `get_indexes(...)` is richer.

## Required action

Keep `get_index(...)` for backward compatibility if existing callers use it, but mark it deprecated.

Implement it internally using:

```python
get_indexes(...)
```

rather than maintaining separate dictionary SQL long-term.

Example:

```python
def get_index(...):
    ...
    # derive matching OracleIndex rows from get_indexes()
```

This prevents metadata logic divergence.

---

# 19. Gap ORA-16 - Table Type / View Distinction

`table_exists(...)` checks only:

```text
ALL_TABLES
```

which is correct for physical tables.

For clean UI validation, consider:

```python
def get_object_type(
    self,
    schema_name: str,
    object_name: str,
) -> OracleObjectType:
    ...
```

Possible result:

```text
TABLE
VIEW
MATERIALIZED_VIEW
UNKNOWN
```

This is useful if users accidentally select an Oracle view.

For current workflow scope:

```text
Oracle Table -> Fabric
```

views may be explicitly rejected.

Priority: P1.

---

# 20. Gap ORA-17 - Schema/Table Listing Helpers

Useful for UI onboarding.

Add optionally:

```python
def list_schemas(self) -> list[str]:
    ...
```

Add:

```python
def list_tables(
    self,
    schema_name: str,
    *,
    search_text: str | None = None,
    limit: int = 200,
) -> list[OracleTableSummary]:
    ...
```

These are not strictly required if the UI already knows table names.

Priority: P1/P2 depending on UI requirements.

---

# 21. Gap ORA-18 - Preview Data Helper

For user verification, optionally add:

```python
def preview_table(
    self,
    schema_name: str,
    table_name: str,
    *,
    max_rows: int = 20,
) -> OracleQueryResult:
    ...
```

Important:

- validate identifiers;
- enforce hard upper bound;
- do not allow arbitrary WHERE clauses from untrusted input.

Not required for the first workflow.

Priority: P2.

---

# 22. Gap ORA-19 - Oracle Client Initialization Reentrancy

## Risk

`connect()` calls:

```python
_initialize_native_client(...)
```

for every connection.

Depending on `python-oracledb` thick-mode initialization state, repeated initialization with different parameters can cause errors.

## Required review

Ensure `_initialize_native_client(...)` is process-safe/idempotent.

Suggested strategy:

```python
_ORACLE_CLIENT_INITIALIZED = False
_ORACLE_CLIENT_PATH = None
```

with an appropriate lock if multiple threads may initialize concurrently.

Behavior:

```text
first call -> initialize
same config -> no-op
different client path after initialization -> explicit error
```

This is important once Temporal workers execute concurrent Oracle activities.

---

# 23. Gap ORA-20 - Connection Pooling Decision

Current behavior opens a new Oracle connection for every query.

This is functionally correct.

For workflow metadata discovery, this may be acceptable.

Before optimizing, measure.

Do not introduce pooling purely for architecture.

If many tables are inspected concurrently, consider a future:

```python
OracleConnectionPool
```

but keep it out of the first workflow unless needed.

Priority: P2.

---

# 24. Gap ORA-21 - Error Normalization

Current adapter exposes raw Oracle/database exceptions.

Before Temporal workflow implementation, add lightweight Oracle-specific exception normalization if useful.

Suggested:

```python
class OracleAdapterError(Exception):
    pass

class OracleConnectionError(OracleAdapterError):
    pass

class OracleObjectNotFoundError(OracleAdapterError):
    pass

class OraclePermissionError(OracleAdapterError):
    pass
```

Do not create a large hierarchy.

At minimum, the service/activity layer must be able to distinguish:

```text
cannot connect
permission denied
table not found
metadata query failed
```

Priority: P1.

---

# 25. Gap ORA-22 - Permissions Diagnostics

A table may exist but metadata views may not expose it due to privileges.

`table_exists(...) == False` can therefore mean:

```text
not found
```

or:

```text
not visible to this Oracle user
```

Where practical, expose a useful diagnostic.

Potential method:

```python
def validate_table_access(
    self,
    schema_name: str,
    table_name: str,
) -> OracleTableAccess:
    ...
```

Suggested output:

```python
class OracleTableAccess(BaseModel):
    exists_or_visible: bool
    metadata_readable: bool
    select_access: bool | None
    message: str | None = None
```

This is helpful for enterprise source onboarding.

Priority: P1.

---

# 26. Methods Explicitly NOT Needed in OracleClient

Do not add:

```text
Fabric target selection
Fabric table creation
Config DB writes
Temporal workflow logic
LLM analysis
Phoenix tracing
pipeline execution
```

The Oracle client remains a source-system discovery adapter.

---

# 27. Recommended Public Oracle API After This Change

Approximate target:

```python
class OracleClient:

    # health
    test_connection(...)

    # schema/table validation
    schema_exists(...)
    table_exists(...)
    get_object_type(...)          # optional P1
    validate_table_access(...)    # optional P1

    # table discovery
    get_table_info(...)
    get_table_schema(...)
    get_primary_key(...)
    get_unique_keys(...)
    get_indexes(...)
    is_column_indexed(...)
    get_watermark_candidates(...)
    get_table_statistics(...)
    get_partition_info(...)

    # composite inspection
    inspect_table(...)

    # UI helpers
    list_schemas(...)             # optional
    list_tables(...)              # optional
    preview_table(...)            # optional

    # low level
    execute_query(...)            # bounded typed response
    execute_sql(...)              # backward compatibility if needed
```

Shared utility outside Oracle client:

```python
map_oracle_column_to_fabric(...)
```

---

# 28. Priority

## P0 - Must complete before Oracle Temporal workflow

```text
ORA-01 identifier hardening
ORA-02 schema existence
ORA-03 table info wrapper
ORA-04 table statistics
ORA-06 watermark candidate ranking/semantics
ORA-10 datatype mapper dependency
ORA-11 NUMBER precision/scale mapping rules
ORA-12 inspect_table context
ORA-13 query result-size safety
ORA-19 Oracle client initialization idempotency
```

## P1 - Strongly recommended

```text
ORA-05 partition metadata
ORA-07 watermark index detail
ORA-14 typed query result
ORA-15 deprecate duplicate get_index SQL
ORA-16 object type
ORA-17 listing helpers
ORA-21 error normalization
ORA-22 permissions diagnostics
```

## P2 - Optional

```text
ORA-08 default metadata
ORA-09 identity / virtual-column detection
ORA-18 preview data
ORA-20 connection pooling
```

---

# 29. Tests Required

## Connection

```text
successful connection
bad password / authentication failure
bad DSN
host/service configuration
unsupported OS
missing Instant Client
repeated thick-client initialization
different client path after initialization
```

## Identifier validation

```text
valid schema
valid table
valid $/#/underscore names
whitespace normalization
invalid dotted name
invalid SQL characters
```

## Schema/table

```text
schema exists
schema missing
table exists
table missing
```

## Keys

```text
single-column PK
composite PK
no PK
single unique key
multiple unique keys
disabled constraint ignored
```

## Indexes

```text
single-column index
composite index
unique index
candidate as first index column
candidate as later index column
no index
```

## Columns

```text
VARCHAR2
NVARCHAR2
CHAR
NUMBER
NUMBER(p)
NUMBER(p,s)
DATE
TIMESTAMP
nullable
not nullable
column comments
```

## Watermarks

```text
LAST_UPDATE_DATE indexed
LAST_UPDATE_DATE not indexed
generic timestamp
creation date
nullable candidate
non-null candidate
priority ranking
```

## Statistics

```text
statistics available
NUM_ROWS null
LAST_ANALYZED null
large estimated row count
```

## Partitions

```text
non-partitioned table
partitioned table
multiple partitions
statistics unavailable
```

## Query safety

```text
SELECT allowed
WITH allowed
INSERT rejected
UPDATE rejected
DELETE rejected
bounded result
```

## Inspection

```text
inspect normal table
inspect table with no PK
inspect table with no stats
inspect large partitioned table
```

---

# 30. Acceptance Criteria

Oracle adapter is ready for Temporal workflow development when:

- [ ] connection health check works
- [ ] thick-client initialization is safe for repeated/concurrent activity usage
- [ ] schema can be validated
- [ ] table can be validated
- [ ] identifier validation is hardened
- [ ] rich table schema is available
- [ ] composite PK is available
- [ ] unique keys are available
- [ ] indexes are available
- [ ] watermark candidates are ranked deterministically
- [ ] table statistics are available without forcing COUNT(*)
- [ ] table inspection can be returned as one typed context
- [ ] query result size is bounded for generic reads
- [ ] Oracle -> Fabric datatype mapping exists outside the client
- [ ] NUMBER precision/scale mapping policy is tested
- [ ] all P0 behavior is covered by tests

---

# 31. Final Instruction to Coding Agent

Implement the Oracle gaps above only.

Do not begin Temporal workflow development in this change.

Do not move Fabric, ConfigDB, Temporal, or LLM responsibilities into the Oracle client.

Preserve the already-working metadata methods.

The desired state is:

```text
Oracle connection
      READY
        |
        v
Oracle table discovery
      READY
        |
        +--> schema
        +--> columns
        +--> keys
        +--> indexes
        +--> watermark candidates
        +--> stats / partitions
        |
        v
typed OracleTableInspection
        |
        v
deterministic datatype mapping
        |
        v
NEXT CHANGE:
OracleTableMigrationWorkflow
```

Most importantly:

**The Oracle workflow should consume a complete deterministic table-inspection contract rather than assembling low-level Oracle metadata calls throughout Temporal workflow code.**
