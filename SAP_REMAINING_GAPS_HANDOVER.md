# Coding Agent Handover
## Remaining SAP Adapter Gaps Before Temporal Workflow Development

**Scope:** Only the remaining gaps in the SAP ECC adapter layer.  
**Baseline:** Current `utils(9).py` and `verification.py`.  
**Do not redesign the SAP client.** Extend and correct the existing implementation minimally.

---

# 1. Current State

The SAP adapter is already significantly more complete than the original version.

The following capabilities are already implemented and should be preserved:

- SAP HTTPS client with Basic authentication
- Configurable TLS verification / CA bundle
- SAP connection test
- Generic metadata retrieval for SAP table/view/structure
- SAP table schema wrapper
- SAP table primary-key extraction from metadata
- SAP object-type resolution through DD02L
- SAP DataSource metadata lookup
- SAP DataSource field lookup
- SAP ODP capability lookup
- SAP delta metadata wrapper
- SAP extraction-route resolver
- SAP DB-view analysis
- SAP InfoSet analysis
- Recursive ABAP scraper
- ABAP scrape completeness assessment
- Batch metadata lookup for multiple objects
- Deterministic verification of LLM-identified SAP tables/views

The adapter is therefore close to workflow-ready, but several correctness and completeness gaps remain.

---

# 2. SAP Workflows This Adapter Must Support

The SAP adapter must support three of the four migration workflows:

## Workflow A - SAP Table -> Fabric

Required SAP behavior:

```text
validate connection
    ->
validate table
    ->
read table schema
    ->
read primary key
    ->
identify watermark candidates
    ->
return deterministic metadata
```

## Workflow B - SAP ODP DataSource -> Fabric

Required SAP behavior:

```text
validate connection
    ->
read DataSource metadata
    ->
validate ODP capability
    ->
read exposed DataSource fields
    ->
read extract structure
    ->
read delta metadata
```

## Workflow C - SAP DataSource Rebuild -> Fabric

Required SAP behavior:

```text
read DataSource metadata
    ->
resolve extraction type
    ->
TABLE
DB_VIEW
INFOSET_QUERY
FUNCTION_MODULE
CLASS_BASED
OTHER
    ->
discover deterministic dependencies
    ->
LLM only where deterministic discovery is insufficient
    ->
verify all LLM-identified SAP objects
```

---

# 3. Overall SAP Readiness

Current assessment:

```text
Connection handling                  READY
Generic metadata                     READY
Table schema / PK                    READY
DataSource metadata                  READY
Object type resolution               READY
DB view discovery                    READY
InfoSet discovery                    READY
ABAP scraper                         READY
Scrape completeness assessment       READY
LLM object verification              PARTIAL
ODP capability                       PARTIAL - semantic validation required
DataSource exposed fields            PARTIAL - field semantics need correction
Delta details                        PARTIAL - current logic is unsafe
Watermark candidate discovery        MISSING
Class-based extractor support        MISSING / explicitly unsupported
DataSource Append handling            MISSING / unsupported routing
Object existence helpers             MISSING
Search/list helpers                  OPTIONAL
ABAP scraper object-type coverage    NEEDS VALIDATION
```

The main remaining work is therefore not large-scale new functionality. It is mostly:

1. fixing ambiguous SAP metadata interpretation;
2. adding missing workflow-oriented wrappers;
3. formalizing unsupported extractor branches;
4. improving deterministic verification before LLM output enters a migration plan.

---

# 4. Gap SAP-01 - Fix / Validate DataSource Field Semantics

## Current behavior

`get_datasource_fields(...)` reads:

```sql
SELECT FIELD, SELECTION
FROM ROOSFIELD
```

and then derives:

```text
hidden
extractable
```

from the value of `SELECTION`.

This is risky.

The current implementation assumes that `SELECTION` can determine whether a field is hidden or extractable.

That interpretation must not be relied upon unless it is confirmed against the target SAP ECC metadata semantics.

## Required change

Separate raw metadata from interpreted metadata.

Change the model to preserve the raw field attributes explicitly.

Suggested model:

```python
class SAPDatasourceField(BaseModel):
    datasource: str
    field_name: str
    position: int | None

    selection: str | None = None

    hidden: bool | None = None
    extractable: bool | None = None

    raw_attributes: dict[str, JsonValue] = Field(default_factory=dict)
```

Then update the SQL query to retrieve all relevant ROOSFIELD attributes available in the target ECC release.

The method must:

1. return the raw values;
2. only populate `hidden` / `extractable` when the mapping is deterministic;
3. otherwise leave them as `None`.

## Acceptance criterion

No migration decision may depend on a guessed interpretation of `SELECTION`.

---

# 5. Gap SAP-02 - Correct / Validate Delta Support Logic

## Current behavior

Current code uses logic equivalent to:

```python
delta_supported =
    bool(indicator.strip()) and indicator.strip().upper() != "X"
```

This is unsafe.

A non-empty SAP delta indicator must not automatically mean:

```text
supported = True unless value == X
```

The meaning of the DataSource delta metadata must be handled explicitly.

## Required change

Refactor:

```python
get_datasource_delta_details(...)
```

so that it does not infer unsupported semantics.

Suggested output:

```python
class SAPDeltaDetails(BaseModel):
    datasource_name: str

    delta_indicator: str | None

    delta_supported: bool | None

    delta_mechanism: str | None

    details_resolved: bool

    raw_attributes: dict[str, JsonValue] = Field(default_factory=dict)

    warnings: list[str] = Field(default_factory=list)
```

Use:

```text
True
False
None
```

for `delta_supported`.

`None` means:

```text
SAP metadata was read but the adapter cannot deterministically classify it.
```

This is better than returning a false certainty.

## Workflow rule

The Temporal workflow may ask the user for confirmation when:

```text
details_resolved = False
```

Do not let the LLM invent SAP delta semantics.

---

# 6. Gap SAP-03 - Add Watermark Candidate Discovery for SAP Tables

## Problem

The direct SAP-table workflow needs to support generic table replication.

The current SAP adapter can return:

```text
columns
keys
datatypes
```

but has no workflow-oriented method for possible incremental watermark fields.

## Required method

Add:

```python
def get_watermark_candidates(
    self,
    table_name: str,
) -> list[SAPWatermarkCandidate]:
    ...
```

Suggested model:

```python
class SAPWatermarkCandidate(BaseModel):
    field_name: str
    datatype: str
    indexed: bool | None = None
    nullable: bool | None = None
    reason: str
```

Initial deterministic candidate logic may consider SAP fields with date/time/timestamp-compatible types and commonly used change fields.

However:

- this method proposes candidates;
- it does not select the final watermark;
- the user/workflow owns the final decision.

## Important

Do not use an LLM for simple datatype-based candidate discovery.

---

# 7. Gap SAP-04 - Table Existence Helper

## Current behavior

The adapter can infer object type with:

```python
get_object_type(...)
```

but workflows would benefit from explicit validation APIs.

## Required methods

Add:

```python
def object_exists(
    self,
    object_name: str,
) -> bool:
    ...
```

Add:

```python
def table_exists(
    self,
    table_name: str,
) -> bool:
    ...
```

Expected behavior:

```text
TRANSPARENT_TABLE -> True for table_exists
DB_VIEW           -> False for table_exists
STRUCTURE         -> False for table_exists
UNKNOWN           -> False
```

This keeps workflow code simple and avoids repeated object-type comparisons.

---

# 8. Gap SAP-05 - DataSource Existence Helper

Add:

```python
def datasource_exists(
    self,
    datasource_name: str,
) -> bool:
    ...
```

This should wrap:

```python
get_datasource_details(...)
```

and not duplicate the SQL.

Useful for:

- UI validation
- Temporal activity preconditions
- clean error handling

---

# 9. Gap SAP-06 - Handle DataSource Append Extraction Method

## Current state

The enum includes:

```text
A = DataSource Append
```

but `resolve_datasource_extraction(...)` currently falls through to:

```text
UNSUPPORTED_EXTRACTION_METHOD
```

## Required change

Make this explicit.

Either:

### Option A - officially unsupported in V1

Add route:

```python
SAPExtractionRoute.DATASOURCE_APPEND
```

and allow the workflow to return a clear:

```text
NOT_SUPPORTED_IN_V1
```

or:

### Option B - implement deterministic handling

Only if the current SAP environment has a reliable metadata path.

## Requirement

Do not silently classify this as generic `UNSUPPORTED`.

The workflow should know exactly why the DataSource cannot proceed automatically.

---

# 10. Gap SAP-07 - Class-Based Extractor Handling

## Current state

The enum includes:

```text
CA = Class-Based BI Agent
```

but the router explicitly maps it to:

```text
UNSUPPORTED
```

This is acceptable only if it is an intentional V1 limitation.

## Required action

Perform one controlled verification against the existing ABAP scraper endpoint.

Test:

```python
abap_code_scraper(
    name=<class>,
    object_type="CLASS",
    recursive=True,
)
```

Confirm whether the endpoint returns:

```text
class source
methods
includes
dependencies
called function modules
tables/views
```

### If supported

Add:

```python
SAPExtractionRoute.CLASS_BASED
```

and implement:

```python
def scrape_class_extractor(...) -> ABAPScrapeAssessment:
    ...
```

### If not supported

Keep it unsupported, but introduce a typed result/reason:

```text
CLASS_BASED_NOT_SUPPORTED
```

instead of a generic unsupported classification.

---

# 11. Gap SAP-08 - Formal Unsupported Extraction Result

## Problem

Multiple extractor methods may not be automatically rebuildable.

Current routing returns one generic:

```text
UNSUPPORTED_EXTRACTION_METHOD
```

That loses useful context.

## Required model

Add:

```python
class SAPExtractionResolution(BaseModel):
    route: SAPExtractionRoute
    supported: bool
    reason_code: str | None = None
    reason: str | None = None
```

Suggested reason codes:

```text
OBJECT_TYPE_UNRESOLVED
CLASS_BASED_NOT_SUPPORTED
DATASOURCE_APPEND_NOT_SUPPORTED
DOMAIN_FIXED_VALUE_REQUIRES_SPECIAL_HANDLER
UNKNOWN_EXTRACTION_METHOD
```

Then replace or wrap:

```python
resolve_datasource_extraction(...)
```

with:

```python
resolve_datasource_extraction_details(...)
```

Keep the simple enum function for backward compatibility if useful.

---

# 12. Gap SAP-09 - DB View Completeness Validation

## Current behavior

`SAPDBViewQueryDetails` already includes:

```text
sqlQuery
sqlQueryComplete
tables
filters
```

This is good.

## Missing workflow helper

Add:

```python
def assess_dbview(
    self,
    dbview_name: str,
) -> SAPDBViewAssessment:
    ...
```

Suggested model:

```python
class SAPDBViewAssessment(BaseModel):
    details: SAPDBViewQueryDetails
    complete: bool
    unresolved_tables: list[str]
    warnings: list[str]
```

Behavior:

1. retrieve view details;
2. verify every listed source table through `get_object_type`;
3. flag any unresolved source object;
4. propagate `sqlQueryComplete`.

## Workflow rule

If:

```text
complete = False
```

the system must not directly persist the generated SQL as an approved Fabric view without review.

---

# 13. Gap SAP-10 - InfoSet Completeness Validation

## Current behavior

`SAPInfoSetQueryDetails` supports:

```text
sqlTemplate
sqlTemplateComplete
tables
```

and correctly allows SQL template fields to be `None`.

## Required helper

Add:

```python
def assess_infoset_query(
    self,
    infoset_query: str,
) -> SAPInfoSetAssessment:
    ...
```

Suggested model:

```python
class SAPInfoSetAssessment(BaseModel):
    details: SAPInfoSetQueryDetails
    complete: bool
    unresolved_tables: list[str]
    warnings: list[str]
```

The assessment should:

- verify every table;
- flag missing/incomplete SQL template;
- provide a deterministic completeness state.

This allows the SAP rebuild workflow to decide whether:

```text
deterministic conversion is possible
```

or:

```text
LLM/manual review is required
```

---

# 14. Gap SAP-11 - ABAP Scrape Completeness Needs Stronger Semantics

## Current behavior

`assess_abap_scrape(...)` marks a scrape truncated if:

```text
objects_returned >= max_objects
OR a node reaches max_depth
OR warnings mention truncation/limits
```

This is useful but slightly conservative.

## Risk

A dependency occurring exactly at `max_depth` does not necessarily prove the scrape was truncated.

The scraper endpoint should ideally expose explicit information about why traversal stopped.

## Required action

If the SAP endpoint can expose it, extend scraper response metadata with:

```text
hit_max_depth
hit_max_objects
unresolved_count
complete
```

Then prefer those explicit flags.

If the endpoint cannot expose them:

keep the current heuristic but rename/document it clearly as:

```text
possibly_truncated
```

instead of treating it as guaranteed truncation.

## Suggested model change

```python
class ABAPScrapeAssessment(BaseModel):
    response: ABAPScraperResponse

    complete: bool | None

    possibly_truncated: bool

    unresolved_dependencies: list[ABAPObjectReference]

    warnings: list[str]
```

---

# 15. Gap SAP-12 - ABAP Scraper Supported Object Types

## Problem

The adapter accepts arbitrary:

```python
object_type: str
```

No validated list exists.

## Required change

Introduce:

```python
class ABAPObjectType(StrEnum):
    FUNCTION_MODULE = "FM"
    CLASS = "CLASS"
    PROGRAM = "PROG"
    INCLUDE = "INCLUDE"
    ...
```

Only include object types that the installed SAP scraper endpoint actually supports.

Then change:

```python
abap_code_scraper(...)
```

to accept:

```python
ABAPObjectType | str
```

with validation.

Unknown types should fail early.

This matters before the class-based extractor branch is implemented.

---

# 16. Gap SAP-13 - Strengthen LLM Object Verification

## Current behavior

`verification.py` verifies only objects whose final dictionary type is:

```text
TRANSPARENT_TABLE
DB_VIEW
```

and requires the LLM-supplied object type to exactly equal SAP's resolved type.

This is a good start.

## Remaining gaps

### A. Normalize model object types

The LLM may say:

```text
TABLE
TRANSPARENT_TABLE
SAP_TABLE
VIEW
DB_VIEW
```

Do not reject valid objects solely because vocabulary differs.

Add deterministic normalization:

```python
normalize_candidate_object_type(...)
```

Example:

```text
TABLE -> TRANSPARENT_TABLE
SAP_TABLE -> TRANSPARENT_TABLE
VIEW -> DB_VIEW
```

No fuzzy matching.

---

### B. Preserve verification reason codes

Current unresolved result contains the original candidate + warning string.

Add:

```python
class UnresolvedSAPObject(BaseModel):
    candidate: SAPObjectCandidate
    reason_code: str
    reason: str
```

Suggested codes:

```text
NOT_FOUND
UNSUPPORTED_OBJECT_TYPE
MODEL_TYPE_MISMATCH
METADATA_READ_FAILED
VIEW_DETAILS_FAILED
```

This is easier to persist and show in UI.

---

### C. Verify nested DB-view dependencies

If the model identifies a DB view, verification currently gets the view details.

Also verify:

```text
every source table/view listed by the DB view adapter
```

and include them in the verified lineage.

Do not stop verification at the view itself.

---

### D. Deduplicate candidates

LLM output may mention the same object more than once.

Deduplicate by normalized:

```text
object_type + name
```

before SAP calls.

---

### E. Return enriched verified objects

Add useful metadata:

```python
class VerifiedSAPObject(BaseModel):
    name: str
    object_type: str
    fields: list[SAPFieldMetadata]

    primary_key_columns: list[str] = []

    view_details: SAPDBViewQueryDetails | None = None

    source_objects: list[str] = []
```

---

# 17. Gap SAP-14 - Generic Verified Object Metadata Batch API

Current method:

```python
get_objects_metadata(...)
```

returns only:

```text
name -> fields
```

For the rebuild workflow, the service needs richer object information.

Add:

```python
def inspect_objects(
    self,
    object_names: Sequence[str],
) -> dict[str, SAPObjectInspection]:
    ...
```

Suggested model:

```python
class SAPObjectInspection(BaseModel):
    object_name: str
    object_type: str

    schema: SAPTableSchema | None = None

    view_details: SAPDBViewQueryDetails | None = None

    exists: bool

    warnings: list[str] = Field(default_factory=list)
```

This can internally reuse:

```text
get_object_type
get_table_schema
get_dbview_query_details
```

The workflow/service should not repeatedly orchestrate these low-level calls itself.

---

# 18. Gap SAP-15 - Read Metadata Input Validation Consistency

## Current behavior

`read_metadata(...)` checks:

```text
non-empty
trimmed
```

but does not reuse `_sap_name(...)`.

Other newer methods do use `_sap_name(...)`.

## Required change

Use:

```python
_sap_name(object_name, "object_name")
```

inside:

```python
read_metadata(...)
```

This gives consistent validation and avoids accepting unexpected object-name characters.

Likewise check:

```text
get_infoset_query_details
get_dbview_query_details
abap_code_scraper
```

and reuse consistent SAP name validation wherever appropriate.

If a particular SAP object type legitimately permits extra characters, create a dedicated validator rather than falling back to unvalidated strings.

---

# 19. Gap SAP-16 - SQL Query Interface Safety Boundary

## Current behavior

The client has a general:

```python
execute_sql_query(sql, max_rows)
```

method.

This is required internally, but it is powerful.

## Required change

Clearly mark it as a low-level/internal adapter method.

Do not expose arbitrary SAP SQL execution directly to:

- chat UI;
- LLM tool access;
- untrusted user input.

Recommended structure:

```python
_execute_sql_query(...)
```

or retain the method but document:

```text
trusted application-generated SQL only
```

All normal workflow behavior should use semantic wrapper methods.

---

# 20. Gap SAP-17 - Query Result Truncation Contract

## Current state

Only `get_datasource_fields(...)` explicitly checks:

```python
row_count >= max_rows
```

and treats this as possibly truncated.

Other metadata methods do not expose a common truncation contract.

## Required change

Add to `SAPSQLQueryResponse` if supported by the endpoint:

```python
truncated: bool | None = None
```

If endpoint cannot provide it, semantic wrapper methods that use a hard limit must detect:

```text
row_count == max_rows
```

and either:

- retry with a safe larger bound for metadata queries; or
- return a warning / raise if completeness is required.

For metadata discovery, silent truncation is not acceptable.

---

# 21. Gap SAP-18 - SAP Datatype -> Fabric Mapping Is Still External

The SAP adapter returns SAP datatype metadata, which is correct.

The workflow still requires deterministic:

```text
SAP datatype -> Fabric datatype
```

mapping.

This should **not** be implemented inside `SAPClient`, but must exist before SAP workflows.

Expected shared utility:

```python
map_sap_field_to_fabric(
    field: SAPFieldMetadata,
) -> FabricTypeMapping
```

At minimum cover the SAP datatypes observed in:

```text
DDIC tables
extract structures
DataSource fields
```

Return explicit unsupported/lossy mappings.

This is a dependency of SAP workflow readiness even though it belongs in the shared datatype-mapping module.

---

# 22. Gap SAP-19 - ODP Capability Result Needs "Unknown"

## Current behavior

Current `get_odp_capability(...)` returns:

```python
odp_capable = EXPOSE_EXTERNAL == "X"
```

If no ROOSATTR row is returned, it currently becomes:

```text
False
```

## Risk

"No metadata row returned" and "explicitly not ODP capable" are different states.

## Required model change

Prefer:

```python
class SAPODPCapability(BaseModel):
    datasource_name: str
    odp_capable: bool | None
    metadata_found: bool
    context: str | None = None
    raw_attributes: dict[str, JsonValue]
```

Suggested behavior:

```text
ROOSATTR row + positive flag  -> True
ROOSATTR row + negative flag  -> False
no ROOSATTR row               -> None
```

The workflow can then distinguish:

```text
not supported
```

from:

```text
cannot determine automatically
```

---

# 23. Gap SAP-20 - DataSource Text Fallback

## Current behavior

`get_datasource_details(...)` uses a left join to English text and correctly allows:

```text
DATASOURCE_NAME = None
```

This is good.

## Optional improvement

If English description is absent, optionally try:

```text
SAP logon language
```

or return a language-tagged text collection.

This is not a workflow blocker.

Priority:

```text
P2 / UX only
```

---

# 24. Gap SAP-21 - Explicit DataSource Rebuild Context Builder

For the Function Module branch, the LLM must receive one consistent object.

Do not let workflow code manually concatenate:

```text
DataSource metadata
extract structure
DataSource fields
scraped ABAP
dependency tree
warnings
```

Add a higher-level service/helper, not necessarily a low-level client method:

```python
def build_datasource_rebuild_context(
    client: SAPClient,
    datasource_name: str,
) -> SAPDatasourceRebuildContext:
    ...
```

Suggested model:

```python
class SAPDatasourceRebuildContext(BaseModel):
    datasource: SAPDatasourceDetails

    extract_structure_schema: list[SAPFieldMetadata]

    datasource_fields: list[SAPDatasourceField]

    extraction_resolution: SAPExtractionResolution

    dbview: SAPDBViewAssessment | None = None

    infoset: SAPInfoSetAssessment | None = None

    abap: ABAPScrapeAssessment | None = None

    warnings: list[str] = Field(default_factory=list)
```

This object becomes the clean input contract for the future LLM activity.

Keep this in:

```text
services/sap_analysis_service.py
```

rather than bloating `SAPClient`.

---

# 25. Methods Explicitly NOT Needed in SAP Client

Do not put the following responsibilities into `SAPClient`:

```text
Fabric target selection
Fabric view generation
Fabric SQL translation
LLM prompt generation
LLM model invocation
Config DB persistence
Temporal workflow state
user approval
Phoenix tracing
```

SAPClient should remain the SAP system integration adapter.

---

# 26. Recommended Public SAP API After This Change

Approximate target API:

```python
class SAPClient:

    # health
    test_connection(...)

    # generic object inspection
    get_object_type(...)
    object_exists(...)
    table_exists(...)
    read_metadata(...)
    get_table_schema(...)
    get_objects_metadata(...)
    inspect_objects(...)

    # SAP table migration support
    get_watermark_candidates(...)

    # DataSource support
    datasource_exists(...)
    get_datasource_details(...)
    get_datasource_fields(...)
    get_odp_capability(...)
    get_datasource_delta_details(...)

    # deterministic extractor inspection
    get_dbview_query_details(...)
    assess_dbview(...)

    get_infoset_query_details(...)
    assess_infoset_query(...)

    # ABAP
    abap_code_scraper(...)
    # optionally:
    scrape_class_extractor(...)

    # low-level
    execute_sql_query(...)
```

Module-level helpers/services:

```python
resolve_datasource_extraction(...)
resolve_datasource_extraction_details(...)

assess_abap_scrape(...)

verify_llm_identified_sap_objects(...)

build_datasource_rebuild_context(...)
```

---

# 27. Priority

## P0 - Must complete before workflows

Implement/fix:

```text
SAP-01 DataSource field semantics
SAP-02 delta semantics
SAP-03 watermark candidates
SAP-04 object/table existence
SAP-05 DataSource existence
SAP-06 DataSource Append explicit handling
SAP-07 Class-based support decision
SAP-08 typed unsupported extraction result
SAP-09 DB view assessment
SAP-10 InfoSet assessment
SAP-11 scraper completeness semantics
SAP-12 validated ABAP object types
SAP-13 strengthen LLM verification
SAP-15 validation consistency
SAP-17 truncation handling
SAP-19 ODP unknown state
SAP-21 rebuild-context builder
```

## P1 - Strongly recommended

```text
SAP-14 rich batch object inspection
SAP-16 restrict arbitrary SQL exposure
SAP-18 datatype mapper dependency
```

## P2 - Optional UX

```text
SAP-20 DataSource description-language fallback
search/list tables
search/list DataSources
preview table data
```

---

# 28. Tests Required

## Connection

```text
valid connection
authentication error
wrong SAP client
TLS verification enabled
custom CA bundle
invalid TLS config
```

## Object type

```text
transparent table
DB view
structure
missing object
invalid object name
```

## Table schema

```text
columns returned in order
single primary key
composite primary key
no primary key
```

## DataSource

```text
DataSource exists
DataSource missing
English text missing
extract structure present
empty extract structure
```

## DataSource fields

```text
field list complete
positions mapped from extract structure
unknown selection semantics remain None
truncation detection
```

## ODP

```text
positive ODP capability
negative capability
ROOSATTR row missing -> unknown
```

## Delta

```text
known supported metadata
known unsupported metadata
unresolved indicator -> delta_supported=None
```

## Extraction routing

```text
table
DB view
function module F1
function module F2
function module FS
InfoSet
ODP cursor
domain fixed values
DataSource append
class-based
unknown type
```

## DB view

```text
complete SQL
incomplete SQL
source table missing
nested view
```

## InfoSet

```text
complete template
template None
template marked incomplete
missing source object
```

## ABAP

```text
complete scrape
max object limit
max depth condition
unresolved dependency
warnings
invalid object type
class scrape if supported
```

## Verification

```text
verified table
verified DB view
normalized TABLE -> TRANSPARENT_TABLE
normalized VIEW -> DB_VIEW
type mismatch
missing object
duplicate candidates
nested DB view dependencies
metadata error
```

---

# 29. Acceptance Criteria

SAP adapter can be considered ready for Temporal workflow development when:

- [ ] SAP connection can be validated
- [ ] SAP table can be validated explicitly
- [ ] table schema and composite PK are available
- [ ] watermark candidates are available
- [ ] DataSource existence can be checked
- [ ] DataSource metadata is typed
- [ ] DataSource exposed fields do not rely on guessed semantics
- [ ] ODP capability supports True / False / Unknown
- [ ] delta capability supports True / False / Unknown
- [ ] extraction route returns typed reason codes
- [ ] table vs DB view is deterministic
- [ ] DB view completeness can be assessed
- [ ] InfoSet completeness can be assessed
- [ ] Function Module scraper completeness is surfaced
- [ ] scraper object types are validated
- [ ] class-based extractor is either supported or explicitly rejected
- [ ] DataSource Append is explicitly handled
- [ ] LLM objects are normalized, verified and enriched
- [ ] nested DB view dependencies are verified
- [ ] metadata truncation cannot occur silently
- [ ] DataSource rebuild context can be built as one typed object
- [ ] all P0 methods are covered by tests

---

# 30. Final Instruction to Coding Agent

Implement the above SAP gaps only.

Do not begin the Temporal workflows in this change.

Do not move Fabric, Config DB, LLM, or Temporal responsibilities into `SAPClient`.

Preserve the currently working methods wherever possible.

The desired end state is:

```text
SAP deterministic discovery
        READY
           |
           v
DataSource route resolution
        READY
           |
           +--> TABLE
           +--> DB VIEW
           +--> INFOSET
           +--> FUNCTION MODULE
           +--> CLASS / explicit unsupported
           |
           v
verified structured source context
           |
           v
NEXT CHANGE:
Temporal workflow + LLM activities
```

Most importantly:

**Do not allow ambiguous SAP metadata interpretation or unverified LLM object names to become runtime migration configuration.**
