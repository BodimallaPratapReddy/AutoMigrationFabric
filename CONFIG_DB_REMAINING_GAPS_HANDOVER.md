# Coding Agent Handover
## Remaining ConfigDB Repository Gaps Before Temporal Workflow Development

**Scope:** Only the remaining gaps in the ConfigDB utility/repository layer.  
**Baseline:** Current `repository.py` implementation.  
**Do not redesign or replace the existing repository.** Extend it minimally.

---

# 1. Current State

The current `ConfigDBRepository` is already largely complete and should be treated as the source of truth for Config DB persistence.

The following capabilities are already implemented and should not be reworked unless required for bug fixes:

- Connection registry CRUD
- Migration plan creation / lookup
- Migration plan status changes
- Migration plan versioning
- Plan approval
- Temporal workflow/run ID persistence
- SAP analysis creation
- SAP analysis object persistence
- SAP analysis approval
- Analysis version increment
- Analysis supersede
- Batch run start / complete / fail
- Batch object run start / complete / fail
- Replication config create / read / update
- Replication enable / disable
- Replication state create / read
- Replication started / succeeded / failed
- Source table runtime read APIs
- Source column runtime read APIs
- Table provisioning status updates
- Fabric view read APIs
- Fabric view dependency read APIs
- Fabric view provisioning status updates
- Approved runtime-plan lookup
- Atomic `persist_approved_runtime_plan(...)`
- Runtime-plan fingerprint / idempotency protection
- Transaction context manager

**Important:** Keep `persist_approved_runtime_plan(...)` atomic and idempotent.

---

# 2. Objective of This Change

Implement only the remaining repository methods required so that the ConfigDB layer is complete enough for the four Temporal workflows:

1. Oracle Table -> Fabric
2. SAP Table -> Fabric
3. SAP ODP DataSource -> Fabric
4. SAP DataSource Rebuild -> Fabric

After this change, Temporal workflow code should not need ad-hoc SQL against Config DB.

---

# 3. Gap 1 - SAP Analysis Status Transition

## Problem

`record_analysis_approval(...)` currently requires:

```text
AnalysisStatus = WAITING_APPROVAL
```

but there is no explicit repository method to move an analysis from:

```text
DRAFT
```

to:

```text
WAITING_APPROVAL
```

The workflow should not issue raw SQL for this transition.

## Required method

Add:

```python
def update_sap_analysis_status(
    self,
    analysis_guid: UUID,
    *,
    expected_status: str,
    new_status: str,
    expected_version: int,
) -> None:
    ...
```

### Required behavior

Use optimistic concurrency:

```sql
UPDATE bronze_replication.SAPAnalysis
SET AnalysisStatus = ?,
    UpdatedTimestamp = SYSUTCDATETIME()
WHERE AnalysisGUID = ?
  AND AnalysisStatus = ?
  AND AnalysisVersion = ?
```

If exactly one row is not updated:

```python
raise ValueError("analysis status or version changed")
```

### Expected workflow usage

```text
DRAFT
  ->
WAITING_APPROVAL
  ->
APPROVED
```

or:

```text
WAITING_APPROVAL
  ->
DRAFT
```

if user feedback requires a revision before approval.

---

# 4. Gap 2 - Get SAP Analysis by Plan and Version

## Problem

Current code supports:

```python
get_sap_analysis(analysis_guid)
```

but the workflow typically owns:

```text
plan_guid
analysis_version
```

rather than an `analysis_guid`.

## Required method

Add:

```python
def get_sap_analysis_for_plan(
    self,
    plan_guid: UUID,
    *,
    analysis_version: int | None = None,
) -> SAPAnalysisRecord | None:
    ...
```

### Behavior

If `analysis_version` is provided:

```sql
WHERE PlanGUID = ?
  AND AnalysisVersion = ?
```

If omitted:

return the latest analysis version:

```sql
WHERE PlanGUID = ?
ORDER BY AnalysisVersion DESC
```

Return one row.

### Optional helper

Also add:

```python
def list_sap_analyses_for_plan(
    self,
    plan_guid: UUID,
) -> list[SAPAnalysisRecord]:
    ...
```

This is useful for UI history / audit.

---

# 5. Gap 3 - Standalone Watermark Update

## Problem

`mark_replication_succeeded(...)` can update:

```text
LastWatermarkValue
```

which is correct for the normal success path.

However, the replication pipeline or recovery logic may need to update only the watermark without changing the full replication state.

## Required method

Add:

```python
def update_watermark(
    self,
    source_table_guid: UUID,
    watermark_value: str | None,
) -> None:
    ...
```

### SQL behavior

```sql
UPDATE bronze_replication.ReplicationState
SET LastWatermarkValue = ?,
    UpdatedTimestamp = SYSUTCDATETIME()
WHERE SourceTableGUID = ?
```

If no row exists:

```python
raise ValueError("replication state does not exist")
```

### Important

Do not automatically modify:

```text
Status
LastSuccessfulTimestamp
RowsRead
RowsWritten
```

This method is only for explicit watermark correction / recovery.

---

# 6. Gap 4 - Batch Run Read Model

## Problem

Current repository can write batch runs but cannot retrieve them.

Temporal and the UI need to inspect existing execution records.

## Required models

Add:

```python
class BatchRunRecord(DBRow):
    batch_run_id: UUID = Field(alias="BatchRunId")
    plan_guid: UUID | None = Field(alias="PlanGUID")
    batch_type: str = Field(alias="BatchType")
    trigger_type: str | None = Field(alias="TriggerType")
    temporal_workflow_id: str | None = Field(alias="TemporalWorkflowId")
    temporal_run_id: str | None = Field(alias="TemporalRunId")
    fabric_job_instance_id: str | None = Field(alias="FabricJobInstanceId")
    started_timestamp: datetime = Field(alias="StartedTimestamp")
    completed_timestamp: datetime | None = Field(alias="CompletedTimestamp")
    status: str = Field(alias="Status")
    total_objects: int | None = Field(alias="TotalObjects")
    succeeded_objects: int | None = Field(alias="SucceededObjects")
    failed_objects: int | None = Field(alias="FailedObjects")
    error_message: str | None = Field(alias="ErrorMessage")
```

Add:

```python
class BatchObjectRunRecord(DBRow):
    batch_object_run_id: UUID = Field(alias="BatchObjectRunId")
    batch_run_id: UUID = Field(alias="BatchRunId")
    object_guid: UUID | None = Field(alias="ObjectGUID")
    object_type: str = Field(alias="ObjectType")
    object_name: str | None = Field(alias="ObjectName")
    fabric_pipeline_run_id: str | None = Field(alias="FabricPipelineRunId")
    fabric_notebook_run_id: str | None = Field(alias="FabricNotebookRunId")
    started_timestamp: datetime = Field(alias="StartedTimestamp")
    completed_timestamp: datetime | None = Field(alias="CompletedTimestamp")
    rows_read: int | None = Field(alias="RowsRead")
    rows_written: int | None = Field(alias="RowsWritten")
    old_watermark_value: str | None = Field(alias="OldWatermarkValue")
    new_watermark_value: str | None = Field(alias="NewWatermarkValue")
    status: str = Field(alias="Status")
    error_message: str | None = Field(alias="ErrorMessage")
```

---

# 7. Gap 5 - Get/List Batch Runs

## Required methods

Add:

```python
def get_batch_run(
    self,
    batch_run_id: UUID,
) -> BatchRunRecord | None:
    ...
```

Add:

```python
def list_batch_runs_for_plan(
    self,
    plan_guid: UUID,
    *,
    limit: int = 100,
) -> list[BatchRunRecord]:
    ...
```

Suggested ordering:

```sql
ORDER BY StartedTimestamp DESC
```

Validate:

```python
limit > 0
```

Add:

```python
def list_batch_object_runs(
    self,
    batch_run_id: UUID,
) -> list[BatchObjectRunRecord]:
    ...
```

Suggested ordering:

```sql
ORDER BY StartedTimestamp, BatchObjectRunId
```

---

# 8. Gap 6 - Set Fabric Job ID After Submission

## Problem

Typical Temporal execution sequence:

```text
1. start BatchRun
2. submit Fabric notebook/pipeline
3. Fabric returns job instance ID
4. persist job ID
```

Current `BatchRunCreate` supports a Fabric job ID at creation time, but the ID often does not exist yet.

## Required method

Add:

```python
def set_batch_fabric_job_id(
    self,
    batch_run_id: UUID,
    fabric_job_instance_id: str,
) -> None:
    ...
```

### Validation

Reject empty IDs.

### SQL

```sql
UPDATE bronze_replication.BatchRuns
SET FabricJobInstanceId = ?
WHERE BatchRunId = ?
  AND Status = 'RUNNING'
```

If the batch is not running or does not exist:

raise `ValueError`.

---

# 9. Gap 7 - Set Notebook/Pipeline Run IDs on Batch Object Runs

## Required methods

Add:

```python
def set_batch_object_notebook_run_id(
    self,
    batch_object_run_id: UUID,
    notebook_run_id: str,
) -> None:
    ...
```

Add:

```python
def set_batch_object_pipeline_run_id(
    self,
    batch_object_run_id: UUID,
    pipeline_run_id: str,
) -> None:
    ...
```

Only update rows currently in:

```text
RUNNING
```

Do not overwrite a populated run ID with a different value unless explicitly required.

Recommended guard:

```sql
AND FabricNotebookRunId IS NULL
```

or:

```sql
AND FabricPipelineRunId IS NULL
```

If the same value is supplied again, idempotent success is acceptable.

---

# 10. Gap 8 - Batch Cancellation

## Problem

Temporal workflows and Fabric notebook/pipeline runs can be cancelled.

Config DB needs to represent this state.

## Required methods

Add:

```python
def cancel_batch_run(
    self,
    batch_run_id: UUID,
    *,
    reason: str | None = None,
) -> None:
    ...
```

SQL behavior:

```text
Status = CANCELLED
CompletedTimestamp = current UTC
ErrorMessage = cancellation reason if supplied
```

Only cancel:

```text
RUNNING
```

Add:

```python
def cancel_batch_object_run(
    self,
    batch_object_run_id: UUID,
    *,
    reason: str | None = None,
) -> None:
    ...
```

Same behavior.

Do not allow cancellation of:

```text
SUCCEEDED
FAILED
CANCELLED
```

---

# 11. Gap 9 - Preserve Previous Watermark in Batch Object Run

## Context

`BatchObjectRuns` already contains:

```text
OldWatermarkValue
NewWatermarkValue
```

but `start_batch_object_run(...)` only persists fields supplied in `BatchObjectRunCreate`.

The current create model does not expose the old watermark.

## Change

Extend:

```python
class BatchObjectRunCreate(DBRow):
```

with:

```python
old_watermark_value: str | None = Field(
    default=None,
    alias="OldWatermarkValue",
)
```

This lets the workflow record:

```text
watermark before replication
```

and later:

```text
watermark after successful replication
```

using the existing `complete_batch_object_run(...)`.

This is valuable for audit and recovery.

---

# 12. Gap 10 - Optional Direct Last Pipeline Run ID Update

Current:

```python
mark_replication_started(
    source_table_guid,
    pipeline_run_id=None,
)
```

is enough in most cases.

However, the sequence may be:

```text
mark replication running
submit pipeline
receive run ID
```

If workflow code cannot submit first, add:

```python
def set_replication_pipeline_run_id(
    self,
    source_table_guid: UUID,
    pipeline_run_id: str,
) -> None:
    ...
```

Only allowed when:

```text
ReplicationState.Status = RUNNING
```

This item is optional if the workflow can always submit the Fabric job before calling `mark_replication_started(...)`.

---

# 13. Methods Explicitly NOT Required

Do not add methods simply because they existed in the old `ConfigDB` utility.

The following are **not required** for new workflow code:

```python
insert_watermark_control(...)
```

The new model uses:

```text
ReplicationConfig
ReplicationState
```

Do not use the old `watermark_control` contract.

---

## Standalone source-table insert APIs

The new repository already has:

```python
persist_approved_runtime_plan(...)
```

which atomically creates:

```text
SourceTables
SourceTableColumns
ReplicationConfig
ReplicationState
FabricViews
FabricViewDependencies
```

Therefore do not add public:

```python
insert_source_table(...)
insert_source_table_columns(...)
insert_fabric_view(...)
insert_fabric_view_dependencies(...)
```

unless a real use case exists outside the approved-plan persistence path.

Keeping runtime config creation behind:

```python
persist_approved_runtime_plan(...)
```

is safer.

---

# 14. Do Not Modify These Existing Behaviors

## 14.1 Atomic runtime-plan persistence

Do not break:

```python
persist_approved_runtime_plan(...)
```

It must remain:

- one transaction;
- all-or-nothing;
- version checked;
- approval checked;
- idempotent;
- fingerprint protected.

---

## 14.2 Approved plan must be immutable for runtime persistence

If plan payload changes after approval:

```text
new PlanVersion
```

must be created.

Do not overwrite the already persisted runtime configuration.

---

## 14.3 Replication config and state remain separate

Keep:

```text
ReplicationConfig = static behavior
ReplicationState  = mutable runtime state
```

Do not merge the old watermark-table model back into the repository.

---

# 15. Recommended Exact Public API After This Change

The repository should expose approximately:

```python
class ConfigDBRepository(ConfigDB):

    # connections
    list_connections(...)
    get_connection(...)
    insert_connection(...)
    update_connection(...)

    # migration plans
    create_migration_plan(...)
    get_migration_plan(...)
    update_migration_plan_status(...)
    update_migration_plan_version(...)
    update_analysis_version(...)
    record_plan_approval(...)
    set_temporal_ids(...)

    # SAP analysis
    create_sap_analysis(...)
    get_sap_analysis(...)
    get_sap_analysis_for_plan(...)
    list_sap_analyses_for_plan(...)
    insert_sap_analysis_objects(...)
    list_sap_analysis_objects(...)
    update_sap_analysis_status(...)
    record_analysis_approval(...)
    supersede_analysis(...)

    # approved runtime plan
    persist_approved_runtime_plan(...)
    get_approved_plan_runtime_config(...)

    # source tables / columns
    get_source_table(...)
    list_source_tables_for_plan(...)
    list_source_table_columns(...)
    update_source_table_provisioning_status(...)

    # replication configuration
    create_replication_config(...)
    get_replication_config(...)
    update_replication_config(...)
    set_replication_enabled(...)

    # replication state
    create_replication_state(...)
    get_replication_state(...)
    mark_replication_started(...)
    mark_replication_succeeded(...)
    mark_replication_failed(...)
    update_watermark(...)
    set_replication_pipeline_run_id(...)  # optional

    # Fabric views
    get_fabric_view(...)
    list_fabric_views_for_plan(...)
    list_fabric_view_dependencies(...)
    update_view_provisioning_status(...)

    # batch execution
    start_batch_run(...)
    get_batch_run(...)
    list_batch_runs_for_plan(...)
    set_batch_fabric_job_id(...)
    complete_batch_run(...)
    fail_batch_run(...)
    cancel_batch_run(...)

    start_batch_object_run(...)
    list_batch_object_runs(...)
    set_batch_object_notebook_run_id(...)
    set_batch_object_pipeline_run_id(...)
    complete_batch_object_run(...)
    fail_batch_object_run(...)
    cancel_batch_object_run(...)
```

---

# 16. Suggested Implementation Details

## SQL column constants

To avoid repeating long `SELECT` lists, add:

```python
_BATCH_RUN_COLUMNS = (...)
_BATCH_OBJECT_RUN_COLUMNS = (...)
_SAP_ANALYSIS_COLUMNS = (...)
```

similar to existing:

```python
_TABLE_COLUMNS
_VIEW_COLUMNS
```

---

## Validation

Validate non-empty external run IDs:

```python
if not run_id.strip():
    raise ValueError(...)
```

---

## Error semantics

Continue using the repository's current convention:

```python
ValueError
```

for:

- missing entity;
- stale version;
- illegal state transition;
- duplicate/conflicting operation.

Use:

```python
RuntimeError
```

for internal invariant failures only.

Do not introduce a large exception hierarchy in this small change.

---

# 17. Idempotency Requirements

These methods may be retried by Temporal activities.

Design appropriately.

## Set job IDs

If the DB already contains the **same** run ID:

return successfully.

If it contains a **different** run ID:

raise `ValueError`.

This prevents a retry from accidentally overwriting correlation with another Fabric execution.

## Cancel operations

If already:

```text
CANCELLED
```

with the same target record:

idempotent success is acceptable.

Do not convert:

```text
SUCCEEDED
```

to:

```text
CANCELLED
```

on retry.

---

# 18. Tests Required

Add tests for every new method.

## SAP analysis tests

```text
DRAFT -> WAITING_APPROVAL succeeds
wrong expected status fails
wrong analysis version fails
get latest analysis for plan
get exact analysis version
list analysis history
```

## Watermark tests

```text
update existing watermark
set NULL watermark if supported
missing ReplicationState fails
status remains unchanged
```

## Batch query tests

```text
get existing BatchRun
missing BatchRun returns None
list by PlanGUID newest first
list BatchObjectRuns
limit validation
```

## Run ID tests

```text
set Fabric job ID
retry with same ID succeeds
different ID fails
set notebook run ID
set pipeline run ID
non-running record fails
```

## Cancellation tests

```text
RUNNING -> CANCELLED
CompletedTimestamp populated
reason stored
already CANCELLED retry behavior
SUCCEEDED cannot be cancelled
FAILED cannot be cancelled
```

## Old watermark audit test

```text
start BatchObjectRun with OldWatermarkValue
complete with NewWatermarkValue
both values remain queryable
```

---

# 19. Acceptance Criteria

ConfigDB repository is complete for Temporal workflow development when:

- [ ] SAP analysis can transition explicitly to `WAITING_APPROVAL`
- [ ] SAP analysis can be retrieved by `PlanGUID` and version
- [ ] SAP analysis history can be listed
- [ ] watermark can be updated independently
- [ ] BatchRun can be retrieved
- [ ] BatchRuns can be listed by plan
- [ ] BatchObjectRuns can be listed
- [ ] Fabric job ID can be assigned after batch creation
- [ ] notebook run ID can be assigned after object-run creation
- [ ] pipeline run ID can be assigned after object-run creation
- [ ] running batch can be cancelled
- [ ] running batch object can be cancelled
- [ ] old watermark can be recorded at batch-object start
- [ ] all methods are covered by unit/integration tests
- [ ] `persist_approved_runtime_plan(...)` behavior remains unchanged
- [ ] no new workflow performs raw SQL against Config DB

---

# 20. Final Instruction

Implement these gaps only.

Do **not** begin Temporal workflow implementation in this change.

Do **not** replace the existing repository design.

Do **not** reintroduce the old `watermark_control` mutation pattern.

Once these methods are implemented and tested, the ConfigDB adapter can be considered complete for the first version of the four migration workflows.
