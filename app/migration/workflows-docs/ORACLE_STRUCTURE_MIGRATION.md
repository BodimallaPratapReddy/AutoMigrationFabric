# Oracle migration workflow and Config DB handling

This document describes what `OracleTableMigrationWorkflow` does and how it
reads and updates the `bronze_replication` Config DB tables.

The workflow discovers Oracle table metadata, obtains approvals, saves the
approved configuration, and optionally provisions or changes the Fabric table
structure. The scheduled replication pipeline owns data loading. The workflow
and provisioning notebook do not load Oracle rows or start that pipeline.

## 1. Config DB tables involved

| Table | Purpose in the Oracle workflow |
|---|---|
| `DBConnections` | Resolve the selected active Oracle connection. The worker reads connection details for metadata discovery. |
| `FabricWorkspaces` | Validate that the selected workspace is registered and active. |
| `MigrationPlans` | Store each migration's identity, version, approval, status, Temporal IDs and approved target-change instructions. |
| `SourceTables` | Store source/target identity and provisioning status for the runtime table configuration. |
| `SourceTableColumns` | Store ordered source columns, approved Fabric types, descriptions, nullability and key/watermark flags. |
| `ReplicationConfig` | Store load method, watermark column/type/index, keys, write strategy and ingestion eligibility. |
| `ReplicationState` | Store the current watermark and replication execution state. Historical reload actions reset this state. |
| `BatchRuns` | Audit a new-table provisioning run and its Fabric job ID. |
| `BatchObjectRuns` | Audit each object in that provisioning batch. Oracle normally has one table. |
| `TargetChangeRuns` | Guard and audit `ALTER_BACKFILL` and `REPLACE_FULL` operations on an existing table. |

The Oracle workflow does not create `SAPAnalysis`, `SAPAnalysisObjects`,
`FabricViews` or `FabricViewDependencies` records.

## 2. Validate the selection and create the plan

The API reads `DBConnections` to check that the selected connection is active
and allows an Oracle table migration. It reads `FabricWorkspaces` to check the
selected active workspace. These registry rows are not changed by migration.

After Temporal starts, the worker creates a `MigrationPlans` row in `DRAFT`
status, with source connection, source object, system `ORACLE`, object type
`TABLE`, approach `ORACLE_TABLE` and initial version. It saves the Temporal
workflow/run IDs on that row. The plan GUID is derived from the workflow ID so
creation retries use the same identity.

The worker validates the Fabric target and selected notebook, if supplied,
before discovering Oracle metadata.

## 3. Discover Oracle metadata and propose the structure

The worker reads the selected Oracle connection from `DBConnections` inside an
activity, tests it, and inspects the table. It discovers columns, types,
descriptions, nullability, primary/unique keys, indexes and watermark candidates.
It deterministically proposes Fabric column types without an LLM.

The proposal is held in workflow state for review. At this stage no
`SourceTables`, `SourceTableColumns` or replication runtime rows are inserted.
`MigrationPlans.Status` becomes `WAITING_PLAN_APPROVAL`.

## 4. Obtain column and load-method approval

The reviewer first approves or edits every column's Fabric datatype. Each
discovered column must be represented exactly once. Confirming this grid only
updates the workflow proposal; runtime rows are not persisted yet.

The reviewer then chooses the future loading method:

| Choice | Proposed `ReplicationConfig` values |
|---|---|
| Full load | `IncrementalMethod=FULL`, `WriteStrategy=REPLACE`; no watermark column or index. |
| Discovered watermark column | `IncrementalMethod=WATERMARK`, `WriteStrategy=UPSERT`, selected watermark column/type and an available index name. Requires a source primary key. |

The reviewer can return to column mapping before completing load-method
approval. Rejecting either approval sets the migration plan and workflow to
`REJECTED`; no new runtime configuration is saved.

## 5. Compare with an existing saved Fabric target

After the approvals, the worker reads active `SourceTables` records for the
same Fabric workspace, Lakehouse, schema and table, including pending records.
It reads their `SourceTableColumns` and compares target column names, Fabric
types and nullability with the proposed columns.

This compares saved configuration. It does not establish whether the physical
Fabric table exists or matches the saved schema; the notebook checks that later.
It also does not compare every replication setting or description.

The workflow branches as follows:

- **No existing record:** save a new approved runtime configuration.
- **Same recorded columns:** end in phase `DUPLICATE_TARGET` with status
  `REJECTED`; do not insert a second runtime configuration.
- **Changed recorded columns:** ask the reviewer to approve an existing-target
  action. Prefer a provisioned matching record; otherwise use the first match.

## 6. New-table path

The worker records approver and approval timestamp on `MigrationPlans`, then
persists the approved runtime plan in one Config DB transaction:

| Table | Result |
|---|---|
| `SourceTables` | Insert the source/target configuration with provisioning status `PENDING`. |
| `SourceTableColumns` | Insert the approved source/Fabric column definitions. |
| `ReplicationConfig` | Insert approved load settings with `IngestionFlag=0`. |
| `ReplicationState` | Insert initial replication state, including a `NULL` watermark. |
| `MigrationPlans` | Save `RuntimePlanHash` and move to `READY_TO_PROVISION`. |

Persistence verifies approval/version and uses stable child IDs and a payload
fingerprint. An identical retry reuses the saved configuration; a different
payload is rejected. Child insertion failure rolls back the transaction.

If no provisioning notebook is selected, the workflow ends as `PLANNED`.
The configuration remains saved, the physical table is not created, and
replication remains disabled.

With a notebook, the worker moves the plan to `PROVISIONING`, creates
`BatchRuns` and `BatchObjectRuns`, submits `plan_guid` to Fabric and records the
job ID. The notebook reads the approved plan and runtime columns and creates
an empty Delta table. It does not silently overwrite an existing physical table.

After a successful notebook report, the worker:

1. Marks `SourceTables.ProvisioningStatus=PROVISIONED` and records the job ID.
2. Completes `BatchObjectRuns` and `BatchRuns` as successful.
3. Sets `MigrationPlans.Status=PROVISIONED`.
4. Sets `ReplicationConfig.IngestionFlag=1` for the separately scheduled loader.

The workflow ends `COMPLETED`. It does not set replication state to `RUNNING`,
advance its watermark or submit a data-loading job.

## 7. Existing-target change path

The reviewer approves one action. The worker saves the approval and
`MigrationPlans.Status=CHANGE_REQUESTED`. `MigrationPlans.Notes.target_change`
contains the action, existing source-table GUID, reviewed differences, target
identity and proposed table/column/replication configuration.

The change plan does not insert another `SourceTables` record. The notebook
updates the existing runtime table identified by
`Notes.target_change.existing_source_table_guid`. Its columns, replication
configuration and state remain linked to that existing GUID.

Without a notebook, the workflow ends `CHANGE_REQUESTED` and makes no physical
change. With a notebook, it executes the approved action below.

| Action | Physical Fabric effect | Config DB effect |
|---|---|---|
| `REVISE_PLANNED` | None. Requires a pending record and no physical table. | Rewrite existing columns and proposed replication settings; retain pending status and watermark. |
| `ALTER_FUTURE` | Apply supported schema changes; retain existing rows. | Update columns/replication settings; preserve watermark. |
| `ALTER_BACKFILL` | Apply supported schema changes; retain existing rows. | Update columns/replication settings and reset watermark/state for historical replay. |
| `REPLACE_FULL` | Replace the target with an empty table using the approved schema. | Update columns/replication settings and reset watermark/state for repopulation. |

The notebook rechecks target identity, current recorded configuration and
physical schema. The alter paths reject unsafe changes such as column removal,
narrowing or incompatible key changes; they do not automatically turn into
replacement. Supported in-place changes include nullable additions and safe
type widening. Replacement can recreate the approved structure because it
explicitly replaces the target contents.

### Watermark reset and ingestion handling

For `ALTER_BACKFILL` and `REPLACE_FULL`, the notebook requires an existing
`ReplicationState` row that is not `RUNNING`, pauses ingestion if enabled, and
rechecks that no replication is running. A per-target lease serializes notebook
changes.

It creates or validates `TargetChangeRuns`, records `MUTATION_STARTED` before
the physical table change, and verifies the resulting schema. It then commits
these changes in one Config DB transaction:

- Replace the existing table's `SourceTableColumns` with approved definitions.
- Update approved `ReplicationConfig` settings.
- Set `ReplicationState.LastWatermarkValue=NULL`.
- Set `ReplicationState.Status=NOT_STARTED` and clear `ErrorMessage`.
- Mark `TargetChangeRuns.Phase=COMPLETED`.
- Restore the ingestion flag to its value before the operation.

Prior replication run IDs, timestamps and row counts remain available as audit
history. A previously paused table remains paused. No source staging table is
created; the legacy required `TargetChangeRuns.StageTableName` is an empty
string. The notebook does not read Oracle rows or hold Oracle source locks.

The scheduled pipeline must interpret the `NULL` watermark as the starting
boundary for historical replay and apply its configured merge/overwrite
strategy. Backfill retains target rows for that replay; replacement leaves an
empty target for repopulation. The notebook does not perform either data load.
`ALTER_FUTURE` preserves the watermark and restores the prior ingestion setting
without creating a historical reload change-run record.

After the worker validates one successful TABLE result and zero views, it sets
`MigrationPlans.Status=CHANGE_APPLIED` and ends the workflow `COMPLETED`.
The change plan does not use new-table provisioning batch ownership or enable
replication independently of the notebook's preserved ingestion setting.

## 8. Failure, cancellation and completion meaning

A Fabric job reporting `Completed` is insufficient. The worker also requires a
valid notebook exit report with `SUCCESS`, matching plan GUID and consistent
object counts. A logical notebook failure is a failed migration even if the
Fabric execution itself completed normally.

On application failure or cancellation, the worker updates the migration plan
and any running provisioning batch/object audit. New-table provisioning failures
in the provisioning/submitted-job phases also mark affected table records
`FAILED`. These updates do not undo physical DDL already committed.

For historical reload actions, failure before physical mutation attempts to
restore the previous enabled ingestion flag. Failure after possible mutation
leaves ingestion paused and the change run at `MUTATION_STARTED`; automatic
reuse of that run is blocked until reconciliation. Delta and Config DB cannot
commit as one distributed transaction, so physical structure and saved metadata
must be reconciled before resuming the pipeline.

Submission with a lost response can have an uncertain outcome; the workflow
does not blindly repeat notebook submission. Cancellation during a running job
requests Fabric cancellation and monitors the result, but cannot reverse an
already-applied structure change.

Workflow and plan statuses describe different things:

| Outcome | Workflow status | Config DB plan status |
|---|---|---|
| New structure successfully provisioned | `COMPLETED` | `PROVISIONED` |
| Existing-target action successfully applied | `COMPLETED` | `CHANGE_APPLIED` |
| New configuration saved without notebook | `PLANNED` | `READY_TO_PROVISION` |
| Change approved without notebook | `CHANGE_REQUESTED` | `CHANGE_REQUESTED` |
| Duplicate structure or rejected approval | `REJECTED` | `REJECTED` |
| Failed/cancelled operation, when audit update succeeds | `FAILED` / `CANCELLED` | `FAILED` / `CANCELLED` |

The workflow's completion means configuration and structure processing finished.
The separate pipeline later loads eligible active, provisioned tables, updates
`ReplicationState` and advances the watermark. Its replication queue is global,
not limited to the migration plan GUID.

## Implementation references

- [Workflow orchestration](../workflow_base.py)
- [Oracle discovery](../source_activities.py) and [plan construction](../plans.py)
- [Config DB activities](../config_activities.py) and [repository](../../../thirdparty/configdb/repository.py)
- [Provisioning notebook](../../../thirdparty/fabric/nb_migration_provisioning.py)
- [Config DB schema](../../../thirdparty/configdb/config_db_new_database.sql)
