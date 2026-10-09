# SAP table replication workflow and Config DB handling

`SAPTableMigrationWorkflow` discovers a transparent SAP table, obtains datatype
and watermark approvals, and creates or changes the corresponding Fabric
Lakehouse structure. It prepares replication configuration for the separately
scheduled ingestion pipeline. Neither the workflow nor the provisioning
notebook extracts SAP rows, runs the ingestion pipeline, or advances its watermark.

This document covers `SAP_TABLE`. SAP ODP and datasource rebuild workflows are
separate routes and do not use this existing-table approval sequence.

## Config DB tables

All tables below are in the `bronze_replication` schema.

| Table | Responsibility |
|---|---|
| `DBConnections` | Resolve the selected active SAP connection for metadata discovery. |
| `FabricWorkspaces` | Supply registered Fabric workspace information. |
| `MigrationPlans` | Record the request, version, Temporal IDs, approval, status and approved target-change instructions. |
| `SourceTables` | Store source/target identity, active status and provisioning status. An approved existing-target change retains the existing GUID. |
| `SourceTableColumns` | Store ordered source columns, configured Fabric types, descriptions, key flags, nullability, watermark eligibility and column selection. |
| `ReplicationConfig` | Store full/watermark load method, source watermark column and datatype, primary/merge keys, write strategy and ingestion flag. |
| `ReplicationState` | Store the last watermark, execution status and replication audit references. |
| `BatchRuns` / `BatchObjectRuns` | Audit new-table provisioning and its Fabric job/result. |
| `TargetChangeRuns` | Guard and audit backfill and replacement actions through mutation and completion. |

## 1. Create a request and discover metadata

The API starts a Temporal workflow with the selected SAP connection/table and
Fabric workspace, Lakehouse, schema and optional provisioning notebook ID.
The worker creates a `MigrationPlans` record in `DRAFT`, records Temporal IDs,
validates the Fabric selection and verifies the SAP table exists.

Discovery reads SAP field metadata and primary keys. Datatype proposals come
from `thirdparty/sap/sap_type_mapping.json`. Important Bronze mappings include:

| SAP datatype | Configured Fabric datatype | Purpose |
|---|---|---|
| `CLNT` | `VARCHAR(3)` | Keep the client identifier and leading zeros. |
| `CHAR(n)` / `NUMC(n)` | `VARCHAR(n)` | Preserve source text and numeric-character identifiers. |
| `DATS` | `VARCHAR(8)` | Preserve the SAP `YYYYMMDD` representation, including initial values. |
| `TIMS` | `VARCHAR(6)` | Preserve `HHMMSS`; do not interpret it as a standalone change watermark. |
| `DEC`, `CURR`, `QUAN` | `DECIMAL(p,s)` | Retain configured numeric precision and scale. |

The notebook normalizes configured text types to physical Delta `STRING`.
Configured lengths remain metadata; the notebook does not enforce them.
SAP namespaced column names such as `/DMBE/DEALNUMBER` are safely quoted.

## 2. Approve mappings, then choose a watermark

The plan moves to `WAITING_PLAN_APPROVAL`. The UI presents column name, source
datatype, proposed target datatype and mapping explanation. Approving this
review does not save runtime configuration yet.

The workflow next waits in `WAITING_FOR_WATERMARK_APPROVAL`. The user chooses:

- **Full load:** `IncrementalMethod=FULL`, `WriteStrategy=REPLACE`, no watermark.
- **Watermark:** `IncrementalMethod=WATERMARK`, `WriteStrategy=UPSERT`, the selected
  source column/datatype and discovered primary keys. A primary key is required.

Candidates include date types and timestamp types or recognized SAP timestamp
data elements (`TIMESTAMP`, `TIMESTAMPL`, `TZNTSTMPS`, `TZNTSTMPL` for packed
decimal timestamps). Generic decimal fields are not automatically timestamps.
Standalone `TIMS` fields are excluded because times repeat every day.
SAP index metadata is unverified; no Oracle index discovery is implied.

Select a field that tracks modifications. Creation dates can miss subsequent
updates. A date watermark has day-level granularity: the ingestion pipeline
must handle equal values, an overlap window, initial dates and late updates.
Packed timestamps remain decimal values in Bronze and require source-aware
interpretation by the loader or downstream transformations.

The user can return to mapping review or reject the request before persistence.

## 3. Review the existing Fabric target

After watermark approval, the worker finds active `SourceTables` records with
the same workspace ID, Lakehouse ID, schema and target table name.

It compares selected columns, canonical physical datatype, known nullability,
primary-key membership, source datatype and supplied description. It also
compares replication method, write strategy, keys, watermark settings and the
other persisted replication controls. The staging `IngestionFlag=False` value
is not treated as a requested change to the existing ingestion flag.

The UI shows added, removed and changed columns and changed replication settings.
The review is based on saved Config DB metadata. Physical Fabric schema is
unverified at review time and checked by the notebook before executing a change.

- **No existing target:** follow new-table provisioning below.
- **Identical structure and replication settings:** report `DUPLICATE_TARGET`
  and mark the new request `REJECTED`; create no second runtime configuration.
- **Different definition:** require explicit target-change approval.
- **Different source identity or multiple active records:** stop for reconciliation
  rather than choosing an unrelated or ambiguous configuration.

Runtime persistence also checks for a competing active target inside its
transaction, preventing a second configuration if one appeared after review.

## 4. Provision a new table

Approval records the approver in `MigrationPlans`. A single transaction creates
`SourceTables`, its `SourceTableColumns`, `ReplicationConfig` with ingestion
disabled, and `ReplicationState` with an initial NULL watermark. It records the
runtime fingerprint and moves the plan to `READY_TO_PROVISION`.

Column inserts are batched within SQL parameter limits. Persistence has a
180-second activity timeout and bounded retries. Deterministic object GUIDs and
the fingerprint make a retry of the same approved payload idempotent.

With a notebook selected, the workflow records a provisioning batch, submits
the Fabric job, polls it and parses the notebook object report. Fabric job
completion alone is not sufficient: the notebook must report successful objects.
After provisioning succeeds, the configuration becomes eligible for scheduled
ingestion. With no notebook selected, the workflow ends `PLANNED` and leaves the
configuration waiting to be provisioned.

## 5. Approve and apply an existing-target change

The new request records `CHANGE_REQUESTED`, its approver, the existing table GUID,
the reviewed baseline and proposed columns/replication configuration in
`MigrationPlans.Notes`. It does not create another `SourceTables` record.

| Action | Table behavior | Watermark behavior |
|---|---|---|
| `REVISE_PLANNED` | Update an active PENDING configuration only; require that no physical table exists. No Fabric DDL. | No reset; no ingestion run. |
| `ALTER_FUTURE` | Apply supported in-place changes and retain existing rows. | Preserve the last watermark. |
| `ALTER_BACKFILL` | Apply supported in-place changes and retain existing rows. A replication-only change may require no column DDL. | Reset to NULL and mark replication `NOT_STARTED`. |
| `REPLACE_FULL` | Recreate an empty table with the approved structure. | Reset to NULL and mark replication `NOT_STARTED`. |

Changing primary/merge keys requires replacement. Changing the incremental
method, watermark column or watermark datatype requires a reset action, so
`ALTER_FUTURE` is not offered. The API and workflow enforce these restrictions;
the notebook independently validates them. Unsafe narrowing or column removal
cannot be applied in place and requires an appropriate replacement approval.

The notebook acquires the target lease, checks provisioning status, pauses
ingestion where enabled, requires the loader to be idle and verifies the physical
schema against the recorded baseline. It rechecks the approved replication
baseline so a stale approval cannot overwrite a subsequent configuration change.

The notebook updates existing `SourceTableColumns` and `ReplicationConfig` records.
Backfill/replacement commits these changes, the watermark reset, change-run
completion and restoration of the previous ingestion flag in one Config DB
transaction. It preserves historical run references, timestamps and row counts,
clears the replication error and sets its status to `NOT_STARTED`.

A successful object report lets the workflow move the change request to
`CHANGE_APPLIED`. The existing table GUID remains the pipeline's configuration
identity. Without a notebook, the request remains `CHANGE_REQUESTED` for execution
later. Revising a PENDING configuration does not itself provision its table;
that existing configuration still needs its normal provisioning run.

## Failure and recovery

Rejection saves no runtime changes. Pre-mutation failures restore the previous
ingestion flag where possible. Failures after a possible mutation keep ingestion
paused for reconciliation. `TargetChangeRuns` prevents automatic repetition of
an uncertain historical reload mutation. Missing/running replication state,
stale configuration, unexpected physical schema or unsafe DDL stops execution.

Failure reporting rereads plan status when a timed-out synchronous persistence
operation commits late. Always inspect Config DB and Fabric job outcomes before
retrying a failed request: a Temporal timeout does not prove the SQL save rolled back.

Temporal patch markers preserve older SAP histories that skipped watermark or
target-change approval. Start a new workflow after updating the worker to use
the complete review sequence. Upload the current
`thirdparty/fabric/nb_migration_provisioning.py` to Fabric before executing changes.
`ALTER_BACKFILL` and `REPLACE_FULL` require the Config DB migration
`thirdparty/configdb/migrations/003_target_change_runs.sql`.
