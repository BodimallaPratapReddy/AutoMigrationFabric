# Coding Agent Handover
## Temporal Workflows + Thin UI for Automated Legacy-to-Fabric Migration

**Project:** Automated migration from Oracle DB / SAP ECC to Microsoft Fabric  
**Orchestration:** Temporal.io  
**UI:** Thin workflow-control UI first; richer conversational UI later  
**Observability:** Arize Phoenix for LLM/agent traces; Temporal history for durable workflow execution  
**Persistence:** Config DB  
**Execution target:** Microsoft Fabric notebooks + replication pipelines

---

# 1. Objective

Build the Temporal orchestration layer and a minimal UI/API on top of the existing adapters.

The existing adapter layer is assumed to provide:

- Oracle adapter
- SAP ECC adapter
- Microsoft Fabric adapter
- Config DB repository
- deterministic datatype mapping
- LLM / analysis adapter for SAP rebuild flow

Do not duplicate adapter logic inside workflows.

The Temporal workflows should orchestrate the adapters and services, persist durable state, wait for human approvals, trigger Fabric execution, and recover safely from failures.

The thin UI should allow users to:

- start one of the four workflows
- enter source/target information
- see workflow state
- see discovered metadata
- approve or reject
- submit feedback for SAP rebuild analysis
- see Fabric provisioning/replication state
- cancel a workflow

The thin UI is not the final chatbot experience. It is primarily a workflow-control and validation surface.

---

# 2. Four Workflows

Implement exactly four top-level workflows.

```python
OracleTableMigrationWorkflow
SAPTableMigrationWorkflow
SAPODPDataSourceMigrationWorkflow
SAPDataSourceRebuildWorkflow
```

Recommended canonical migration approach values:

```text
ORACLE_TABLE
SAP_TABLE
SAP_ODP
SAP_REBUILD
```

Keep these values consistent across:

- UI
- API
- Temporal workflow input
- Config DB `MigrationPlans`
- logs
- metrics

---

# 3. Shared Architecture

```text
Thin UI
   |
   v
FastAPI / API Layer
   |
   v
Temporal Client
   |
   v
Temporal Workflows
   |
   +--> Oracle Activities
   +--> SAP Activities
   +--> Config DB Activities
   +--> Fabric Activities
   +--> LLM Activities
   |
   v
Config DB / Fabric / Source Systems
```

Important responsibility split:

```text
Workflow
    = orchestration and state transitions

Activity
    = external I/O / adapter invocation

Adapter
    = system-specific transport/discovery

Config DB
    = persistent configuration and audit state

Fabric Notebook
    = deterministic provisioning implementation

Replication Pipeline
    = physical data movement

UI
    = user interaction / approval / feedback

LLM
    = interpretation and plan generation only where required
```

---

# 4. Temporal Determinism Rules

Do not perform external I/O directly in Temporal workflow code.

All of the following must be activities:

```text
Oracle calls
SAP calls
Fabric REST calls
Config DB calls
LLM calls
Phoenix-related model tracing calls
```

Workflow code may perform:

- branching
- state updates
- waiting
- Temporal timers
- signal/update handling
- child workflow execution
- deterministic validation
- orchestration decisions from already-returned activity results

Do not call:

```python
requests
httpx
oracledb
mssql_python
OpenAI/LLM SDK
Fabric REST API
SAP HTTP endpoint
```

directly from workflow code.

---

# 5. Recommended Project Structure

```text
backend/
  api/
    routes/
      migrations.py
      workflow_status.py
    models/
      migration_requests.py
      migration_responses.py

  temporal/
    client.py

    workflows/
      oracle_table.py
      sap_table.py
      sap_odp.py
      sap_rebuild.py
      base.py

    activities/
      oracle_activities.py
      sap_activities.py
      config_activities.py
      fabric_activities.py
      llm_activities.py

    models/
      workflow_inputs.py
      workflow_state.py
      approval_models.py

  services/
    oracle_inspection_service.py
    sap_analysis_service.py
    migration_plan_service.py
    fabric_validation.py
    fabric_provisioning_service.py

  integrations/
    oracle/
    sap/
    fabric/
    llm/

  repositories/
    config_db/

frontend/
  migration-ui/
```

Do not create one giant Temporal file.

---

# 6. Shared Workflow Input Model

Define a stable input contract.

Example:

```python
class MigrationWorkflowInput(BaseModel):
    migration_approach: Literal[
        "ORACLE_TABLE",
        "SAP_TABLE",
        "SAP_ODP",
        "SAP_REBUILD",
    ]

    source_connection_name: str

    source_object_name: str

    source_schema_name: str | None = None

    fabric_workspace_id: str
    fabric_lakehouse_id: str
    fabric_schema_name: str

    fabric_target_name: str | None = None

    provisioning_notebook_id: str

    replication_pipeline_id: str | None = None

    requested_by: str | None = None
```

The workflow should create the Config DB migration plan early.

Do not require the UI to provide every runtime configuration field at startup.

The workflow should discover and propose configuration.

---

# 7. Shared Workflow Runtime State

Define a queryable workflow state model.

Example:

```python
class MigrationWorkflowState(BaseModel):
    workflow_id: str

    plan_guid: UUID | None = None

    migration_approach: str

    phase: str

    status: str

    source_object_name: str

    analysis_version: int | None = None
    plan_version: int | None = None

    waiting_for_user: bool = False

    current_message: str | None = None

    last_error: str | None = None

    fabric_job_instance_id: str | None = None

    pipeline_job_instance_id: str | None = None
```

Expose this through a Temporal Query.

Recommended query:

```python
@workflow.query
def get_state(self) -> MigrationWorkflowState:
    ...
```

The UI should read workflow state through API endpoints rather than reading Temporal internals directly.

---

# 8. Shared Workflow Phases

Use common phase names wherever possible.

Recommended:

```text
CREATED
VALIDATING_SOURCE
DISCOVERING_SOURCE
BUILDING_PLAN
WAITING_FOR_PLAN_APPROVAL
PERSISTING_RUNTIME_CONFIG
VALIDATING_FABRIC
PROVISIONING
WAITING_FOR_PROVISIONING
READY_FOR_REPLICATION
REPLICATING
WAITING_FOR_REPLICATION
VALIDATING_RESULT
COMPLETED
FAILED
CANCELLED
```

SAP rebuild additionally uses:

```text
ANALYZING_EXTRACTOR
WAITING_FOR_ANALYSIS_APPROVAL
GENERATING_FABRIC_PLAN
WAITING_FOR_FABRIC_PLAN_APPROVAL
```

Keep the state machine explicit.

---

# 9. Shared Temporal Activities

Implement reusable activities.

## Config DB Activities

```python
create_migration_plan_activity(...)
set_temporal_ids_activity(...)
update_plan_status_activity(...)

persist_approved_runtime_plan_activity(...)

start_batch_run_activity(...)
complete_batch_run_activity(...)
fail_batch_run_activity(...)
cancel_batch_run_activity(...)

start_batch_object_run_activity(...)
complete_batch_object_run_activity(...)
fail_batch_object_run_activity(...)
cancel_batch_object_run_activity(...)

set_batch_fabric_job_id_activity(...)
set_batch_object_notebook_run_id_activity(...)
set_batch_object_pipeline_run_id_activity(...)
```

Use ConfigDB repository methods only.

No raw SQL inside activity code unless repository intentionally exposes it.

---

## Fabric Activities

```python
validate_fabric_target_activity(...)

submit_provisioning_notebook_activity(...)

get_provisioning_notebook_status_activity(...)

cancel_provisioning_notebook_activity(...)

submit_replication_pipeline_activity(...)

get_replication_pipeline_status_activity(...)

cancel_replication_pipeline_activity(...)
```

Each activity should perform one bounded external operation.

Do not put long polling loops in activities.

---

## Oracle Activities

```python
test_oracle_connection_activity(...)

inspect_oracle_table_activity(...)

map_oracle_columns_activity(...)
```

Prefer the high-level Oracle inspection service instead of many low-level calls in the workflow.

---

## SAP Activities

```python
test_sap_connection_activity(...)

inspect_sap_table_activity(...)

get_sap_datasource_context_activity(...)

resolve_sap_extraction_route_activity(...)

inspect_sap_dbview_activity(...)

inspect_sap_infoset_activity(...)

scrape_sap_abap_activity(...)

verify_sap_analysis_objects_activity(...)

map_sap_columns_activity(...)
```

---

## LLM Activities

```python
analyze_sap_extractor_activity(...)

revise_sap_extractor_analysis_activity(...)

generate_fabric_plan_activity(...)

revise_fabric_plan_activity(...)
```

LLM activities should be Phoenix-instrumented.

---

# 10. Activity Retry Strategy

Do not use one retry policy for every activity.

## Safe retry activities

GET/read-only metadata activities may retry.

Examples:

```text
Oracle metadata reads
SAP metadata reads
Config DB reads
Fabric status GETs
```

Suggested retry policy:

```text
initial interval: 2-5 seconds
backoff: 2
maximum interval: 30-60 seconds
maximum attempts: 4-6
```

Adjust according to environment.

---

## Config DB writes

Write activities should rely on:

- transactions
- optimistic version checks
- idempotent identifiers

Temporal retries are acceptable only when repository behavior is idempotent.

---

## Fabric submission POSTs

Be conservative.

A network timeout can happen after Fabric accepted the job but before the client received the response.

Do not blindly retry:

```text
submit provisioning notebook
submit replication pipeline
cancel notebook
cancel pipeline
```

Use the Fabric execution contract.

Submission activities should use low retry counts or no automatic retry for ambiguous transport failures.

---

# 11. Durable Fabric Status Monitoring Pattern

Do not poll inside an activity.

Use:

```text
submit activity
    ->
job ID
    ->
Temporal timer
    ->
status activity
    ->
running?
    -> timer
    -> status activity
```

Example workflow helper:

```python
async def wait_for_fabric_job(...):
    while True:
        status = await workflow.execute_activity(
            get_status_activity,
            ...,
        )

        if is_terminal(status.normalized_status):
            return status

        delay = status.retry_after_seconds or 10

        await workflow.sleep(delay)
```

Keep this deterministic.

---

# 12. Human-in-the-Loop Design

Use Temporal Signals or Updates for approvals and feedback.

Recommended commands:

```text
approve_plan
reject_plan
submit_plan_feedback

approve_analysis
reject_analysis
submit_analysis_feedback

cancel_migration
```

For SAP rebuild:

```text
analysis approval
    and
fabric-plan approval
```

must be separate gates.

---

# 13. Approval Data Models

Example:

```python
class ApprovalDecision(BaseModel):
    approved_by: str
    comment: str | None = None
```

Example feedback:

```python
class AnalysisFeedback(BaseModel):
    submitted_by: str
    message: str
```

Do not pass raw UI session objects into Temporal.

---

# 14. Workflow 1 - Oracle Table -> Fabric

Class:

```python
OracleTableMigrationWorkflow
```

## Flow

```text
1. Create MigrationPlan
2. Save Temporal IDs
3. Validate Oracle connection
4. Inspect Oracle table
5. Map Oracle -> Fabric datatypes
6. Build proposed runtime plan
7. Set status WAITING_PLAN_APPROVAL
8. Wait for user approval
9. Persist approved runtime plan
10. Validate Fabric target
11. Start provisioning batch
12. Submit provisioning notebook(plan_guid)
13. Persist Fabric job ID
14. Wait for notebook terminal state
15. Parse provisioning result
16. Update table provisioning status
17. Mark plan READY / PROVISIONED as appropriate
18. Enable replication
19. Submit replication pipeline
20. Persist pipeline job ID
21. Wait for pipeline terminal state
22. Update replication/batch state
23. Validate completion
24. Mark plan PROVISIONED / COMPLETED
```

---

## Proposed Oracle runtime plan content

```text
SourceTables
SourceTableColumns
ReplicationConfig
ReplicationState
```

No Fabric view is normally required.

---

## Oracle approval screen should show

```text
Source
Schema.Table

Columns
Oracle type
Fabric type

Primary key
Unique keys

Watermark candidates

Selected load method:
FULL / WATERMARK

Write strategy:
APPEND / UPSERT / SCD1 / SCD2 / REPLACE

Fabric target
Workspace
Lakehouse
Schema
Table
```

---

# 15. Workflow 2 - SAP Table -> Fabric

Class:

```python
SAPTableMigrationWorkflow
```

## Flow

```text
1. Create MigrationPlan
2. Save Temporal IDs
3. Validate SAP connection
4. Validate SAP table
5. Inspect SAP table metadata
6. Identify key fields
7. Identify watermark candidates
8. Map SAP -> Fabric datatypes
9. Build runtime plan
10. Wait for plan approval
11. Persist approved runtime plan
12. Validate Fabric target
13. Trigger provisioning notebook
14. Wait for provisioning
15. Enable replication
16. Trigger replication pipeline
17. Wait for replication
18. Update batch + replication state
19. Complete
```

No LLM is required.

---

# 16. Workflow 3 - SAP ODP DataSource -> Fabric

Class:

```python
SAPODPDataSourceMigrationWorkflow
```

## Flow

```text
1. Create MigrationPlan
2. Save Temporal IDs
3. Validate SAP connection
4. Validate DataSource
5. Read DataSource details
6. Read ODP capability
7. If ODP unsupported -> fail with clear reason
8. If ODP unknown -> wait for user/manual decision or fail gracefully
9. Read DataSource exposed fields
10. Read extract structure metadata
11. Read delta metadata
12. Map SAP -> Fabric datatypes
13. Build ODP runtime plan
14. Wait for plan approval
15. Persist approved runtime plan
16. Validate Fabric target
17. Trigger provisioning notebook
18. Wait for provisioning
19. Enable ODP replication config
20. Trigger ODP replication pipeline
21. Wait for pipeline
22. Update replication state
23. Complete
```

---

## Important ODP rule

Do not reverse engineer ABAP in this workflow.

The user explicitly selected:

```text
Use ODP DataSource as-is
```

The DataSource remains the source contract.

---

# 17. Workflow 4 - SAP DataSource Rebuild -> Fabric

Class:

```python
SAPDataSourceRebuildWorkflow
```

This is the most important human-in-the-loop workflow.

---

## Stage A - DataSource discovery

```text
1. Create MigrationPlan
2. Save Temporal IDs
3. Validate SAP connection
4. Read DataSource metadata
5. Read extract structure metadata
6. Read exposed DataSource fields
7. Resolve extraction route
```

---

## Stage B - Branch by extraction route

### TABLE

```text
identify source table
inspect table
map datatypes
build analysis
```

### DB_VIEW

```text
get DB view details
validate SQL completeness
identify source tables/views
inspect dependencies
build lineage
```

### INFOSET_QUERY

```text
get InfoSet details
validate template completeness
identify source tables
inspect dependencies
build lineage
```

### FUNCTION_MODULE

```text
run ABAP scraper
assess scrape completeness
build SAPDatasourceRebuildContext
call LLM analysis activity
verify every LLM-identified table/view against SAP
enrich verified objects
build lineage
```

### CLASS_BASED

```text
use supported class scraper if verified
otherwise fail with explicit unsupported reason
```

### Unsupported

```text
fail gracefully with reason code
```

---

# 18. SAP Rebuild Analysis Approval

Once analysis is built:

```text
save SAPAnalysis
save SAPAnalysisObjects
set status WAITING_APPROVAL
```

Expose to UI:

```text
Observed lineage
LLM interpretation
Verified SAP objects
Unresolved objects
Warnings
Tables
Views
Joins
Filters
Derived fields
Delta logic
```

Then wait.

User may:

```text
approve analysis
submit feedback
reject
cancel
```

---

## Feedback loop

If feedback arrives:

```text
1. create new analysis version
2. call LLM revision activity
3. verify changed SAP objects
4. persist new analysis version
5. return to WAITING_FOR_ANALYSIS_APPROVAL
```

Never overwrite approved analysis.

---

# 19. Fabric Plan Generation in SAP Rebuild

After analysis approval:

```text
call generate_fabric_plan_activity
```

The proposed plan should contain:

```text
tables to replicate
tables to reuse
target Fabric locations
view definitions
view SQL
view dependencies
creation order
replication methods
watermarks
merge keys
write strategies
```

Persist the plan in draft form / workflow state as appropriate.

Then:

```text
WAITING_FOR_FABRIC_PLAN_APPROVAL
```

---

# 20. Fabric Plan Approval UI

Show:

```text
Tables to Replicate
Views to Create
Existing Objects to Reuse
Dependencies
Fabric target names
SQL / transformation logic
Replication strategy
Watermarks
Merge keys
```

User may:

```text
approve
request changes
reject
cancel
```

If feedback:

```text
revise plan
increment PlanVersion
show again
```

---

# 21. Persist Approved SAP Rebuild Plan

Once approved:

```python
persist_approved_runtime_plan(...)
```

This should atomically create:

```text
SourceTables
SourceTableColumns
ReplicationConfig
ReplicationState
FabricViews
FabricViewDependencies
```

Do not write these independently from the workflow.

---

# 22. Fabric Provisioning

After approved runtime config is persisted:

```text
validate Fabric target
start provisioning batch
submit provisioning notebook(plan_guid)
persist notebook job ID
wait for notebook completion
parse notebook result
update provisioning states
```

The notebook should read Config DB by:

```text
plan_guid
```

Do not send:

```text
table DDL
view SQL
column JSON
```

through notebook parameters.

---

# 23. Replication

After provisioning:

```text
enable replication config
submit replication pipeline
persist pipeline job ID
wait for terminal state
update ReplicationState
update batch logs
```

The pipeline scope contract must already be finalized:

```text
GLOBAL_CONFIG_LOOKUP
or
PLAN_SCOPED_LOOKUP
```

---

# 24. Failure Handling

Every workflow should catch terminal failures and update Config DB.

Examples:

## Source discovery failure

```text
MigrationPlan.Status = FAILED
BatchRun = FAILED if already started
workflow state = FAILED
```

## Fabric provisioning failure

```text
ProvisioningStatus = FAILED
BatchRun = FAILED
Plan Status = FAILED
```

## Replication failure

```text
ReplicationState = FAILED
BatchRun = FAILED
Plan Status may remain PROVISIONED but replication failed
```

Decide whether plan status should be:

```text
FAILED
```

or:

```text
PROVISIONED_WITH_REPLICATION_ERROR
```

If a new status is needed, add it consistently.

---

# 25. Cancellation Handling

Each workflow must respond to cancellation.

Use a signal/update such as:

```text
cancel_migration
```

Behavior:

```text
if waiting for user:
    mark CANCELLED

if Fabric notebook running:
    call cancel notebook activity
    wait/check cancellation status

if Fabric pipeline running:
    call cancel pipeline activity if supported
    wait/check cancellation status

update Config DB batch records
set workflow state CANCELLED
```

Do not assume cancellation POST means execution is already stopped.

---

# 26. Workflow Queries

Expose at least:

```python
get_state()
```

SAP rebuild may additionally expose:

```python
get_analysis_summary()
get_fabric_plan_summary()
```

However, avoid exposing huge ABAP source blobs through Temporal queries.

The UI should retrieve large analysis artifacts from application persistence if needed.

---

# 27. Thin API Layer

Build a small FastAPI layer.

Recommended endpoints:

```http
POST /migrations/oracle-table
POST /migrations/sap-table
POST /migrations/sap-odp
POST /migrations/sap-rebuild
```

Each starts the corresponding Temporal workflow.

Return:

```json
{
  "workflow_id": "...",
  "run_id": "...",
  "plan_guid": "..."
}
```

If `plan_guid` is created inside the workflow after start, return workflow ID first and expose plan GUID through status.

---

## Status

```http
GET /migrations/{workflow_id}
```

Return the Temporal workflow query state.

---

## Approval

```http
POST /migrations/{workflow_id}/approve-plan
POST /migrations/{workflow_id}/reject-plan
POST /migrations/{workflow_id}/plan-feedback
```

SAP rebuild additionally:

```http
POST /migrations/{workflow_id}/approve-analysis
POST /migrations/{workflow_id}/reject-analysis
POST /migrations/{workflow_id}/analysis-feedback
```

---

## Cancel

```http
POST /migrations/{workflow_id}/cancel
```

---

# 28. Thin UI

Do not build the final chatbot first.

Build a minimal UI sufficient to exercise the workflows.

Recommended pages:

```text
New Migration
Migration Detail / Status
SAP Analysis Review
Fabric Plan Review
Execution Monitor
```

---

# 29. New Migration UI

Simple selection:

```text
What do you want to migrate?

○ Oracle Table
○ SAP Table
○ SAP ODP DataSource
○ SAP DataSource - Rebuild Logic in Fabric
```

Then show only required fields.

Example Oracle:

```text
Connection
Schema
Table
Fabric Workspace
Lakehouse
Schema
Target Table
```

Example SAP rebuild:

```text
SAP Connection
DataSource
Fabric Workspace
Lakehouse
Fabric Schema
```

Do not ask users for metadata the system can discover itself.

---

# 30. Migration Status UI

Display:

```text
Workflow ID
Migration type
Source
Current phase
Current status
Plan GUID
Last error
Waiting for user?
```

Use a timeline:

```text
✓ Source validated
✓ Metadata discovered
○ Waiting for approval
○ Provision Fabric
○ Replication
```

---

# 31. Oracle / SAP Table Plan Review UI

Show:

```text
Source metadata
Columns
Source datatype
Fabric datatype
Primary key
Watermark candidates
Replication strategy
Fabric destination
```

Actions:

```text
Approve
Reject
Cancel
```

Optionally allow editing selected fields before approval.

---

# 32. SAP Rebuild Analysis Review UI

Use a split layout.

```text
Left:
  conversation / feedback input

Right:
  structured analysis
```

Show:

```text
Observed lineage
Verified source tables
Verified source views
Joins
Filters
Transformations
Derived fields
Delta logic
Unresolved objects
Warnings
```

Actions:

```text
Approve Analysis
Submit Feedback
Reject
Cancel
```

This is the first thin version of the future chatbot.

---

# 33. SAP Rebuild Fabric Plan Review UI

Show:

```text
Tables to replicate
Existing tables to reuse
Views to create
View dependencies
SQL / logic
Watermark strategy
Write strategy
Fabric target
```

Actions:

```text
Approve Plan
Submit Feedback
Reject
Cancel
```

---

# 34. Execution Monitor UI

Show:

```text
Provisioning notebook job ID
Provisioning status

Replication pipeline job ID
Replication status

Batch status
Rows read
Rows written
Last watermark

Errors
```

Do not expose raw credentials or sensitive connection info.

---

# 35. Thin UI Technology

Use the existing frontend stack if the project has one.

If none exists, a simple React/Next.js UI is sufficient.

The UI must not call Temporal directly.

Architecture:

```text
Browser
   ->
FastAPI
   ->
Temporal client
```

---

# 36. UI Polling

The thin UI may poll:

```http
GET /migrations/{workflow_id}
```

every few seconds.

This is acceptable for the first version.

Do not implement WebSockets unless already available.

Later the UI can be upgraded to server-sent events / WebSockets.

---

# 37. Workflow IDs

Use deterministic, readable workflow IDs.

Examples:

```text
oracle-table-<uuid>
sap-table-<uuid>
sap-odp-<uuid>
sap-rebuild-<uuid>
```

Do not use source table names alone because duplicate requests may exist.

---

# 38. Temporal Task Queues

Recommended initial design:

```text
migration-workflows
migration-oracle-activities
migration-sap-activities
migration-fabric-activities
migration-llm-activities
```

A simpler first version may use one activity task queue.

Do not over-engineer task queues initially.

Split later if scaling or dependency isolation requires it.

---

# 39. Worker Strategy

A practical initial worker layout:

```text
Worker 1:
  workflows
  Config DB activities
  Fabric activities

Worker 2:
  Oracle activities
  requires Oracle Instant Client

Worker 3:
  SAP activities

Worker 4:
  LLM activities
  Phoenix instrumentation
```

This is especially useful because Oracle has native client dependencies.

If deployment simplicity matters more initially, combine where safe.

---

# 40. Secrets

Do not pass source-system passwords through Temporal workflow payloads.

Temporal workflow input should contain:

```text
connection_name
```

not credentials.

Activities should resolve connection metadata/secrets through approved configuration/secret mechanisms.

Never store:

```text
Oracle passwords
SAP passwords
Fabric client secret
access tokens
```

in workflow state, history, logs, or Phoenix traces.

---

# 41. Phoenix Usage

Phoenix traces only the AI/agent components.

Trace:

```text
SAP extractor analysis
analysis revision
Fabric-plan generation
Fabric-plan revision
```

Do not use Phoenix as the source of truth for workflow execution state.

Temporal remains authoritative for:

```text
activities
retries
waiting
signals
timers
execution state
```

Config DB remains authoritative for:

```text
approved migration configuration
runtime replication configuration
batch audit history
```

---

# 42. Versioning Rules

## SAP Analysis

Every user revision:

```text
AnalysisVersion + 1
```

Do not overwrite approved analysis.

## Fabric Plan

Every material revision:

```text
PlanVersion + 1
```

Do not change already-approved/persisted runtime plan in place.

---

# 43. Idempotency Rules

All activities that may be retried should be safe.

Examples:

```text
create plan
persist approved runtime plan
set run IDs
update statuses
```

Use existing Config DB idempotency/version checks.

For Fabric submission, ambiguity must be handled according to the Fabric execution contract.

---

# 44. Workflow Timeouts

Do not set short workflow execution timeouts.

Human approval may take:

```text
minutes
hours
days
```

Temporal workflows should be allowed to remain waiting durably.

Activity timeouts should remain bounded.

---

# 45. Suggested Activity Timeouts

Example starting point:

```text
Oracle metadata:
  30-120 seconds

SAP metadata:
  30-120 seconds

ABAP scraper:
  several minutes if required

LLM analysis:
  2-5 minutes

Config DB:
  30 seconds

Fabric submission:
  30-60 seconds

Fabric status:
  30 seconds
```

Tune based on actual environment.

---

# 46. Workflow Testing

Before UI integration, create Temporal workflow tests.

Use Temporal testing environment/time skipping where possible.

Test:

## Oracle

```text
happy path
approval rejection
source failure
Fabric provisioning failure
replication failure
cancel while waiting
cancel while Fabric running
```

## SAP Table

Same categories.

## SAP ODP

```text
ODP capable
ODP unsupported
ODP unknown
delta supported
delta unresolved
```

## SAP Rebuild

```text
TABLE branch
DB_VIEW branch
INFOSET branch
FUNCTION_MODULE branch
unsupported branch

analysis feedback
analysis approval
analysis rejection

Fabric-plan feedback
Fabric-plan approval
Fabric-plan rejection

LLM failure
SAP verification failure
incomplete ABAP scrape
Fabric failure
```

---

# 47. Thin UI Testing

Test:

```text
start each workflow
display status
approval action
feedback action
cancel action
workflow failure display
workflow completion
```

For SAP rebuild:

```text
analysis shown
feedback submitted
new version shown
approval moves workflow forward
Fabric plan shown
second approval moves to provisioning
```

---

# 48. Definition of Done - Phase 1

The Temporal + thin UI phase is complete when:

- [ ] four workflows exist
- [ ] workflows use adapters only through activities/services
- [ ] Oracle happy path works end-to-end
- [ ] SAP table happy path works end-to-end
- [ ] SAP ODP happy path works end-to-end
- [ ] SAP rebuild supports all intended extraction routes
- [ ] SAP rebuild function-module branch uses LLM + deterministic verification
- [ ] analysis approval loop works
- [ ] Fabric plan approval loop works
- [ ] approved runtime plan persists atomically
- [ ] provisioning notebook is triggered by plan_guid
- [ ] Fabric status is monitored durably
- [ ] replication pipeline is triggered
- [ ] batch logs are updated
- [ ] cancellation works
- [ ] failure states are persisted
- [ ] workflow state is queryable
- [ ] thin UI can start/monitor/approve/cancel workflows
- [ ] no credentials appear in Temporal history
- [ ] Phoenix traces LLM calls
- [ ] automated tests cover major branches

---

# 49. Do Not Build Yet

Do not build the full polished chatbot in this phase.

Do not add:

```text
general free-form assistant
complex chat memory
voice
rich graph editing
full visual workflow designer
advanced notifications
```

The thin SAP rebuild review UI may use a chat-like input box for feedback, but the interaction should remain structured around:

```text
analysis review
approval
plan review
approval
```

---

# 50. Next Phase After This Handover

Once these workflows and the thin UI are stable, the next phase can build the richer conversational experience.

That UI can allow commands such as:

```text
"Why did you include BKPF?"

"Show where BSEG is used."

"Ignore this helper function."

"Analyze this function module one level deeper."

"Use the existing T001 table instead."

"Change this Silver table to a view."
```

These conversational actions must translate into the same stable Temporal signal/update contracts implemented in this phase.

Therefore the work done now becomes the backend foundation for the future chatbot.

---

# 51. Final Instruction to Coding Agent

Build the Temporal orchestration layer first, with only the thin UI needed to control and validate it.

Do not replicate adapter logic in workflows.

Do not bypass Config DB.

Do not let LLM output directly execute Fabric changes.

Maintain the core contract:

```text
Source adapters
     ->
Discovery

LLM where required
     ->
Interpretation

User
     ->
Approval

Config DB
     ->
Approved structured runtime configuration

Fabric notebook / pipeline
     ->
Deterministic execution

Temporal
     ->
Durable orchestration

Thin UI
     ->
Human control surface
```

Implement the simplest reliable vertical slice first:

```text
Oracle Table -> Fabric
```

Then reuse the shared patterns for:

```text
SAP Table
SAP ODP
SAP DataSource Rebuild
```

The Temporal state model, approval contracts, Fabric execution pattern, Config DB persistence, and thin UI interaction model should be reusable across all four workflows.
