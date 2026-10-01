# Handover: Fabric Utilities Changes for Automated Migration Platform

## 1. Purpose

Extend the existing Fabric utility layer so the application can move from an **approved migration plan** to deterministic Fabric execution.

The application already performs source discovery through Oracle/SAP adapters and stores approved runtime configuration in the Config DB. Fabric utilities should **not interpret SAP/Oracle logic and should not generate DDL from LLM output directly**.

The Fabric utility layer is responsible for:

1. Authenticating to Microsoft Fabric.
2. Discovering / validating Fabric workspaces and Lakehouses.
3. Triggering a parameterized Fabric notebook after a migration plan is approved.
4. Passing a small stable identifier, preferably `plan_guid`, to the notebook.
5. Returning the Fabric job instance ID.
6. Polling / reading notebook execution status.
7. Returning failure details / notebook exit value.
8. Optionally triggering the existing dedicated replication pipeline.
9. Polling / reading pipeline execution status.
10. Exposing clean typed Python models for the orchestration layer / Temporal activities.

The Config DB is the source of truth for **what** must be created. The notebook is responsible for reading the approved Config DB records and implementing Fabric runtime objects.

---

## 2. Existing Fabric Utility

Current utility already contains:

- Fabric service-principal authentication.
- `FABRIC_API_URL`.
- `FABRIC_SCOPE`.
- `connect_to_fabric()`.
- `list_lakehouses(workspace_id)` with pagination.

Keep these capabilities and refactor them into a reusable `FabricClient` if useful.

Do not break existing callers unless required.

---

## 3. Target Architecture

```text
UI / Conversational Review
        |
        v
Temporal Workflow
        |
        v
Approved Migration Plan
        |
        v
Config DB
        |
        | plan_guid
        v
Fabric Utility
        |
        +--> Trigger parameterized provisioning notebook
        |
        +--> Monitor notebook run
        |
        +--> Optionally trigger replication pipeline
        |
        +--> Monitor pipeline run
        v
Fabric
```

Notebook flow:

```text
ProvisioningNotebook(plan_guid)
        |
        +--> read MigrationPlans
        +--> read SourceTables
        +--> read SourceTableColumns
        +--> read FabricViews
        +--> read FabricViewDependencies
        +--> create/validate tables
        +--> create/validate views
        +--> update provisioning status
```

The notebook must not receive the full table/view JSON from the API. Pass only stable identifiers, primarily `plan_guid`.

---

## 4. Config DB Contract

The new Config DB migration introduces these major objects:

### Design / approval

- `bronze_replication.MigrationPlans`
- `bronze_replication.SAPAnalysis`
- `bronze_replication.SAPAnalysisObjects`

### Physical table runtime configuration

- `bronze_replication.SourceTables`
- `bronze_replication.SourceTableColumns`

### Replication

- `bronze_replication.ReplicationConfig`
- `bronze_replication.ReplicationState`

### Fabric derived objects

- `bronze_replication.FabricViews`
- `bronze_replication.FabricViewDependencies`

### Operational logs

- `bronze_replication.BatchRuns`
- `bronze_replication.BatchObjectRuns`

### Runtime views

- `bronze_replication.vw_ApprovedPlansReadyToProvision`
- `bronze_replication.vw_TableProvisioningQueue`
- `bronze_replication.vw_ViewProvisioningQueue`
- `bronze_replication.vw_ReplicationQueue`
- `bronze_replication.vw_FabricViewDependencies`

Fabric utilities should not duplicate these joins.

---

## 5. Required Fabric Utility API

Implement a class-oriented API similar to:

```python
class FabricClient:
    def list_workspaces(self) -> list[FabricWorkspace]:
        ...

    def get_workspace(self, workspace_id: str) -> FabricWorkspace:
        ...

    def list_lakehouses(self, workspace_id: str) -> list[Lakehouse]:
        ...

    def get_lakehouse(self, workspace_id: str, lakehouse_id: str) -> Lakehouse:
        ...

    def find_lakehouse_by_name(
        self,
        workspace_id: str,
        display_name: str,
    ) -> Lakehouse | None:
        ...

    def run_notebook(
        self,
        workspace_id: str,
        notebook_id: str,
        parameters: dict[str, object] | None = None,
    ) -> FabricJobSubmission:
        ...

    def get_notebook_run(
        self,
        workspace_id: str,
        notebook_id: str,
        job_instance_id: str,
    ) -> FabricJobInstance:
        ...

    def cancel_notebook_run(
        self,
        workspace_id: str,
        notebook_id: str,
        job_instance_id: str,
    ) -> None:
        ...

    def run_pipeline(
        self,
        workspace_id: str,
        pipeline_id: str,
        execution_data: dict[str, object] | None = None,
    ) -> FabricJobSubmission:
        ...

    def get_pipeline_run(
        self,
        workspace_id: str,
        pipeline_id: str,
        job_instance_id: str,
    ) -> FabricJobInstance:
        ...
```

A convenience method can be added:

```python
def trigger_provisioning(
    self,
    *,
    workspace_id: str,
    notebook_id: str,
    plan_guid: str,
) -> FabricJobSubmission:
    return self.run_notebook(
        workspace_id=workspace_id,
        notebook_id=notebook_id,
        parameters={
            "plan_guid": plan_guid
        },
    )
```

Keep the low-level methods generic. `trigger_provisioning()` may be application-specific.

---

## 6. Typed Models

Add Pydantic models or TypedDicts for responses.

Suggested minimum:

```python
class FabricWorkspace(BaseModel):
    id: str
    display_name: str

class Lakehouse(BaseModel):
    id: str
    display_name: str

class FabricJobSubmission(BaseModel):
    job_instance_id: str
    location: str | None = None
    retry_after_seconds: int | None = None

class FabricJobFailureReason(BaseModel):
    error_code: str | None = None
    message: str | None = None

class FabricJobInstance(BaseModel):
    id: str
    item_id: str | None = None
    job_type: str | None = None
    invoke_type: str | None = None
    status: str
    start_time_utc: datetime | None = None
    end_time_utc: datetime | None = None
    failure_reason: object | None = None
    exit_value: str | None = None
```

Do not expose raw unvalidated API JSON to workflow code when a stable model is possible.

---

## 7. Notebook Execution

Use the Fabric notebook run-on-demand API.

Current release endpoint:

```text
POST
/v1/workspaces/{workspaceId}/notebooks/{notebookId}/jobs/execute/instances?beta=false
```

Parameter payload example:

```json
{
  "parameters": [
    {
      "name": "plan_guid",
      "value": "<migration-plan-guid>",
      "type": "Text"
    }
  ]
}
```

Important:

- Use `beta=false` for the release API.
- Parameter casing should match the notebook parameter name.
- Service-principal authentication is supported.
- A successful submission normally returns HTTP 202.
- Capture the `Location` response header.
- Parse/store the returned job instance identifier.
- Capture `Retry-After` when returned.
- Do not block inside `run_notebook()` until completion.

The utility should separate:

```text
submit
```

from:

```text
get status
```

Temporal should own waiting/retry orchestration.

---

## 8. Notebook Run Status

Implement status retrieval.

Expected concepts:

```text
NotStarted / Queued
InProgress / Running
Completed
Failed
Cancelled
```

Do not hard-code only one exact spelling without inspecting Fabric responses.

Normalize Fabric values into an internal enum if useful:

```python
class FabricJobStatus(StrEnum):
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"
```

Preserve the original Fabric status in the response model if normalization is used.

For notebook jobs, capture:

- status
- start time
- end time
- failure reason
- exit value

The orchestration layer should be able to distinguish:

```text
HTTP/API call failed
```

from:

```text
Notebook submitted successfully but notebook execution failed
```

---

## 9. Workspace Discovery

Add:

```python
list_workspaces()
```

Use Fabric Core REST API and support pagination.

The purpose is to allow UI / orchestration code to validate the configured workspace and to avoid hard-coded workspace lookup logic elsewhere.

Do not use the tenant admin endpoint for normal application discovery unless the product explicitly requires tenant-wide admin behavior.

---

## 10. Lakehouse Discovery / Validation

Keep the current `list_lakehouses(workspace_id)` behavior.

Add:

```python
get_lakehouse(...)
find_lakehouse_by_name(...)
```

Requirements:

- Validate workspace/lakehouse IDs as UUIDs.
- Support pagination.
- Treat duplicate display names defensively.
- Prefer IDs as the execution contract after discovery.

The UI can display names, but persisted/runtime calls should use Fabric IDs.

---

## 11. Pipeline Execution

The project already has a dedicated replication pipeline.

Fabric utilities need an optional generic pipeline trigger:

```python
run_pipeline(
    workspace_id,
    pipeline_id,
    execution_data=None,
)
```

The utility must:

1. Submit the pipeline job.
2. Return job instance ID / Location.
3. Provide a separate status getter.
4. Never poll indefinitely inside the submit call.
5. Surface HTTP errors with enough response context for logs.

The existing replication pipeline will read the Config DB (`vw_ReplicationQueue` or underlying tables). It should not require the API layer to send every table definition.

Possible application-level trigger:

```python
trigger_replication_pipeline(
    workspace_id=<pipeline-workspace>,
    pipeline_id=<pipeline-id>,
)
```

If the pipeline supports a `plan_guid` filter, pass it. Otherwise the pipeline can pick all enabled entries from Config DB.

---

## 12. Temporal Integration

Fabric utilities should be safe to call from Temporal activities.

Recommended activity split:

```text
validate_fabric_target
submit_provisioning_notebook
get_provisioning_notebook_status
submit_replication_pipeline
get_replication_pipeline_status
```

Do not put long sleeps/poll loops inside the utility.

Temporal workflow example:

```text
Config persisted
    |
    v
submit_provisioning_notebook activity
    |
    v
job_instance_id
    |
    v
Temporal timer
    |
    v
get_notebook_status activity
    |
    +-- still running --> timer --> check again
    |
    +-- failed --> record failure
    |
    +-- completed --> validation / replication
```

This keeps durable waiting in Temporal instead of Python process memory.

---

## 13. Error Handling

Create Fabric-specific exception types if practical:

```python
class FabricError(Exception):
    pass

class FabricAuthenticationError(FabricError):
    pass

class FabricNotFoundError(FabricError):
    pass

class FabricPermissionError(FabricError):
    pass

class FabricJobSubmissionError(FabricError):
    pass
```

At minimum:

- `response.raise_for_status()` must not be the only context.
- Capture Fabric request/correlation IDs from headers when available.
- Never include access tokens or client secrets in exception strings.
- Do not log notebook parameter values if future parameters may contain secrets.

---

## 14. HTTP / Reliability Requirements

Reuse one authenticated `httpx.Client` where practical.

Support:

- request timeout
- 429 handling
- `Retry-After`
- transient 5xx retry policy at the Temporal activity level or a clearly bounded utility level
- pagination
- request/response correlation metadata

Do not automatically retry non-idempotent POST submissions blindly after an ambiguous timeout. A duplicate notebook/pipeline run may be created.

If POST retry behavior is required, first design idempotency around the Config DB / Temporal workflow and job IDs.

---

## 15. Authentication

Current implementation uses:

```python
ClientSecretCredential
```

Keep it for now.

Environment variables:

```text
FABRIC_TENANT_ID
FABRIC_CLIENT_ID
FABRIC_CLIENT_SECRET
```

Do not move secrets into Config DB `ConnectionDetails` unless an approved secret-management design requires it.

Future enhancement may use Managed Identity, but it is not required for this change.

---

## 16. Provisioning Notebook Contract

The initial notebook contract should be intentionally small:

### Required parameter

```text
plan_guid : Text
```

Optional later parameters:

```text
force_redeploy : Boolean
dry_run        : Boolean
```

Avoid passing:

- full table definitions
- SQL text
- column lists
- watermark rules
- full JSON migration plan

Those belong in Config DB.

The notebook should verify:

```text
MigrationPlans.Status in:
APPROVED
READY_TO_PROVISION
PROVISIONING
```

before changing Fabric objects.

---

## 17. Idempotency

The notebook must be designed so re-running the same `plan_guid` is safe.

Expected behavior:

### Table

```text
does not exist -> create
exists + compatible -> mark/keep provisioned
exists + incompatible -> fail with actionable message
```

### View

```text
does not exist -> create
exists -> create-or-alter / replace according to approved design
```

### Config status

Update:

```text
PENDING
IN_PROGRESS
PROVISIONED
FAILED
```

The Fabric utility itself must not infer or update object-level provisioning success before the notebook reports completion.

---

## 18. Batch Logging

When Temporal submits a Fabric job, the application/workflow layer should record:

```text
BatchRuns
BatchObjectRuns
```

Suggested correlation:

```text
TemporalWorkflowId
TemporalRunId
PlanGUID
FabricJobInstanceId
```

The Fabric utility should return enough information for callers to persist those IDs.

Do not make Fabric utilities write directly to the Config DB unless the project deliberately wants that coupling. Prefer:

```text
Fabric utility -> return result
Temporal activity/service -> persist result
```

---

## 19. Out of Scope for Fabric Utilities

Do not implement these responsibilities in `FabricClient`:

- Decide which SAP tables must be replicated.
- Interpret ABAP.
- Decide whether a table is lookup / transaction / config.
- Choose watermark columns with an LLM.
- Generate migration-plan approval state.
- Generate arbitrary Fabric SQL directly from chat.
- Read Phoenix traces.
- Own Temporal workflow state.
- Maintain business approval.

These belong to analysis/planning/workflow services.

---

## 20. Suggested File Layout

```text
backend/
  integrations/
    fabric/
      __init__.py
      client.py
      auth.py
      models.py
      exceptions.py

  services/
    fabric_provisioning_service.py

  temporal/
    activities/
      fabric_activities.py
```

If the project currently keeps one `utils.py`, incremental refactoring is acceptable.

Suggested separation:

### `auth.py`

- credential creation
- authenticated httpx client

### `client.py`

- raw Fabric REST operations

### `models.py`

- Pydantic response/request models
- enums

### `fabric_provisioning_service.py`

- application-specific `plan_guid` orchestration helper

### Temporal activity

- calls service/client
- applies Temporal retry policy
- persists job IDs/status through Config DB service

---

## 21. Testing Requirements

Add unit tests for:

- missing Fabric env vars
- invalid UUID
- workspace pagination
- lakehouse pagination
- notebook parameter payload
- parsing `Location`
- parsing `Retry-After`
- notebook status mapping
- notebook failure payload
- pipeline submission
- pipeline status
- 401 / 403
- 404
- 429
- 5xx
- malformed JSON
- duplicate-name lakehouse lookup

Mock HTTP responses; do not require live Fabric for unit tests.

Add at least one integration test in a DEV workspace for:

```text
submit notebook(plan_guid)
-> obtain job_instance_id
-> fetch run status
-> observe terminal state
```

---

## 22. Acceptance Criteria

The Fabric utility change is complete when all of the following work:

1. Application can list accessible workspaces.
2. Application can list/validate Lakehouses.
3. Application can submit the provisioning notebook using only `plan_guid`.
4. Submission returns a job instance identifier.
5. Application can query notebook run status.
6. Failure details and exit values are available.
7. Application can trigger the existing replication pipeline.
8. Application can query pipeline run status.
9. All API methods are typed and tested.
10. No Fabric execution method contains an infinite polling loop.
11. No LLM/SAP decision logic exists in the Fabric utility.
12. Temporal can use submit/status methods as separate activities.

---

## 23. Current Microsoft Fabric API Notes

At handover time, Microsoft documentation shows:

### Workspaces

```text
GET /v1/workspaces
```

Supports pagination.

### Lakehouses

```text
GET /v1/workspaces/{workspaceId}/lakehouses
```

Supports pagination.

### Notebook run-on-demand

```text
POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/jobs/execute/instances?beta=false
```

Supports parameters and service-principal authentication.

### Pipeline on-demand run

Fabric Data Factory exposes an on-demand pipeline job API and job-instance status API.

Before implementation, use Microsoft Fabric REST documentation as the source of truth for the exact request/response schema because these APIs can evolve.

---

## 24. Final Architectural Rule

Keep this contract explicit:

```text
LLM + user
   -> approved design

approved design
   -> Config DB

Config DB
   -> deterministic notebook/pipeline

Fabric utility
   -> transport + execution control

Temporal
   -> durable orchestration

Phoenix
   -> AI trace / evaluation
```

The Fabric utility is an integration adapter, not a migration-planning engine.
