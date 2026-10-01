# Coding Agent Handover
## Remaining Microsoft Fabric Adapter Gaps Before Temporal Workflow Development

**Scope:** Only the remaining gaps in the Fabric integration adapter.  
**Baseline:** Current `client.py`, `exceptions.py`, `models.py`, and auth `utils.py`.  
**Do not redesign the package.** Extend and correct the existing implementation minimally.

---

# 1. Current State

The Fabric integration is now substantially implemented.

The following capabilities already exist and should be preserved:

- Service-principal authentication using `ClientSecretCredential`
- Fabric API base URL and scope
- Authenticated `httpx.Client`
- Typed Fabric exception hierarchy
- Request error normalization
- Request/correlation ID capture
- `Retry-After` extraction
- Workspace listing with pagination
- Workspace lookup by ID
- Lakehouse listing with pagination
- Lakehouse lookup by ID
- Lakehouse lookup by display name with duplicate protection
- Generic item lookup and type validation
- Notebook validation
- Data pipeline validation
- Parameterized notebook submission
- Notebook job status lookup
- Notebook cancel
- Provisioning convenience method using `plan_guid`
- Pipeline submission
- Pipeline job status lookup
- Typed Fabric models
- Normalized job status

This means the Fabric adapter is already close to Temporal-workflow readiness.

The remaining work is mainly around API-path correctness, job-instance URL handling, execution validation, retries/idempotency, and some lifecycle helpers.

---

# 2. Fabric Responsibilities in the Four Workflows

The same Fabric adapter is used by all four migration workflows:

1. Oracle Table -> Fabric
2. SAP Table -> Fabric
3. SAP ODP DataSource -> Fabric
4. SAP DataSource Rebuild -> Fabric

The Fabric adapter is responsible only for:

```text
authenticate
    ->
discover/validate Fabric target
    ->
validate notebook/pipeline items
    ->
submit notebook/pipeline job
    ->
return job instance ID
    ->
read status
    ->
cancel where supported
```

The Fabric adapter must not:

```text
decide which tables/views to build
interpret SAP/Oracle logic
write Config DB business configuration
own Temporal workflow state
perform durable polling
call the LLM
```

---

# 3. Overall Fabric Readiness

Current assessment:

```text
Authentication                          READY
Typed exceptions                       READY
Workspace discovery                    READY
Lakehouse discovery                    READY
Notebook item validation               READY
Notebook parameters                    READY
Notebook submission                    READY
Notebook status                        PARTIAL - endpoint semantics need verification
Notebook cancel                        PARTIAL - verify endpoint
Pipeline item validation               READY
Pipeline submission                    READY
Pipeline status                        NEEDS CORRECTION
Job Location parsing                   NEEDS GENERALIZATION
Retry-After                            READY
Job status normalization               READY
Transient retry strategy               MISSING / belongs partly to Temporal
Submission idempotency                 NEEDS explicit contract
Managed identity                       OPTIONAL
Generic item listing                   OPTIONAL
Fabric target validation wrapper       MISSING
Job terminal-state helper              MISSING
```

---

# 4. Critical Gap FAB-01 - Pipeline Job Status Uses the Wrong Generic Path

## Current behavior

`get_pipeline_run(...)` delegates to:

```python
_job(workspace_id, pipeline_id, job_instance_id)
```

The generic `_job(...)` builds:

```text
/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobInstanceId}
```

However, the documented Data Pipeline job-status endpoint is:

```text
GET /v1/workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/jobs/execute/instances/{jobInstanceId}
```

## Required change

Do not use `_job(...)` for pipeline status.

Implement a dedicated method:

```python
def get_pipeline_run(
    self,
    workspace_id: str,
    pipeline_id: str,
    job_instance_id: str,
) -> FabricJobInstance:
    ...
```

with path:

```python
f"workspaces/{ws}/dataPipelines/{pipeline}/jobs/execute/instances/{job}"
```

Reuse common response parsing through a helper such as:

```python
_parse_job_instance_response(response)
```

This is P0 and must be corrected before Temporal workflow development.

---

# 5. Critical Gap FAB-02 - Generalize Submission `Location` Parsing

## Current behavior

`_submission(...)` expects the `Location` header to have the path:

```text
/v1/workspaces/{workspaceId}/items/{itemId}/jobs/instances/{jobId}
```

This matches documented notebook submission responses.

But pipeline submission uses a DataPipeline-specific run endpoint and Microsoft may still return a generic item-job location, depending on API behavior/version.

The current helper is too tightly coupled to one exact URL shape.

## Required change

Make `Location` parsing tolerant of documented Fabric job-instance URL shapes while still validating:

- HTTPS
- host is `api.fabric.microsoft.com`
- workspace ID matches
- item/pipeline ID matches when available
- final path segment is a UUID job ID

Suggested helper:

```python
def _parse_job_location(
    location: str,
    *,
    workspace_id: str,
    item_id: str,
) -> str:
    ...
```

Accept documented shapes such as:

```text
/v1/workspaces/{workspace}/items/{item}/jobs/instances/{job}
/v1/workspaces/{workspace}/dataPipelines/{pipeline}/jobs/execute/instances/{job}
```

Do not blindly trust arbitrary external URLs.

---

# 6. Gap FAB-03 - Notebook Status Endpoint Needs Explicit Release/Beta Handling

## Current behavior

Notebook submission correctly uses:

```text
POST /notebooks/{id}/jobs/execute/instances?beta=false
```

The current status getter uses the generic item-job path.

Current Microsoft documentation exposes notebook-specific run-status semantics and `exitValue`.

## Required action

Verify the supported release endpoint for notebook status in the target tenant/API version.

Implement notebook status explicitly rather than assuming generic item-job status forever.

Preferred method:

```python
def get_notebook_run(...):
    ...
```

with the documented notebook endpoint and required query parameters for the release API.

If the generic item job endpoint is intentionally used and verified to return:

```text
exitValue
failureReason
status
```

document that decision and add an integration test.

The important requirement is:

```text
FabricJobInstance.exit_value
```

must be reliably populated when the notebook returns an exit value.

---

# 7. Gap FAB-04 - Verify Notebook Cancel Endpoint

## Current behavior

Current method posts to:

```text
/workspaces/{workspace}/items/{notebook}/jobs/instances/{job}/cancel
```

This may be valid through generic scheduler APIs, but it needs explicit verification against current Fabric documentation / DEV tenant behavior.

## Required action

Add an integration test proving:

```text
submitted notebook
    ->
cancel_notebook_run(...)
    ->
terminal Cancelled/Canceled status
```

If the endpoint differs, correct it.

Do not remove cancellation support unless Fabric does not support it.

---

# 8. Gap FAB-05 - Pipeline Cancel

## Current state

There is no:

```python
cancel_pipeline_run(...)
```

## Required action

Check current Fabric DataPipeline API support.

If cancellation is supported, add:

```python
def cancel_pipeline_run(
    self,
    workspace_id: str,
    pipeline_id: str,
    job_instance_id: str,
) -> None:
    ...
```

If the API does not support cancellation:

document this explicitly so Temporal cancellation logic does not assume it can stop the Fabric pipeline.

This is P1.

---

# 9. Gap FAB-06 - Shared Job Parsing Helper

## Current behavior

`_job(...)` does both:

- HTTP GET
- job response parsing

With notebook and pipeline endpoints diverging, split these concerns.

## Required helper

Add:

```python
def _parse_job_instance(
    self,
    response: httpx.Response,
) -> FabricJobInstance:
    ...
```

Responsibilities:

```text
parse JSON
validate status
normalize status
capture Retry-After
validate Pydantic model
```

Then:

```text
get_notebook_run(...)
get_pipeline_run(...)
```

each use their own endpoint but share parsing.

---

# 10. Gap FAB-07 - Terminal-State Helper

Temporal workflow code should not duplicate:

```text
SUCCEEDED?
FAILED?
CANCELLED?
UNKNOWN?
```

Add pure helper methods/properties.

Example:

```python
def is_terminal_job_status(status: FabricJobStatus) -> bool:
    return status in {
        FabricJobStatus.SUCCEEDED,
        FabricJobStatus.FAILED,
        FabricJobStatus.CANCELLED,
    }
```

Optionally:

```python
def is_successful_job_status(...)
```

This can live in `models.py`.

No I/O involved.

---

# 11. Gap FAB-08 - Unknown Job Status Must Be Observable

Current normalizer correctly maps unexpected values to:

```text
UNKNOWN
```

Do not silently treat `UNKNOWN` as running or failed.

## Required behavior

Temporal activity/service layer must receive:

```text
normalized_status = UNKNOWN
raw status value
```

The model already preserves raw `status`, which is good.

Add tests proving unknown values survive intact.

Potentially add:

```python
FabricUnknownJobStatusError
```

only at a higher orchestration layer if desired.

The low-level adapter should continue returning the model.

---

# 12. Gap FAB-09 - Target Validation Wrapper

All workflows need to validate:

```text
workspace
lakehouse
provisioning notebook
replication pipeline
```

Workflow code should not repeat four separate calls.

Add a higher-level service/helper outside the raw client:

```python
class FabricTargetValidation(BaseModel):
    workspace: FabricWorkspace
    lakehouse: Lakehouse
    notebook: FabricItem
    pipeline: FabricItem | None
```

Function:

```python
def validate_fabric_target(
    client: FabricClient,
    *,
    workspace_id: str,
    lakehouse_id: str,
    notebook_id: str,
    pipeline_id: str | None = None,
) -> FabricTargetValidation:
    ...
```

Place this in:

```text
services/fabric_validation.py
```

not inside the HTTP transport if possible.

---

# 13. Gap FAB-10 - Pipeline Trigger Scoping Contract

Current `run_pipeline(...)` intentionally rejects `execution_data` because the specialized Data Pipeline API does not document an arbitrary body.

That is a good safety choice.

However, the project may need to scope a replication pipeline run by:

```text
plan_guid
```

if the pipeline supports it.

## Required design decision

Pick one explicit model:

### Option A - pipeline reads all enabled Config DB rows

Then:

```python
run_pipeline(workspace_id, pipeline_id)
```

is sufficient.

### Option B - pipeline accepts a plan scope through supported pipeline parameters

Implement only after confirming the supported Fabric API payload for the pipeline.

Do not invent an undocumented JSON body.

For V1, Option A is acceptable if it matches the existing pipeline design.

---

# 14. Gap FAB-11 - Submission Retry / Idempotency Contract

Current `_request(..., submission=True)` correctly does not automatically retry transport failures.

Keep this.

## Why

For POST submission:

```text
network timeout
```

may occur after Fabric accepted the job.

Blind retry could create duplicate notebook/pipeline executions.

## Required addition

Document this behavior in code and expose enough correlation for Temporal.

Recommended service-level behavior:

```text
submit job
    ->
if response received:
    persist job ID
    ->
if transport ambiguity:
    fail activity
```

Temporal retry for submission activities should be configured carefully.

For first version:

- no automatic POST retry in the client;
- Temporal submission activity retry count should be small or disabled for ambiguous transport errors;
- GET/status activities may be retried safely.

This should be explicit in the handover/tests.

---

# 15. Gap FAB-12 - Safe GET Retry Policy

Current client does not retry any requests.

For GET discovery/status calls, bounded retries for transient errors are safe and useful.

Implement either:

### Preferred

Leave retries to Temporal activity retry policies.

or

### Client-level bounded GET retry

Only for:

```text
429
502
503
504
```

respecting `Retry-After`.

Do not sleep for long periods inside the client.

Given the architecture, prefer Temporal-level retry behavior.

No client change is required if documented and tested through activities later.

---

# 16. Gap FAB-13 - Parse `Retry-After` HTTP Date Format

Current implementation only accepts numeric seconds:

```text
Retry-After: 60
```

HTTP also permits an HTTP-date format.

Fabric documentation currently describes integer seconds, so the current behavior is sufficient for Fabric-specific use.

Still, implement a utility if desired:

```python
parse_retry_after(...)
```

Priority: P2.

---

# 17. Gap FAB-14 - Workspace/Lakehouse Models Should Validate UUIDs

Current Pydantic models accept IDs as plain strings.

The client validates IDs supplied by the caller, but API response IDs are not validated as UUIDs.

Optional hardening:

```python
id: UUID
```

or Pydantic UUID type, then serialize as string when needed.

Do this only if it does not create unnecessary compatibility issues.

Priority: P2.

---

# 18. Gap FAB-15 - Generic Item Listing

Current package can fetch one item by ID but cannot list notebooks or pipelines.

Useful UI helpers:

```python
def list_items(
    self,
    workspace_id: str,
    *,
    item_type: str | None = None,
) -> list[FabricItem]:
    ...
```

Then optional:

```python
list_notebooks(...)
list_pipelines(...)
```

This is useful if the UI needs selection rather than storing IDs in environment/config.

Not a workflow blocker if IDs are configured centrally.

Priority: P1/P2 depending on UI.

---

# 19. Gap FAB-16 - Find Notebook/Pipeline by Name

Similar to Lakehouse convenience lookup:

```python
find_notebook_by_name(...)
find_pipeline_by_name(...)
```

with duplicate-name protection.

Prefer IDs in persisted runtime config.

Priority: P2.

---

# 20. Gap FAB-17 - Fabric Connection Health Check

There is no single health method.

Add:

```python
def test_connection(self) -> FabricConnectionStatus:
    ...
```

Suggested implementation:

```text
list_workspaces()
```

or a bounded workspace call.

Model:

```python
class FabricConnectionStatus(BaseModel):
    success: bool
    accessible_workspace_count: int | None = None
```

This is useful during application startup/onboarding.

P0 or P1 depending on UI.

---

# 21. Gap FAB-18 - Authentication Strategy Abstraction

Current auth is hard-coded to:

```python
ClientSecretCredential
```

This is acceptable for V1.

But the client should not depend conceptually on service principal forever.

## Recommended small refactor

Allow credential injection:

```python
def connect_to_fabric(
    credential: TokenCredential | None = None,
) -> Iterator[httpx.Client]:
    ...
```

Default remains:

```python
ClientSecretCredential
```

This makes future Managed Identity support straightforward.

Do not implement Managed Identity now unless required.

Priority: P2.

---

# 22. Gap FAB-19 - Remove Legacy Duplicate `list_lakehouses` API

Current auth `utils.py` still exposes an older module-level:

```python
list_lakehouses(workspace_id)
```

while `FabricClient` now contains the proper typed implementation.

## Required action

Mark the module-level function deprecated.

New workflow/service code must use:

```python
FabricClient.list_lakehouses(...)
```

Avoid maintaining two implementations.

Keep backward compatibility temporarily if existing code calls it.

---

# 23. Gap FAB-20 - Response Model Strictness

Current `_FabricModel` uses:

```python
extra="ignore"
```

This is practical for evolving Fabric APIs.

Keep it.

However, critical required fields should remain required:

```text
job id
status
item id where expected
```

Current `FabricJobInstance.item_id` is optional.

For notebook/pipeline status, consider validating that returned `itemId` matches the requested item when the API provides it.

Add in getter methods:

```python
if result.item_id and result.item_id != requested_item_id:
    raise FabricResponseError(...)
```

This prevents cross-item confusion.

---

# 24. Gap FAB-21 - Validate Submission Job Item ID from Location

Current `_submission(...)` validates the Location path against:

```text
workspace ID
item ID
```

That is good.

When generalizing location parsing, preserve this safety property.

Do not reduce validation to:

```text
take last UUID from URL
```

without verifying workspace/item identity.

---

# 25. Gap FAB-22 - Pipeline/Notebook Job Correlation Metadata

The adapter returns:

```text
job_instance_id
location
retry_after_seconds
```

which is enough.

Optional model additions:

```text
workspace_id
item_id
job_kind
```

inside `FabricJobSubmission`.

This can simplify persistence/logging:

```python
class FabricJobSubmission:
    workspace_id: str
    item_id: str
    job_instance_id: str
    job_kind: Literal["NOTEBOOK", "PIPELINE"]
    ...
```

Not required if caller already has workspace/item IDs.

Priority: P2.

---

# 26. Gap FAB-23 - Notebook Parameter Contract

Current `run_notebook(...)` supports:

```text
Boolean
Number
Text
```

This is sufficient for:

```text
plan_guid
```

Future notebook parameters may require explicit Fabric types.

Do not infer beyond supported types.

Recommended improvement:

```python
class FabricJobParameter(BaseModel):
    value: str | bool | int | float
    type: Literal["Text", "Boolean", "Number"]
```

Then allow either:

```python
parameters: dict[str, object]
```

or typed parameter objects.

This is P1.

---

# 27. Gap FAB-24 - Provisioning Convenience Method Validation

Current:

```python
trigger_provisioning(...)
```

validates the `plan_guid` as UUID and submits it.

Good.

Optional improvement:

```python
def trigger_provisioning(...):
    self.get_notebook(...)
    return self.run_notebook(...)
```

Do not necessarily perform validation every time if it adds unnecessary API calls.

Better approach:

- validate target once before execution;
- then submit.

So keep `trigger_provisioning()` lightweight.

---

# 28. Gap FAB-25 - Notebook Exit Value Contract

The future workflow needs to distinguish:

```text
Fabric job status = Completed
```

from notebook-level logical failure if the notebook returns an exit value indicating failure.

Define service-level semantics.

Example:

```python
def notebook_execution_succeeded(job: FabricJobInstance) -> bool:
    if job.normalized_status != SUCCEEDED:
        return False
    if job.exit_value is None:
        return True
    return job.exit_value.lower() in {"success", "succeeded", "ok"}
```

However, this depends on the project's notebook convention.

Recommended convention:

Notebook exits structured JSON or a stable value such as:

```text
SUCCESS
```

or:

```json
{"status":"SUCCESS","plan_guid":"..."}
```

Document and test this once the notebook contract is finalized.

This belongs in the Fabric provisioning service, not raw HTTP client.

---

# 29. Gap FAB-26 - Fabric API Version / Endpoint Centralization

API paths are currently embedded directly in methods.

This is acceptable, but because notebook and DataPipeline APIs evolve independently, centralize endpoint builders.

Example:

```python
def _notebook_submit_path(...)
def _notebook_status_path(...)
def _pipeline_submit_path(...)
def _pipeline_status_path(...)
```

This reduces path drift.

Priority: P1.

---

# 30. Required API Corrections Verified Against Current Microsoft Documentation

At the time of this handover, Microsoft documents:

## Notebook submission

```http
POST /v1/workspaces/{workspaceId}/notebooks/{notebookId}/jobs/execute/instances?beta=false
```

Supported identities include service principal and managed identity.

Request body supports notebook parameters.

A successful call returns:

```text
202 Accepted
Location
Retry-After
```

## DataPipeline submission

```http
POST /v1/workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/jobs/execute/instances
```

A successful call returns:

```text
202 Accepted
Location
Retry-After
```

## DataPipeline status

```http
GET /v1/workspaces/{workspaceId}/dataPipelines/{dataPipelineId}/jobs/execute/instances/{jobInstanceId}
```

Response includes:

```text
id
itemId
jobType
invokeType
status
rootActivityId
startTimeUtc
endTimeUtc
failureReason
```

Use current Microsoft Fabric REST documentation as the implementation source of truth.

---

# 31. Methods Explicitly NOT Needed in `FabricClient`

Do not add:

```text
Config DB reads/writes
table DDL generation
view SQL generation
Oracle/SAP interpretation
Temporal workflow logic
durable polling loops
LLM calls
Phoenix tracing
business approval
```

The Fabric client remains a transport/execution adapter.

---

# 32. Recommended Public Fabric API After This Change

Approximate target:

```python
class FabricClient:

    # health
    test_connection(...)

    # workspaces
    list_workspaces(...)
    get_workspace(...)

    # Lakehouses
    list_lakehouses(...)
    get_lakehouse(...)
    find_lakehouse_by_name(...)

    # generic items
    get_notebook(...)
    get_pipeline(...)
    list_items(...)                 # optional

    # notebook jobs
    run_notebook(...)
    get_notebook_run(...)
    cancel_notebook_run(...)

    # project-specific convenience
    trigger_provisioning(...)

    # pipeline jobs
    run_pipeline(...)
    get_pipeline_run(...)
    cancel_pipeline_run(...)        # if supported
```

Service helpers outside raw client:

```python
validate_fabric_target(...)
is_terminal_job_status(...)
notebook_execution_succeeded(...)
```

---

# 33. Priority

## P0 - Must complete before Temporal workflows

```text
FAB-01 correct pipeline status endpoint
FAB-02 generalize secure Location parsing
FAB-03 verify/correct notebook status endpoint
FAB-04 verify notebook cancel endpoint
FAB-06 shared job parser
FAB-07 terminal-state helper
FAB-09 target validation helper
FAB-11 submission idempotency/retry contract
FAB-17 connection health check
FAB-20 validate returned job item identity
FAB-21 preserve secure Location identity validation
FAB-26 centralize endpoint builders
```

## P1 - Strongly recommended

```text
FAB-05 pipeline cancel support decision
FAB-10 pipeline scoping contract
FAB-15 generic item listing
FAB-19 deprecate old list_lakehouses
FAB-23 typed notebook parameter model
FAB-25 notebook exit-value convention
```

## P2 - Optional

```text
FAB-13 HTTP-date Retry-After
FAB-14 UUID response model fields
FAB-16 find notebook/pipeline by name
FAB-18 managed identity abstraction
FAB-22 richer submission correlation model
```

---

# 34. Tests Required

## Authentication

```text
missing tenant/client/secret
token acquisition failure
401
403
credential close
```

## Workspace / Lakehouse

```text
workspace pagination
invalid continuation token
duplicate continuation token
workspace get
Lakehouse pagination
duplicate Lakehouse display names
404
malformed item
```

## Notebook

```text
validate Notebook item
wrong item type
submit without parameters
submit plan_guid
Text parameter
Boolean parameter
Number parameter
unsupported parameter type
202 Location parse
missing Location
invalid Location host
wrong workspace in Location
wrong item in Location
Retry-After
status running
status completed
status failed
exitValue
unknown status
cancel
```

## Pipeline

```text
validate DataPipeline item
submit pipeline
202 Location parse
pipeline-specific status endpoint
running
completed
failed
rootActivityId
Retry-After
cancel if supported
```

## Errors

```text
401 -> FabricAuthenticationError
403 -> FabricPermissionError
404 -> FabricNotFoundError
429 -> FabricRateLimitError
500/503 -> FabricResponseError or submission error
request timeout
malformed JSON
secret-safe error text
```

## Idempotency / Temporal behavior

```text
GET activity may retry
submission POST is not blindly retried
ambiguous transport failure surfaces cleanly
job ID can be persisted after successful submit
```

---

# 35. Integration Tests Required in DEV Fabric Workspace

Before declaring adapter complete, run actual DEV tests.

## Test 1 - Notebook happy path

```text
validate notebook
    ->
submit with plan_guid
    ->
capture job ID
    ->
honor Retry-After
    ->
get status until terminal using test harness
    ->
verify exitValue
```

Do not put polling loop into the production client. The integration test harness may poll.

## Test 2 - Notebook failure

Run a DEV notebook that intentionally fails.

Verify:

```text
normalized_status = FAILED
failureReason captured
```

## Test 3 - Pipeline happy path

```text
validate pipeline
    ->
submit
    ->
capture job ID
    ->
use DataPipeline-specific status endpoint
    ->
terminal Completed
```

## Test 4 - Wrong item type

Pass a pipeline ID to `get_notebook(...)` and confirm deterministic error.

---

# 36. Acceptance Criteria

Fabric adapter is ready for Temporal workflow development when:

- [ ] service-principal authentication works
- [ ] workspaces can be listed/read
- [ ] Lakehouses can be listed/read
- [ ] notebook item can be validated
- [ ] pipeline item can be validated
- [ ] parameterized notebook submission works
- [ ] secure Location parsing works for notebook/pipeline responses
- [ ] notebook status uses a verified supported endpoint
- [ ] notebook exit value is available
- [ ] notebook cancellation works or is explicitly documented unsupported
- [ ] pipeline submission works
- [ ] pipeline status uses the DataPipeline endpoint
- [ ] pipeline cancellation is supported or explicitly documented unsupported
- [ ] Retry-After is surfaced
- [ ] raw + normalized job status are preserved
- [ ] target validation helper exists
- [ ] POST submission retries remain safe
- [ ] no long polling exists in `FabricClient`
- [ ] legacy module-level Lakehouse listing is deprecated
- [ ] all P0 behavior is covered by tests
- [ ] DEV integration tests pass

---

# 37. Final Instruction to Coding Agent

Implement the Fabric gaps above only.

Do not begin the Temporal workflows in this change.

Do not move Config DB, Oracle, SAP, LLM, or Temporal responsibilities into `FabricClient`.

Preserve the currently working client/models/exceptions structure.

The desired end state is:

```text
Fabric auth
   READY
     |
     v
workspace / Lakehouse validation
   READY
     |
     v
notebook submission + status
   READY
     |
     v
pipeline submission + status
   READY
     |
     v
typed execution result
     |
     v
NEXT CHANGE:
Temporal activities/workflows
```

Most importantly:

**FabricClient should perform short-lived transport operations only. Temporal must own durable waiting, retry timing, cancellation orchestration, and workflow state.**
