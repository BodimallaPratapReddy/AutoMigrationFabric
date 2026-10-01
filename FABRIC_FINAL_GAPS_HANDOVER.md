# Coding Agent Handover
## Final Microsoft Fabric Adapter Gaps Before Temporal Workflow Development

**Scope:** Only the final remaining gaps in the Microsoft Fabric adapter.  
**Baseline:** Current `client.py`, `models.py`, `exceptions.py`, `utils.py`, and `EXECUTION_CONTRACT.md`.  
**Goal:** Close the last integration gaps so the Fabric adapter is ready for use by Temporal activities.

---

# 1. Current Fabric Adapter Status

The current Fabric adapter is already largely complete.

The following capabilities are implemented and should be preserved:

- Service-principal authentication
- Authenticated synchronous `httpx.Client`
- Typed Fabric exception hierarchy
- Safe error messages without request-body / secret leakage
- Request/correlation ID extraction
- `Retry-After` extraction
- Workspace listing and lookup
- Lakehouse listing and lookup
- Lakehouse lookup by display name with duplicate protection
- Generic Fabric item lookup
- Notebook validation
- Data pipeline validation
- Generic item listing
- Notebook/pipeline lookup by display name
- Parameterized notebook submission
- `plan_guid` provisioning helper
- Notebook status retrieval
- Notebook `exitValue` extraction
- Notebook cancellation
- Pipeline submission
- Pipeline status retrieval using the DataPipeline-specific endpoint
- Pipeline cancellation
- Secure parsing of Fabric `Location` headers
- Job ID and item ID validation
- Job status normalization
- Terminal-state helper
- Typed notebook parameters
- Connection test
- Explicit execution contract documenting:
  - no long polling in the client
  - no automatic retry for submission POSTs
  - Temporal/caller owns durable waiting
  - `Retry-After` is returned to callers

Do not redesign these pieces.

---

# 2. Remaining Items

Only the following items remain before the Fabric adapter should be considered ready for workflow implementation.

---

# 3. Gap FAB-FINAL-01 - Confirm Replication Pipeline Scope Contract

## Current situation

The current method:

```python
run_pipeline(
    workspace_id,
    pipeline_id,
)
```

submits the Fabric Data Pipeline without an execution payload.

This is correct according to the currently used specialized DataPipeline execution API.

However, the project must decide how the replication pipeline determines **which configuration rows to process**.

There are two possible models.

## Option A - Pipeline processes all enabled runtime configuration

The pipeline queries:

```text
bronze_replication.vw_ReplicationQueue
```

or the equivalent underlying configuration tables.

It processes every row with:

```text
IngestionFlag = 1
ProvisioningStatus = PROVISIONED
```

In this model, no Fabric pipeline parameter is required.

### Required action

Confirm the existing replication pipeline behaves this way.

Document:

```text
Pipeline Scope Mode = GLOBAL_CONFIG_LOOKUP
```

## Option B - Pipeline processes only a migration plan

The pipeline should execute only objects associated with:

```text
PlanGUID
```

Example:

```text
Temporal workflow
    ->
run replication pipeline for MP-1001
    ->
pipeline queries Config DB
WHERE PlanGUID = MP-1001
```

### Required action

If this mode is required, first confirm the supported Fabric Data Pipeline parameter mechanism and pipeline implementation.

Do **not** invent an undocumented request body.

Once supported, add a dedicated method, for example:

```python
def run_replication_pipeline(
    self,
    workspace_id: str,
    pipeline_id: str,
    *,
    plan_guid: str | None = None,
) -> FabricJobSubmission:
    ...
```

## Acceptance criterion

Before Temporal workflow development, the project must explicitly choose:

```text
GLOBAL_CONFIG_LOOKUP
```

or:

```text
PLAN_SCOPED_LOOKUP
```

and document the decision.

---

# 4. Gap FAB-FINAL-02 - Define Provisioning Notebook Exit Contract

## Current situation

The Fabric adapter correctly returns:

```python
FabricJobInstance.exit_value
```

for notebook execution.

But the project has not yet defined what the provisioning notebook must return.

Do not let Temporal interpret arbitrary notebook strings.

## Required contract

Standardize the provisioning notebook result.

Recommended notebook exit payload:

```json
{
  "status": "SUCCESS",
  "plan_guid": "00000000-0000-0000-0000-000000000000",
  "tables_processed": 3,
  "views_processed": 1
}
```

Failure example:

```json
{
  "status": "FAILED",
  "plan_guid": "00000000-0000-0000-0000-000000000000",
  "message": "View dependency was not available"
}
```

The notebook may serialize this JSON into the notebook exit value.

## Required model

Add outside the raw HTTP client:

```python
class ProvisioningNotebookResult(BaseModel):
    status: Literal["SUCCESS", "FAILED"]
    plan_guid: UUID
    tables_processed: int | None = None
    views_processed: int | None = None
    message: str | None = None
```

## Required parser

Add in the Fabric provisioning service:

```python
def parse_provisioning_notebook_result(
    job: FabricJobInstance,
) -> ProvisioningNotebookResult:
    ...
```

Rules:

```text
Fabric job FAILED
    -> raise / return execution failure

Fabric job CANCELLED
    -> cancellation

Fabric job SUCCEEDED + no exit value
    -> invalid provisioning result

Fabric job SUCCEEDED + malformed JSON
    -> invalid provisioning result

Fabric job SUCCEEDED + {"status":"FAILED"}
    -> provisioning logical failure

Fabric job SUCCEEDED + {"status":"SUCCESS"}
    -> success
```

Do not put project-specific notebook result semantics inside the low-level `FabricClient`.

---

# 5. Gap FAB-FINAL-03 - Normalize Authentication Failures

## Current situation

`connect_to_fabric()` directly calls:

```python
credential.get_token(...)
```

Azure Identity errors may therefore escape as Azure SDK exceptions.

The rest of the Fabric client already normalizes HTTP errors into:

```text
FabricAuthenticationError
FabricPermissionError
FabricNotFoundError
...
```

Authentication should follow the same pattern.

## Required change

Wrap token acquisition.

Example:

```python
try:
    credential.get_token(FABRIC_SCOPE)
except Exception as exc:
    raise FabricAuthenticationError(
        "Unable to authenticate with Microsoft Fabric"
    ) from exc
```

Prefer catching appropriate Azure Identity exception classes if practical.

Do not include:

```text
tenant secret
client secret
token
credential contents
```

in the error.

---

# 6. Gap FAB-FINAL-04 - Return Cancellation Metadata

## Current situation

Current methods:

```python
cancel_notebook_run(...)
cancel_pipeline_run(...)
```

return:

```python
None
```

Cancellation is asynchronous.

The Fabric API may return:

```text
202
Location
Retry-After
```

The caller may need the recommended next-check interval.

## Required model

Add:

```python
class FabricCancellationSubmission(BaseModel):
    location: str | None = None
    retry_after_seconds: int | None = None
```

## Required method changes

Change:

```python
cancel_notebook_run(...) -> FabricCancellationSubmission
```

and:

```python
cancel_pipeline_run(...) -> FabricCancellationSubmission
```

Capture:

```text
Location
Retry-After
```

when present.

Do not wait for cancellation to complete.

Temporal will:

```text
submit cancel
    ->
durable timer
    ->
get status
```

---

# 7. Gap FAB-FINAL-05 - Add Fabric Target Validation Service

The low-level client already contains:

```python
get_workspace(...)
get_lakehouse(...)
get_notebook(...)
get_pipeline(...)
```

Do not repeat these calls inside all four workflows.

Add a service-level helper.

## Required model

```python
class FabricTargetValidation(BaseModel):
    workspace: FabricWorkspace
    lakehouse: Lakehouse
    notebook: FabricItem
    pipeline: FabricItem | None = None
```

## Required method

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

Behavior:

```text
validate workspace
validate Lakehouse
validate notebook is Notebook
if pipeline supplied:
    validate item is DataPipeline
```

Place this in:

```text
services/fabric_validation.py
```

not inside the HTTP transport if possible.

---

# 8. Gap FAB-FINAL-06 - DEV Integration Verification

The adapter code now aligns closely with the intended APIs, but live DEV verification is still required.

This is especially important because notebook submission and status currently use different API maturity levels.

The current execution contract documents:

```text
Notebook submit:
    beta=false

Notebook status:
    beta=true
```

Keep this unless DEV verification shows otherwise.

## Required integration test 1 - Notebook success

Use a simple DEV notebook.

Flow:

```text
get_notebook
    ->
run_notebook(plan_guid)
    ->
capture job ID
    ->
status checks outside FabricClient
    ->
SUCCEEDED
    ->
validate exitValue
```

Verify:

```text
job ID
item ID
raw status
normalized status
exitValue
Retry-After
```

## Required integration test 2 - Notebook failure

Use a notebook that intentionally raises an exception.

Verify:

```text
normalized_status = FAILED
failureReason populated
```

## Required integration test 3 - Notebook cancel

Run a notebook that lasts long enough to cancel.

Flow:

```text
submit
    ->
cancel
    ->
status check
```

Verify final status becomes:

```text
Cancelled
```

or Fabric's equivalent raw value.

## Required integration test 4 - Pipeline success

Flow:

```text
get_pipeline
    ->
run_pipeline
    ->
capture job ID
    ->
get_pipeline_run
    ->
terminal status
```

Verify:

```text
rootActivityId
start/end timestamps
failureReason when applicable
```

## Required integration test 5 - Pipeline cancel

If cancellation is supported in the DEV tenant:

```text
submit long-running pipeline
    ->
cancel
    ->
poll from test harness
    ->
Cancelled
```

If Fabric does not support this reliably, document that limitation.

---

# 9. Gap FAB-FINAL-07 - Confirm Pipeline Item Type

Current `get_pipeline(...)` expects:

```text
DataPipeline
```

During DEV integration test, confirm the Fabric item response actually returns:

```json
{
  "type": "DataPipeline"
}
```

Do not change the expected string unless the actual Fabric response requires it.

---

# 10. Gap FAB-FINAL-08 - Confirm Notebook Item Type

Similarly confirm notebook item type:

```text
Notebook
```

using the real DEV Fabric workspace.

Only change if actual API behavior differs.

---

# 11. Gap FAB-FINAL-09 - Submission Idempotency Guidance for Temporal

The current client intentionally does not retry submission POSTs.

Keep that behavior.

The Temporal activities later must distinguish:

```text
submission returned job ID
```

from:

```text
transport failed before job ID was received
```

## Required activity guidance

### Notebook submission

```text
Retry policy:
    conservative

Do not blindly retry on:
    HTTP client timeout
    connection reset after request may have been sent
```

### Status GET

Safe to retry:

```text
429
502
503
504
network transport errors
```

### Cancel POST

Also potentially ambiguous.

If cancellation POST fails after request may have reached Fabric:

```text
check status before retrying cancel
```

This is already described in `EXECUTION_CONTRACT.md`.

No major client code change is needed, but preserve this contract.

---

# 12. Gap FAB-FINAL-10 - Keep Raw Fabric Status

Current model correctly contains:

```python
status: str
normalized_status: FabricJobStatus
```

Keep this.

Do not replace raw status with only the normalized enum.

If Fabric introduces a new status:

```text
raw status remains visible
normalized status = UNKNOWN
```

This behavior is desirable.

---

# 13. Optional - Richer Submission Correlation

Current submission model contains:

```text
job_instance_id
location
retry_after_seconds
```

This is sufficient.

Optionally add:

```text
workspace_id
item_id
job_kind
```

if useful for batch logging.

This is not required before workflow development because the caller already knows these values.

Priority:

```text
P2
```

---

# 14. Optional - Managed Identity

Current authentication uses:

```python
ClientSecretCredential
```

This is acceptable for V1.

Managed Identity can be added later by allowing credential injection.

Do not block workflow development on this.

Priority:

```text
P2
```

---

# 15. Optional - Remove Legacy Lakehouse Wrapper Later

The old module-level:

```python
list_lakehouses(...)
```

is now explicitly deprecated.

Keep it only for compatibility.

All new code must use:

```python
FabricClient.list_lakehouses(...)
```

Remove the compatibility wrapper only after all old callers are migrated.

---

# 16. Public API Expected After Final Changes

```python
class FabricClient:

    # connection
    test_connection(...)

    # workspaces
    list_workspaces(...)
    get_workspace(...)

    # lakehouses
    list_lakehouses(...)
    get_lakehouse(...)
    find_lakehouse_by_name(...)

    # items
    list_items(...)
    get_notebook(...)
    get_pipeline(...)
    find_notebook_by_name(...)
    find_pipeline_by_name(...)

    # notebook execution
    run_notebook(...)
    trigger_provisioning(...)
    get_notebook_run(...)
    cancel_notebook_run(...) -> FabricCancellationSubmission

    # pipeline execution
    run_pipeline(...)
    get_pipeline_run(...)
    cancel_pipeline_run(...) -> FabricCancellationSubmission
```

Service layer:

```python
validate_fabric_target(...)
parse_provisioning_notebook_result(...)
```

---

# 17. Do Not Add These Responsibilities to FabricClient

Do not add:

```text
Config DB SQL
migration-plan creation
Oracle metadata logic
SAP metadata logic
LLM reasoning
Fabric view-generation decisions
watermark selection
Temporal workflow loops
Phoenix tracing
human approval state
```

The Fabric client must remain a short-lived transport/execution adapter.

---

# 18. Priority

## P0 - Complete before Temporal workflows

```text
FAB-FINAL-01 Confirm replication pipeline scope
FAB-FINAL-02 Define notebook exit contract
FAB-FINAL-03 Normalize authentication errors
FAB-FINAL-05 Add target validation service
FAB-FINAL-06 Complete DEV integration tests
FAB-FINAL-07 Confirm pipeline item type
FAB-FINAL-08 Confirm notebook item type
```

## P1 - Recommended before workflow rollout

```text
FAB-FINAL-04 Return cancellation Retry-After/location metadata
FAB-FINAL-09 Preserve/document Temporal submission idempotency contract
```

## P2 - Optional later

```text
Richer submission model
Managed Identity
Remove deprecated module-level Lakehouse wrapper
```

---

# 19. Tests Required

Add/confirm tests for the final changes.

## Authentication

```text
Azure token acquisition failure
-> FabricAuthenticationError
-> no secret leakage
```

## Cancellation

```text
notebook cancel 202
Retry-After captured
Location captured

pipeline cancel 202
Retry-After captured
Location captured
```

## Notebook result parser

```text
SUCCEEDED + valid SUCCESS JSON
SUCCEEDED + valid FAILED JSON
SUCCEEDED + no exit value
SUCCEEDED + malformed JSON
FAILED Fabric job
CANCELLED Fabric job
wrong plan_guid in notebook result
```

## Target validation

```text
valid workspace/Lakehouse/notebook/pipeline
missing workspace
missing Lakehouse
wrong notebook item type
wrong pipeline item type
optional pipeline omitted
```

---

# 20. Acceptance Criteria

Fabric integration can be considered ready for Temporal workflow development when:

- [ ] connection/authentication errors are normalized
- [ ] workspace lookup works
- [ ] Lakehouse lookup works
- [ ] notebook validation works
- [ ] pipeline validation works
- [ ] notebook submission works
- [ ] notebook status works
- [ ] notebook exit contract is defined
- [ ] notebook cancellation works
- [ ] pipeline submission works
- [ ] pipeline status works
- [ ] pipeline cancellation behavior is verified
- [ ] replication pipeline scope behavior is confirmed
- [ ] secure Location parsing remains intact
- [ ] Retry-After is available to caller
- [ ] target-validation helper exists
- [ ] DEV notebook success test passes
- [ ] DEV notebook failure test passes
- [ ] DEV notebook cancellation test passes
- [ ] DEV pipeline success test passes
- [ ] pipeline cancellation is verified/documented
- [ ] no polling/sleep exists inside `FabricClient`

---

# 21. Final Instruction to Coding Agent

Implement only the remaining Fabric items in this handover.

Do not redesign the existing client.

Do not start the Temporal workflows as part of this change.

Preserve:

```text
short HTTP operations
typed responses
typed errors
no long polling
no blind submission retry
```

The desired final state is:

```text
Fabric authentication
        READY
          |
          v
target validation
        READY
          |
          v
notebook execution
        READY
          |
          v
pipeline execution
        READY
          |
          v
typed job/cancel/result contracts
          |
          v
NEXT CHANGE:
Temporal activities and workflows
```

The key remaining project decisions are:

1. **How the replication pipeline determines its scope.**
2. **What exact structured result the provisioning notebook returns.**

Once those two contracts are finalized and DEV execution tests pass, the Fabric adapter is ready for the four migration workflows.
