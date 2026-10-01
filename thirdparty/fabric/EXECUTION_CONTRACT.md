# Fabric execution contract

The client performs one short HTTP operation per method call. It never polls or sleeps.
The caller stores the returned job instance ID and checks the status in a separate
activity. `Retry-After` is exposed on submissions, status responses, and HTTP errors.
The caller owns retry timing and durable waiting.

Submission POSTs have **no automatic retry**. A timeout may mean Fabric accepted the
job but the response was lost. Treat that result as ambiguous; do not automatically
repeat the POST. Persist the job ID immediately when a submission response is received.
GET activities may use bounded retries for transport errors and HTTP 429/502/503/504,
respecting `Retry-After`. A cancellation POST may also have an ambiguous outcome; check
the job status before attempting it again.

Notebook submission uses the release `beta=false` API. Notebook status uses the
documented notebook `beta=true` API, which returns `properties.exitValue`; the client
maps it to `FabricJobInstance.exit_value`. This mixed release/beta behavior needs DEV
workspace verification before production use. Pipeline status uses the DataPipeline
execute endpoint. Both notebook and pipeline cancellation use the generic item-job
cancel endpoint. Cancellation returns `FabricCancellationSubmission` with any Location
and `Retry-After` headers. It is asynchronous; a 202 response does not mean the job is
already cancelled. `UNKNOWN` status stays visible together with the raw status string.

Replication pipeline scope is `GLOBAL_CONFIG_LOOKUP`: one submission processes all
enabled, provisioned rows in the Config DB replication queue. The DataPipeline execute
API documents no parameter body, so `run_pipeline` submits without a PlanGUID. The
repository's `vw_ReplicationQueue` view implements this scope with `IngestionFlag = 1`
and `ProvisioningStatus = 'PROVISIONED'`, without a PlanGUID filter. Before workflow
use, verify that the deployed pipeline reads this view and applies the same scope.
The adapter cannot establish that from its HTTP execution contract alone.

The provisioning notebook must return a JSON string from its exit call with `status`
(`SUCCESS` or `FAILED`) and the matching `plan_guid` UUID. Nonnegative
`tables_processed` and `views_processed` counts and a `message` are optional. The
service parser in `services/fabric_provisioning.py` accepts this only for a successful
Fabric job. A failed or cancelled Fabric job, a missing or malformed exit value, a
different plan GUID, and a logical `FAILED` result each raise a distinct service
error. The low-level client only surfaces the raw exit value.

Reference: Microsoft [notebook submission](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/run-on-demand-notebook),
[notebook status](https://learn.microsoft.com/en-us/rest/api/fabric/notebook/background-jobs/get-notebook-job-instance%28beta%29),
[pipeline submission](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/background-jobs/run-on-demand-execute),
[pipeline status](https://learn.microsoft.com/en-us/rest/api/fabric/datapipeline/background-jobs/get-execute-job-instance),
and [job cancellation](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/cancel-item-job-instance).
