# OpenCode Oracle discovery in the Fabric workflow

The existing UI and Temporal own the migration. Select **OpenCode agent** under Oracle
discovery. The API starts `OpenCodeOracleMigrationWorkflow`; built-in discovery remains
available, and old workflow types and their activity sequences remain unchanged.

## Worker setup

Merge the settings from `opencode.env.example` into the ignored `.env` in the project
root, or export them in the Temporal worker environment. Use the current password
printed by `opencode serve`. `OPENCODE_DIRECTORY` is the Linux directory as seen by
the OpenCode server. `OPENCODE_URL` must be reachable from the worker; localhost inside
a container is that container, not the Windows/WSL host.

Set `OPENCODE_SOURCE_CONNECTIONS` to a JSON mapping from the exact ConfigDB registry
connection name to the SQLcl saved connection representing the same Oracle database.
There is no automatic fallback to another connection. The existing registered Oracle
connection is still used for key validation after discovery.

Restart the API and Temporal worker after deploying the code. The worker command is
`python -m app.migration.worker`. Keep the existing task queue and namespace settings.
The new workflow type must be registered before starting an OpenCode migration.

Configure SQLcl startup/execution timeouts in OpenCode for your environment. Testing
against OpenCode 2.0.26 required a fresh MCP connection, sequential small queries, and
120-second startup/execution allowances. The integration does not silently mutate
OpenCode's MCP settings. The specific cause of the earlier execution timeouts was
not isolated. Ensure both MCP servers report connected before use.

## Flow and contract

1. The existing workflow creates a draft audit plan and validates the Fabric target.
2. Activities create an OpenCode session with an explicit agent, model, and project
   location, then send the read-only discovery prompt. Secrets remain in the worker.
3. Temporal polls using bounded activities and timers. No worker activity waits for a human.
4. Native agent forms appear in **Agent discovery** in the existing UI. Replies are
   submitted to `POST /migrations/{workflow_id}/agent-reply` and signaled to Temporal.
   Permission requests support **Allow once** or **Reject**, never automatic/permanent approval.
   External/conditional forms require OpenCode's own interface; the workflow observes
   their resolution on its next poll.
5. The agent returns JSON matching `AgentPlan` in `opencode_plan.py`. The backend checks
   the source identity, field types, keys, supported Fabric types, unique column names,
   and unresolved decisions. It never invokes the old Oracle datatype mapper. It preserves
   target names, source expressions, transformation notes and the original agent plan.
6. Invalid/incomplete output appears with an agent revision field. Submit decisions or
   corrections with `POST /migrations/{workflow_id}/agent-feedback`. The same session resumes.
7. A valid plan enters the existing column, key, watermark, delete-policy and target-change
   approval flow. Agent column mappings are read-only at the column approval step; reject
   and start a revised discovery if they need changing after that point. The final approved
   runtime configuration is persisted, then the configured Fabric notebook is submitted.
   Data loading remains the responsibility of the separately scheduled pipeline.

`AgentPlan` is an explicit application contract, not arbitrary agent-generated executable
Python. Source expressions are proposals for the existing pipeline configuration and must
be reviewed. A valid JSON plan is not an execution approval. Runtime load and delete-policy
choices remain explicit UI decisions, and keys are checked against the registered source.

## Recovery and limits

Session IDs are deterministic per workflow and creation can be reconciled on retry.
Prompt/reply POSTs are not blindly retried: a lost response fails the workflow and attempts
to interrupt the agent. Inspect the recorded `agent_session_id` before restarting.
Polling is bounded to 720 iterations. User waits use durable timers and do not consume
that poll budget. Cancellation attempts to interrupt discovery and marks the draft cancelled.
Final plan text is limited to 256 KB; individual API responses are limited to 2 MB.
Credentials and reasoning/tool transcripts are not returned to the UI or workflow history.

The application's existing reviewer-name convention is preserved; it is not a new
authentication mechanism. Deploy behind the application's access controls.

## Verification

Run `python -m pytest tests/test_opencode_integration.py tests/test_migration_approval_api.py
tests/test_replication_handoff.py`. The new tests cover mapping preservation, malformed plans,
stale replies, a real local Temporal approval/cancellation flow with mocked adapters, and
replay of the resulting histories. UI checks use `node --test tests/test_*ui*.cjs`.
These checks do not write to live ConfigDB or submit Fabric jobs.

## Discovery diagnostics

`SUBMITTING_AGENT_PROMPT` means the worker is waiting for OpenCode to acknowledge the request.
Prompt submission allows 60 seconds inside a 90-second activity budget. If the response fails,
the worker reads the exact caller-chosen message ID with a 10-second timeout. An accepted user
message allows polling to continue; otherwise the original error is reported. The prompt is
never blindly resent. Transport failures identify the exception class without exposing URLs
or credentials. `WAITING_FOR_AGENT` begins after submission succeeds.

The UI shows recent tool names/statuses while OpenCode is working and displays validation blockers separately from the full plan. Tool arguments, results, and reasoning are not included in this progress summary. Progress refreshes with workflow polling; it is not a token stream.

Every initial and revision prompt includes the selected Fabric workspace, Lakehouse, schema, and target table. Revisions retain the JSON schema and read-only discovery constraints. Replication watermark and deletion approval remain in the existing downstream review gates. Unresolved decisions still block plan acceptance; they are not silently cleared.

A `WAITING_FOR_AGENT_REVISION` phase means the agent finished but its plan needs corrections or decisions. It is not an automatic retry. Submit specific feedback addressing the displayed blocker. A terminated workflow must be replaced with a new run.

## Oracle view bundles

Choose **Oracle view and base tables**. `POST /migrations/oracle-view` takes the same destination fields with `migration_approach=ORACLE_VIEW`, `discovery_provider=OPENCODE`, and the exact Oracle owner/view. The new `OpenCodeOracleViewMigrationWorkflow` leaves existing table histories unchanged.

The worker independently reads the view definition, recursive ALL_DEPENDENCIES graph, local synonyms and base-table metadata. It caps the graph at 64 objects and rejects cycles, database links and PL/SQL/object dependencies requiring an additional migration design. The separate OpenCode agent `oracle-view-to-fabric` uses `oracle-to-fabric-view-design` in the Oracle project workspace; set OPENCODE_VIEW_AGENT to override its name.

The agent returns `AgentViewBundle`: all verified base tables exactly once and every verified nested view. Source columns/nullability/keys and dependency identities are checked against Oracle evidence. Spark SQL is parsed with sqlglot, must preserve output names/order, and must reference exactly the declared target dependencies. These checks do not prove semantic equivalence. Review Oracle/Spark SQL together, especially numeric casts, Oracle empty-string/null behavior and date/time semantics. Fabric execution and output comparisons remain separate validation steps.

The UI presents a single bundle review with expandable table mappings and Oracle/Spark view definitions. Approval transactionally saves SourceTables, SourceTableColumns, ReplicationConfig, FabricViews and FabricViewDependencies. The existing notebook provisions tables before views. This version configures FULL/REPLACE with ingestion disabled until provisioning, then hands loading to the scheduler. Incremental configuration is not implemented for view bundles.

Existing registered targets are blocked before review and rechecked before persistence. This first version requires new target names; it does not automatically reuse or replace previously configured tables/views. A failure to check a target does not imply that the target is absent. Physical unregistered Fabric objects remain subject to the existing notebook's provisioning rules.

Run `python -m pytest tests/test_oracle_view_integration.py tests/test_migration_types.py` in addition to the OpenCode table tests.

### Persisted view CTE compatibility

The deployed Fabric runtime was observed storing local WITH names as qualified object
names and failing view resolution with INVALID_VIEW_TEXT. SQL_VIEW provisioning expands
scoped non-recursive CTEs into derived tables using sqlglot==29.0.1 before CREATE VIEW.
The approved SQL remains unchanged in ConfigDB. Expressions, joins, aggregation and
windows are preserved; recursive/volatile CTE definitions and CTE column alias lists
are rejected rather than transformed silently. This is a runtime workaround, not an
Oracle datatype mapper. Derived/materialized table provisioning keeps its original SQL.

Notebook deployments should provide sqlglot==29.0.1 through the environment. If absent,
the CTE handler installs the pure Python package with a bounded pip subprocess and
imports it without restarting the interpreter. Installation failures become object
errors; injected parameters survive. Preserve Fabric cell language metadata and notebook
Lakehouse attachments when updating definitions.
Provisioning logical failures carry the object report to the view workflow's UI state
and set provisioning_status=FAILED even when the Fabric job's execution status is Completed.

For a manually verified partial provisioning recovery, the notebook accepts
VALIDATE_EXISTING_TABLES_FOR_PLAN equal to the exact approved plan GUID. Only that plan
may treat identical existing table schemas as a successful validation-only operation;
different schemas still fail without DDL. Leave this parameter empty for ordinary runs.
An operator must establish prior creation evidence, check approval and inactive loaders,
and record a separate recovery batch. The original completed Temporal history remains
failed; it is not rewritten by a manual recovery.
# Reviewed notebook SQL repairs

New Oracle table proposals and view dependency table proposals include a generated
Oracle `ExtractSql`, shown in the column review and final view bundle. It reuses
approved scalar `source_expression` values, quotes source/target identifiers and
projects selected columns in source order. No watermark predicate is saved.
Config DB persists it in `SourceTables` with the approved columns; the replication
queue exposes it for the external pipeline. See
[the extraction pipeline contract](../../../thirdparty/fabric/ORACLE_EXTRACTION_PIPELINE_CONTRACT.md)
for source metadata drift checks and loading/checkpoint responsibilities. Apply
`thirdparty/configdb/migrations/005_oracle_extract_sql.sql` before using the updated
repository. Existing rows remain NULL until explicitly reviewed; no backfill or
pipeline changes are performed by that schema migration.

New `/migrations/oracle-view` requests use `OpenCodeOracleViewReplicationWorkflow`,
which retains the SQL correction behavior and adds per-base-table replication review.
For each discovered table, approve the agent-authored columns, select a replication
key if no usable primary key exists, validate source key nulls/duplicates, choose a
watermark or full refresh, and approve the deletion policy. Key profiling uses that
table's own owner/name, including dependencies in other schemas. An explicit no-key
decision restricts the table to full refresh. Final approval shows all table choices
alongside the translated views before Config DB persistence or notebook execution.
Replication settings remain disabled until successful provisioning; scheduled Fabric
pipelines own loading and deletion execution. SQL-only repairs retain all approved
replication settings and never repeat the table review or change its mappings.
Previously started view workflows keep their original behavior; they do not gain the
new review steps midway through execution.
The original workflow remains registered for existing histories. A structured
SQL view failure is recorded per object: successfully provisioned tables/views
retain their configuration IDs and are excluded from the next attempt. Operational
failures, missing physical dependencies, failed tables and unknown error classes
stop without asking the agent to repair SQL.

For recognized SQL failures the same OpenCode session receives the original plan,
Oracle definitions, failing SQL and notebook diagnostics. Only failed/blocked
view SQL and its translation notes may change. Existing strict graph, reference
and output-column validation still applies; base table schemas, datatype mappings,
extraction, destination and dependencies are frozen. A required schema change is
a blocker and needs the separate schema-change path. Output type equivalence is
not proven by SQL parsing: the reviewer must assess the revised expressions.

The UI shows old/new SQL and the failure under the existing plan approval gate.
Approval atomically updates failed view SQL, increments `PlanVersion`, records
reviewer/time and preserves prior and revised agent plans plus SQL in
`MigrationPlans.Notes.view_sql_revisions`. ConfigDB persistence retries are
fingerprinted; they cannot increment the version twice. `RuntimePlanHash` remains
the original provisioning snapshot; revision fingerprints in Notes audit later
SQL-only changes. Revisions retain object GUIDs and must not be passed through
the initial `persist_approved_runtime_plan` operation.

Each attempt has its own deterministic batch ID and records its own results.
There are at most two SQL repairs (three notebook attempts). Rejection or
cancellation stops the workflow. Native agent forms work during repair; active
agent discovery is interrupted on cancellation. A Fabric job cancellation is
handled through the existing cancellation/status polling path. Successful final
provisioning enables configuration for the separately scheduled loader, without
loading source data in this workflow.

Both design skills now use `references/datetime-semantics.md`: DATE/plain
TIMESTAMP preserve wall-clock fields using pinned UTC Spark evaluation, never
an inferred source timezone or clock shift. The provisioning notebook pins UTC;
the external loader and other consumers must enforce the same convention.
Timezone-aware types and precision above six follow their exact-preservation
rules. The one-source-column/one-target-column contract does not support TSTZ
timestamp-plus-offset expansion, so the skill uses an exact STRING fallback or
reports a material translation blocker. Existing plans/data are not remapped.
