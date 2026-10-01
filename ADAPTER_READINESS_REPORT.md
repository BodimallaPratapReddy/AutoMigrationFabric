# Adapter readiness report

Reviewed 2026-10-01. Scope: the local adapter and service code, its tests, the
Config DB schema, and bounded read-only connection checks. No Temporal workflow,
Fabric job, Config DB mutation, or LLM call was run against a live system.

## Oracle

**NOT READY** for production workflow execution.

Implemented:

- `OracleClient` supports DSN or host/service settings, bundled Windows and Linux
  Instant Client paths, locked repeated initialization, health checks, schema and
  table existence, and identifier validation.
- `inspect_table` returns columns, composite primary and multiple unique keys,
  index definitions, ranked temporal watermark candidates, optimizer statistics,
  partition metadata, and warnings. Discovery does not run `COUNT(*)`.
- The separate deterministic mapper covers the requested Oracle types. Bounded or
  ambiguous `NUMBER` and timezone conversions are explicit unsupported decisions.
- Unit tests cover connection setup, identifiers, metadata, keys, indexes,
  watermark ranking, missing/stale statistics, and partitions. The read-only
  live connection probe passed.

Remaining blockers:

- **P0:** Dictionary discovery and datatype assumptions have not been checked
  against the intended Oracle schema and grants. The live probe only executed
  `SELECT 1 FROM DUAL`; no full table inspection or composition chain ran.

Non-blocking improvements:

- Add a representative live table fixture with composite keys and partitioned
  metadata to the eventual integration test environment.

## SAP

**NOT READY** for production workflow execution.

Implemented:

- `SAPClient` provides Basic authentication, SAP client routing, configurable TLS
  verification/CA bundle, health checks, table DDIC metadata and keys, DataSource
  details and exposed fields, tri-state ODP capability, conservative delta details,
  extraction route reason codes, DB view and InfoSet assessments, and recursive FM
  scraping with a typed completeness assessment.
- Field metadata preserves raw `SELECTION` and does not invent visibility or
  extractability. Class-based extraction has `CLASS_BASED_NOT_SUPPORTED` and the
  scraper accepts only its verified `FM` object type.
- `SAPDatasourceRebuildContext` composes the source evidence. The SAP verifier
  normalizes aliases, deduplicates candidates, checks dictionary type and fields,
  and recursively checks DB view sources with unresolved reason codes.
- Deterministic SAP datatype mapping lives outside `SAPClient` and marks unknown
  types unsupported. A contradictory ABAP completion flag is now forced to false
  when traversal is truncated or dependencies are unresolved.
- Unit tests cover these behaviors. A read-only SAP connection probe passed.

Remaining blockers:

- **P0:** ROOSFIELD, ROOSATTR, delta metadata, DB views, and FM scrape completeness
  have not been verified with representative DataSources on the target ECC release.
  The health probe does not establish those endpoint contracts.
- **P0:** No service binds `SAPObjectVerification.verified` to approved runtime
  source objects. `ApprovedRuntimePlan` accepts source table names without evidence
  of verification. A caller could persist an LLM-proposed SAP name after plan
  approval without passing it through the verifier.
- **P0 for the rebuild branch:** The target FM scrape -> model analysis -> SAP
  verification -> approved Fabric plan chain has no end-to-end test.

Non-blocking improvements:

- Verify class traversal against the SAP endpoint if class-based extractors enter
  V1 scope; the current explicit unsupported route is safe.

## Config DB

**NOT READY** for production workflow execution.

Implemented:

- `ConfigDBRepository` has the requested connection, migration plan, SAP
  analysis, source table/column, replication config/state, Fabric view/dependency,
  and batch/object-run operations. Old watermark mutation methods remain only in
  the legacy utility and its tests; production services do not call them.
- `persist_approved_runtime_plan` uses one transaction, locks and checks the plan
  row, checks version and approval, fingerprints the payload, assigns stable
  child IDs, and writes all six runtime tables. Unit tests cover rollback and
  same/different-payload retries. Replication config and state remain separate;
  batch object records include old/new watermark values.
- The read-only live Config DB probe passed.

Remaining blockers:

- **P0:** Atomicity, locking, idempotent retries, status transitions, and row
  parsing have only mock coverage. Run write/read integration tests on a disposable
  SQL Server database created from `config_db_new_database.sql`, including a
  forced child insert failure and concurrent same-plan persistence.
- **P0:** Approved SAP analysis and verified source objects are not checked by
  `persist_approved_runtime_plan`; its gate checks plan approval and version only.
  The SAP rebuild route needs a repository or service-level provenance gate before
  any model-derived object reaches runtime configuration.
- **P0:** The provisioning notebook and deployed replication pipeline have not
  been shown to read the new Config DB views and approved records.

Non-blocking improvements:

- `SourceSystemType`, `SourceObjectType`, and `MigrationApproach` are free strings
  in repository creation models. Centralize their canonical values before callers
  begin creating plans. Incremental methods and write strategies already have
  constrained values.

## Fabric

**READY** at the adapter contract and unit-test level; live job integration is deferred.

Implemented:

- `FabricClient` handles service-principal authentication, typed auth/API errors,
  workspace/Lakehouse/item discovery, `Notebook` and `DataPipeline` item checks,
  notebook and pipeline submission/status/cancellation, and `Retry-After` metadata.
  Jobs retain raw and normalized status, timestamps, failure details, exit value,
  and root activity ID. Notebook provisioning sends only `plan_guid`.
- `validate_fabric_target` checks workspace, Lakehouse, notebook and, when
  supplied, pipeline IDs. The service parser validates a successful notebook's
  structured exit result and matching plan GUID. Unit tests cover success,
  failure, cancellation, authentication, and response parsing. The read-only
  workspace/Lakehouse probe passed.
- The project replication scope is **`GLOBAL_CONFIG_LOOKUP`**: the SQL view
  `vw_ReplicationQueue` includes all enabled, provisioned source tables, and
  pipeline submission carries no PlanGUID. The notebook exit contract is the
  JSON result defined in `EXECUTION_CONTRACT.md` and parsed in
  `services/fabric_provisioning.py`.

Remaining blockers:

- None identified in the Fabric adapter code or its current contract tests.

Non-blocking improvements:

- Require a pipeline ID at the application boundary for routes that replicate;
  the reusable validation service currently permits it to be omitted.
- When the notebook and pipeline are ready, verify their deployed definitions,
  exit values, submission/status/cancellation behavior, and queue scope in a
  nonproduction workspace. These are deferred integration checks, not current
  adapter defects.

## LLM / Phoenix

**NOT READY** for production workflow execution.

Implemented:

- `MigrationAnalysisClient` isolates model SDK calls and exposes initial/revision
  operations for SAP analysis and Fabric proposals. Pydantic validates the output;
  proposals do not call Fabric. Manual OpenTelemetry spans record plan/version,
  model, prompt version, latency, token usage when present, result status, and a
  trace ID, without prompt/source text or credentials. `SAPAnalysis` has a
  `PhoenixTraceId` field.
- Unit tests cover structured output and missing output. A previous local stub
  span reached the Phoenix exporter, as recorded in
  `ADAPTER_IMPLEMENTATION_STATUS.md`.

Remaining blockers:

- **P0:** No representative real-model call has verified schema compatibility
  with sanitized ECC source context, SAP object verification, and storage of its
  trace ID in `SAPAnalysis.PhoenixTraceId`.
- **P0:** Model-generated Fabric proposals are not bound to the verified SAP
  object set before runtime persistence. Human approval alone does not check that
  all proposed source names exist in SAP.

Non-blocking improvements:

- Tool-call tracing is not exercised because this adapter currently makes no
  model tool calls. Add it if a tool-using model path is introduced.

## Cross-adapter composition and test gate

The normal Python entry points exist for Oracle inspection/mapping, SAP table
metadata/mapping, SAP ODP discovery, SAP FM rebuild context/verification, Config DB
persistence, and Fabric validation/execution. There is no production service or
integration test that completes any of the four requested chains. In particular,
the approved/verified SAP analysis to runtime plan boundary is missing. The
adapters keep source discovery, persistence, Fabric transport, and LLM calls in
separate modules; no Temporal workflow was added.

Verification performed on 2026-10-01:

- `python -m unittest discover -s tests -q`: **106 passed** (mock/unit suite).
- `python -m compileall -q thirdparty services app tests`: **passed**.
- `python tests/smoke_readonly_adapters.py`: **ConfigDB, Fabric, Oracle, SAP OK**.
  This checks connectivity only and does not prove the four composition chains.

**OVERALL WORKFLOW READINESS: NOT READY**

P0 blocker list: SAP metadata/scrape validation on representative ECC objects;
verified SAP object provenance through approved persistence; disposable Config DB
write/concurrency integration tests; and end-to-end composition tests for the
adapter chains that do not require the unfinished Fabric notebook or pipeline.
Temporal workflow implementation should wait for these gates.
