# Adapter implementation status

This records the work against `ADAPTER_GAP_ANALYSIS_AND_CODING_HANDOVER.md`.
The adapter layer is **not yet ready** for Temporal workflow development.

Read-only checks validated the Fabric workspace and Lakehouse and an Oracle
connection. The Config DB points to the new database and uses the existing
Azure service identity without interactive sign-in. `ConfigDB.connect()`,
`list_fabric_workspaces()`, and `ConfigDBRepository.get_migration_plan()`
passed live read-only checks. The live `MigrationPlans.RuntimePlanHash` column
and six provisioning/replication views are present. The database currently has
no configured workspaces, connections, or plan rows, so populated-row parsing
and writes remain unverified. SAP stopped at TLS verification because the
endpoint uses a self-signed certificate that is not trusted on this machine.
The configured SAP host was changed from its IP address to the certificate's
DNS name after DNS and a pinned-certificate TLS handshake confirmed the match.
At the user's request, `SAP_VERIFY_TLS=false` disables certificate checking
for this workspace. Live `SAPClient.test_connection()`, `read_metadata('TCURR')`,
and `get_infoset_query_details('TESTJOIN')` now pass. The live InfoSet response
omits SQL template fields, so the model accepts them as absent. No schema
changes or Fabric jobs were run.

## Implemented

- Oracle: connection check, table existence, composite primary and unique keys,
  full index definitions, richer column metadata, and temporal watermark candidates.
- SAP: connection check, dictionary object type, table schema and keys, DataSource
  exposed fields, ROOSATTR ODP flag, typed delta summary, extraction routing,
  scraper completeness assessment, and batch metadata lookup. Class extractors
  return an explicit unsupported route until class traversal is verified.
- Fabric: workspace and Lakehouse discovery, notebook and pipeline item validation,
  parameterized notebook submission, pipeline submission through the documented
  DataPipeline endpoint, job status, cancellation, and typed retry metadata.
- Config DB: new-schema typed models and repository for connections, migration
  plans and approvals, SAP analyses, replication configuration and state, Fabric
  table and view metadata, view dependencies, and batch audit records. Approved
  runtime persistence is one transaction and rolls back child insert failures.
  Identical activity retries return deterministic child IDs; changed payloads
  are rejected using `MigrationPlans.RuntimePlanHash`.
- Config DB workflow-facing gaps: SAP analysis status transition and plan/version
  lookup, standalone watermark correction, batch and object-run read models,
  guarded Fabric run ID setters, cancellation, and old-watermark audit capture.
  The new read queries passed against the configured database. Mutation behavior
  is unit-tested but has not been exercised against populated live records.
- Deterministic Oracle and SAP type mapping; unsupported cases are explicit.
- Model adapter: structured SAP analysis and Fabric plan proposals, revisions,
  SAP object verification, and manual Phoenix metadata spans. Model output is a
  candidate only; the repository gates runtime persistence on plan approval.
  A local Phoenix smoke span was accepted by the exporter and flushed, with a
  trace ID returned by the adapter.

## Remaining before workflow development

1. Run integration tests against a disposable database created from
   `thirdparty/configdb/config_db_new_database.sql`, including transaction
   rollback and all repository reads. Current database tests use mocks; the
   configured database has the new schema and `RuntimePlanHash`, but is empty.
2. Validate SAP ROOSFIELD, ROOSATTR, DD02L, and delta behavior against the
   target ECC release and the installed custom HTTP endpoint. Verify ABAP class
   traversal if class extractors must be supported.
3. Validate Oracle dictionary queries against the target grants and schemas.
4. Exercise Fabric notebook and pipeline calls in a nonproduction workspace,
   including service principal permissions, `Retry-After`, and exit values.
5. Apply `thirdparty/configdb/migrations/001_runtime_plan_hash.sql` to any
   other existing database missing the column. The configured database already
   has it. Test retry behavior against SQL Server.
6. Run a real model integration test with sanitized source context and confirm
   trace IDs reach SAPAnalysis.PhoenixTraceId. The local Phoenix test used a
   stub model response.
7. Verify the Fabric provisioning notebook and existing replication pipelines
   consume the new schema and only approved runtime records.

## Run checks

```powershell
.\venv\Scripts\python.exe -m unittest discover -s tests -q
.\venv\Scripts\python.exe -m compileall -q thirdparty app tests
```

Set `MIGRATION_ANALYSIS_MODEL` and `OPENAI_API_KEY` to use the model adapter.
Set `PHOENIX_COLLECTOR_ENDPOINT` to export manual metadata spans to Phoenix.
Set `SAP_CA_BUNDLE` to a trusted PEM CA bundle when certificate verification is
enabled. This workspace currently sets `SAP_VERIFY_TLS=false` as requested.
Prompt text, ABAP source, and credentials are not added as span attributes.
