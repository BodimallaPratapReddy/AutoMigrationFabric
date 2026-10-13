# SAP DataSource migration through OpenCode and MCP

New SAP table, view, ODP and rebuild requests require OpenCode. The agent discovers source metadata and code through `sap-abap-adt`, with `sap-help`, `abap-docs` and `microsoft-learn` for documentation. New paths do not call custom SAP client adapters or scraper endpoints. Legacy workflow types remain registered for historical replay.

Install dedicated agents `sap-odp-to-fabric` and `sap-datasource-rebuild-to-fabric`, their mode skills `sap-to-fabric-odp-design` and `sap-to-fabric-datasource-rebuild-design`, the shared `sap-to-fabric-datasource-design`, and the complete table/view-design skills and datatype references. Configure `OPENCODE_SAP_SOURCE_CONNECTIONS` to map the app connection to its MCP destination. `OPENCODE_SAP_ODP_AGENT` and `OPENCODE_SAP_REBUILD_AGENT` override their respective agent names; the old shared `OPENCODE_SAP_DATASOURCE_AGENT` setting is no longer used for new sessions. Metadata SQL and aggregate checks need an MCP capability that permits read-only queries. Credentials and TLS configuration stay in the MCP service.

Agents consult SAP Help and ABAP documentation when blocked and retain a blocker ledger across revisions. After five unsuccessful retries on a blocker, they return `SAP_AGENT_BLOCKED` with attempted actions, references and the required resolution. New MCP SAP workflows also stop after an initial rejected proposal plus five rejected correction attempts and interrupt the agent session. Accepted multipart delivery and polling do not consume correction retries. Existing Temporal histories retain their command sequence through a version marker.

The agent returns strict typed proposals containing its source metadata and plan. The app checks structural consistency, datatype conversion rules, mapped destination and completed MCP call provenance. These checks are not an independent SAP lookup or proof of data equivalence. Data Element and Domain semantics guide every output/base field. Preserve original DDIC positions, including gaps after excluding hidden fields.

## ODP

`POST /migrations/sap-odp` starts `OpenCodeSAPODPMigrationWorkflow`. Discover the actual exposed output fields and ODP capability. Review column mappings and the final schema only. No primary-key, watermark, delta or delete questions are asked: SAP ODP owns change detection and record-mode semantics.

ConfigDB stores one SAP_DATASOURCE/ODP object, `WriteStrategy=ODP_MANAGED`, NULL ExtractSql, and versioned `ODP_POST_EXTRACT` SourceExpression contracts for conversions. These JSON contracts are not SQL. `thirdparty.sap.odp_conversion.decode_odp_row` applies supported typed conversions to raw ODP output. DATS `00000000` becomes NULL; TIMS `000000` remains midnight; long timestamps retain all seven fractional digits as STRING. Other invalid temporal values and lossy packed timestamp floats reject.

The notebook provisions the schema with column comments. Ingestion remains disabled until the external ODP consumer supports these conversion contracts and SAP record modes, ordering, subscriptions and checkpoints. This workflow does not run an extractor, initialize ODQ or execute the external pipeline. There is no new database schema migration for these existing configuration fields.

## Rebuild

`POST /migrations/sap-rebuild` starts `OpenCodeSAPDataSourceRebuildWorkflow`. The same OpenCode session first analyzes the DataSource, extractor source and required tables/views/lookups/configuration. Missing business logic blocks approval. The UI reviews that analysis before planning.

After analysis approval, the agent plans all required base tables and Spark SQL views. Every base table receives the table migration mapping, key, watermark/full-refresh and delete handling reviews. Declared unconverted DDIC primary keys can be validated through enforced constraints; custom/converted keys require complete-table MCP aggregate checks, never sample-based uniqueness claims. The final bundle is approved before persistence and notebook provisioning.

Base tables store explicit native ExtractSql projections without watermark predicates; view SQL uses Spark SQL against the approved Lakehouse targets. External pipelines own source drift checks, typed bounds, client filtering, staging and loading. View-only notebook SQL corrections return to the same agent session without changing approved table contracts, and require another review. Cancellation interrupts the OpenCode session.

## Live ODP validation, 12 October 2026

`0COMP_CODE_ATTR` on SAP ECC Local completed through UI workflow `sap-odp-da2849bd-9a57-4a37-b692-4d60bbf7e0a0`, plan `6a97a9c5-e9d2-59ef-aeff-d10943292e7c`, OpenCode session `ses_fc55268546183973f685d9125c`. MCP discovery identified seven exposed fields and their Data Element/Domain links. Hidden MANDT was excluded while original DDIC positions 2–8 were preserved. Column and final schema approvals were the only review gates.

Notebook job `98a62581-ee0c-4dc2-b17a-33132aeb0a3f` created and validated `SAP_DATASOURCE_TEST.DS_0COMP_CODE_ATTR`. ConfigDB contains seven descriptions, NULL ExtractSql, ODP_MANAGED, no watermark/delete policy, and disabled ingestion. This validates schema migration, not ODQ consumption or data loading.

Transient ConfigDB/Fabric network timeouts required Temporal resets before provisioning and then before polling the existing job. The final run is `5feb0825-2815-47f2-b888-ebc73473aedb`; the existing notebook job was not resubmitted. Safe ODP reads and guarded status transitions now use a 90-second timeout and bounded retries. Submission retains its original non-retrying behavior.

The `0FI_GL_4` rebuild test (`sap-rebuild-4c05efa8-dd82-416a-b453-8c0081eb644b`) remains at agent revision. MCP retrieved actual extractor source and identified BKPF, BSEG, BWFI_AEDAT, BKPF_AEDAT, T001 and X001 dependencies, but subsequent DD04L queries returned HTTP 500/400. Missing Data Element/Domain metadata and uninspected conditional/helper code block analysis approval. No rebuild runtime configuration or notebook job was submitted. This is not a completed live rebuild validation.
