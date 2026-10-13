---
name: sap-to-fabric-odp-design
description: Design SAP ODP DataSource output for Fabric Lakehouse
---

Load `sap-to-fabric-datasource-design` for the shared MCP, datatype, evidence, bounded-retry and delivery rules. Use its ODP mode only.

Design verified extraction-structure output and post-extraction conversions only. SAP ODP owns delta and delete detection; do not ask for base-table keys, watermarks or delete policies. Do not reconstruct the extractor or initialize ODQ.

## ECC and S/4HANA source variants

Support both SAP ECC and SAP S/4HANA. Verify product/release and the actual active extractor implementation through SAP ADT MCP; record the platform evidence in source context. A connection display name is only a binding label, not proof of product/version. If product metadata is unavailable, state that explicitly and verify each required implementation/capability through actual source metadata and code. Do not infer ECC or S/4HANA solely from table names.

Follow the connected system's implementation: ECC accounting/index-table paths, S/4HANA universal-journal/CDS/compatibility paths, or other verified release-specific paths. Do not assume ACDOCA exists on ECC or that BSEG disappears on S/4HANA. Classify transparent tables, database/CDS views, compatibility views and structures before planning physical replication; retain only actual business dependencies and approved business guard inputs. Check Data Elements, Domains, keys, client handling and extractor semantics on that release. Documentation searches must match the verified platform/release; never substitute one platform's generic extractor logic for the other's source code. Preserve this evidence across revisions.

## Extract CDS/database views as sources

For DataSource rebuilds, prefer direct extraction of a connector-readable source CDS/database view into a Fabric Delta table; do not reproduce its internal joins in Spark. This policy supersedes the earlier default of rebuilding every nested CDS view. Retain underlying tables only if extractor business expressions or required guards independently need them. A source view is still `CDS_VIEW` or `DB_VIEW` in ConfigDB `SourceTables.SourceObjectType`; its Fabric target is a table. `SourceTableName` is the actual connector-readable DDIC/SQL object; the original CDS entity and access evidence remain in the approved analysis summary/read contract. No database schema migration is needed for these existing type/name fields.

Verify product/release, source definition, full output schema with Data Elements/Domains, parameterlessness, client semantics, authorization behavior, and actual source read name through SAP MCP. A DDIC-based CDS may have a generated SQL view; a CDS view entity must not be assigned an invented SQL view. Do not assume standard status, release, RFC readability or ODP extraction eligibility. Use the SAP application connector route confirmed by the user. The annotation NOT_REQUIRED alone does not prove there is no DCL; check the actual access-control definition/metadata. A source SQL view must not bypass CDS DCL; require evidence of no relevant CDS DCL or actual connector preservation of it. An ADT SELECT proves source queryability, not that RFC/ODP ingestion ran.

In analysis use the connector read name and `object_type: CDS_VIEW` or `DB_VIEW`. Add `context.materialized_sources` keyed by that name with `source_object_type`, original `entity_name`, `access_method: SAP_APPLICATION_CONNECTOR`, `parameterless: true`, `client`, `client_semantics: SAP_SESSION_CLIENT`, `authorization_behavior` (NO_CDS_DCL or CONNECTOR_PRESERVES_CDS_AUTHORIZATION), `authorization_evidence`, and exact completed read-only `verification_queries`. Remove a source from required dependencies only when its sole purpose is the internal implementation of the materialized view; record the reason.

In plan mode put the materialized source in `graph.tables` with an opaque TABLE node and no internal dependencies. Keep `metadata.source_object_type` and `metadata.materialized_source` identical to the approved contract across every part. Do not also put it in `graph.view_order` or emit a Spark view for it. Generate ExtractSql and column conversions using the actual read name, as for tables, and preserve the native source type in ConfigDB. With RFC, ExtractSql is a projection/conversion recipe for the external consumer; do not claim RFC executes arbitrary HANA scalar SQL. Each materialized source receives column, key, watermark/full-refresh and delete reviews. CDS/DDIC view key declarations do not prove uniqueness; use complete-table MCP aggregate validation for a chosen key, or the approved no-key full-refresh route.

Verify the complete field count through DDIC before emitting an extraction source: count all real active fields excluding technical include markers, page through all fields, and reconcile the emitted count. Do not equate a capped 200-row response with the full schema. Preserve expanded include/append fields and original positions.
