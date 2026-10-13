# Approved Oracle extraction SELECT

New Oracle table plans and Oracle view dependency tables save `SourceTables.ExtractSql` in the same transaction as the source columns and replication settings. `vw_ReplicationQueue` exposes it. It is a complete Oracle SELECT with explicit columns, approved source expressions, quoted target aliases and one schema-qualified source table. It has no watermark WHERE clause, comments or semicolon. The application renders expressions from the approved plan; it does not guess new datatype conversions while rendering SQL.

`SourceTableColumns.SourceDataType` describes the original Oracle type, including declared precision/scale where applicable; `FabricDataType` is the destination type. `SourceExpression` is the approved extraction conversion, or NULL for direct native transport. These meanings are different and must not be compared interchangeably. Timestamp session requirements and separately deferred attachments still apply; saved SQL is not proof that the connector preserves values.

The separately maintained scheduled pipeline must:

1. Refuse extraction when `ExtractSql` is NULL/empty. Existing configurations are not silently backfilled or reapproved by this release.
2. Before every extraction, read all current Oracle columns from source metadata and compare names, declared datatypes, precision and scale with the stored source definitions. Check new and missing columns too, including columns absent from the approved projection. Compare semantic datatype definitions rather than raw spelling (`NUMBER` versus `NUMBER(p,s)`, timestamp precision, etc.). Do not compare Oracle types to Fabric target types.
3. Raise a schema-drift issue and stop that table's extraction/load on a mismatch. Record the table GUID, configuration/plan identity and changed metadata; do not log credentials or row values. Do not regenerate mappings, silently accept new columns or advance the checkpoint.
4. Execute the saved SELECT using the approved connector/session settings. Add watermark predicates against the original source watermark column at the source SELECT, using typed bounds and the pipeline's existing boundary/replay rules. Do not filter a formatted projection alias or concatenate unescaped bound values. Full loads execute the same SELECT without that predicate.
5. Stage to isolated Parquet batch paths and verify complete extraction before loading. Conversion/extraction failures must remain visible, never silently become NULL or truncated values.
6. Merge using the approved replication configuration, and advance the watermark only after successful target commit. Retain failed staging for diagnosis/retry.

Source metadata comparison does not prove all values fit their mapped type. Continue to enforce conversion errors and target validation. Adding a column may require historical backfill; watermark deltas alone do not populate unchanged records.

Approval and persistence validate that `ExtractSql` exactly matches the source column expressions and aliases. New target-change proposals carry the regenerated SELECT; notebook metadata changes update it atomically with the columns before resuming ingestion. Legacy changes without a SELECT clear the field to prevent reuse of stale SQL. View SQL repairs preserve approved extraction SQL and replication choices.

This release does not modify the scheduled extraction pipeline or implement its drift issue reporting. The checks above are its integration contract.
