# Business-specific Oracle BLOB policy

Apply this user-approved policy to table design and every base table/view in a
migration bundle. It supersedes older generic BLOB mapping advice. A BLOB type
indicates binary storage, not the content's business meaning.

## Classify without reading attachment payloads

Use comments, verified business documentation, application context, related
filename/MIME/attachment metadata, constraints and applicable approved memory.
Documents, attachments, images, reports, archives and media are attachment BLOBs.
Confirmed hashes, signatures, binary IDs or small binary attributes may use the
separate exact-hex rule below. Small size alone does not prove non-attachment.
Unknown content is a material classification question: do not assume it is a
hash, inline it or discard it. Ask only for that missing business meaning.
Do not request permission again for the already-approved attachment strategy.
Use bounded read-only length aggregates if necessary; do not retrieve bytes,
sample file contents or expose payloads in reports/MCP output.

## Attachment mapping and extraction

Keep the original source column identity/type/position in the plan. Replace its
target representation with one nullable STRING column named
`<SOURCE_COLUMN>_FILE_PATH` (or an explicitly approved naming convention).
Validate names and collisions; never invent extra metadata columns under the
app's one-source-column/one-target-column contract.

Example full Oracle extraction projection:

```sql
SELECT ORDER_ID,
       CAST(NULL AS VARCHAR2(4000)) AS ATTACHMENT_BLOB_FILE_PATH
FROM ONT.ORDERS
```

`NULL AS ATTACHMENT_BLOB_FILE_PATH` is the policy's logical projection; a typed
NULL gives the connector a predictable string type. For the existing JSON
contract, put ONLY the expression `CAST(NULL AS VARCHAR2(4000))` in
`source_expression`, not SELECT or AS; the mapping provides the target alias.
Use `name=ATTACHMENT_BLOB`, `source_type=BLOB`,
`target_name=ATTACHMENT_BLOB_FILE_PATH`, `fabric_type=STRING`, `nullable=true`.
Preserve the original source NOT NULL constraint in transformation notes if
present: the deferred path is intentionally nullable even when the BLOB is not.

The standard extraction query must enumerate structured columns explicitly,
excluding attachment BLOB payloads. No SELECT *, direct BLOB projection,
RAWTOHEX(attachment_blob), TO_CHAR(attachment_blob), or substring bytes.
Do not generate fictitious paths or use empty strings instead of NULL. Record
classification evidence, the original/target names, the intentional content
deferral and the separate pipeline dependency in existing notes/warnings.

Required migration note (paraphrase as appropriate): the BLOB is attachment
storage; the main data pipeline does not migrate its bytes; a dedicated
attachment process extracts files, uploads them to OneLake/ADLS and populates
the path only after successful upload. This skill designs the handoff; it does
not implement or run that process, and provisioning success does not mean the
attachment bytes were migrated.

Specify the stable record key plus source column identity for that handoff.
Paths must correlate to the correct record; filenames, retries, checksums and
upload/update behavior are responsibilities of the dedicated process. If no
stable correlation key is available, record that unresolved dependency.
Scheduled FULL/REPLACE loads can reset paths to NULL; explicitly flag the need
for the external attachment process to repopulate them after each refresh or
for the external loader to preserve/enrich them using an agreed mechanism.
Do not claim that either mechanism already exists.

## Confirmed non-attachment BLOBs

Exact hex STRING is permitted only when business evidence confirms the binary
attribute meaning and a complete extraction fits the actual Oracle SQL and
connector limits. Preserve the original target name unless otherwise approved.
`RAWTOHEX(BLOB_column)` is NOT a valid direct Oracle SQL conversion. Verify using
oracle-db-doc; use BLOB-to-RAW extraction followed by RAWTOHEX, e.g.
`RAWTOHEX(DBMS_LOB.SUBSTR(col, verified_byte_limit, 1))` ONLY when the entire value
is proven to fit that limit and twice its byte length fits the SQL VARCHAR2
limit. A sample maximum is not a guarantee: require a constraint/domain guarantee
or full-scope fit check plus a runtime guard for subsequent loads. Do not silently
truncate an oversized value, chunk only the first bytes, or confuse empty BLOBs
with NULL. If complete exact extraction is unsupported, report the blocker.
Never apply this exception to attachment BLOBs.

References: [Oracle RAWTOHEX](https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/RAWTOHEX.html),
[Oracle DBMS_LOB](https://docs.oracle.com/en/database/oracle/oracle-database/19/arpls/DBMS_LOB.html).

## Views and Spark SQL

Apply the same classification once per base BLOB and trace it through nested
views and aliases. Generate Lakehouse Spark SQL; verify target capabilities with
microsoft-learn and Oracle source behavior with oracle-db-doc.
For a direct BLOB projection, refer to the mapped base `<COLUMN>_FILE_PATH`.
Under the app's current verified-view-output contract, retain the original view
output alias and document that it now represents a STRING file path, not bytes.
Do not rename verified view outputs unless the contract and reviewer authorize
that separately. Before attachment migration, the path value is NULL.

Do not translate BLOB-content functions, byte lengths, hashes, signatures,
binary comparisons, joins or filters into operations on path text. In particular,
`BLOB IS NOT NULL` does not mean `FILE_PATH IS NOT NULL`: every path is initially
NULL, including records with source attachments. Use an existing independently
verified structured indicator if it preserves the original condition; otherwise
flag a material translation blocker rather than dropping filters or changing
results. Do not invent extra indicator columns outside the current contract.
Clearly distinguish this approved representation change from SQL equivalence.
SQL-only notebook repairs cannot retroactively change BLOB mappings or schemas.
