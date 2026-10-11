---
name: oracle-to-fabric-table-design
description: 'Create an Oracle-to-Fabric Migration Report from an Oracle owner and table name. Use MCP SQLcl to retrieve source columns, datatypes, constraints, indexes, comments, and other Oracle metadata, then produce schema mappings, eTRM-grounded business meanings, executed-query rationale, web references, migration risks, and proposed Fabric DDL.'
---

# Oracle to Fabric Table Design

Produce an evidence-based migration design for one Oracle table. Default to a Fabric Lakehouse Delta table unless the user explicitly requests Fabric Warehouse.

## Required Input

The only required user input is:

- Oracle owner and table name

Retrieve the remaining source metadata through MCP SQLcl. At minimum, collect:

- Every Oracle column ID, column name, and complete Oracle datatype, including length, character semantics, precision, scale, fractional precision, default expression, and nullability
- Primary key, unique, foreign key, and check constraints, including ordered columns and referenced objects
- Unique and non-unique indexes, including uniqueness, ordered columns, sort direction, and function-based expressions
- Table and column comments
- Partitioning metadata and available table statistics, when relevant to the Fabric design

Useful optional context:

- Oracle E-Business Suite release and module
- Column comments, constraints, row count, and representative value/length profiles
- Whether exact source representation or analytical usability has priority
- Fabric destination: Lakehouse or Warehouse

Do not ask the user to manually provide columns, datatypes, constraints, indexes, or comments when MCP SQLcl can read them. Ask only for connection selection, access resolution, or other information that cannot be discovered safely from the database. If optional context is absent, proceed and label assumptions.

## Oracle Database Enrichment with MCP SQLcl

Given an Oracle owner and table name, use MCP SQLcl to build the complete source definition before designing the Fabric table. User-supplied DDL or metadata may supplement this process but is not required.

1. Use `connections_list` to discover configured connections. Reuse an appropriate existing connection; use `connect` only when needed. Never request, reveal, or embed database credentials in skill output.
2. Confirm the target owner and table before querying. If several connections, schemas, or similarly named objects are plausible, ask the user to select the target rather than guessing.
3. Verify that the exact `OWNER.TABLE_NAME` exists and identify its object type. Do not silently substitute a synonym, view, or similarly named object.
4. Prefer `schema_information` for object structure and data dictionary metadata. Use `sql_run` for focused, read-only queries when schema inspection is insufficient.
5. Retrieve the required metadata from appropriate `ALL_*` data dictionary views, such as `ALL_TAB_COLUMNS`, `ALL_COL_COMMENTS`, `ALL_CONSTRAINTS`, `ALL_CONS_COLUMNS`, `ALL_INDEXES`, `ALL_IND_COLUMNS`, `ALL_IND_EXPRESSIONS`, `ALL_PART_TABLES`, and `ALL_TAB_STATISTICS`. Use `DBA_*` views only when already authorized and necessary.
6. Query only what is necessary for the design. Filter every dictionary query by the normalized owner and table name, preserving quoted identifier case when applicable.
7. Restrict SQL to `SELECT` statements against the target object and Oracle data dictionary views. Do not run DDL, DML, PL/SQL with side effects, grants, statistics gathering, or broad row-level extracts.
8. Treat returned database metadata as `Verified from Oracle DB` and record the queried owner/table and relevant query or dictionary view. Database metadata can verify physical definitions and comments, but it does not replace eTRM or Oracle documentation for product-level business meaning.
9. Maintain a query ledger as SQL is executed. Record each exact read-only SQL statement, in execution order, and a concise explanation of why it was needed. Redact connection details and sensitive literal values.
10. If MCP SQLcl is unavailable, no suitable connection exists, access is denied, or a query cannot safely disambiguate the target, state the limitation and ask only for the specific connection choice or missing metadata needed to continue.

## Procedure

For every BLOB column, read [business-specific BLOB handling](./references/blob-handling.md). Attachment content is excluded from standard extraction and represented by one nullable STRING file-path column populated with a typed NULL until a separate attachment process uploads files. Preserve source identity, document classification and intentional deferral, and apply the confirmed non-attachment exception only with evidence.

The app saves a complete Oracle extraction SELECT from the approved column proposals. Supply `source_expression` for every required transport conversion, with explicit numeric/date formats and the existing NUMBER, datetime, BLOB and RAW policies. NULL means direct native extraction; do not rely on the pipeline to rediscover types or invent conversions. Expressions must be scalar Oracle expressions using exact unqualified source column names, without aliases, SELECT/FROM/WHERE clauses, comments or semicolons. The app adds quoting, column order and target aliases. Preserve original Oracle datatype definitions and declared precision/scale in source metadata; target datatypes are separate. Record connector/session requirements and evidence of lossless transport. These rules also apply to base tables in view bundles.

1. Parse and normalize the supplied owner and table name without changing quoted identifiers.
2. Use the MCP SQLcl enrichment workflow above to verify the object and retrieve its complete source metadata. Continue only after every source column has a datatype and the available constraints and indexes have been collected, or explicitly report why some metadata is inaccessible.
3. Read [datatype mappings](./references/datatype-mappings.md). Apply its rules column by column. When NUMBER precision/scale are unspecified or reported as zero placeholders, use [autonomous NUMBER profiling](./references/number-profiling.md): profile the column, infer its business role from evidence, and propose STRING for identifiers/categories or an appropriate exact numeric type for measures. Do not ask the user to choose routine numeric types. Preserve valid declared precision/scale as source evidence; this profiling policy does not override a declared NUMBER(p,s).
4. Research the table and columns in Oracle eTRM using web search. Search exact identifiers first, for example:
   - `site:etrm.live "<TABLE_NAME>" "<COLUMN_NAME>"`
   - `site:etrm.live "<TABLE_NAME>" Oracle eTRM`
   - `site:docs.oracle.com "<TABLE_NAME>" "<COLUMN_NAME>"`
5. Open the relevant result and verify the exact table, column, application/module, and EBS release before using it. Prefer the user's stated release. Never transfer a definition from a similarly named table or another release without labeling the mismatch.
6. Derive each business meaning from verified eTRM text, supplied documentation, database comments, constraints, and naming context, in that order. Distinguish sourced facts from interpretation:
   - `Verified`: directly supported by an opened source; include its URL.
   - `Verified from Oracle DB`: directly supported by MCP SQLcl metadata or a focused read-only query; identify the owner/table and dictionary view or query used.
   - `Inferred`: a restrained interpretation based on name, datatype, or relationships; state the basis.
   - `Unknown`: no reliable evidence found; do not invent a definition.
7. Preserve source column names by default. Propose a renamed Fabric column only to satisfy Fabric naming rules, resolve collisions, or represent a deliberate transformation such as a BLOB file path. Show both names whenever they differ.
8. Evaluate Oracle indexes by purpose. Primary/unique indexes describe integrity and business keys, but Lakehouse indexes do not migrate one-for-one. Use query evidence before recommending partitioning, V-Order, Z-Order/OPTIMIZE, or Warehouse indexes. Do not recommend partitioning on a high-cardinality identifier solely because it is indexed.
9. For ambiguous or risky columns, use MCP SQLcl to run focused read-only aggregate profiling. For unqualified NUMBER, bounded profiling is required before making the routine mapping decision; follow the linked NUMBER policy and record query scope, evidence, mapping rationale and assumptions. Typical other checks include null count, distinct count, `DBMS_LOB.GETLENGTH`, timezone behavior, and maximum character length.
10. Render the result as a `Migration Report` using [the output template](./references/output-template.md). Preserve the required section order. Include proposed DDL only when the destination engine is known.
11. Run a consistency pass: every source column appears exactly once and in Oracle column ID order, mapped precision is supported, nullability is not silently weakened (deferred attachment paths are explicitly nullable with original constraints recorded), renamed columns are traceable, attachment bytes are excluded from standard extraction, index columns exist, every `Verified` business meaning has a source URL, every `Verified from Oracle DB` claim identifies its database evidence, every executed source query appears in the query ledger, and every listed web link was opened and used.

## Evidence Rules

For every DATE or TIMESTAMP variant, read and apply [datetime semantics](./references/datetime-semantics.md). Plain DATE/TIMESTAMP use the authorized Pinned UTC Wall-Clock convention without routine timezone questions. Preserve time and fractional precision. Timezone-aware types use the separate rules and current one-column plan limitation in that reference.

- Web search snippets are discovery aids, not sufficient evidence. Open the page before citing it.
- Cite the exact eTRM or Oracle documentation URL and state the release when visible.
- Do not claim that a column is a foreign key only because its name ends in `_ID`; label that as inferred unless a constraint or source confirms it.
- Do not expose credentials, connection strings, or private values in queries or output.
- Minimize database output. Prefer data dictionary metadata and aggregate profiles; do not include representative row values unless the user explicitly requests them and they are necessary.
- If eTRM is unavailable, continue with supplied metadata, mark meanings `Inferred` or `Unknown`, and list the failed lookup as a limitation.

## Output Quality Bar

The proposal must be actionable but must not hide uncertainty. Preserve exact values in the bronze layer; suggest analyst-friendly derived columns in a silver layer when preservation and usability conflict. Flag lossy conversions explicitly.
