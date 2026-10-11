# Migration Report

## Header

| Report field | Value |
|---|---|
| Report name | `Oracle to Microsoft Fabric Migration Report` |
| Generated on | `<YYYY-MM-DD>` |
| Source system | `Oracle Database` |
| Target platform | `<Fabric Lakehouse Delta or Fabric Warehouse>` |
| Oracle application/release | `<value or Unknown>` |
| Evidence status | `<Complete or Partial, with a concise reason>` |

## Input Details

| Input | Value |
|---|---|
| Owner Name | `<OWNER>` |
| Table Name | `<TABLE_NAME>` |

State the naming policy, key assumptions, and inaccessible metadata immediately below this table. Do not omit the report because optional context is unknown.

## Oracle to Fabric Column Mapping

| Oracle Column ID | Column Name | Oracle Data Type | Proposed Fabric Data Type | Reason for Proposed Fabric Data Type | Column Business Meaning Derived from eTRM Documentation |
|---:|---|---|---|---|---|
| `<COLUMN_ID>` | `<SOURCE_COLUMN>` | `<complete declared Oracle type>` | `<Fabric type>` | `<mapping rationale, precision/length handling, transformation, and risk>` | `<meaning plus Verified/Inferred/Unknown status and inline source link or basis>` |

List every source column exactly once in Oracle column ID order. Preserve the Oracle column name unless a Fabric restriction or deliberate transformation requires a rename; describe any target rename in the datatype reason. The business meaning must distinguish `Verified`, `Verified from Oracle DB`, `Inferred`, and `Unknown`. A `Verified` eTRM meaning must include an inline link to the opened page. Do not present an inferred meaning as eTRM-derived.

## Proposed SQL to Create Table in Fabric

Include executable, engine-appropriate SQL only after confirming Lakehouse versus Warehouse. Fully qualify the target where appropriate. Add SQL comments for lossy conversions, renamed or split columns, and decisions that require confirmation. Do not emit unsupported `DECIMAL` precision/scale or imply that Oracle indexes are automatically recreated.

If the destination engine is unknown, state that SQL generation is blocked pending that decision and show no speculative DDL.

## List of Queries Executed on Source Oracle and Why Each Query Was Used

List every query actually executed against Oracle in execution order. Do not list suggested or unexecuted queries here.

| # | Query or Dictionary Operation | Why It Was Used |
|---:|---|---|
| 1 | `<exact SELECT statement, schema_information operation, or dictionary views accessed>` | `<metadata or profile evidence obtained and why it was required>` |

Use fenced `sql` blocks below the table when a query is too long for a readable table cell. Reference the matching query number. Redact credentials, connection strings, and sensitive literal values, but retain owner and table filters needed to make the report reproducible. If no query could be executed, state the reason explicitly.

## Links Referred from Web Search

List only pages that were opened and used as evidence. Do not include search result pages.

| # | Page Title | URL | Information Used | Release/Version |
|---:|---|---|---|---|
| 1 | `<title>` | `<direct URL>` | `<table or columns supported>` | `<release/version or Unknown>` |

If no reliable page was available, state that explicitly and identify affected business meanings as `Inferred` or `Unknown`.

## Additional Design Details

Use the following supporting sections after all required report sections.

## Keys and Index Disposition

| Oracle object | Type | Ordered columns/expressions | Source purpose | Fabric treatment | Rationale/validation |
|---|---|---|---|---|---|
| `<INDEX_OR_CONSTRAINT>` | `<PK/UK/index/function-based>` | `<columns>` | `<integrity/access path/Unknown>` | `<retain logically, enforce upstream, partition, optimize, no direct equivalent, investigate>` | `<reason and workload evidence needed>` |

State explicitly that a Lakehouse primary or unique key may be informational rather than physically enforced. Recommend deduplication or data-quality checks when integrity enforcement is absent.

## Validation Plan

Include focused checks for:

- Source/target row counts and null counts
- Key uniqueness and duplicate handling
- Exact numeric comparison or canonical-string comparison
- Timestamp UTC conversion and fractional precision
- Character length and Unicode samples
- For attachment BLOBs: nullable STRING file-path schema, NULL extraction, payload exclusion and separate-pipeline correlation; byte-length/checksum validation belongs to the attachment process. For confirmed non-attachment binary attributes: complete exact extraction and fit checks.
- Query plans/workload before physical optimization recommendations

## Open Decisions

List only material decisions requiring business, data-owner, or platform confirmation, such as unknown BLOB content, BLOB-dependent view semantics that cannot be preserved, a timezone-aware translation that cannot preserve semantics, target engine, and expected query patterns. Attachment BLOB deferral is already approved: record the separate-process requirement in warnings, not as a request to reconfirm the policy. Apply the authorized wall-clock and exact-precision datetime policy without routine timezone questions; unknown descriptions are warnings.
