# SAP semantic datatype and conversion decisions

Read the complete [user-approved conversion framework](sap-to-fabric-datatype-conversion.md), including its decision matrix, twelve safeguards and ten SAP-specific recommendations. It supersedes the earlier raw DATS/TIMS and packed-timestamp-as-decimal policy for **new SAP agent proposals**. The checks below protect execution and fidelity; they do not authorize changing existing approved tables automatically.

## Data elements and domains come first, for every field

Apply this priority to every datatype:

1. Data Element.
2. Domain.
3. DDIC documentation.
4. Physical datatype, digit length and scale.
5. Column-name heuristics as a last resort.

Start with the actual `rollname` in the independently verified DDIC snapshot. On the mapped system/client, retrieve the data element's properties and documentation, then its linked domain's definition, fixed values/ranges and documentation through `sap-abap-adt`. Capture domain restrictions, conversion exits, value-table references and currency/unit relationships when available. A data element supplies business meaning; a domain supplies the value representation and constraints. Combine them rather than ignoring domain constraints when the data element is present. Use domain semantics when data-element semantics are missing; DDIC documentation and physical storage are progressively weaker fallbacks.

Cache verified unique elements/domains within this plan, while recording their link to each field. Missing `rollname`, domain or documentation is a limitation to record, not permission to invent metadata. Consult `sap-help`/`abap-docs` for uncertain SAP meaning and `microsoft-learn` for the selected Fabric Spark/Delta runtime. Resolve and document genuine disagreements. Never infer UTC or timestamps from a name or DEC precision alone. Domain fixed values are evidence for Boolean/code meaning, not automatic permission to replace a business status with true/false.

Semantic identifiers default to STRING, even with numeric physical storage. NUMC always stays STRING. Confirmed temporal semantics override ordinary DEC-to-DECIMAL mapping; amounts, quantities, percentages, exchange rates and business measures stay numeric. Preserve the exact physical `DATATYPE(LENGTH,DECIMALS)` in `source_type` even when logical meaning changes the target.

## Complete mapping matrix

| SAP type or confirmed meaning | Fabric Lakehouse target | Extraction/conversion and checks |
|---|---|---|
| DATS, eight-character `YYYYMMDD` | DATE | Parse in native source SQL: `20261012` -> date `2026-10-12`. No timezone conversion. Handle initial/invalid values by explicit policy. |
| TIMS, six-character `HHMMSS` | STRING containing `HH:mm:ss` | `143025` -> `14:30:25`. Keep time-only; no dummy date, fabricated timestamp or timezone shift. STRING is the required time representation in this project; verify the selected runtime rather than adopting TIME from another/newer Spark release. |
| Data Element or Domain TIMESTAMP | TIMESTAMP | Commonly DEC(15,0), format `YYYYMMDDHHMMSS`; `20261012143025` -> `2026-10-12 14:30:25`. Numeric dimensions alone do not establish temporal meaning. |
| Data Element or Domain TIMESTAMPL | TIMESTAMP, subject to precision gate | Commonly DEC(21,7), format `YYYYMMDDHHMMSS.fffffff`. Preserve the entire `.1234567` fraction through source, Parquet and Fabric; do not silently truncate to six digits. |
| Data Element UTC_TIMESTAMP or another documented temporal element/domain | TIMESTAMP, subject to precision gate | Verify the element/domain definition and UTC evidence; a suggestive name is not implementation evidence. |
| DEC(p,s), with ordinary numeric meaning | DECIMAL(p,s) | Preserve DDIC digit precision/scale, not packed bytes/output width. DEC(21,7) remains DECIMAL(21,7) unless semantic metadata proves timestamp/identifier meaning. Check supported precision; never silently round/narrow. |
| CURR(p,s) | DECIMAL(p,s) | CURR(15,2) -> DECIMAL(15,2). Keep currency references/internal values; no unapproved TCURX/display scaling. |
| QUAN(p,s) | DECIMAL(p,s) | QUAN(13,3) -> DECIMAL(13,3). Keep unit references/internal values; no unapproved unit conversion. |
| INT1 | SMALLINT | Preserve unsigned SAP 0–255, which does not fit signed Spark TINYINT. Semantic identifiers instead use STRING. |
| INT2 | SMALLINT | Preserve integer range; semantic identifiers use STRING. |
| INT4 | INT | Preserve integer range; semantic identifiers use STRING. |
| INT8 | BIGINT | Preserve 64-bit values through serialization/Parquet; avoid intermediate JSON floating-point rounding. Semantic identifiers use STRING. |
| FLTP | DOUBLE | Preserve double precision; no FLOAT narrowing or claims of exact decimal arithmetic. A semantic identifier requires an exact string representation and acknowledgment of the current runtime guard. |
| CHAR | STRING | Preserve codes, padding and identifiers. Confirmed Boolean semantics may select BOOLEAN under the flag rules. |
| STRING | STRING | Preserve complete text. |
| SSTRING | STRING | Preserve complete text and length metadata. |
| NUMC | STRING | Preserve leading zeros such as `0000123456`; never numeric, never unapproved ALPHA/display conversion exits. |
| Confirmed Boolean/flag meaning | BOOLEAN | Verify exact domain-specific true/false values and extraction/transport conversion. X, Y, N, 1, 0 and blank are possible values, not a universal truth table. Without confirmed Boolean meaning, use STRING. |
| RAW | BINARY or STRING(Base64) | Choose according to ingestion capability, preserve every byte, record exact encoding/decoding. |
| LRAW | BINARY or STRING(Base64) | As RAW, with complete long-value transport and no truncation. |
| RAWSTRING | BINARY or STRING(Base64) | As RAW; a logical value merely stored as RAW is not automatically binary. |
| Unsupported or ambiguous scalar datatype | Lossless STRING, flagged for review | Use a verified complete serializer and log the fallback. If exact serialization is unavailable, report a capability gap rather than claiming successful ingestion. |

Keep prior compatibility coverage: CLNT/LANG/CUKY/UNIT are STRING identifiers/codes; ACCP stays a YYYYMM period string without inventing a day; LCHR/VARC require complete text transport; legacy PREC needs range verification. DATN/TIMN/UTCLONG need documented native temporal semantics and the same precision/timezone checks. Fixed decimal-floating variants may use exact DECIMAL(p,s) after checking actual storage scale. Exponent/RAW/SCL decimal-floating families need exact STRING when outside Spark DECIMAL's domain; they are not automatically BINARY. Deep/reference/table-valued ABAP objects need an appropriate supported serializer/route rather than an invented scalar cast.

## UTC evidence and clock-value fidelity

UTC requires data-element documentation, domain documentation, relevant SAP documentation, explicit business-owner confirmation, or evidence that the **actual writer of this field** uses `GET TIME STAMP FIELD`. Unrelated ABAP code is not proof. Record the evidence and field linkage. TIMESTAMP/TIMESTAMPL/DEC storage types alone never prove UTC.

Without UTC confirmation, load the clock value exactly as stored and perform no timezone conversion. DATE and time-only STRING never need timezone conversion. Even for proven UTC, do not shift values merely because conversion is permitted; document any necessary operation and prevent double conversion.

Verify `spark.sql.timestampType`, session timezone, connector behavior and Parquet timestamp annotations. Ordinary Spark TIMESTAMP can have instant/session-timezone semantics: unknown local clock values must not acquire an invented timezone. The app does not currently accept TIMESTAMP_NTZ. If preserving timezone-free meaning requires NTZ support, report that gap rather than treating the value as UTC.

## Initial/invalid values and fractional precision

The user's confirmed policy overrides the earlier open initial-value question:

- DATS exactly `00000000` -> SQL NULL; valid dates still map to DATE.
- TIMS exactly `000000` -> STRING `00:00:00` (midnight), never NULL. This supersedes the earlier tentative request to NULL zero times.

Apply these decisions without asking again. Record the deliberate DATS sentinel-to-NULL conversion and preserve nullability; flag a genuine key conflict if this conversion would create NULL in a required replication key. Profile/count conversions for the external ingestion audit, without emitting source rows; do not claim audit execution before the pipeline implements it. Existing SQL NULL remains NULL.

Blanks, other malformed dates/times and zero packed timestamps are not covered by these two approvals. Use bounded aggregate profiles to establish whether a material issue exists, then obtain only the missing policy if necessary. Sampling does not prove all/future values. Do not silently treat every invalid value as NULL, invent a replacement date, or revert valid DATS targets to STRING. Keep long-timestamp precision and timezone questions separate from the now-resolved zero date/time policy.

The document requires TIMESTAMPL -> TIMESTAMP and exact precision, but Spark 3.5 retains only six fractional digits. For this implementation the selected lossless fallback is STRING using the closed LONG_TIMESTAMP template, preserving the compact packed value and all seven fractional digits. Record its temporal Data Element/Domain semantics and the fallback rationale in notes. Do not ask again or truncate to Spark TIMESTAMP. Short timestamps without this precision conflict still map to TIMESTAMP. No companion column is needed; arbitrary NTZ or companion schema extensions remain outside the contract.

Binary conversion requires verified full-length encoding/decoding. Casting Base64 text to Spark BINARY yields encoded text bytes, not the original payload; use STRING(Base64) when no verified decoder exists. Unknown datatype string fallbacks must preserve exponent, special values, padding and full text rather than arbitrary lossy casts.

## Per-column decision audit and conversion ownership

For **each column**, put the chosen metadata level, data element, domain, documentation/source tool, physical type/length/scale, business meaning, target type, exact extraction expression, timezone evidence/status, initial/invalid policy, precision/encoding/loader requirements and profile scope/fallback rationale in `transformation_notes`. Add references to `evidence` and limitations to `warnings`. These existing fields are retained in the agent plan/ConfigDB; do not add unsupported JSON keys. Original DDIC field text alone supplies `description`.

The target schema, source extraction representation, staging Parquet type and notebook conversion must agree. Propose required conversions in native source `SourceExpression`, not Spark functions pasted into HANA SQL or just a different DDL type. Watermark predicates retain the original SAP column/type/native bounds: converting a DATS projection to DATE does not change its source watermark representation.

Read [conversion examples and capability gaps](extraction-conversions.md). Never claim a plan is executable when its required converter is unsupported. Routine mapping decisions are autonomous; genuine missing semantics, fidelity decisions or execution capabilities need intervention. Later UI column/key/watermark/delete approvals remain separate.

Implementation references:

- [Fabric runtime 1.3 / Spark 3.5](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-1-3).
- [Spark 3.5 datetime precision](https://spark.apache.org/docs/3.5.3/sql-ref-datetime-pattern.html).
- [Spark 3.5 timestamp without timezone](https://spark.apache.org/docs/3.5.9/api/scala/org/apache/spark/sql/types/TimestampNTZType.html).
- [SAP Dictionary datatype definitions](https://help.sap.com/doc/saphelp_snc70/7.0/en-US/cf/21f2e5446011d189700000e8322d00/content.htm).
