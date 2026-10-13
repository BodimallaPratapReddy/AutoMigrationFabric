# Oracle to Fabric Lakehouse Datatype Mappings

Use these as defaults for a Fabric Lakehouse Delta table. Confirm destination capabilities if the user requests Fabric Warehouse because its SQL types differ.

## Character Types

| Oracle source | Fabric Lakehouse | Extraction and notes |
|---|---|---|
| `CHAR`, `VARCHAR`, `VARCHAR2`, `NCHAR`, `NVARCHAR2` | `STRING` | Direct. Preserve Unicode. Profile Oracle BYTE versus CHAR length semantics when truncation is possible downstream. |
| `CLOB`, `NCLOB` | `STRING` | Direct when payload sizes fit the ingestion path. Profile with `DBMS_LOB.GETLENGTH`; large values can exceed gateway or batch payload limits. |
| `LONG` | `STRING` | Legacy type. Test the connector and extract separately if required. |

## Numeric Types

| Oracle source | Fabric Lakehouse | Extraction and notes |
|---|---|---|
| `NUMBER(p,0)` with `p <= 9` | `INT` | Use `BIGINT` instead if observed or contractual values exceed the signed 32-bit range. |
| `NUMBER(p,0)` with `10 <= p <= 18` | `BIGINT` | Validate signed 64-bit range. |
| `NUMBER(p,0)` with `19 <= p <= 38` | `DECIMAL(p,0)` | Exact. |
| `NUMBER(p,s)` with `0 <= s <= p <= 38` | `DECIMAL(p,s)` | Exact within Fabric decimal limits. |
| `NUMBER(p,s)` with `s < 0` | `DECIMAL(min(38,p-s),0)` | Oracle rounds to positions left of the decimal. Validate resulting precision; use `STRING` if required precision exceeds 38. |
| `NUMBER(p,s)` with `s > 38` | `STRING` | Preserve with `TO_CHAR(column, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')`. Use a derived `DOUBLE` only for approximate analytics. |
| Unqualified `NUMBER` or missing precision/scale reported as zero placeholders | Autonomous profile and business-role decision | Follow [NUMBER profiling](./number-profiling.md). Identifiers/categories become canonical `STRING`; counts/measures become an exact supported numeric type based on fractions, range and precision. Record sample limitations; no routine user question. |
| `INTEGER`, `SMALLINT` | `DECIMAL(38,0)` initially | These are Oracle `NUMBER` subtypes. Narrow to `INT`/`BIGINT` only from constraints and range evidence. |
| `FLOAT(p)` | `DOUBLE` or `DECIMAL` | Oracle `p` is binary precision. `DOUBLE` can be lossy; use canonical `STRING` when exact source representation is required. |
| `BINARY_FLOAT` | `FLOAT` | Approximate numeric. Test NaN and infinity handling. |
| `BINARY_DOUBLE` | `DOUBLE` | Approximate numeric. Test NaN and infinity handling. |

Fabric decimal precision is capped at 38. Never propose `DECIMAL` with precision or scale above 38.

## Date, Time, and Interval Types

Apply [datetime semantics](./datetime-semantics.md) before using this table. The current app allows one target column per source column; expanded timezone-aware designs must use the documented exact fallback or remain a blocker.

| Oracle source | Fabric Lakehouse | Extraction and notes |
|---|---|---|
| `DATE` | `TIMESTAMP` | Preserve wall-clock fields including time to seconds; pinned UTC runtime, no clock shift or inferred source timezone. |
| `TIMESTAMP`, `TIMESTAMP(0..6)` | `TIMESTAMP` | Preserve wall-clock fields under pinned UTC evaluation; verify connector precision. |
| `TIMESTAMP(7..9)` | `STRING` for exact bronze preservation | Use `TO_CHAR(column, 'YYYY-MM-DD"T"HH24:MI:SS.FF9')`. A derived timestamp may be rounded or truncated for analytics. |
| `TIMESTAMP WITH TIME ZONE` | Exact canonical `STRING` in the current app | Preferred expanded design is UTC timestamp plus offset/region, but multi-column extraction is unsupported by the current plan contract. Preserve precision, offset and named region when present. |
| `TIMESTAMP WITH LOCAL TIME ZONE` | UTC `TIMESTAMP` | Original zone is not stored. Set the extraction session timezone explicitly and convert consistently to UTC. |
| `INTERVAL YEAR TO MONTH` | `STRING` | Preserve with `TO_CHAR`; optionally derive total months. |
| `INTERVAL DAY TO SECOND` | `STRING` | Preserve with `TO_CHAR`; optionally derive a numeric duration at an agreed unit and precision. |

## Binary and LOB Types

For BLOBs, [business-specific BLOB handling](./blob-handling.md) takes precedence over generic preservation defaults. Classify content from evidence before selecting an extraction strategy.

| Oracle source | Fabric Lakehouse | Extraction and notes |
|---|---|---|
| `RAW(n)` | `STRING` | Use `RAWTOHEX(column)`. Output length is `2n` characters and is exactly reversible with `HEXTORAW`. |
| Confirmed small binary attribute/hash/signature in `BLOB` | Exact hex `STRING` | Verify business meaning and complete BLOB-to-RAW extraction before RAWTOHEX; never directly call RAWTOHEX(BLOB) or truncate values. |
| Attachment/document/media `BLOB` | One nullable `<COLUMN>_FILE_PATH STRING` | Main extraction uses typed NULL and excludes bytes. Separate attachment process uploads files to OneLake/ADLS and populates the path. |
| Unneeded `BLOB` | Omit only with approval | Record the business owner and retention decision. |
| `BFILE` | File migration plus metadata | Oracle stores an external locator, not the file bytes. Verify source file access separately. |

The current plan accepts one target column per source column: do not add byte-length, checksum, MIME or filename columns for a BLOB unless an expanded contract is separately approved. Preserve independently existing structured metadata columns.

## Other Types

| Oracle source | Fabric Lakehouse | Extraction and notes |
|---|---|---|
| `ROWID`, `UROWID` | `STRING` | Treat as technical lineage only; do not use as a durable business key. |
| `XMLTYPE` | `STRING` | Serialize deterministically; profile size and encoding. |
| `JSON` or JSON text | `STRING` | Preserve valid JSON text and optionally parse into curated structures. |
| Object, collection, spatial, or user-defined type | Case-specific | Request type DDL and usage. Serialize or normalize only after understanding semantics. |

## Profiling Queries

Replace identifiers with validated Oracle identifiers; do not interpolate untrusted text into executable SQL.

```sql
SELECT
    COUNT(*) AS row_count,
    COUNT(column_name) AS non_null_count,
    COUNT(DISTINCT column_name) AS distinct_count,
    MIN(column_name) AS min_value,
    MAX(column_name) AS max_value
FROM owner.table_name;
```

```sql
SELECT
    MIN(DBMS_LOB.GETLENGTH(lob_column)) AS min_length,
    MAX(DBMS_LOB.GETLENGTH(lob_column)) AS max_length,
    AVG(DBMS_LOB.GETLENGTH(lob_column)) AS avg_length
FROM owner.table_name
WHERE lob_column IS NOT NULL;
```

```sql
SELECT
    MAX(LENGTH(character_column)) AS max_characters,
    MAX(LENGTHB(character_column)) AS max_bytes
FROM owner.table_name;
```
