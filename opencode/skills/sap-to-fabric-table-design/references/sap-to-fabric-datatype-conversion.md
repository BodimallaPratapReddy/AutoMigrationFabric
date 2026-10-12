# SAP to Microsoft Fabric Datatype Conversion Framework
## Agent Skill Guidance (Production Version)

---

# Purpose

This document defines the rules an SAP-to-Microsoft Fabric migration agent must use when converting SAP data types to Fabric Lakehouse data types.

The objective is to:

- Preserve business meaning.
- Avoid data loss.
- Preserve precision.
- Correctly identify timestamps.
- Avoid invalid timezone assumptions.
- Support automated schema generation.

---

# Guiding Principles

## Rule 1: Semantic Metadata Always Wins

The agent must determine target data types using the following priority order:

```text
1. Data Element
2. Domain
3. DDIC Documentation
4. Physical Datatype
5. Column Name Heuristics (Last Resort)
```

### Example

```text
Field Name      : CREATED_AT
Physical Type   : DEC(21,7)
Data Element    : TIMESTAMPL
```

Result:

```text
Fabric TIMESTAMP
```

NOT:

```text
Fabric DECIMAL(21,7)
```

---

## Rule 2: Never Infer Meaning From Physical Type Alone

The following SAP physical datatypes are used for multiple business purposes:

```text
DEC(15,0)
DEC(21,7)
```

These may represent:

```text
Timestamps
Amounts
Quantities
Percentages
Exchange Rates
Business Measures
```

Therefore:

```text
DEC(21,7) ≠ TIMESTAMP
DEC(15,0) ≠ TIMESTAMP
```

unless semantic metadata confirms timestamp semantics.

---

## Rule 3: Preserve Business Meaning

```text
Date         -> Date
Time         -> Time Representation
Timestamp    -> Timestamp
Numeric      -> Numeric
Identifier   -> String
Binary       -> Binary
```

---

## Rule 4: Never Assume UTC

Timestamp datatypes only define storage format.

They do NOT define timezone semantics.

```text
TIMESTAMP     ≠ UTC
TIMESTAMPL    ≠ UTC
DEC(15,0)     ≠ UTC
DEC(21,7)     ≠ UTC
```

UTC can only be assumed when explicitly confirmed.

---

# Date and Time Types

---

# DATS

## SAP Definition

```text
Type   : DATS
Length : 8
Format : YYYYMMDD
```

### Example

```text
20261012
```

Meaning:

```text
2026-10-12
```

## Fabric Mapping

```text
DATE
```

## Conversion

```text
20261012
→
2026-10-12
```

## Agent Rule

```text
IF Datatype = DATS
THEN Fabric DATE
```

## Timezone Handling

```text
No timezone conversion
```

---

# TIMS

## SAP Definition

```text
Type   : TIMS
Length : 6
Format : HHMMSS
```

### Example

```text
143025
```

Meaning:

```text
14:30:25
```

## Fabric Mapping

```text
STRING
```

Reason:

```text
Fabric Lakehouse does not provide a native TIME datatype.
```

## Conversion

```text
143025
→
14:30:25
```

## Agent Rule

```text
IF Datatype = TIMS
THEN Fabric STRING
```

## Timezone Handling

```text
No timezone conversion
```

---

# TIMESTAMP

## SAP Semantic Type

```text
TIMESTAMP
```

Common physical storage:

```text
DEC(15,0)
```

Format:

```text
YYYYMMDDHHMMSS
```

### Example

```text
20261012143025
```

Meaning:

```text
2026-10-12 14:30:25
```

## Fabric Mapping

```text
TIMESTAMP
```

## Conversion

```text
20261012143025
→
2026-10-12 14:30:25
```

## Agent Rule

```text
IF Data Element = TIMESTAMP
OR Domain = TIMESTAMP
THEN Fabric TIMESTAMP
```

## Timezone Handling

```text
Do NOT perform timezone conversion
unless UTC is explicitly confirmed.
```

---

# TIMESTAMPL

## SAP Semantic Type

```text
TIMESTAMPL
```

Common physical storage:

```text
DEC(21,7)
```

Format:

```text
YYYYMMDDHHMMSS.fffffff
```

### Example

```text
20261012143025.1234567
```

Meaning:

```text
2026-10-12 14:30:25.1234567
```

## Fabric Mapping

```text
TIMESTAMP
```

## Conversion

```text
20261012143025.1234567
→
2026-10-12 14:30:25.1234567
```

## Agent Rule

```text
IF Data Element = TIMESTAMPL
OR Domain = TIMESTAMPL
THEN Fabric TIMESTAMP
```

## Timezone Handling

```text
Do NOT perform timezone conversion
unless UTC is explicitly confirmed.
```

---

# UTC Identification Rules

UTC should only be assumed when one or more of the following are true:

```text
Data Element documentation states UTC
Domain documentation states UTC
SAP documentation states UTC
Business owner confirms UTC storage
ABAP implementation uses:

GET TIME STAMP FIELD
```

Otherwise:

```text
Load timestamp exactly as stored.
```

---

# Numeric Types

## DEC

### SAP

```text
DEC(p,s)
```

### Fabric

```text
DECIMAL(p,s)
```

### Example

```text
SAP:
DEC(21,7)

Fabric:
DECIMAL(21,7)
```

### Agent Rule

```text
IF semantic metadata does not indicate TIMESTAMP
THEN preserve DECIMAL precision and scale.
```

---

## CURR

Currency amounts.

### Example

```text
CURR(15,2)
```

### Fabric

```text
DECIMAL(15,2)
```

---

## QUAN

Quantities.

### Example

```text
QUAN(13,3)
```

### Fabric

```text
DECIMAL(13,3)
```

---

# Integer Types

| SAP | Fabric |
|------|---------|
| INT1 | SMALLINT |
| INT2 | SMALLINT |
| INT4 | INT |
| INT8 | BIGINT |

---

# Floating Point Types

## FLTP

SAP:

```text
FLTP
```

Fabric:

```text
DOUBLE
```

---

# Character Types

## CHAR

Fabric:

```text
STRING
```

---

## STRING

Fabric:

```text
STRING
```

---

## SSTRING

Fabric:

```text
STRING
```

---

# NUMC

## Important

NUMC is an identifier.

It is NOT a business number.

Leading zeros must be preserved.

### Example

```text
0000123456
```

## Fabric Mapping

```text
STRING
```

## Agent Rule

```text
Never map NUMC to numeric types.
```

---

# Boolean / Flag Fields

Common SAP values:

```text
X
Y
N
1
0
Blank
```

Recommended mapping:

```text
BOOLEAN
```

only when semantic meaning is confirmed.

Otherwise:

```text
STRING
```

---

# Binary Types

## RAW

```text
RAW
LRAW
RAWSTRING
```

## Fabric Mapping

```text
BINARY
```

or

```text
STRING (Base64)
```

depending on ingestion framework capability.

---

# Decision Matrix

```text
IF datatype = DATS
    → DATE

ELSE IF datatype = TIMS
    → STRING(HH:mm:ss)

ELSE IF Data Element in
        (
         TIMESTAMP,
         TIMESTAMPL,
         UTC_TIMESTAMP
        )
    → TIMESTAMP

ELSE IF datatype = NUMC
    → STRING

ELSE IF datatype IN
        (
          DEC,
          CURR,
          QUAN
        )
    → DECIMAL(p,s)

ELSE IF datatype = INT1
    → SMALLINT

ELSE IF datatype = INT2
    → SMALLINT

ELSE IF datatype = INT4
    → INT

ELSE IF datatype = INT8
    → BIGINT

ELSE IF datatype = FLTP
    → DOUBLE

ELSE IF datatype IN
        (
          CHAR,
          STRING,
          SSTRING
        )
    → STRING

ELSE IF datatype IN
        (
          RAW,
          LRAW,
          RAWSTRING
        )
    → BINARY

ELSE
    → STRING
```

---

# Critical Migration Safeguards

1. Never infer timestamps from `DEC(21,7)` alone.
2. Never infer timestamps from `DEC(15,0)` alone.
3. Never convert `NUMC` to numeric datatypes.
4. Never apply timezone conversion unless UTC is explicitly confirmed.
5. Prefer Data Element and Domain semantics over physical datatype.
6. Preserve numeric precision and scale exactly.
7. Map `TIMS` to `STRING(HH:mm:ss)`.
8. Map `DATS` to `DATE`.
9. Map `TIMESTAMP` and `TIMESTAMPL` to `TIMESTAMP`.
10. Use lossless mappings whenever semantic metadata is unavailable.
11. Store unsupported or ambiguous types as `STRING` rather than risking data loss.
12. Log all datatype decisions and the metadata source used (Data Element, Domain, Physical Type, or Heuristic) to ensure migration traceability and auditability.

---

# SAP-Specific Recommendations for Fabric Migrations

1. Data Element metadata should always override physical datatype mappings.
2. Domain metadata should be used when Data Element metadata is unavailable.
3. NUMC fields frequently represent business keys and identifiers and must retain leading zeros.
4. TIMS should remain time-only values and should not be artificially converted into Fabric timestamps.
5. DATS should be loaded directly into Fabric DATE columns.
6. TIMESTAMP/TIMESTAMPL values should be loaded into Fabric TIMESTAMP columns without timezone conversion unless UTC is explicitly verified.
7. DECIMAL precision and scale should always be preserved exactly as defined in SAP.
8. Any field identified as a business identifier should default to STRING rather than a numeric type.
9. Unsupported SAP datatypes should be ingested as STRING and flagged for review.
10. All automated conversion decisions should be traceable and logged.
