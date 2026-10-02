# Coding Agent Handover
## Using the Oracle -> Fabric Datatype Mapping JSON

**File:** `oracle_to_fabric_datatype_mapping.json`  
**Purpose:** Centralize deterministic Oracle-to-Fabric datatype mapping rules used by the Oracle migration workflow.

---

# 1. Objective

Use the JSON mapping file as the single configuration source for Oracle -> Fabric datatype mapping.

The Oracle adapter already returns rich source metadata such as:

```text
column name
Oracle data type
formatted datatype
precision
scale
length
nullable
description
```

The mapping layer must convert this Oracle metadata into the Fabric datatype that will be persisted into:

```text
bronze_replication.SourceTableColumns.FabricDataType
```

The mapper must be deterministic.

Do not use an LLM for datatype mapping.

---

# 2. Where This Logic Belongs

Do not put this logic directly inside:

```text
Temporal workflow
OracleClient
ConfigDB repository
FabricClient
```

Create a dedicated datatype-mapping module.

Recommended structure:

```text
backend/
  integrations/
    datatype_mapping/
      __init__.py
      models.py
      oracle.py
      oracle_to_fabric_datatype_mapping.json
```

Alternative if configuration files already have a central directory:

```text
backend/
  config/
    datatype_mapping/
      oracle_to_fabric_datatype_mapping.json

  integrations/
    datatype_mapping/
      oracle.py
      models.py
```

Keep the JSON path configurable if needed.

---

# 3. Mapping JSON

The mapper should load:

```text
oracle_to_fabric_datatype_mapping.json
```

The file contains these main sections:

```text
fixed_types
number_rules
date_exact_types
date_target
timestamp_rules
interval_types
unsupported_or_review_types
default_mapping
```

Treat these as configuration.

Do not duplicate the same mapping rules as hard-coded dictionaries elsewhere.

---

# 4. Expected Mapper Contract

Create a typed result model.

Example:

```python
from pydantic import BaseModel

class FabricTypeMapping(BaseModel):
    source_type: str
    target_type: str

    supported: bool = True
    lossy: bool = False

    warning: str | None = None
```

Create the main mapper:

```python
def map_oracle_column_to_fabric(
    column: OracleTableColumn,
) -> FabricTypeMapping:
    ...
```

The mapper should consume the structured Oracle metadata returned by the Oracle adapter.

Do not parse only the formatted string if structured fields are available.

Prefer:

```text
column.data_type
column.precision
column.scale
column.char_length
```

Use:

```text
column.datatype
```

mainly for display / source metadata persistence.

---

# 5. Load the JSON Once

Do not open and parse the JSON file for every column.

Load it once when the mapper/service starts.

Example:

```python
import json
from pathlib import Path

_MAPPING_FILE = Path(__file__).with_name(
    "oracle_to_fabric_datatype_mapping.json"
)

with _MAPPING_FILE.open("r", encoding="utf-8") as handle:
    ORACLE_TO_FABRIC_MAPPING = json.load(handle)
```

A class is also acceptable:

```python
class OracleFabricTypeMapper:

    def __init__(self, config_path: Path | None = None):
        ...
```

This may be easier to test with alternate mapping files.

---

# 6. Normalize the Oracle Base Type

Oracle source metadata may appear as:

```text
VARCHAR2
NUMBER
DATE
TIMESTAMP(6)
TIMESTAMP WITH TIME ZONE
```

The structured Oracle adapter should usually provide:

```text
data_type = NUMBER
precision = 15
scale = 2
```

For timestamp variants, normalize carefully.

Recommended helper:

```python
def normalize_oracle_type(source_type: str) -> str:
    return source_type.strip().upper()
```

Do not remove meaningful timezone words.

---

# 7. Mapping Precedence

Apply rules in this order.

```text
1. NUMBER rules
2. Timestamp / timezone timestamp rules
3. Exact DATE rules
4. Fixed types
5. Interval rules
6. Unsupported/review types
7. Default mapping
```

This order matters.

For example:

```text
TIMESTAMP WITH TIME ZONE
```

must be handled by the timestamp-specific logic before generic fallback.

---

# 8. Fixed Types

The JSON contains mappings such as:

```json
"VARCHAR2": "STRING",
"CHAR": "STRING",
"CLOB": "STRING",
"BLOB": "BINARY",
"BINARY_FLOAT": "FLOAT",
"BINARY_DOUBLE": "DOUBLE",
"INTEGER": "BIGINT"
```

Implementation:

```python
fixed = config["fixed_types"]

if base_type in fixed:
    return FabricTypeMapping(
        source_type=formatted_source_type,
        target_type=fixed[base_type],
        supported=True,
    )
```

Examples:

```text
VARCHAR2(50) -> STRING
CHAR(10)     -> STRING
CLOB         -> STRING
BLOB         -> BINARY
```

The target type should not preserve source length in the current mapping design.

Keep source length separately in Oracle metadata if needed.

---

# 9. NUMBER Mapping

This is the most important dynamic rule.

Do not add:

```json
"NUMBER": "DOUBLE"
```

to `fixed_types`.

Use precision and scale.

---

## 9.1 NUMBER(p,0)

For integer-style Oracle NUMBER:

```text
NUMBER(p,0)
```

apply rules from:

```json
"integer_precision_rules"
```

Expected behavior:

```text
p <= 9
    -> INT

p <= 18
    -> BIGINT

p <= 38
    -> DECIMAL(p,0)
```

Examples:

```text
NUMBER(5,0)  -> INT
NUMBER(9,0)  -> INT
NUMBER(12,0) -> BIGINT
NUMBER(18,0) -> BIGINT
NUMBER(20,0) -> DECIMAL(20,0)
```

---

## 9.2 NUMBER(p,s) where s > 0

Use:

```json
"decimal_target_template": "DECIMAL({precision},{scale})"
```

Example:

```text
NUMBER(15,2)
    ->
DECIMAL(15,2)
```

For the sample Oracle table:

```text
TOTAL_AMOUNT NUMBER(15,2)
```

must map to:

```text
DECIMAL(15,2)
```

---

## 9.3 Plain NUMBER

Oracle may define:

```text
ORDER_ID NUMBER
CUSTOMER_ID NUMBER
```

with no precision/scale.

The JSON currently specifies:

```json
"unbounded_number_target": "DECIMAL(38,18)"
```

Therefore:

```text
NUMBER
    ->
DECIMAL(38,18)
```

under the current project policy.

Do not silently map this to STRING.

Do not automatically map it to BIGINT unless the source definition or an approved override supports that decision.

---

# 10. Negative Scale NUMBER

Oracle permits definitions such as:

```text
NUMBER(10,-2)
```

Use the configured negative-scale rule.

The current JSON uses:

```text
DECIMAL({precision},0)
```

Treat this as a project policy.

Return a warning if desired because Oracle negative-scale rounding semantics may not map exactly.

Example result:

```python
FabricTypeMapping(
    source_type="NUMBER(10,-2)",
    target_type="DECIMAL(10,0)",
    supported=True,
    lossy=True,
    warning="Oracle NUMBER with negative scale may require rounding validation."
)
```

---

# 11. DATE Mapping

The JSON contains:

```json
"date_exact_types": ["DATE"],
"date_target": "TIMESTAMP"
```

Map:

```text
Oracle DATE
    ->
Fabric TIMESTAMP
```

Do not map Oracle DATE to Fabric DATE by default.

Oracle DATE contains:

```text
year
month
day
hour
minute
second
```

The migration should preserve the time component.

---

# 12. TIMESTAMP Mapping

The Oracle metadata query may return:

```text
TIMESTAMP(6)
```

even if the table DDL declared:

```text
TIMESTAMP
```

This is expected.

The mapper must support:

```text
TIMESTAMP
TIMESTAMP(3)
TIMESTAMP(6)
TIMESTAMP(9)
```

using:

```json
"timestamp_rules": {
  "prefix": "TIMESTAMP",
  "target_type": "TIMESTAMP"
}
```

Implementation concept:

```python
if base_type.startswith(config["timestamp_rules"]["prefix"]):
    ...
```

However, handle timezone variants before treating all timestamp variants identically.

---

# 13. TIMESTAMP WITH TIME ZONE

The JSON contains a specific rule for:

```text
TIMESTAMP WITH TIME ZONE
```

Target:

```text
TIMESTAMP
```

but:

```text
lossy = true
```

with a warning.

Return:

```python
FabricTypeMapping(
    source_type="TIMESTAMP WITH TIME ZONE",
    target_type="TIMESTAMP",
    supported=True,
    lossy=True,
    warning="Timezone information may not be preserved in the Fabric target."
)
```

The UI can surface this warning before approval.

---

# 14. TIMESTAMP WITH LOCAL TIME ZONE

Similarly:

```text
TIMESTAMP WITH LOCAL TIME ZONE
    ->
TIMESTAMP
```

but mark:

```text
lossy = true
```

with the configured warning.

Do not silently discard timezone semantics.

---

# 15. Interval Types

The JSON includes:

```text
INTERVAL YEAR TO MONTH
INTERVAL DAY TO SECOND
```

Current policy:

```text
STRING
```

with:

```text
lossy = true
```

and a warning.

Use this rule directly.

---

# 16. Unsupported / Review Types

The JSON currently lists:

```text
BFILE
XMLTYPE
SDO_GEOMETRY
```

These may have a fallback target type, but:

```text
supported = false
```

Example:

```python
FabricTypeMapping(
    source_type="XMLTYPE",
    target_type="STRING",
    supported=False,
    lossy=True,
    warning="XMLTYPE requires explicit serialization or transformation."
)
```

Important:

A target type being present does **not** mean the mapping is approved automatically.

The workflow/UI must flag:

```text
requires review
```

before the migration plan is approved.

---

# 17. Default Mapping

Do not silently map unknown Oracle types.

The JSON has:

```json
"default_mapping": {
  "target_type": "STRING",
  "supported": false,
  "lossy": true,
  "warning": "No explicit Oracle-to-Fabric mapping exists for this source datatype. Review is required."
}
```

Use exactly this behavior.

Example:

```text
Unknown Oracle type
    ->
STRING
    +
supported = false
    +
warning
```

This prevents silent data corruption.

---

# 18. Suggested Mapper Implementation

Conceptual implementation:

```python
def map_oracle_column_to_fabric(
    column: OracleTableColumn,
) -> FabricTypeMapping:

    config = ORACLE_TO_FABRIC_MAPPING

    source_type = (
        column.data_type
        or column.datatype
    ).strip().upper()

    formatted_source = column.datatype.upper()

    # NUMBER
    if source_type == "NUMBER":
        return map_number(
            formatted_source=formatted_source,
            precision=column.precision,
            scale=column.scale,
            config=config["number_rules"],
        )

    # Timestamp variants
    timestamp_config = config["timestamp_rules"]

    for timezone_type, timezone_rule in (
        timestamp_config
        .get("timezone_variants", {})
        .items()
    ):
        if source_type.startswith(timezone_type):
            return FabricTypeMapping(
                source_type=formatted_source,
                target_type=timezone_rule["target_type"],
                supported=True,
                lossy=timezone_rule.get("lossy", False),
                warning=timezone_rule.get("warning"),
            )

    if source_type.startswith(timestamp_config["prefix"]):
        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=timestamp_config["target_type"],
        )

    # DATE
    if source_type in config["date_exact_types"]:
        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=config["date_target"],
        )

    # Fixed mappings
    fixed = config["fixed_types"]
    if source_type in fixed:
        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=fixed[source_type],
        )

    # Intervals
    interval = config["interval_types"].get(source_type)
    if interval:
        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=interval["target_type"],
            supported=True,
            lossy=interval.get("lossy", False),
            warning=interval.get("warning"),
        )

    # Explicit review types
    review = config["unsupported_or_review_types"].get(source_type)
    if review:
        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=review["target_type"],
            supported=review.get("supported", False),
            lossy=True,
            warning=review.get("warning"),
        )

    # Default
    fallback = config["default_mapping"]

    return FabricTypeMapping(
        source_type=formatted_source,
        target_type=fallback["target_type"],
        supported=fallback["supported"],
        lossy=fallback["lossy"],
        warning=fallback["warning"],
    )
```

Do not copy this blindly if the existing project mapper structure differs.

Preserve existing project conventions.

---

# 19. Suggested NUMBER Helper

Conceptual implementation:

```python
def map_number(
    *,
    formatted_source: str,
    precision: int | None,
    scale: int | None,
    config: dict,
) -> FabricTypeMapping:

    if precision is None:
        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=config["unbounded_number_target"],
        )

    effective_scale = scale or 0

    if effective_scale < 0:
        target = config[
            "negative_scale_target_template"
        ].format(
            precision=precision,
            scale=effective_scale,
        )

        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=target,
            supported=True,
            lossy=True,
            warning="Negative-scale Oracle NUMBER requires value validation.",
        )

    if effective_scale > 0:
        target = config[
            "decimal_target_template"
        ].format(
            precision=precision,
            scale=effective_scale,
        )

        return FabricTypeMapping(
            source_type=formatted_source,
            target_type=target,
        )

    for rule in config["integer_precision_rules"]:

        if precision <= rule["max_precision"]:

            if "target_type" in rule:
                target = rule["target_type"]

            else:
                target = rule[
                    "target_type_template"
                ].format(
                    precision=precision,
                )

            return FabricTypeMapping(
                source_type=formatted_source,
                target_type=target,
            )

    raise ValueError(
        f"Oracle NUMBER precision {precision} exceeds supported mapping rules"
    )
```

---

# 20. Apply Mapping to All Columns

Create a service method:

```python
def map_oracle_table_columns(
    columns: list[OracleTableColumn],
) -> list[MappedOracleColumn]:
    ...
```

Suggested model:

```python
class MappedOracleColumn(BaseModel):
    source: OracleTableColumn

    mapping: FabricTypeMapping
```

This will simplify the future Temporal activity:

```text
inspect Oracle table
    ->
map all columns
    ->
build runtime plan
```

---

# 21. Persist to Config DB

When building:

```text
SourceTableColumns
```

persist:

```text
ColumnName
SourceDataType
FabricDataType
Description
IsPrimaryKey
IsNullable
IsWatermarkCandidate
```

Example:

```text
TOTAL_AMOUNT

SourceDataType:
NUMBER(15,2)

FabricDataType:
DECIMAL(15,2)
```

For:

```text
CREATED_DATE
```

persist:

```text
SourceDataType:
TIMESTAMP(6)

FabricDataType:
TIMESTAMP
```

Do not replace the source metadata with the normalized type.

---

# 22. Example Mapping for SALES_ORDER_HEADER

Expected mappings:

```text
ORDER_ID
Oracle: NUMBER
Fabric: DECIMAL(38,18)
```

under the current unbounded NUMBER policy.

```text
ORDER_NUMBER
Oracle: VARCHAR2(50)
Fabric: STRING
```

```text
CUSTOMER_ID
Oracle: NUMBER
Fabric: DECIMAL(38,18)
```

```text
CUSTOMER_NAME
Oracle: VARCHAR2(200)
Fabric: STRING
```

```text
ORDER_DATE
Oracle: DATE
Fabric: TIMESTAMP
```

```text
ORDER_STATUS
Oracle: VARCHAR2(30)
Fabric: STRING
```

```text
CURRENCY_CODE
Oracle: VARCHAR2(10)
Fabric: STRING
```

```text
TOTAL_AMOUNT
Oracle: NUMBER(15,2)
Fabric: DECIMAL(15,2)
```

```text
CREATED_DATE
Oracle: TIMESTAMP(6)
Fabric: TIMESTAMP
```

```text
LAST_UPDATED_DATE
Oracle: TIMESTAMP(6)
Fabric: TIMESTAMP
```

---

# 23. Unbounded NUMBER Review

The current mapping policy is:

```text
NUMBER
    ->
DECIMAL(38,18)
```

This is safe as a generic numeric fallback but may not be ideal for ID fields.

For example:

```text
ORDER_ID NUMBER
CUSTOMER_ID NUMBER
```

may logically contain only integer values.

Do not silently override them to BIGINT based only on the column name.

Possible future enhancement:

```text
source range analysis
+
user-approved override
```

For V1, keep the JSON policy deterministic.

If the team decides to change the policy later, update the JSON rather than changing workflow code.

---

# 24. Mapping Warnings in the UI

The mapping service should expose warnings.

Example:

```text
Column: EVENT_TIME
Oracle: TIMESTAMP WITH TIME ZONE
Fabric: TIMESTAMP

Warning:
Timezone information may not be preserved.
```

Example:

```text
Column: XML_DOCUMENT
Oracle: XMLTYPE
Fabric: STRING

Status:
Requires review
```

The Oracle migration-plan approval UI should highlight:

```text
supported = false
or
lossy = true
```

---

# 25. Plan Approval Rule

Before plan approval:

If any selected column has:

```text
supported = false
```

require one of:

```text
explicit user acknowledgement
custom transformation
column exclusion
custom datatype override
```

Do not automatically approve unsupported mappings.

---

# 26. Override Support

If manual datatype overrides are required later, do not edit the JSON at runtime per migration.

Store the approved override in the migration plan / column config.

Example:

```text
Default mapping:
NUMBER -> DECIMAL(38,18)

Approved column override:
ORDER_ID -> BIGINT
```

Persist final approved:

```text
FabricDataType = BIGINT
```

while retaining the original mapping/audit information if required.

The JSON remains the system default.

---

# 27. Validation at Startup

Validate the JSON structure at application startup.

Create a Pydantic config model if practical.

Example:

```python
class OracleMappingConfig(BaseModel):
    fixed_types: dict[str, str]
    number_rules: NumberMappingRules
    date_exact_types: list[str]
    date_target: str
    timestamp_rules: TimestampRules
    interval_types: dict[str, MappingRule]
    unsupported_or_review_types: dict[str, MappingRule]
    default_mapping: MappingRule
```

If the JSON is malformed:

```text
fail startup / fail mapper initialization
```

Do not discover configuration errors during a migration workflow.

---

# 28. Tests Required

Add unit tests driven by the JSON.

At minimum:

## Strings

```text
VARCHAR2(50) -> STRING
NVARCHAR2(100) -> STRING
CHAR(10) -> STRING
CLOB -> STRING
```

## Binary

```text
RAW -> BINARY
BLOB -> BINARY
LONG RAW -> BINARY
```

## Numeric

```text
NUMBER(5,0) -> INT
NUMBER(9,0) -> INT
NUMBER(10,0) -> BIGINT
NUMBER(18,0) -> BIGINT
NUMBER(20,0) -> DECIMAL(20,0)

NUMBER(15,2) -> DECIMAL(15,2)

NUMBER -> DECIMAL(38,18)
```

## Floating point

```text
FLOAT -> DOUBLE
BINARY_FLOAT -> FLOAT
BINARY_DOUBLE -> DOUBLE
```

## Date/time

```text
DATE -> TIMESTAMP
TIMESTAMP -> TIMESTAMP
TIMESTAMP(6) -> TIMESTAMP
TIMESTAMP(3) -> TIMESTAMP
```

## Timezone

```text
TIMESTAMP WITH TIME ZONE
-> TIMESTAMP
-> lossy=True

TIMESTAMP WITH LOCAL TIME ZONE
-> TIMESTAMP
-> lossy=True
```

## Review types

```text
XMLTYPE -> STRING + supported=False
BFILE -> STRING + supported=False
SDO_GEOMETRY -> STRING + supported=False
```

## Unknown

```text
UNKNOWN_TYPE
-> STRING
-> supported=False
-> warning populated
```

---

# 29. JSON Configuration Tests

Also test the configuration itself.

Validate:

```text
required top-level sections exist
fixed target types are non-empty
number precision thresholds are ascending
number max precision does not exceed 38
default mapping exists
timestamp target exists
```

Fail fast if invalid.

---

# 30. Do Not Do

Do not:

```text
hard-code NUMBER rules in Temporal workflows
duplicate mapping dictionaries in multiple modules
call an LLM for datatype mapping
silently map unknown types without warnings
discard source datatype precision
change source metadata to the target datatype
```

---

# 31. Final Target Flow

The Oracle workflow should use the mapper like this:

```text
Oracle adapter
    |
    v
get_table_schema()
    |
    v
OracleTableColumn[]
    |
    v
Oracle -> Fabric type mapper
    |
    v
FabricTypeMapping[]
    |
    v
Migration plan review
    |
    v
user approval
    |
    v
SourceTableColumns
    |
    v
Fabric provisioning notebook
```

---

# 32. Definition of Done

The datatype mapping implementation is complete when:

- [ ] JSON is loaded from one central location
- [ ] JSON is validated at startup
- [ ] fixed mappings work
- [ ] NUMBER mapping uses precision/scale rules
- [ ] plain NUMBER does not fall back to STRING
- [ ] DATE maps to TIMESTAMP
- [ ] TIMESTAMP(6) maps to TIMESTAMP
- [ ] timezone timestamps generate lossy warnings
- [ ] unsupported types require review
- [ ] unknown types require review
- [ ] mapper returns typed mapping metadata
- [ ] table-level mapping helper exists
- [ ] final Fabric datatype is persisted to Config DB
- [ ] original Oracle datatype is preserved
- [ ] unit tests cover all configured mapping categories
- [ ] Temporal workflow contains no datatype-mapping logic

---

# 33. Final Instruction to Coding Agent

Treat:

```text
oracle_to_fabric_datatype_mapping.json
```

as the default Oracle-to-Fabric datatype policy.

The code should interpret that policy, not duplicate it.

Keep the design:

```text
Oracle metadata
    +
JSON rules
    ->
deterministic mapping result
    ->
user review when required
    ->
Config DB
```

This mapper will later be called by the Oracle Temporal migration activity before the migration plan is presented for approval.
