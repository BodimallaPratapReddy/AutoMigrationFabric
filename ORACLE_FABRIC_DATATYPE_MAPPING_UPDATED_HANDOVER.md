# Coding Agent Handover
## Updated Oracle -> Fabric Datatype Mapping Rules

**Configuration file:** `oracle_to_fabric_datatype_mapping_updated.json`

## Objective

Update the Oracle-to-Fabric datatype mapper to use the revised JSON configuration.

The mapper must remain deterministic and must not use an LLM.

The two important corrections are:

1. Oracle `NUMBER` values with scale greater than 38 must map to `STRING`, not `DOUBLE`.
2. Oracle negative-scale `NUMBER(p,s)` must use:

```text
adjusted_precision = precision + abs(scale)
```

and map to `STRING` if adjusted precision exceeds Fabric DECIMAL precision 38.

---

## Required NUMBER rule order

Evaluate Oracle `NUMBER` rules in this exact order:

```text
1. precision is NULL
      -> STRING

2. scale >= 39
      -> STRING via scale_overflow_rules

3. scale < 0
      -> adjusted precision handling

4. scale = 0
      -> integer precision rules

5. scale > 0 and <= 38
      -> DECIMAL(p,s)
```

Do not generate a DECIMAL target before checking overflow conditions.

---

## Unbounded NUMBER

If Oracle metadata returns:

```text
data_type = NUMBER
precision = NULL
scale = NULL
```

map to:

```text
STRING
```

Examples:

```text
ORDER_ID NUMBER
    -> STRING

CUSTOMER_ID NUMBER
    -> STRING
```

Use the configured warning:

```text
Oracle NUMBER has no declared precision or scale. Stored as STRING to avoid assumptions about numeric range and scale.
```

Do not infer BIGINT from a column name such as `_ID`.

---

## Integer NUMBER

For:

```text
NUMBER(p,0)
```

map:

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
NUMBER(5,0)   -> INT
NUMBER(9,0)   -> INT
NUMBER(12,0)  -> BIGINT
NUMBER(18,0)  -> BIGINT
NUMBER(25,0)  -> DECIMAL(25,0)
NUMBER(38,0)  -> DECIMAL(38,0)
```

---

## Decimal NUMBER

For:

```text
NUMBER(p,s)
```

where:

```text
1 <= s <= 38
```

map to:

```text
DECIMAL(p,s)
```

Examples:

```text
NUMBER(10,2)  -> DECIMAL(10,2)
NUMBER(15,2)  -> DECIMAL(15,2)
NUMBER(20,6)  -> DECIMAL(20,6)
NUMBER(38,18) -> DECIMAL(38,18)
NUMBER(38,38) -> DECIMAL(38,38)
```

---

## Scale overflow

If:

```text
scale >= 39
```

map to:

```text
STRING
```

Example:

```text
NUMBER(38,127)
    -> STRING
```

Return:

```text
supported = true
lossy = false
```

and the configured warning.

Important: this must hit the explicit scale-overflow rule, not `default_mapping`.

The UI should show something like:

```text
Fabric type:
STRING

Mapping note:
Oracle NUMBER scale 127 exceeds Fabric DECIMAL maximum scale 38.
Stored as STRING to preserve the source value.
```

---

## Negative scale

For:

```text
NUMBER(p,-s)
```

calculate:

```text
adjusted_precision = precision + abs(scale)
```

Example:

```text
NUMBER(10,-2)

adjusted_precision = 10 + 2 = 12

Fabric:
DECIMAL(12,0)
```

Therefore:

```text
NUMBER(10,-2)
    -> DECIMAL(12,0)
```

not:

```text
DECIMAL(10,0)
```

If:

```text
adjusted_precision > 38
```

map to:

```text
STRING
```

Example:

```text
NUMBER(38,-5)

adjusted_precision = 43

43 > 38
    -> STRING
```

---

## Recommended mapper logic

```python
def map_number(column, rules):

    precision = column.precision
    scale = column.scale

    if precision is None:
        return FabricTypeMapping(
            source_type=column.datatype,
            target_type=rules["unbounded_number_target"],
            supported=True,
            lossy=False,
            warning=rules.get("unbounded_number_warning"),
            rule_name="UNBOUNDED_NUMBER",
        )

    effective_scale = 0 if scale is None else scale

    for rule in rules.get("scale_overflow_rules", []):
        if effective_scale >= rule["min_scale"]:
            return FabricTypeMapping(
                source_type=column.datatype,
                target_type=rule["target_type"],
                supported=rule.get("supported", True),
                lossy=rule.get("lossy", False),
                warning=rule.get("warning"),
                rule_name="NUMBER_SCALE_OVERFLOW",
            )

    if effective_scale < 0:
        rule = rules["negative_scale_rule"]
        adjusted_precision = precision + abs(effective_scale)

        if adjusted_precision > rule["max_target_precision"]:
            return FabricTypeMapping(
                source_type=column.datatype,
                target_type=rule["overflow_target"],
                supported=True,
                lossy=False,
                warning=rule.get("warning"),
                rule_name="NUMBER_NEGATIVE_SCALE_OVERFLOW",
            )

        return FabricTypeMapping(
            source_type=column.datatype,
            target_type=rule["target_type_template"].format(
                adjusted_precision=adjusted_precision
            ),
            supported=True,
            lossy=False,
            warning=rule.get("warning"),
            rule_name="NUMBER_NEGATIVE_SCALE",
        )

    if effective_scale == rules["integer_scale"]:
        for rule in rules["integer_precision_rules"]:
            if precision <= rule["max_precision"]:
                if "target_type" in rule:
                    target = rule["target_type"]
                else:
                    target = rule["target_type_template"].format(
                        precision=precision
                    )

                return FabricTypeMapping(
                    source_type=column.datatype,
                    target_type=target,
                    rule_name="NUMBER_INTEGER",
                )

    return FabricTypeMapping(
        source_type=column.datatype,
        target_type=rules["decimal_target_template"].format(
            precision=precision,
            scale=effective_scale,
        ),
        rule_name="NUMBER_DECIMAL",
    )
```

Adapt this to the existing mapper structure.

---

## Mapping result model

Recommended:

```python
class FabricTypeMapping(BaseModel):
    source_type: str
    target_type: str
    supported: bool = True
    lossy: bool = False
    warning: str | None = None
    rule_name: str | None = None
```

Suggested `rule_name` values:

```text
UNBOUNDED_NUMBER
NUMBER_INTEGER
NUMBER_DECIMAL
NUMBER_SCALE_OVERFLOW
NUMBER_NEGATIVE_SCALE
NUMBER_NEGATIVE_SCALE_OVERFLOW
FIXED_TYPE
TIMESTAMP
TIMESTAMP_TIMEZONE
INTERVAL
REVIEW_REQUIRED
DEFAULT
```

This will improve UI mapping notes.

---

## Keep existing non-NUMBER behavior

Keep:

```text
VARCHAR/VARCHAR2/CHAR/NVARCHAR2/NCHAR -> STRING
CLOB/NCLOB/LONG -> STRING
RAW/LONG RAW/BLOB -> BINARY
FLOAT -> DOUBLE
BINARY_FLOAT -> FLOAT
BINARY_DOUBLE -> DOUBLE
DATE -> TIMESTAMP
TIMESTAMP(n) -> TIMESTAMP
```

Keep timezone timestamp warnings.

Keep interval mappings to STRING with warnings.

Keep explicit review handling for:

```text
BFILE
XMLTYPE
SDO_GEOMETRY
```

---

## Expected test table results

```text
NUMBER            -> STRING
NUMBER(5,0)       -> INT
NUMBER(9,0)       -> INT
NUMBER(12,0)      -> BIGINT
NUMBER(18,0)      -> BIGINT
NUMBER(25,0)      -> DECIMAL(25,0)
NUMBER(38,0)      -> DECIMAL(38,0)
NUMBER(10,2)      -> DECIMAL(10,2)
NUMBER(15,2)      -> DECIMAL(15,2)
NUMBER(20,6)      -> DECIMAL(20,6)
NUMBER(38,18)     -> DECIMAL(38,18)
NUMBER(38,38)     -> DECIMAL(38,38)
NUMBER(38,127)    -> STRING
NUMBER(10,-2)     -> DECIMAL(12,0)
```

Additional negative-scale tests:

```text
NUMBER(5,-1)   -> DECIMAL(6,0)
NUMBER(10,-2)  -> DECIMAL(12,0)
NUMBER(20,-5)  -> DECIMAL(25,0)
NUMBER(38,-1)  -> STRING
```

Additional scale-overflow tests:

```text
NUMBER(38,38)  -> DECIMAL(38,38)
NUMBER(38,39)  -> STRING
NUMBER(38,127) -> STRING
```

Verify overflow cases use the explicit rule rather than `DEFAULT`.

---

## Config DB persistence

Persist:

```text
SourceTableColumns.SourceDataType
SourceTableColumns.FabricDataType
```

Examples:

```text
NUMBER(38,127) -> STRING
NUMBER(10,-2)  -> DECIMAL(12,0)
```

Preserve the original Oracle source type exactly as discovered.

---

## UI behavior

For `NUMBER(38,127)` show the explicit overflow warning.

For `NUMBER(10,-2)` show:

```text
Oracle negative scale increases required integer precision.
Adjusted precision = 10 + 2 = 12.
```

Mappings with:

```text
supported = false
```

require review.

Mappings with:

```text
lossy = true
```

must display warnings before approval.

Scale-overflow NUMBER -> STRING is intentionally supported and non-lossy under the project's Bronze preservation policy.

---

## Startup validation

Validate the JSON at mapper initialization.

Required top-level sections:

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

Required `number_rules`:

```text
integer_precision_rules
scale_overflow_rules
decimal_target_template
unbounded_number_target
negative_scale_rule
```

Fail fast on invalid configuration.

---

## Do not do

Do not:

```text
map scale > 38 to DOUBLE
```

Do not:

```text
map NUMBER(10,-2) to DECIMAL(10,0)
```

Do not:

```text
infer BIGINT from `_ID`
```

Do not:

```text
fall through to default mapping for NUMBER scale overflow
```

Do not put datatype rules inside Temporal workflow code.

---

## Definition of done

- [ ] Updated JSON loads successfully
- [ ] Plain NUMBER maps to STRING
- [ ] NUMBER scale >= 39 maps to STRING
- [ ] Overflow rule warning is shown correctly
- [ ] Negative-scale adjusted precision is implemented
- [ ] Adjusted precision > 38 maps to STRING
- [ ] NUMBER(10,-2) maps to DECIMAL(12,0)
- [ ] NUMBER(38,127) maps through explicit overflow rule
- [ ] NUMBER(38,38) remains DECIMAL(38,38)
- [ ] No scale-overflow NUMBER maps to DOUBLE
- [ ] Source datatype remains preserved
- [ ] Fabric datatype persists to Config DB
- [ ] Unit tests cover all cases above
- [ ] UI note reflects the actual rule used
- [ ] Temporal contains no datatype mapping logic

## Final rule summary

```text
NUMBER with no p/s
    -> STRING

NUMBER(p,0)
    -> INT / BIGINT / DECIMAL(p,0)

NUMBER(p,s), 1 <= s <= 38
    -> DECIMAL(p,s)

NUMBER(p,s), s >= 39
    -> STRING

NUMBER(p,-s)
    -> DECIMAL(p + abs(s),0)
       if adjusted precision <= 38
    -> STRING otherwise
```

Treat `oracle_to_fabric_datatype_mapping_updated.json` as the authoritative default policy.
