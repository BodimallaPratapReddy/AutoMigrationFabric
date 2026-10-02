"""Map source columns with the Oracle and SAP JSON type policies."""

from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel

from thirdparty.oracle.utils import OracleTableColumn
from thirdparty.sap.utils import SAPFieldMetadata


class FabricTypeMapping(BaseModel):
    source_type: str
    fabric_type: str | None
    supported: bool
    lossy: bool = False
    warning: str | None = None
    rule_name: str | None = None


@lru_cache(maxsize=2)
def _policy(source: str) -> dict:
    path = Path(__file__).parent / source / f"{source}_type_mapping.json"
    policy = json.loads(path.read_text(encoding="utf-8"))
    if source == "oracle":
        _validate_oracle_policy(policy)
    return policy


def _validate_oracle_policy(policy: dict) -> None:
    required = {"fixed_types", "number_rules", "date_exact_types", "date_target",
                "timestamp_rules", "interval_types", "unsupported_or_review_types",
                "default_mapping"}
    if not isinstance(policy, dict) or required - policy.keys():
        raise ValueError("Oracle datatype policy is missing required sections")
    if not isinstance(policy["fixed_types"], dict) or any(
        not isinstance(value, str) or not value for value in policy["fixed_types"].values()
    ):
        raise ValueError("Oracle fixed type mappings must have non-empty targets")
    number = policy["number_rules"]
    if not isinstance(number.get("integer_scale"), int):
        raise ValueError("Oracle NUMBER integer scale is required")
    thresholds = [rule["max_precision"] for rule in number["integer_precision_rules"]]
    if (not thresholds or any(not isinstance(value, int) or value < 1 for value in thresholds)
            or thresholds != sorted(set(thresholds)) or thresholds[-1] > 38):
        raise ValueError("Oracle NUMBER precision thresholds must increase up to 38")
    if any(not (rule.get("target_type") or rule.get("target_type_template"))
           for rule in number["integer_precision_rules"]):
        raise ValueError("Oracle NUMBER precision rules need targets")
    overflow_rules = number.get("scale_overflow_rules")
    if not isinstance(overflow_rules, list) or not overflow_rules or any(
        not isinstance(rule, dict) or not isinstance(rule.get("min_scale"), int)
        or rule["min_scale"] < 1 or not isinstance(rule.get("target_type"), str)
        or not rule["target_type"] for rule in overflow_rules
    ) or [rule["min_scale"] for rule in overflow_rules] != sorted({
        rule["min_scale"] for rule in overflow_rules
    }):
        raise ValueError("Oracle NUMBER scale overflow rules must have increasing thresholds and targets")
    for key in ("unbounded_number_target", "unbounded_number_warning", "decimal_target_template"):
        if not isinstance(number.get(key), str) or not number[key]:
            raise ValueError(f"Oracle NUMBER rule {key} is required")
    negative = number.get("negative_scale_rule")
    if (not isinstance(negative, dict)
            or negative.get("adjusted_precision_formula") != "precision + abs(scale)"
            or not isinstance(negative.get("max_target_precision"), int)
            or not 1 <= negative["max_target_precision"] <= 38
            or any(not isinstance(negative.get(key), str) or not negative[key]
                   for key in ("target_type_template", "overflow_target", "warning"))):
        raise ValueError("Oracle NUMBER negative scale rule is invalid")
    timestamp = policy["timestamp_rules"]
    if not policy["date_target"] or not timestamp.get("target_type") or not timestamp.get("prefix"):
        raise ValueError("Oracle date and timestamp targets are required")
    if not isinstance(timestamp.get("timezone_variants"), dict):
        raise ValueError("Oracle timestamp timezone rules are required")
    for section in ("interval_types", "unsupported_or_review_types"):
        if not isinstance(policy[section], dict) or any(
            not isinstance(rule, dict) or not rule.get("target_type")
            for rule in policy[section].values()
        ):
            raise ValueError(f"Oracle {section} mappings need targets")
    if not policy["default_mapping"].get("target_type"):
        raise ValueError("Oracle default mapping target is required")


def _default(source_type: str, policy: dict, reason: str) -> FabricTypeMapping:
    target = policy["default_type"]
    return FabricTypeMapping(source_type=source_type, fabric_type=target,
                             supported=True, warning=f"{reason}; using configured default {target}")


def _oracle_policy_mapping(source_type: str, rule: dict,
                           rule_name: str | None = None) -> FabricTypeMapping:
    return FabricTypeMapping(
        source_type=source_type, fabric_type=rule["target_type"],
        supported=rule.get("supported", True), lossy=rule.get("lossy", False),
        warning=rule.get("warning"),
        rule_name=rule_name,
    )


def map_oracle_column(column: OracleTableColumn) -> FabricTypeMapping:
    policy = _policy("oracle")
    source = (column.data_type or column.datatype).upper().strip()
    kind = source.split("(", 1)[0].strip()
    timestamp_rules = policy["timestamp_rules"]
    source_type = column.datatype
    if source.startswith(timestamp_rules["prefix"]) and timestamp_rules.get(
        "preserve_source_precision_in_metadata"
    ):
        source_type = column.data_type or column.datatype
    if kind == "NUMBER":
        rules = policy["number_rules"]
        precision, scale = column.precision, column.scale
        if precision is None:
            match = re.fullmatch(r"NUMBER\s*\(\s*(\d+)\s*(?:,\s*(-?\d+)\s*)?\)",
                                 column.datatype.upper().strip())
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2) or 0)
        if precision is not None and scale is None:
            scale = 0
        if precision is not None and not 1 <= precision <= 38:
            return _oracle_policy_mapping(source_type, policy["default_mapping"], "DEFAULT")
        if precision is None:
            target = rules["unbounded_number_target"]
            return FabricTypeMapping(source_type=source_type,
                                     fabric_type=target, supported=True,
                                     warning=rules["unbounded_number_warning"],
                                     rule_name="UNBOUNDED_NUMBER")
        overflow = next((rule for rule in reversed(rules["scale_overflow_rules"])
                         if scale >= rule["min_scale"]), None)
        if overflow is not None:
            return FabricTypeMapping(
                source_type=source_type, fabric_type=overflow["target_type"],
                supported=overflow.get("supported", True), lossy=overflow.get("lossy", False),
                warning=f"Scale {scale}: {overflow['warning']}" if overflow.get("warning") else None,
                rule_name="NUMBER_SCALE_OVERFLOW",
            )
        if scale < 0:
            rule = rules["negative_scale_rule"]
            adjusted_precision = precision + abs(scale)
            target = (rule["overflow_target"] if adjusted_precision > rule["max_target_precision"]
                      else rule["target_type_template"].format(adjusted_precision=adjusted_precision))
            return FabricTypeMapping(source_type=source_type,
                                     fabric_type=target, supported=rule.get("supported", True),
                                     lossy=rule.get("lossy", False),
                                     warning=f"Adjusted precision = {precision} + {abs(scale)} = "
                                             f"{adjusted_precision}. {rule['warning']}",
                                     rule_name=("NUMBER_NEGATIVE_SCALE_OVERFLOW"
                                                if adjusted_precision > rule["max_target_precision"]
                                                else "NUMBER_NEGATIVE_SCALE"))
        if scale == rules["integer_scale"]:
            for rule in rules["integer_precision_rules"]:
                if precision <= rule["max_precision"]:
                    target = rule.get("target_type") or rule["target_type_template"].format(precision=precision)
                    return FabricTypeMapping(source_type=source_type, fabric_type=target,
                                             supported=True, rule_name="NUMBER_INTEGER")
            return _oracle_policy_mapping(source_type, policy["default_mapping"], "DEFAULT")
        if scale > precision:
            return _oracle_policy_mapping(source_type, policy["default_mapping"], "DEFAULT")
        return FabricTypeMapping(source_type=source_type,
                                 fabric_type=rules["decimal_target_template"].format(precision=precision, scale=scale),
                                 supported=True, rule_name="NUMBER_DECIMAL")
    target = policy["fixed_types"].get(kind)
    if target is not None:
        approximate = kind in {"FLOAT", "BINARY_FLOAT", "BINARY_DOUBLE"}
        return FabricTypeMapping(source_type=source_type, fabric_type=target,
                                 supported=True, lossy=approximate,
                                 warning="Floating point values are approximate" if approximate else None)
    if kind in policy["date_exact_types"]:
        return FabricTypeMapping(source_type=source_type,
                                 fabric_type=policy["date_target"], supported=True)
    normalized = re.sub(r"\(\s*\d+\s*\)", "", source)
    if source.startswith(timestamp_rules["prefix"]):
        for variant, rule in timestamp_rules["timezone_variants"].items():
            if normalized == variant:
                return _oracle_policy_mapping(source_type, rule)
        return FabricTypeMapping(source_type=source_type,
                                 fabric_type=timestamp_rules["target_type"], supported=True)
    for interval_type, rule in policy["interval_types"].items():
        if normalized == interval_type:
            return _oracle_policy_mapping(source_type, rule)
    rule = policy["unsupported_or_review_types"].get(kind)
    if rule is not None:
        return _oracle_policy_mapping(source_type, rule)
    return _oracle_policy_mapping(source_type, policy["default_mapping"])


def map_oracle_column_to_fabric(column: OracleTableColumn) -> FabricTypeMapping:
    """Map Oracle source metadata with the configured policy."""
    return map_oracle_column(column)


def map_oracle_table_columns(columns: list[OracleTableColumn]) -> list[FabricTypeMapping]:
    """Apply the same cached Oracle policy to every discovered column."""
    return [map_oracle_column(column) for column in columns]


def map_sap_field(field: SAPFieldMetadata) -> FabricTypeMapping:
    policy = _policy("sap")
    kind = field.datatype.upper().strip()
    source = f"{kind}({field.leng},{field.decimals})"
    if kind in policy["length_bound_types"]:
        if field.leng < 1:
            return _default(source, policy, "SAP field length needs review")
        target = f"{policy['length_bound_types'][kind]}({field.leng})"
    elif kind in policy["binary_length_types"]:
        if field.leng < 1:
            return FabricTypeMapping(source_type=source, fabric_type=None, supported=False,
                                     warning="Binary field length needs review")
        target = f"VARBINARY({field.leng})"
    elif kind in policy["decimal_types"]:
        if not 1 <= field.leng <= 38 or not 0 <= field.decimals <= field.leng:
            return _default(source, policy, "Decimal precision or scale needs review")
        target = f"DECIMAL({field.leng},{field.decimals})"
    else:
        target = policy["fixed_types"].get(kind)
        if target is None:
            return _default(source, policy, "No explicit SAP type mapping")
    approximate = kind == "FLTP"
    return FabricTypeMapping(source_type=source, fabric_type=target, supported=True,
                             lossy=approximate,
                             warning="Floating point values are approximate" if approximate else None)


def map_sap_field_to_fabric(field: SAPFieldMetadata) -> FabricTypeMapping:
    """Map SAP source metadata with the configured policy."""
    return map_sap_field(field)


_policy("oracle")
