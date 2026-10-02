"""Fabric Lakehouse types accepted for Oracle column overrides."""

import re


FABRIC_TYPES = frozenset({
    "STRING", "BINARY", "BOOLEAN", "DATE", "TIMESTAMP",
    "TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE",
})


def normalize_fabric_type(value: str) -> str:
    target = value.strip().upper()
    if target in FABRIC_TYPES:
        return target
    match = re.fullmatch(r"DECIMAL\s*\(\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)", target)
    if match:
        precision, scale = int(match.group(1)), int(match.group(2))
        if 1 <= precision <= 38 and 0 <= scale <= precision:
            return f"DECIMAL({precision},{scale})"
    raise ValueError("Choose a supported Fabric type or DECIMAL(p,s) with 1 <= p <= 38 and 0 <= s <= p")
