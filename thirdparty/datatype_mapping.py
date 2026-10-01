"""Deterministic source-to-Fabric Lakehouse column mapping.

Oracle NUMBER(p) maps to DECIMAL(p,0), and NUMBER(p,s) maps to
DECIMAL(p,s) when 1 <= p <= 38 and 0 <= s <= p. Unbounded NUMBER,
negative scale, and precision above 38 require review and return
supported=False. Mappings that need a human choice must not be provisioned
silently.
"""

from __future__ import annotations

import re

from pydantic import BaseModel

from thirdparty.oracle.utils import OracleTableColumn
from thirdparty.sap.utils import SAPFieldMetadata


class FabricTypeMapping(BaseModel):
    source_type: str
    fabric_type: str | None
    supported: bool
    lossy: bool = False
    warning: str | None = None


def map_oracle_column(column: OracleTableColumn) -> FabricTypeMapping:
    source = (column.data_type or column.datatype).upper().strip()
    base = re.match(r"^[A-Z][A-Z0-9_]*", source)
    kind = base.group() if base else source
    if kind in {"VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB", "LONG"}:
        return FabricTypeMapping(source_type=column.datatype, fabric_type="STRING", supported=True)
    if kind in {"RAW", "BLOB"}:
        return FabricTypeMapping(source_type=column.datatype, fabric_type="BINARY", supported=True)
    if kind == "DATE":
        # Oracle DATE includes a time component.
        return FabricTypeMapping(source_type=column.datatype, fabric_type="TIMESTAMP", supported=True)
    if source.startswith("TIMESTAMP"):
        if "TIME ZONE" in source:
            return FabricTypeMapping(source_type=column.datatype, fabric_type=None,
                                     supported=False, warning="Time zone conversion requires a policy")
        return FabricTypeMapping(source_type=column.datatype, fabric_type="TIMESTAMP", supported=True)
    if kind in {"FLOAT", "BINARY_DOUBLE"}:
        return FabricTypeMapping(source_type=column.datatype, fabric_type="DOUBLE", supported=True,
                                 lossy=True, warning="Floating point values are approximate")
    if kind == "BINARY_FLOAT":
        return FabricTypeMapping(source_type=column.datatype, fabric_type="FLOAT", supported=True,
                                 lossy=True, warning="Floating point values are approximate")
    if kind == "NUMBER":
        precision, scale = column.precision, column.scale
        if precision is None:
            match = re.search(r"NUMBER\((\d+)(?:,\s*(-?\d+))?\)", column.datatype.upper())
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2) or 0)
        if precision is not None and scale is None:
            scale = 0
        if precision is None or scale is None or precision > 38 or scale < 0 or scale > precision:
            return FabricTypeMapping(source_type=column.datatype, fabric_type=None,
                                     supported=False, warning="NUMBER precision or scale needs review")
        return FabricTypeMapping(source_type=column.datatype,
                                 fabric_type=f"DECIMAL({precision},{scale})", supported=True)
    return FabricTypeMapping(source_type=column.datatype, fabric_type=None,
                             supported=False, warning="Unsupported Oracle datatype")


def map_oracle_column_to_fabric(column: OracleTableColumn) -> FabricTypeMapping:
    """Map Oracle source metadata to a Fabric type using the shared policy."""
    return map_oracle_column(column)


def map_sap_field(field: SAPFieldMetadata) -> FabricTypeMapping:
    kind = field.datatype.upper().strip()
    source = f"{kind}({field.leng},{field.decimals})"
    simple = {
        "CHAR": "STRING", "NUMC": "STRING", "CLNT": "STRING", "LANG": "STRING",
        "UNIT": "STRING", "CUKY": "STRING", "STRING": "STRING", "SSTRING": "STRING",
        "DATS": "DATE", "TIMS": "STRING", "INT1": "SMALLINT", "INT2": "SMALLINT",
        "INT4": "INT", "INT8": "BIGINT", "RAW": "BINARY", "LRAW": "BINARY",
    }
    if kind in simple:
        return FabricTypeMapping(source_type=source, fabric_type=simple[kind], supported=True)
    if kind in {"DEC", "CURR", "QUAN"}:
        if field.leng < 1 or field.leng > 38 or field.decimals < 0 or field.decimals > field.leng:
            return FabricTypeMapping(source_type=source, fabric_type=None, supported=False,
                                     warning="Decimal precision or scale needs review")
        return FabricTypeMapping(source_type=source,
                                 fabric_type=f"DECIMAL({field.leng},{field.decimals})", supported=True)
    if kind == "FLTP":
        return FabricTypeMapping(source_type=source, fabric_type="DOUBLE", supported=True,
                                 lossy=True, warning="Floating point values are approximate")
    return FabricTypeMapping(source_type=source, fabric_type=None, supported=False,
                             warning="Unsupported SAP datatype")


def map_sap_field_to_fabric(field: SAPFieldMetadata) -> FabricTypeMapping:
    """Return an explicit supported or unsupported Fabric mapping for an SAP field."""
    return map_sap_field(field)
