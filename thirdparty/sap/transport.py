"""Decode the custom SAP JSON endpoint's values before typed staging.

External pipelines may use this adapter; provisioning does not load business rows.
The endpoint serializes SQL NULL DATE/BOOLEAN as empty text and booleans as 1/0.
Never apply those sentinel rules to arbitrary STRING columns.
"""
from datetime import date, datetime
from decimal import Decimal
import re


def decode_extracted_value(value, fabric_type: str, conversion_kind: str | None = None):
    if value is None:
        return None
    scalar = fabric_type in {'DATE','TIMESTAMP','BOOLEAN','SMALLINT','INT','BIGINT','DOUBLE','FLOAT'} or fabric_type.startswith('DECIMAL(')
    if (scalar or conversion_kind in {'LONG_TIMESTAMP','TIME','LEGACY_TIME'}) and isinstance(value, str) and not value.strip():
        return None
    if fabric_type == 'DATE':
        return value if isinstance(value, date) else date.fromisoformat(value.strip())
    if fabric_type == 'BOOLEAN':
        if isinstance(value, bool):
            return value
        text = str(value).strip().lower()
        if text in {'1','true'}: return True
        if text in {'0','false'}: return False
        raise ValueError('Invalid extracted BOOLEAN value')
    if fabric_type == 'TIMESTAMP':
        if isinstance(value, str):
            fraction = re.search(r'\.(\d+)', value)
            if fraction and any(d != '0' for d in fraction[1][6:]):
                raise ValueError('TIMESTAMP transport would lose fractional precision; use exact STRING')
        return value if isinstance(value, datetime) else datetime.fromisoformat(value.strip())
    if fabric_type.startswith('DECIMAL('):
        if isinstance(value, float):
            raise ValueError('Exact decimal transport must not use floating-point JSON values')
        return Decimal(str(value))
    if fabric_type in {'SMALLINT','INT','BIGINT'}:
        if isinstance(value, float): raise ValueError('Integer transport must preserve exact digits')
        return int(value)
    if fabric_type == 'STRING':
        if not isinstance(value, str): raise ValueError('STRING extraction requires exact text transport')
        return value
    if fabric_type == 'BINARY':
        if not isinstance(value, (bytes, bytearray)):
            raise ValueError('BINARY text transport requires an explicit verified encoding decoder')
        return bytes(value)
    if fabric_type in {'DOUBLE','FLOAT'}:
        return float(value)
    return value


def decode_extracted_row(row: dict, columns: list[dict]) -> dict:
    """Apply approved column types/expressions to a custom-endpoint result row."""
    from .extraction import recognized_conversion
    output = {}
    for column in columns:
        if not column.get('is_selected', True): continue
        name = column.get('target_column_name') or column['column_name']
        conversion = recognized_conversion(column.get('source_expression') or '', column['column_name'])
        output[name] = decode_extracted_value(row[name], column['fabric_data_type'], conversion[0] if conversion else None)
    return output
