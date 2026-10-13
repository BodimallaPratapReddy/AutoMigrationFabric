"""Approved native SQL projection for the application's z_execute_sql endpoint.

The endpoint executes database SQL, not ABAP Open SQL. Quote identifiers and
emit a single line for its SELECT guard. Conversion support is intentionally
limited; the discovery activity verifies the complete query with a zero-row read.
"""
import re


def identifier(value: str) -> str:
    if not isinstance(value, str) or not re.fullmatch(r'[A-Z0-9_/$]+', value):
        raise ValueError('Use an exact uppercase SAP DDIC identifier')
    return value


def quoted(value: str) -> str:
    return '"' + identifier(value) + '"'


def conversion_expression(field: str, kind: str, true_value: str = 'X', false_value: str = '') -> str:
    """Closed native HANA templates; no arbitrary executable agent SQL."""
    q = quoted(field)
    if kind == 'DATE':
        return (f"TO_DATE(CASE WHEN {q} IS NULL OR {q} = '00000000' THEN NULL "
                f"WHEN {q} LIKE_REGEXPR '^[0-9]{{8}}$' THEN {q} "
                f"ELSE {q} || '_INVALID_DATE' END, 'YYYYMMDD')")
    if kind == 'LEGACY_DATE':
        return f"TO_DATE(NULLIF({q}, '00000000'), 'YYYYMMDD')"
    if kind == 'TIME':
        return (f"TO_VARCHAR(TO_TIME(CASE WHEN {q} IS NULL THEN NULL WHEN {q} LIKE_REGEXPR '^([01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]$' "
                f"THEN {q} ELSE {q} || '_INVALID_TIME' END, 'HH24MISS'), 'HH24:MI:SS')")
    if kind == 'LEGACY_TIME':
        return f"TO_VARCHAR(TO_TIME({q}, 'HH24MISS'), 'HH24:MI:SS')"
    if kind == 'TIMESTAMP':
        return f"TO_TIMESTAMP(TO_VARCHAR({q}, '00000000000000'), 'YYYYMMDDHH24MISS')"
    if kind == 'LONG_TIMESTAMP':
        # Keep the complete packed value, including initial zero timestamps.
        # Parsing as a Spark timestamp would silently lose the seventh digit.
        return f"TO_VARCHAR({q}, '00000000000000.0000000')"
    if kind == 'STRING':
        return f'TO_VARCHAR({q})'
    if kind == 'BOOLEAN':
        if not re.fullmatch(r'[A-Z0-9]', true_value) or not re.fullmatch(r'[A-Z0-9]?', false_value) or true_value == false_value:
            raise ValueError('Boolean conversion requires distinct, verified single-character domain values')
        false_condition = f"TRIM({q}) = ''" if not false_value else f"{q} = '{false_value}'"
        # Unknown values must fail, not disappear into NULL or generic truthiness.
        return (f"CASE WHEN {q} IS NULL THEN NULL WHEN {q} = '{true_value}' THEN TRUE "
                f"WHEN {false_condition} THEN FALSE ELSE CAST({q} || '_INVALID_BOOLEAN' AS BOOLEAN) END")
    raise ValueError('Unsupported SAP conversion kind')


def recognized_conversion(expression: str, field: str) -> tuple[str, str | None, str | None] | None:
    """Normalize formatting only outside SQL literals, then match closed templates."""
    def normalize(sql):
        pieces = re.split(r"('(?:''|[^'])*')", sql)
        return ''.join(p if i % 2 else re.sub(r'\s+', '', p).upper() for i, p in enumerate(pieces))
    candidate = normalize(expression)
    for kind in ('DATE', 'TIME', 'TIMESTAMP', 'LONG_TIMESTAMP', 'STRING','LEGACY_DATE','LEGACY_TIME'):
        if candidate == normalize(conversion_expression(field, kind)):
            return kind, None, None
    match = re.search(r"THEN\s+NULL\s+WHEN\s+\"?[A-Z0-9_/$]+\"?\s*=\s*'([A-Z0-9])'", expression, re.I)
    if match:
        for false_value in ('', 'N', '0'):
            if candidate == normalize(conversion_expression(field, 'BOOLEAN', match[1], false_value)):
                return 'BOOLEAN', match[1], false_value
    return None


def build_extract_sql(table: dict, columns: list[dict]) -> str:
    if table['source_system_type'] != 'SAP_ECC' or table['source_object_type'] not in {'TABLE','CDS_VIEW','DB_VIEW'}:
        raise ValueError('SAP extraction SQL requires a table or connector-readable source view')
    name = identifier(table['source_table_name'])
    selected = sorted((c for c in columns if c.get('is_selected', True)), key=lambda c: c['sno'])
    if not selected:
        raise ValueError('SAP extraction requires selected columns')
    available = {identifier(c['column_name']) for c in columns}
    aliases, projections = set(), []
    for column in selected:
        source = identifier(column['column_name'])
        alias = column.get('target_column_name') or source
        if not re.fullmatch(r'[A-Z_/][A-Z0-9_/$]{0,29}', alias) or alias in aliases:
            raise ValueError('SAP extraction aliases must be unique uppercase DDIC identifiers, at most 30 characters')
        aliases.add(alias)
        expression = column.get('source_expression') or source
        conversion = recognized_conversion(expression, source)
        if conversion:
            expression = conversion_expression(source, conversion[0], conversion[1] or 'X', conversion[2] or '')
        elif expression in available:
            expression = quoted(expression)
        else:
            cast = re.fullmatch(r'CAST\(\s*"?([A-Z0-9_/$]+)"?\s+AS\s+(VARCHAR|NVARCHAR|DECIMAL|SMALLINT|INTEGER|BIGINT|DOUBLE)(?:\((\d+)(?:\s*,\s*(\d+))?\))?\s*\)', expression)
            if not cast or cast[1] not in available:
                raise ValueError('SAP SourceExpression must be a discovered field or a supported native SQL scalar CAST')
            kind, precision, scale = cast[2], cast[3], cast[4]
            if kind in {'VARCHAR','NVARCHAR'}:
                if precision is None or not 1 <= int(precision) <= 5000 or scale is not None:
                    raise ValueError('Native text casts require a supported length from 1 to 5000')
            elif kind == 'DECIMAL':
                if precision is None or not 1 <= int(precision) <= 38 or not 0 <= int(scale or 0) <= int(precision):
                    raise ValueError('Native decimal cast precision/scale is unsupported')
            elif precision is not None:
                raise ValueError('This native cast type does not accept a length')
            arguments = f'({precision}' + (f',{scale}' if scale is not None else '') + ')' if precision is not None else ''
            expression = f'CAST({quoted(cast[1])} AS {kind}{arguments})'
        projections.append(f'{expression} AS {quoted(alias)}')
    query = 'SELECT ' + ', '.join(projections) + f' FROM {quoted(name)}'
    if len(query.encode()) > 256000:
        raise ValueError('SAP extraction SQL exceeds 256 KB')
    return query
