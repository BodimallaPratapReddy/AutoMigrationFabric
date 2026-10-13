"""Render approved Oracle projection expressions; never infer datatype mappings."""

def quote_identifier(name: str) -> str:
    if not name or len(name) > 128 or any(ord(c) < 32 for c in name):
        raise ValueError("Invalid Oracle extraction identifier")
    return '"' + name.replace('"', '""') + '"'


def build_extract_sql(table: dict, columns: list[dict]) -> str:
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import SqlglotError

    if table['source_system_type'] != 'ORACLE' or table['source_object_type'] != 'TABLE':
        raise ValueError('Extraction SELECT is supported for Oracle base tables only')
    owner, name = table.get('source_schema_name'), table['source_table_name']
    if not owner:
        raise ValueError('Oracle extraction requires an explicit source schema')
    selected = sorted((c for c in columns if c.get('is_selected', True)), key=lambda c: c['sno'])
    if not selected:
        raise ValueError('Oracle extraction requires selected columns')
    targets = [c.get('target_column_name') or c['column_name'] for c in selected]
    if len({n.upper() for n in targets}) != len(targets):
        raise ValueError('Extraction output aliases must be unique')
    available = {c['column_name'] for c in columns}
    projections = []
    for column, target in zip(selected, targets, strict=True):
        expression = column.get('source_expression') or quote_identifier(column['column_name'])
        if any(token in expression for token in (';', '--', '/*', '*/')) or len(expression) > 16000:
            raise ValueError('Extraction expressions cannot contain statements or comments')
        try:
            trees = sqlglot.parse('SELECT ' + expression, read='oracle')
            tree = trees[0]
        except (SqlglotError, IndexError):
            raise ValueError('Invalid Oracle extraction expression') from None
        if (len(trees) != 1 or not isinstance(tree, exp.Select) or len(tree.expressions) != 1
                or isinstance(tree.expressions[0], exp.Alias)
                or any(value for key, value in tree.args.items() if key != 'expressions')
                or tree.find(exp.Subquery, exp.Star, exp.Window, exp.AggFunc)
                or len(list(tree.find_all(exp.Select))) != 1):
            raise ValueError('Provide one scalar Oracle expression without an alias or subquery')
        for ref in tree.find_all(exp.Column):
            if ref.name not in available or ref.table or ref.db or ref.catalog:
                raise ValueError('Extraction expressions must reference discovered source columns without qualifiers')
        projections.append(f'    {expression} AS {quote_identifier(target)}')
    sql = 'SELECT\n' + ',\n'.join(projections) + f'\nFROM {quote_identifier(owner)}.{quote_identifier(name)}'
    if len(sql.encode('utf-8')) > 256000:
        raise ValueError('Oracle extraction SELECT exceeds 256 KB')
    return sql


def attach_extract_sql(plan) -> str:
    sql = build_extract_sql(plan.table.model_dump(), [c.model_dump() for c in plan.columns])
    plan.table.extract_sql = sql
    return sql
