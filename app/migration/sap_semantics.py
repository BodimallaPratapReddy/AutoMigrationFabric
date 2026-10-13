"""Read verified Data Element/Domain links for tables or extract structures."""
from thirdparty.sap.extraction import identifier


def read_semantics(client, columns):
    elements = sorted({identifier(c.rollname) for c in columns if c.rollname})
    if not elements:
        return {}
    values = ','.join("'" + e + "'" for e in elements)
    definitions = client.execute_sql_query(
        "SELECT ROLLNAME, DOMNAME FROM DD04L WHERE AS4LOCAL = 'A' AND AS4VERS = '0000' "
        f"AND ROLLNAME IN ({values})", max_rows=1000)
    if not definitions.success or definitions.truncated:
        raise ValueError('SAP data-element metadata was unavailable or truncated')
    semantics = {r['ROLLNAME']: {'domain': r['DOMNAME'], 'fixed_values': []} for r in definitions.rows}
    domains = sorted({identifier(semantics[c.rollname]['domain']) for c in columns
        if c.datatype == 'CHAR' and c.leng == 1 and c.rollname in semantics and semantics[c.rollname]['domain']})
    if domains:
        names = ','.join("'" + d + "'" for d in domains)
        fixed = client.execute_sql_query("SELECT DOMNAME, DOMVALUE_L, DOMVALUE_H FROM DD07L "
            f"WHERE AS4LOCAL = 'A' AND AS4VERS = '0000' AND DOMNAME IN ({names})", max_rows=1000)
        if not fixed.success or fixed.truncated:
            raise ValueError('SAP domain values were unavailable or truncated')
        for definition in semantics.values():
            definition['fixed_values'] = [r['DOMVALUE_L'] for r in fixed.rows
                if r['DOMNAME'] == definition['domain'] and not str(r['DOMVALUE_H']).strip()]
    return semantics
