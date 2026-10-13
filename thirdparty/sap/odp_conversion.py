"""Closed post-extraction conversion contract for ODP records; never native SQL."""
import json
import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from .extraction import recognized_conversion
from .transport import decode_extracted_value as decode_value


def conversion_contract(expression, field):
    if not expression:
        return None
    match=recognized_conversion(expression,field)
    if not match or match[0].startswith('LEGACY_'):
        raise ValueError('ODP supports direct fields or the approved closed conversion kinds, not native SQL casts')
    result=dict(version=1,stage='ODP_POST_EXTRACT',field=field,kind=match[0])
    if match[0]=='BOOLEAN':result.update(true_value=match[1],false_value=match[2])
    return json.dumps(result,sort_keys=True)


def decode_odp_row(row,columns):
    output={}
    for column in columns:
        field=column['column_name']; target=column.get('target_column_name') or field
        if field not in row:raise ValueError('ODP record is missing an approved field')
        value=row[field]; expression=column.get('source_expression')
        if expression:
            contract=json.loads(expression)
            required={'version','stage','field','kind'}
            expected=required|({'true_value','false_value'} if contract.get('kind')=='BOOLEAN' else set())
            if set(contract)!=expected or contract['version']!=1 or contract['stage']!='ODP_POST_EXTRACT' or contract['field']!=field:
                raise ValueError('Invalid ODP conversion contract')
            kind=contract['kind']
            if value is not None:
                text=str(value)
                if kind=='DATE':
                    if text=='00000000':value=None
                    elif isinstance(value,date) and not isinstance(value,datetime):pass
                    elif re.fullmatch(r'\d{8}',text):value=datetime.strptime(text,'%Y%m%d').date()
                    else:raise ValueError('Invalid ODP date')
                elif kind=='TIME':
                    if not re.fullmatch(r'([01][0-9]|2[0-3])[0-5][0-9][0-5][0-9]',text):raise ValueError('Invalid ODP time')
                    value=f'{text[:2]}:{text[2:4]}:{text[4:]}'
                elif kind in {'LONG_TIMESTAMP','TIMESTAMP'}:
                    if isinstance(value,float):raise ValueError('Packed ODP timestamps must not arrive as JSON floats')
                    try:number=Decimal(text)
                    except InvalidOperation:raise ValueError('Invalid ODP packed timestamp') from None
                    scale=7 if kind=='LONG_TIMESTAMP' else 0
                    if not number.is_finite() or number<0 or number>=Decimal('1e14') or number!=number.quantize(Decimal(1).scaleb(-scale)):
                        raise ValueError('ODP timestamp precision/range mismatch')
                    packed=f'{number:0{22 if scale else 14}.{scale}f}'
                    value=packed if scale else datetime.strptime(packed,'%Y%m%d%H%M%S')
                elif kind=='BOOLEAN':
                    if text==contract['true_value']:value=True
                    elif text.strip()==contract['false_value']:value=False
                    else:raise ValueError('Invalid ODP Boolean domain value')
                elif kind=='STRING':value=text
                else:raise ValueError('Unsupported ODP conversion kind')
        dtype=column['fabric_data_type']
        if isinstance(value,str) and not value.strip() and dtype not in {'STRING','BINARY'}:
            raise ValueError('Blank ODP scalar is not an approved NULL representation')
        decoded=decode_value(value,dtype)
        if decoded is not None and dtype.startswith('DECIMAL('):
            precision,scale=map(int,re.fullmatch(r'DECIMAL\((\d+),(\d+)\)',dtype).groups())
            if not decoded.is_finite() or abs(decoded)>=Decimal(10)**(precision-scale) or decoded!=decoded.quantize(Decimal(1).scaleb(-scale)):
                raise ValueError('ODP decimal exceeds the approved precision/scale')
        output[target]=decoded
    return output
