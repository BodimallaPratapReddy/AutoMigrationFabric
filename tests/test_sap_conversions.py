import copy
import json
from datetime import date
import pytest
from thirdparty.sap.extraction import build_extract_sql, conversion_expression, recognized_conversion
from thirdparty.sap.transport import decode_extracted_value
from thirdparty.sap.transport import decode_extracted_row
from app.migration.opencode_sap_plan import adapt_sap_plan


def test_closed_conversion_templates_and_injection_rejection():
    for kind in ['DATE','TIME','TIMESTAMP','LONG_TIMESTAMP','BOOLEAN','STRING']:
        expression=conversion_expression('F',kind)
        assert recognized_conversion(expression,'F')[0] == kind
        sql=build_extract_sql(dict(source_system_type='SAP_ECC',source_object_type='TABLE',source_table_name='T'),
            [dict(sno=1,column_name='F',source_expression=expression)])
        assert expression in sql
        assert recognized_conversion(expression,'OTHER') is None
        with pytest.raises(ValueError):
            build_extract_sql(dict(source_system_type='SAP_ECC',source_object_type='TABLE',source_table_name='T'),
                [dict(sno=1,column_name='F',source_expression=expression+'; DELETE FROM T')])


def test_namespace_field_is_preserved_as_a_quoted_native_alias():
    table=dict(source_system_type='SAP_ECC',source_object_type='TABLE',source_table_name='BKPF')
    assert build_extract_sql(table,[dict(sno=1,column_name='/SAPF15/STATUS')])=='SELECT "/SAPF15/STATUS" AS "/SAPF15/STATUS" FROM "BKPF"'
    for alias in ['/SAPF15/STATUS"; DELETE FROM BKPF','F--COMMENT']:
        with pytest.raises(ValueError):build_extract_sql(table,[dict(sno=1,column_name='F',target_column_name=alias)])


def test_json_endpoint_null_boolean_and_long_timestamp_transport():
    assert decode_extracted_value('', 'DATE') is None
    assert decode_extracted_value('2026-10-12','DATE') == date(2026,10,12)
    assert decode_extracted_value('1 ','BOOLEAN') is True
    assert decode_extracted_value('0 ','BOOLEAN') is False
    assert decode_extracted_value('00:00:00','STRING') == '00:00:00'
    assert decode_extracted_value('20261012143025.1234567','STRING') == '20261012143025.1234567'
    assert decode_extracted_value('','STRING') == ''
    with pytest.raises(ValueError): decode_extracted_value('X','BOOLEAN')
    with pytest.raises(ValueError): decode_extracted_value(9007199254740992.0,'BIGINT')
    with pytest.raises(ValueError): decode_extracted_value('2026-10-12 14:30:25.1234567','TIMESTAMP')
    with pytest.raises(ValueError): decode_extracted_value('aGVsbG8=','BINARY')


def test_approved_row_decoder_preserves_aliases_and_typed_nulls():
    cols=[dict(column_name='D',fabric_data_type='DATE'),
          dict(column_name='L',target_column_name='LONG_TIME',fabric_data_type='STRING',source_expression=conversion_expression('L','LONG_TIMESTAMP')),
          dict(column_name='S',fabric_data_type='STRING'),dict(column_name='B',fabric_data_type='BOOLEAN')]
    assert decode_extracted_row({'D':'','LONG_TIME':'','S':'','B':'0 '},cols)=={'D':None,'LONG_TIME':None,'S':'','B':False}


def test_semantic_timestamp_precision_and_boolean_domain_guards():
    from tests.test_opencode_sap import SOURCE, META, PLAN
    meta=copy.deepcopy(META); plan=copy.deepcopy(PLAN)
    meta['schema']['columns'][3].update(rollname='FLAG_ELEMENT')
    meta['semantic_metadata']={'FLAG_ELEMENT':dict(domain='XFELD',fixed_values=['X',''])}
    plan['columns'][3].update(fabric_type='BOOLEAN',source_expression=conversion_expression('LOEKZ','BOOLEAN'))
    result=adapt_sap_plan(json.dumps(plan),{**SOURCE,'source_sap_metadata':meta},'Bronze')
    assert result['table_plan']['columns'][3]['fabric_data_type']=='BOOLEAN'
    meta['semantic_metadata']['FLAG_ELEMENT']['fixed_values']=['Y','N']
    with pytest.raises(ValueError,match='domain fixed values'):
        adapt_sap_plan(json.dumps(plan),{**SOURCE,'source_sap_metadata':meta},'Bronze')
    meta['schema']['columns'][3].update(rollname='TIMESTAMPL',datatype='DEC',leng=21,decimals=7)
    plan['columns'][3].update(source_type='DEC(21,7)',fabric_type='TIMESTAMP',source_expression=conversion_expression('LOEKZ','TIMESTAMP'))
    with pytest.raises(ValueError,match='seven-digit'):
        adapt_sap_plan(json.dumps(plan),{**SOURCE,'source_sap_metadata':meta},'Bronze')
    plan['columns'][3].update(fabric_type='STRING',source_expression=conversion_expression('LOEKZ','LONG_TIMESTAMP'))
    assert adapt_sap_plan(json.dumps(plan),{**SOURCE,'source_sap_metadata':meta},'Bronze')['table_plan']['columns'][3]['fabric_data_type']=='STRING'
