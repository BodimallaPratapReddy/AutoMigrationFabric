import copy
import json
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from thirdparty.oracle.extraction import build_extract_sql
from thirdparty.configdb.repository import ApprovedRuntimePlan, ConfigDBRepository, _runtime_fingerprint
from app.migration.opencode_plan import adapt_plan
from test_oracle_view_integration import BUNDLE, SOURCE, adapt_view_bundle


TABLE = dict(source_system_type='ORACLE', source_object_type='TABLE', source_schema_name='ONT', source_table_name='ORDERS')


def test_preserves_exact_expressions_order_aliases_and_deferred_blobs():
    sql = build_extract_sql(TABLE, [
        dict(sno=3, column_name='DOC', target_column_name='DOC_FILE_PATH', source_expression='CAST(NULL AS VARCHAR2(4000))'),
        dict(sno=1, column_name='ID', source_expression="TO_CHAR(ID, 'TM9', 'NLS_NUMERIC_CHARACTERS=''.,''')"),
        dict(sno=2, column_name='TS', source_expression="TO_CHAR(TS, 'YYYY-MM-DD\"T\"HH24:MI:SS.FF9')"),
        dict(sno=4, column_name='IGNORED', is_selected=False)])
    assert sql.startswith('SELECT\n    TO_CHAR(ID')
    assert sql.index('AS "ID"') < sql.index('AS "TS"') < sql.index('AS "DOC_FILE_PATH"')
    assert 'CAST(NULL AS VARCHAR2(4000)) AS "DOC_FILE_PATH"' in sql
    assert sql.endswith('FROM "ONT"."ORDERS"')
    assert 'IGNORED' not in sql and 'WHERE' not in sql and ';' not in sql


@pytest.mark.parametrize('expr', ['ID; DELETE FROM ORDERS', 'ID FROM OTHER',
    '(SELECT ID FROM OTHER)', 'ID AS OTHER', 'SUM(ID)', '*', 'UNKNOWN', 't.ID', 'ID -- comment'])
def test_rejects_non_scalar_or_unverified_expressions(expr):
    with pytest.raises(ValueError):
        build_extract_sql(TABLE, [dict(sno=1, column_name='ID', source_expression=expr)])


def test_quotes_identifiers_and_detects_duplicate_outputs():
    table={**TABLE, 'source_table_name':'odd"table'}
    assert '"odd""table"' in build_extract_sql(table, [dict(sno=1,column_name='Odd Name')])
    with pytest.raises(ValueError, match='unique'):
        build_extract_sql(TABLE, [dict(sno=1,column_name='ID'), dict(sno=2,column_name='X',target_column_name='ID')])


def test_table_and_view_adapters_save_the_same_projection_contract():
    view = adapt_view_bundle(json.dumps(BUNDLE), SOURCE, 'Bronze')
    table = adapt_plan(json.dumps({k:v for k,v in BUNDLE['tables'][0].items() if k!='target_table'}),
        {**SOURCE, 'migration_approach':'ORACLE_TABLE', 'source_object_name':'T'}, 'Bronze')
    assert view['tables'][0]['table']['extract_sql'] == table['table_plan']['table']['extract_sql']
    assert table['review']['extract_sql'].startswith('SELECT\n    TO_CHAR(ID) AS "ID"')
    assert view['review']['table_reviews'][0]['extract_sql'] == table['review']['extract_sql']


def test_persistence_writes_extract_sql_and_rejects_divergent_query():
    bundle=adapt_view_bundle(json.dumps(BUNDLE), SOURCE, 'Bronze')
    from uuid import uuid4
    payload=dict(plan_guid=str(uuid4()), plan_version=1, tables=bundle['tables'], views=[])
    plan=ApprovedRuntimePlan.model_validate(payload)
    connection=MagicMock()
    cursor=connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect=[('APPROVED',1,'reviewer',datetime.now(),None),None]
    cursor.rowcount=1
    with patch('thirdparty.configdb.utils.load_dotenv'):
        repo=ConfigDBRepository('fake')
    with patch.object(repo,'connect',return_value=connection): repo.persist_approved_runtime_plan(plan)
    statement=next(c.args for c in cursor.execute.call_args_list if 'INSERT INTO bronze_replication.SourceTables' in c.args[0])
    assert 'ExtractSql' in statement[0] and plan.tables[0].table.extract_sql in statement[1]
    bad=copy.deepcopy(payload)
    bad['tables'][0]['table']['extract_sql']='SELECT * FROM OTHER'
    with pytest.raises(ValueError,match='ExtractSql'): ApprovedRuntimePlan.model_validate(bad)


def test_nullable_field_preserves_legacy_runtime_fingerprint():
    from uuid import uuid4
    from hashlib import sha256
    bundle=adapt_view_bundle(json.dumps(BUNDLE), SOURCE, 'Bronze')
    bundle['tables'][0]['table'].pop('extract_sql')
    plan=ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,tables=bundle['tables'],views=[]))
    old=plan.model_dump(mode='json')
    for table in old['tables']:
        for key in ('extract_sql','description','etrm_description'): table['table'].pop(key,None)
        for column in table['columns']: column.pop('etrm_description',None)
        for field in ('delete_policy','key_validation'):
            if table['replication'].get(field) is None: table['replication'].pop(field,None)
    old.update(dependency_indices=[],parent_source_indices=[None])
    assert _runtime_fingerprint(plan)==sha256(json.dumps(old,sort_keys=True,separators=(',',':')).encode()).hexdigest()


@pytest.mark.parametrize('legacy', [False, True])
def test_actual_repair_activity_preserves_approved_replication_and_extract_sql(monkeypatch, legacy):
    from app.migration.opencode_activities import opencode_validate
    from app.migration import oracle_view_targets
    monkeypatch.setattr(oracle_view_targets,'check_oracle_view_targets',lambda _: None)
    previous=adapt_view_bundle(json.dumps(BUNDLE),SOURCE,'Bronze')
    previous['tables'][0]['replication']['key_validation']={'valid':True,'columns':['ID']}
    if legacy: previous['tables'][0]['table'].pop('extract_sql')
    revised=copy.deepcopy(BUNDLE)
    revised['views'][1]['spark_sql']='SELECT ID FROM bronze.T'
    source={**SOURCE,'plan_guid':'plan','view_correction':dict(previous=previous,eligible=['bronze.V_BASE'])}
    result=opencode_validate(dict(input=source,lakehouse_name='Bronze',text=json.dumps(revised)))
    assert 'validation_error' not in result
    assert result['tables']==previous['tables']
