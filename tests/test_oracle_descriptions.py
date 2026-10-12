import copy
import json
from uuid import uuid4

from app.migration.opencode_plan import adapt_plan
from test_oracle_view_integration import SOURCE, BUNDLE, adapt_view_bundle
from test_provisioning_notebook import load_notebook


def test_agent_table_descriptions_stay_separate():
    plan={k:v for k,v in copy.deepcopy(BUNDLE['tables'][0]).items() if k!='target_table'}
    plan.update(description='Oracle database table comment',etrm_description='Detailed ETRM table description')
    plan['columns'][0].update(description='Oracle column comment',etrm_description='Detailed ETRM column description')
    result=adapt_plan(json.dumps(plan),{**SOURCE,'migration_approach':'ORACLE_TABLE','source_object_name':'T'},'Bronze')
    assert result['table_plan']['table']['description']==plan['description']
    assert result['table_plan']['table']['etrm_description']==plan['etrm_description']
    assert result['table_plan']['columns'][0]['description']=='Oracle column comment'
    assert result['table_plan']['columns'][0]['etrm_description']=='Detailed ETRM column description'


def test_view_tables_preserve_dictionary_comments_and_etrm_text():
    source=copy.deepcopy(SOURCE);bundle=copy.deepcopy(BUNDLE)
    source['source_view_graph']['tables']['ONT.T']['description']='Verified Oracle table comment'
    source['source_view_graph']['tables']['ONT.T']['columns'][0]['description']=None
    bundle['tables'][0].update(description='Agent summary',etrm_description='ETRM table text')
    bundle['tables'][0]['columns'][0].update(description='ETRM must not go here',etrm_description='ETRM column text')
    result=adapt_view_bundle(json.dumps(bundle),source,'Bronze')
    assert result['tables'][0]['table']['description']=='Verified Oracle table comment'
    assert result['tables'][0]['columns'][0]['description'] is None
    assert result['tables'][0]['columns'][0]['etrm_description']=='ETRM column text'


def test_notebook_comments_only_database_descriptions_and_escapes_quotes():
    m=load_notebook()
    row=dict(GUID=str(uuid4()),PlanGUID=str(uuid4()),FabricWorkspaceId='ws',FabricLakehouseName='Lake',
        FabricLakehouseSchema='ONT',FabricTableName='T',ProvisioningStatus='PENDING',IsActive=True,
        Description="Owner's orders",EtrmDescription='ETRM TABLE ONLY')
    cols=[dict(Sno=1,ColumnName='ID',FabricDataType='STRING',IsNullable=True,
        Description="Customer's identifier",EtrmDescription='ETRM COLUMN ONLY')]
    sql=m.build_create_table_sql(m.build_table_spec(row,cols))
    assert 'COMMENT '+m.sql_literal(row['Description']) in sql
    assert 'COMMENT '+m.sql_literal(cols[0]['Description']) in sql
    assert 'ETRM' not in sql
    row['Description']=None;cols[0]['Description']=None
    sql=m.build_create_table_sql(m.build_table_spec(row,cols))
    assert 'COMMENT' not in sql and 'ETRM' not in sql


def test_oracle_table_comment_is_read_from_dictionary_with_binds():
    from thirdparty.oracle.utils import OracleClient
    c=OracleClient.__new__(OracleClient)
    calls=[]
    def execute(sql,parameters):
        calls.append((sql,parameters))
        return json.dumps(dict(columns=['COMMENTS'],rows=[['  Original comment  ']],row_count=1))
    c.execute_sql=execute
    assert c.get_table_description('ont','t')=='  Original comment  '
    assert 'all_tab_comments' in calls[0][0]
    assert calls[0][1]=={'owner':'ONT','table_name':'T'}


def test_table_activity_verifies_database_comments_without_replacing_etrm(monkeypatch):
    from types import SimpleNamespace
    from app.migration import source_connections
    from app.migration.opencode_activities import opencode_validate
    monkeypatch.setattr(source_connections,'oracle_client',lambda _: SimpleNamespace(
        get_table_schema=lambda *_:[SimpleNamespace(column_name='ID',description=None)],
        get_table_description=lambda *_:'Original Oracle table comment'))
    plan={k:v for k,v in copy.deepcopy(BUNDLE['tables'][0]).items() if k!='target_table'}
    plan.update(description='Inferred agent summary',etrm_description='ETRM table text')
    plan['columns'][0].update(description='Wrong inferred comment',etrm_description='ETRM column text')
    result=opencode_validate(dict(text=json.dumps(plan),input={**SOURCE,'migration_approach':'ORACLE_TABLE','source_object_name':'T'},lakehouse_name='Bronze'))
    assert result['table_plan']['table']['description']=='Original Oracle table comment'
    assert result['table_plan']['columns'][0]['description'] is None
    assert result['table_plan']['columns'][0]['etrm_description']=='ETRM column text'
