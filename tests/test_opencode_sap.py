import asyncio
import copy
import json
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer
from app.migration.opencode_sap_plan import adapt_sap_plan
from app.migration.workflow_sap_opencode import OpenCodeSAPTableMigrationWorkflow
from thirdparty.configdb.repository import ApprovedRuntimePlan
from thirdparty.sap.extraction import build_extract_sql, conversion_expression


SOURCE=dict(migration_approach='SAP_TABLE',discovery_provider='OPENCODE',source_connection_name='SAP ECC Local',
    source_object_name='VBAK',fabric_workspace_id=str(uuid4()),fabric_lakehouse_id=str(uuid4()),fabric_schema_name='SAP')
FIELDS=[dict(fieldname=name,position=i+1,keyflag='X' if name in ('MANDT','VBELN') else '',
    rollname='',datatype=kind,leng=length,decimals=0,ddtext='SAP '+name,checktable='',reftable='',reffield='')
    for i,(name,kind,length) in enumerate([('MANDT','CLNT',3),('VBELN','CHAR',10),('AEDAT','DATS',8),('LOEKZ','CHAR',1)])]
META=dict(schema=dict(table_name='VBAK',columns=FIELDS,primary_key_columns=['MANDT','VBELN']),
    watermark_candidates=[dict(field_name='AEDAT',datatype='DATS',reason='Change date',indexed=False,nullable=False)],
    description='Sales header',source_binding=dict(sap_client='100',host='sap.example',connection_name='SAP ECC Local'))
PLAN=dict(schema_version=1,source_table='VBAK',columns=[dict(position=f['position'],name=f['fieldname'],
    source_type=f"{f['datatype']}({f['leng']},{f['decimals']})",fabric_type='STRING',nullable=True) for f in FIELDS],
    primary_key=['MANDT','VBELN'],watermark_candidates=['AEDAT'],evidence=['Verified DDIC'],warnings=[],unresolved_decisions=[])
PLAN['columns'][2].update(fabric_type='DATE', source_expression=conversion_expression('AEDAT','DATE'))


def test_sap_prompt_uses_explicit_mcp_connection_and_defers_ui_approvals(monkeypatch):
    from app.migration.opencode_activities import _prompt
    monkeypatch.setenv('OPENCODE_SAP_SOURCE_CONNECTIONS', '{"SAP ECC Local":"ECC-LOCAL"}')
    prompt = _prompt({'input': {**SOURCE, 'source_sap_metadata': META}})
    assert '"ECC-LOCAL"' in prompt
    assert 'Never list those pending approvals in unresolved_decisions' in prompt


@pytest.mark.parametrize('mapping', ['{}', '[]', '{', '{"SAP ECC Local":""}'])
def test_sap_prompt_rejects_missing_or_invalid_connection_mapping(monkeypatch, mapping):
    from app.migration.opencode_activities import _prompt
    monkeypatch.setenv('OPENCODE_SAP_SOURCE_CONNECTIONS', mapping)
    with pytest.raises(Exception, match='OPENCODE_SAP_SOURCE_CONNECTIONS'):
        _prompt({'input': {**SOURCE, 'source_sap_metadata': META}})


def adapted(plan=PLAN,metadata=META):
    return adapt_sap_plan(json.dumps(plan),{**SOURCE,'source_sap_metadata':metadata},'Bronze')


def test_sap_agent_adapter_extract_sql_and_metadata_integrity():
    result=adapted()
    assert conversion_expression('AEDAT','DATE')+' AS "AEDAT"' in result['table_plan']['table']['extract_sql']
    assert result['table_plan']['columns'][0]['description']=='SAP MANDT'
    assert result['table_plan']['columns'][2]['is_nullable'] is None
    assert result['table_plan']['replication']['ingestion_flag'] is False
    plan=ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,tables=[result['table_plan']],views=[]))
    assert plan.tables[0].table.source_system_type=='SAP_ECC'
    changed=copy.deepcopy(result['table_plan']);changed['table']['extract_sql']+=' WHERE 1 = 1'
    with pytest.raises(ValueError,match='ExtractSql'):
        ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,tables=[changed],views=[]))


@pytest.mark.parametrize('change',['field','type','position','key','watermark','target_type','source_expression'])
def test_agent_proposal_cannot_change_verified_sap_metadata(change):
    plan=copy.deepcopy(PLAN)
    if change=='field':plan['columns'].pop()
    if change=='type':plan['columns'][0]['source_type']='CHAR(3,0)'
    if change=='position':plan['columns'][0]['position']=50
    if change=='key':plan['primary_key']=['VBELN']
    if change=='watermark':plan['watermark_candidates']=['LOEKZ']
    if change=='target_type':plan['columns'][0]['fabric_type']='VARCHAR(MAX)'
    if change=='source_expression':plan['columns'][0]['source_expression']='TO_CHAR(MANDT)'
    with pytest.raises(ValueError):adapted(plan)


def test_namespaced_sap_fields_and_portable_cast():
    table=dict(source_system_type='SAP_ECC',source_object_type='TABLE',source_table_name='/BIC/ZSOURCE')
    sql=build_extract_sql(table,[dict(sno=1,column_name='/BIC/ZFIELD',target_column_name='ZFIELD'),
        dict(sno=2,column_name='AMOUNT',source_expression='CAST(AMOUNT AS DECIMAL(20,3))')])
    assert '"/BIC/ZFIELD" AS "ZFIELD"' in sql and 'CAST("AMOUNT" AS DECIMAL(20,3))' in sql
    assert '\n' not in sql


@pytest.mark.parametrize('expression', ['CAST(AMOUNT AS INT4)', 'CAST(AMOUNT AS DATS)',
    'CAST(AMOUNT AS DECIMAL(39,0))', 'CAST(AMOUNT AS DECIMAL(10,11))',
    'CAST(AMOUNT AS NVARCHAR(5001))', 'AMOUNT; DELETE FROM VBAK'])
def test_native_extract_sql_rejects_abap_casts_and_unsafe_expressions(expression):
    with pytest.raises(ValueError):
        build_extract_sql(dict(source_system_type='SAP_ECC', source_object_type='TABLE',
            source_table_name='VBAK'), [dict(sno=1, column_name='AMOUNT', source_expression=expression)])


@pytest.mark.parametrize('accepted', [True, False])
def test_sap_validation_executes_generated_projection_with_zero_rows(monkeypatch, accepted):
    from contextlib import contextmanager
    from types import SimpleNamespace
    from app.migration import source_connections
    from app.migration.opencode_activities import opencode_validate
    calls = []
    class Client:
        def execute_sql_query(self, query, max_rows):
            calls.append((query, max_rows))
            return SimpleNamespace(success=accepted, rows=[], truncated=False)
    @contextmanager
    def client(name):
        assert name == SOURCE['source_connection_name']
        yield Client()
    monkeypatch.setattr(source_connections, 'sap_client', client)
    result = opencode_validate(dict(input={**SOURCE, 'source_sap_metadata': META},
        text=json.dumps(PLAN), lakehouse_name='Bronze'))
    assert calls == [(adapted()['review']['extract_sql'] + ' WHERE 1 = 0', 1)]
    if accepted:
        assert result['review']['extract_sql_validation'] == 'ZERO_ROW_EXECUTION_PASSED'
    else:
        assert 'validation_error' in result and 'table_plan' not in result


@pytest.mark.parametrize('mode',['success','reject','cancel','key_failure'])
def test_durable_sap_agent_workflow_and_replay(mode):
    async def exercise():
        calls=[];persisted=[]
        def fake(name,value=None):
            @activity.defn(name=name)
            async def run(payload):
                calls.append((name,payload))
                if name=='opencode_validate':return adapted()
                if name=='persist_runtime_plan':
                    ApprovedRuntimePlan.model_validate(payload);persisted.append(payload)
                return value
            return run
        activities=[fake('create_migration_plan',str(uuid4())),fake('set_temporal_ids'),
            fake('validate_fabric',{'lakehouse_name':'Bronze'}),fake('sap_agent_metadata',META),
            fake('opencode_create',{'session_id':'ses_sap'}),fake('opencode_prompt'),
            fake('opencode_poll',{'status':'COMPLETED','text':json.dumps(PLAN)}),fake('opencode_validate'),
            fake('validate_sap_key',dict(valid=mode!='key_failure',columns=['MANDT','VBELN'],scope='FULL_TABLE',status='CHECKED')),
            fake('transition_plan'),fake('review_existing_target',{'definitions':[]}),fake('approve_runtime_plan'),
            fake('persist_runtime_plan'),fake('fail_migration'),fake('opencode_interrupt')]
        async with await WorkflowEnvironment.start_local() as env:
            queue=str(uuid4())
            async with Worker(env.client,task_queue=queue,workflows=[OpenCodeSAPTableMigrationWorkflow],activities=activities):
                handle=await env.client.start_workflow(OpenCodeSAPTableMigrationWorkflow.run,SOURCE,id=str(uuid4()),task_queue=queue)
                answered=set()
                for _ in range(500):
                    state=await handle.query('get_state');phase=state['phase']
                    if state['status'] in ('PLANNED','REJECTED','FAILED','CANCELLED'):break
                    if phase.startswith('WAITING_FOR_') and state.get('waiting_for_user') and phase not in answered:
                        answered.add(phase);command=dict(actor='Test reviewer')
                        if mode=='cancel':command.update(action='cancel')
                        elif mode=='reject':command.update(action='reject_columns')
                        elif phase=='WAITING_FOR_COLUMN_MAPPING_APPROVAL':
                            command.update(action='approve_columns',columns=[dict(name=c['name'],fabric_type=c['fabric_type']) for c in PLAN['columns']])
                        elif phase=='WAITING_FOR_PRIMARY_KEY_APPROVAL':command.update(action='approve_primary_key',primary_key_columns=[])
                        elif phase=='WAITING_FOR_WATERMARK_APPROVAL':command.update(action='approve_watermark',watermark_column=None if mode=='key_failure' else 'AEDAT')
                        elif phase=='WAITING_FOR_DELETE_POLICY_APPROVAL':
                            command.update(action='approve_delete_policy',delete_policy={'mode':'NONE'} if mode=='key_failure' else dict(mode='RECONCILE',behavior='MARK',reconcile_interval_minutes=1440))
                        else:raise AssertionError(phase)
                        await handle.signal('command',command)
                    await asyncio.sleep(.02)
                else:pytest.fail('SAP workflow did not finish')
                result=await handle.result()
                if mode in ('success','key_failure'):
                    assert result['status']=='PLANNED' and len(persisted)==1
                    rep=persisted[0]['tables'][0]['replication']
                    assert rep['incremental_method']==('FULL' if mode=='key_failure' else 'WATERMARK')
                    assert rep['delete_policy']['mode']==('NONE' if mode=='key_failure' else 'RECONCILE')
                    assert rep['ingestion_flag'] is False
                else:
                    assert not persisted and result['status']==('CANCELLED' if mode=='cancel' else 'REJECTED')
                assert any(n=='sap_agent_metadata' for n,_ in calls)
                assert not any(n=='validate_oracle_key' for n,_ in calls)
                await Replayer(workflows=[OpenCodeSAPTableMigrationWorkflow]).replay_workflow(await handle.fetch_history())
    asyncio.run(exercise())
