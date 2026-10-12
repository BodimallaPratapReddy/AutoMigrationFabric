import asyncio
from contextlib import contextmanager
from copy import deepcopy
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer

from app.migration.contracts import MigrationInput
from app.migration.opencode_sap_view_plan import adapt_sap_view_bundle
from app.migration.workflow_sap_view import OpenCodeSAPViewMigrationWorkflow
from thirdparty.configdb.repository import ApprovedRuntimePlan
from test_opencode_sap import SOURCE as TABLE_SOURCE, META, PLAN


SOURCE={**TABLE_SOURCE,'migration_approach':'SAP_VIEW','source_object_name':'ZTEST_VIEW'}
GRAPH=dict(root='ZTEST_VIEW',tables={'VBAK':META},view_order=['ZTEST_VIEW'],nodes={
    'VBAK':dict(kind='TABLE',dependencies=[]),
    'ZTEST_VIEW':dict(kind='VIEW',dependencies=['VBAK'],source_sql='SELECT VBELN FROM VBAK',output_columns=['VBELN'])})
BUNDLE=dict(schema_version=1,source_view='ZTEST_VIEW',tables=[{**PLAN,'target_table':'ZTEST_VIEW_VBAK'}],
    views=[dict(source_view='ZTEST_VIEW',target_name='ZTEST_VIEW',spark_sql='SELECT VBELN FROM SAP.ZTEST_VIEW_VBAK',
                dependencies=['VBAK'],translation_notes=['Preserved output'])],
    evidence=['DDIC definition'],warnings=[],unresolved_decisions=[])


def adapted(bundle=BUNDLE):
    return adapt_sap_view_bundle(json.dumps(bundle),{**SOURCE,'source_view_graph':GRAPH},'Bronze')


def test_sap_view_contract_prompt_and_complete_runtime_bundle(monkeypatch):
    from app.migration.opencode_activities import _prompt
    from app.migration.migration_types import load_migration_types
    assert MigrationInput.model_validate(SOURCE).migration_approach=='SAP_VIEW'
    assert 'SAP_VIEW' in {x.value for x in load_migration_types()['SAP_ECC']}
    with pytest.raises(ValueError): MigrationInput.model_validate({**SOURCE,'discovery_provider':'BUILTIN'})
    monkeypatch.setenv('OPENCODE_SAP_SOURCE_CONNECTIONS','{"SAP ECC Local":"ECC-LOCAL"}')
    prompt=_prompt({'input':{**SOURCE,'source_view_graph':GRAPH}})
    assert 'sap-to-fabric-view-design' in prompt and '"ECC-LOCAL"' in prompt
    assert 'SAPViewTable' in prompt and 'Oracle' not in prompt
    result=adapted()
    plan=ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,**{k:result[k] for k in ('tables','views')}))
    assert plan.tables[0].table.source_system_type=='SAP_ECC'
    assert plan.views[0].dependencies[0].source_table_index==0


@pytest.mark.parametrize('change',['missing_table','missing_dependency','unsafe_sql','wrong_output','wrong_root','source_type','duplicate_target'])
def test_sap_view_rejects_incomplete_or_forged_bundles(change):
    bundle=deepcopy(BUNDLE)
    if change=='missing_table':bundle['tables']=[]
    elif change=='missing_dependency':bundle['views'][0]['dependencies']=[]
    elif change=='unsafe_sql':bundle['views'][0]['spark_sql']='DELETE FROM SAP.ZTEST_VIEW_VBAK'
    elif change=='wrong_output':bundle['views'][0]['spark_sql']='SELECT MANDT FROM SAP.ZTEST_VIEW_VBAK'
    elif change=='wrong_root':bundle['views'][0]['target_name']='OTHER'
    elif change=='source_type':bundle['tables'][0]['columns'][0]['source_type']='CHAR(3,0)'
    else:bundle['tables'][0]['target_table']='ZTEST_VIEW'
    with pytest.raises(ValueError):adapted(bundle)


@pytest.mark.parametrize('scenario',['success','cycle','incomplete','cds'])
def test_verified_sap_view_graph(monkeypatch,scenario):
    from app.migration import sap_view_source
    class Client:
        def get_object_type(self,name):
            return SimpleNamespace(object_type='TRANSPARENT_TABLE' if name=='VBAK' else 'CDS_VIEW' if scenario=='cds' else 'DB_VIEW')
        def get_dbview_query_details(self,name):
            return SimpleNamespace(sqlQueryComplete=scenario!='incomplete',sqlQuery='SELECT VBELN FROM VBAK',
                tables=[SimpleNamespace(tableName='ZTEST_VIEW' if scenario=='cycle' else 'VBAK')])
        def read_metadata(self,name):
            return [SimpleNamespace(fieldname='VBELN',position=1,model_dump=lambda **kw:dict(fieldname='VBELN',position=1))]
    @contextmanager
    def client(name):yield Client()
    monkeypatch.setattr(sap_view_source,'sap_client',client)
    monkeypatch.setattr(sap_view_source,'sap_agent_metadata',lambda payload:META)
    if scenario=='success':assert sap_view_source.sap_view_graph({'input':SOURCE})['tables']=={'VBAK':META}
    else:
        with pytest.raises(ValueError):sap_view_source.sap_view_graph({'input':SOURCE})


@pytest.mark.parametrize('accepted',[True,False])
def test_sap_view_validates_each_extraction_before_target_check(monkeypatch,accepted):
    from app.migration import source_connections, oracle_view_targets
    from app.migration.opencode_activities import opencode_validate
    checks=[];targets=[]
    @contextmanager
    def client(name):
        def execute(query,max_rows):
            checks.append(query)
            return SimpleNamespace(success=accepted,rows=[],truncated=False)
        yield SimpleNamespace(execute_sql_query=execute)
    monkeypatch.setattr(source_connections,'sap_client',client)
    monkeypatch.setattr(oracle_view_targets,'check_oracle_view_targets',lambda payload:targets.append(payload))
    result=opencode_validate(dict(input={**SOURCE,'source_view_graph':GRAPH,'plan_guid':str(uuid4())},text=json.dumps(BUNDLE),lakehouse_name='Bronze'))
    assert len(checks)==1 and checks[0].endswith(' WHERE 1 = 0')
    if accepted:
        assert result['review']['table_reviews'][0]['extract_sql_validation']=='ZERO_ROW_EXECUTION_PASSED'
        assert len(targets)==1
    else:assert 'validation_error' in result and not targets


@pytest.mark.parametrize('change_base',[False,True])
def test_sap_view_sql_repair_retains_approved_table_settings(monkeypatch,change_base):
    from app.migration import source_connections, oracle_view_targets
    from app.migration.opencode_activities import opencode_validate
    @contextmanager
    def client(name):
        yield SimpleNamespace(execute_sql_query=lambda *a,**kw:SimpleNamespace(success=True,rows=[],truncated=False))
    monkeypatch.setattr(source_connections,'sap_client',client)
    monkeypatch.setattr(oracle_view_targets,'check_oracle_view_targets',lambda payload:None)
    previous=adapted()
    previous['tables'][0]['replication'].update(incremental_method='WATERMARK',watermark_column='AEDAT',write_strategy='UPSERT')
    context=dict(previous=previous,eligible=['SAP.ZTEST_VIEW'])
    bundle=deepcopy(BUNDLE)
    bundle['views'][0]['spark_sql']='SELECT CAST(VBELN AS STRING) AS VBELN FROM SAP.ZTEST_VIEW_VBAK'
    bundle['views'][0]['translation_notes']=['Corrected cast']
    if change_base:bundle['tables'][0]['columns'][1]['transformation_notes']='Changed base proposal'
    result=opencode_validate(dict(input={**SOURCE,'source_view_graph':GRAPH,'plan_guid':str(uuid4()),'view_correction':context},text=json.dumps(bundle),lakehouse_name='Bronze'))
    if change_base:assert 'validation_error' in result
    else:assert result['tables']==previous['tables'] and result['views']!=previous['views']


@pytest.mark.parametrize('mode',['success','reject','cancel'])
def test_durable_sap_view_base_table_review_and_replay(mode):
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
            fake('validate_fabric',{'lakehouse_name':'Bronze'}),fake('sap_view_graph',GRAPH),
            fake('opencode_create',{'session_id':'ses_sap_view'}),fake('opencode_prompt'),
            fake('opencode_poll',{'status':'COMPLETED','text':json.dumps(BUNDLE)}),fake('opencode_validate'),
            fake('validate_sap_key',dict(valid=True,columns=['MANDT','VBELN'],scope='FULL_TABLE',status='CHECKED')),
            fake('transition_plan'),fake('check_oracle_view_targets'),fake('approve_runtime_plan'),
            fake('persist_runtime_plan'),fake('fail_migration'),fake('opencode_interrupt')]
        async with await WorkflowEnvironment.start_local() as env:
            queue=str(uuid4())
            async with Worker(env.client,task_queue=queue,workflows=[OpenCodeSAPViewMigrationWorkflow],activities=activities):
                handle=await env.client.start_workflow(OpenCodeSAPViewMigrationWorkflow.run,SOURCE,id=str(uuid4()),task_queue=queue)
                answered=set()
                for _ in range(500):
                    state=await handle.query('get_state');phase=state['phase']
                    if state['status'] in ('PLANNED','REJECTED','FAILED','CANCELLED'):break
                    if state.get('waiting_for_user') and phase not in answered:
                        answered.add(phase);command=dict(actor='Test reviewer')
                        if mode=='cancel':command.update(action='cancel')
                        elif mode=='reject':command.update(action='reject_columns')
                        elif phase=='WAITING_FOR_COLUMN_MAPPING_APPROVAL':
                            assert state['review']['view_base_table'] is True
                            command.update(action='approve_columns',columns=[dict(name=c['name'],fabric_type=c['fabric_type']) for c in PLAN['columns']])
                        elif phase=='WAITING_FOR_WATERMARK_APPROVAL':command.update(action='approve_watermark',watermark_column='AEDAT')
                        elif phase=='WAITING_FOR_DELETE_POLICY_APPROVAL':command.update(action='approve_delete_policy',delete_policy=dict(mode='RECONCILE',behavior='MARK',reconcile_interval_minutes=1440))
                        elif phase=='WAITING_FOR_PLAN_APPROVAL':command.update(action='approve_plan')
                        else:raise AssertionError(phase)
                        await handle.signal('command',command)
                    await asyncio.sleep(.02)
                else:pytest.fail('SAP view workflow did not finish')
                result=await handle.result()
                assert result['status']=={'success':'PLANNED','reject':'REJECTED','cancel':'CANCELLED'}[mode]
                if mode=='success':
                    assert persisted[0]['tables'][0]['replication']['watermark_column']=='AEDAT'
                    assert persisted[0]['tables'][0]['replication']['delete_policy']['mode']=='RECONCILE'
                    assert any(n=='validate_sap_key' and p['input']['migration_approach']=='SAP_TABLE' for n,p in calls)
                else:assert not persisted
                history=await handle.fetch_history()
            await Replayer(workflows=[OpenCodeSAPViewMigrationWorkflow]).replay_workflow(history)
    asyncio.run(exercise())
