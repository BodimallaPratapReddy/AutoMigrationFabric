import asyncio
from copy import deepcopy
import json
from uuid import uuid4
from datetime import date
from decimal import Decimal

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer
from app.migration.workflow_sap_datasource import OpenCodeSAPODPMigrationWorkflow, OpenCodeSAPDataSourceRebuildWorkflow
from app.migration.opencode_sap_odp_plan import adapt_sap_odp_plan
from app.migration.opencode_sap_view_plan import adapt_sap_view_bundle
from thirdparty.configdb.repository import ApprovedRuntimePlan
from thirdparty.sap.extraction import conversion_expression
from thirdparty.sap.odp_conversion import conversion_contract, decode_odp_row
from test_opencode_sap import SOURCE as TABLE_SOURCE, META, PLAN


SOURCE={**TABLE_SOURCE,'source_object_name':'ZDS','fabric_target_name':'DS_ZDS','migration_approach':'SAP_ODP'}
META_DS=deepcopy(META);META_DS['schema']['table_name']='ZDS';META_DS['watermark_candidates']=[]
CONTEXT=dict(datasource={'DELTA':'D'},output_metadata=META_DS,odp_capability={'odp_capable':True},
    delta=dict(details_resolved=True,delta_supported=True),rebuild={'datasource':{'EXTRACTOR':'VBAK'}})
ODP_PLAN={**PLAN,'source_table':'ZDS','watermark_candidates':[]}
TABLES={'VBAK':META,'VBUK':{**META,'schema':{**META['schema'],'table_name':'VBUK'}}}
GRAPH=dict(root='ZDS',tables=TABLES,view_order=['ZDS'],approved_analysis={},nodes={
    'VBAK':dict(kind='TABLE',dependencies=[]),'VBUK':dict(kind='TABLE',dependencies=[]),
    'ZDS':dict(kind='VIEW',dependencies=['VBAK','VBUK'],source_sql='SELECT VBELN FROM VBAK',output_columns=['VBELN'])})
BUNDLE=dict(schema_version=1,source_view='ZDS',tables=[{**PLAN,'source_table':name,'target_table':'DS_ZDS_'+name} for name in TABLES],
    views=[dict(source_view='ZDS',target_name='DS_ZDS',spark_sql='SELECT a.VBELN AS VBELN FROM SAP.DS_ZDS_VBAK a JOIN SAP.DS_ZDS_VBUK b ON a.MANDT=b.MANDT AND a.VBELN=b.VBELN',
        dependencies=['VBAK','VBUK'],translation_notes=['Verified extractor output'])],
    evidence=['Verified extractor'],warnings=[],unresolved_decisions=[])


def odp():return adapt_sap_odp_plan(json.dumps(ODP_PLAN),{**SOURCE,'source_datasource_context':CONTEXT},'Bronze')
def rebuilt():
    result=adapt_sap_view_bundle(json.dumps(BUNDLE),{**SOURCE,'migration_approach':'SAP_REBUILD','source_view_graph':GRAPH},'Bronze')
    for review in result['review']['table_reviews']:review['source_metadata']=TABLES[review['source']]
    return result


def test_odp_plan_has_output_conversions_without_table_sql_delta_delete_choices():
    result=odp();table=result['tables'][0]
    assert table['table']['extract_sql'] is None and table['table']['data_source_type']=='ODP'
    assert table['replication']['write_strategy']=='ODP_MANAGED'
    assert table['replication']['watermark_column'] is None and table['replication']['delete_policy'] is None
    contract=json.loads(table['columns'][2]['source_expression'])
    assert contract['stage']=='ODP_POST_EXTRACT' and contract['kind']=='DATE'
    plan=ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,tables=result['tables'],views=[]))
    assert plan.tables[0].table.source_object_type=='SAP_DATASOURCE'
    table['table']['source_object_type']='TABLE'
    with pytest.raises(ValueError):ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,tables=result['tables'],views=[]))


@pytest.mark.parametrize('kind,value,expected',[('DATE','00000000',None),('DATE','20261012',date(2026,10,12)),
    ('TIME','000000','00:00:00'),('TIME','235959','23:59:59'),('LONG_TIMESTAMP',Decimal('20261012123456.1234567'),'20261012123456.1234567'),
    ('BOOLEAN','X',True),('BOOLEAN',' ',False)])
def test_odp_post_extract_preserves_approved_semantics(kind,value,expected):
    dtype={'DATE':'DATE','BOOLEAN':'BOOLEAN'}.get(kind,'STRING')
    columns=[dict(column_name='F',fabric_data_type=dtype,source_expression=conversion_contract(conversion_expression('F',kind),'F'))]
    assert decode_odp_row({'F':value},columns)=={'F':expected}


@pytest.mark.parametrize('kind,value',[('DATE','20260230'),('DATE',''),('TIME','240000'),('TIME',''),
    ('LONG_TIMESTAMP',20261012123456.12),('LONG_TIMESTAMP',Decimal('20261012123456.12345678')),('BOOLEAN','?')])
def test_odp_conversion_rejects_invalid_or_lossy_values(kind,value):
    dtype={'DATE':'DATE','BOOLEAN':'BOOLEAN'}.get(kind,'STRING')
    columns=[dict(column_name='F',fabric_data_type=dtype,source_expression=conversion_contract(conversion_expression('F',kind),'F'))]
    with pytest.raises(ValueError):decode_odp_row({'F':value},columns)


def test_datasource_prompts_distinguish_stages_and_do_not_ask_odp_replication(monkeypatch):
    from app.migration.opencode_activities import _prompt
    monkeypatch.setenv('OPENCODE_SAP_SOURCE_CONNECTIONS','{"SAP ECC Local":"ECC-LOCAL"}')
    prompt=_prompt({'input':{**SOURCE,'sap_mcp_discovery':True}})
    assert 'watermark_candidates MUST be []' in prompt and 'Do not ask keys, delta' in prompt
    assert 'ODP_POST_EXTRACT' in prompt and 'MCPODPProposal' in prompt
    analysis=_prompt({'input':{**SOURCE,'sap_mcp_discovery':True,'migration_approach':'SAP_REBUILD','datasource_stage':'ANALYSIS'}})
    assert 'SAPExtractorAnalysis' in analysis and 'sap-to-fabric-datasource-design' in analysis
    planning=_prompt({'input':{**SOURCE,'plan_guid':'00000000-0000-0000-0000-000000000001','sap_mcp_discovery':True,'migration_approach':'SAP_REBUILD','source_datasource_context':CONTEXT,'datasource_stage':'PLAN'}})
    assert 'MCPViewProposal' in planning and 'SAPViewBundle' in planning


@pytest.mark.parametrize('mode',['odp','odp_provision','rebuild','reject_analysis','reject_columns','cancel_agent','retry_limit','terminal_blocker','rebuild_terminal_blocker','mcp_retry_limit'])
def test_durable_datasource_reviews_and_replay(mode):
    async def exercise():
        is_odp=mode.startswith('odp') or mode in {'retry_limit','terminal_blocker','mcp_retry_limit'}; approach='SAP_ODP' if is_odp else 'SAP_REBUILD'
        cls=OpenCodeSAPODPMigrationWorkflow if is_odp else OpenCodeSAPDataSourceRebuildWorkflow
        source={**SOURCE,'migration_approach':approach};calls=[];persisted=[]
        if mode=='odp_provision':source['provisioning_notebook_id']=str(uuid4())
        def fake(name,value=None):
            @activity.defn(name=name)
            async def run(payload):
                calls.append((name,payload))
                if name=='opencode_poll' and mode=='mcp_retry_limit':
                    assert payload['sap_tool_retry_guard'] is True
                    return {'status':'RUNNING','tool_attempts':[{'id':str(i),'signature':'same-query',
                            'tool':'sap-abap-adt.runQuery','status':'error'} for i in range(12)]}
                if name=='opencode_validate':
                    if mode=='retry_limit':return {'validation_error':'Unresolved helper semantics'}
                    if mode in {'terminal_blocker','rebuild_terminal_blocker'}:return {'failure_reason':'SAP agent exhausted retries','agent_blocker':{'attempts':5}}
                    if is_odp:return odp()
                    if payload['input'].get('datasource_stage')=='ANALYSIS':
                        return dict(analysis_guid=str(uuid4()),route='FUNCTION_MODULE',analysis={},context=CONTEXT,warnings=[],verified={'verified':[]})
                    return rebuilt()
                if name=='persist_runtime_plan':
                    ApprovedRuntimePlan.model_validate(payload);persisted.append(payload)
                return value
            return run
        activities=[fake('create_migration_plan',str(uuid4())),fake('set_temporal_ids'),
            fake('validate_fabric',{'lakehouse_name':'Bronze'}),
            fake('opencode_create',{'session_id':'ses_datasource'}),fake('opencode_prompt'),
            fake('opencode_poll',{'status':'WORKING'} if mode=='cancel_agent' else {'status':'COMPLETED','text':'{}'}),fake('opencode_validate'),
            fake('approve_analysis'),fake('transition_plan'),fake('check_oracle_view_targets'),fake('approve_runtime_plan'),
            fake('opencode_sap_key_check',dict(valid=True,columns=['MANDT','VBELN'],scope='FULL_TABLE',status='CHECKED',method='DDIC_PRIMARY_KEY_CONSTRAINT')),
            fake('persist_runtime_plan'),fake('fail_migration'),fake('opencode_interrupt'),
            fake('start_batch',str(uuid4())),fake('submit_provisioning',{'job_instance_id':str(uuid4())}),
            fake('record_batch_job'),fake('get_provisioning_status',{'normalized_status':'SUCCEEDED'}),
            fake('parse_provisioning',{'status':'SUCCEEDED'}),fake('finish_provisioning')]
        async with await WorkflowEnvironment.start_local() as env:
            queue=str(uuid4())
            async with Worker(env.client,task_queue=queue,workflows=[cls],activities=activities):
                handle=await env.client.start_workflow(cls.run,source,id=str(uuid4()),task_queue=queue)
                answered=set();phases=[]
                for _ in range(900):
                    state=await handle.query('get_state');phase=state['phase'];review=state.get('review') or {}
                    if state['status'] in ('COMPLETED','PLANNED','REJECTED','FAILED','CANCELLED'):break
                    token=(phase,review.get('source'),state.get('agent_prompt_revision'))
                    if mode=='cancel_agent' and phase=='WAITING_FOR_AGENT' and token not in answered:
                        answered.add(token);await handle.signal('command',{'action':'cancel'});continue
                    if state.get('waiting_for_user') and token not in answered:
                        answered.add(token);phases.append((phase,review.get('source')));cmd={'actor':'Test reviewer'}
                        if phase=='WAITING_FOR_ANALYSIS_APPROVAL':cmd.update(action='reject_analysis' if mode=='reject_analysis' else 'approve_analysis')
                        elif phase=='WAITING_FOR_AGENT_REVISION':cmd.update(action='agent_feedback',message='Use documentation and correct the known blocker')
                        elif phase=='WAITING_FOR_COLUMN_MAPPING_APPROVAL':cmd.update(action='reject_columns' if mode=='reject_columns' else 'approve_columns',
                            columns=[dict(name=c['name'],fabric_type=c['fabric_type']) for c in review['columns']])
                        elif phase=='WAITING_FOR_WATERMARK_APPROVAL':cmd.update(action='approve_watermark',watermark_column='AEDAT')
                        elif phase=='WAITING_FOR_DELETE_POLICY_APPROVAL':cmd.update(action='approve_delete_policy',delete_policy=dict(mode='RECONCILE',behavior='MARK',reconcile_interval_minutes=1440))
                        elif phase=='WAITING_FOR_PLAN_APPROVAL':cmd.update(action='approve_plan')
                        else:raise AssertionError(phase)
                        await handle.signal('command',cmd)
                    await asyncio.sleep(.02)
                else:pytest.fail('DataSource workflow did not finish')
                result=await handle.result()
                if mode in {'retry_limit','terminal_blocker','rebuild_terminal_blocker','mcp_retry_limit'}:
                    assert result['status']=='FAILED',result
                    if mode in {'terminal_blocker','rebuild_terminal_blocker'}:
                        assert 'SAP agent exhausted retries' in result['last_error']
                        assert result['agent_blocker']['attempts'] == 5
                        from app.migration.contracts import MigrationState
                        assert MigrationState.model_validate(result).agent_blocker == {'attempts':5}
                    assert not persisted
                    assert any(n=='opencode_interrupt' for n,p in calls)
                    assert len([n for n,p in calls if n=='opencode_validate'])==(6 if mode=='retry_limit' else 0 if mode=='mcp_retry_limit' else 1)
                    assert len([n for n,p in calls if n=='opencode_prompt'])==(6 if mode=='retry_limit' else 1)
                    assert not any(n=='submit_provisioning' for n,p in calls)
                    history=await handle.fetch_history()
                    await Replayer(workflows=[cls]).replay_workflow(history)
                    return
                assert result['status']==('COMPLETED' if mode=='odp_provision' else 'CANCELLED' if mode=='cancel_agent' else 'REJECTED' if mode.startswith('reject') else 'PLANNED'),result
                if is_odp:
                    assert [p for p,n in phases]==['WAITING_FOR_COLUMN_MAPPING_APPROVAL','WAITING_FOR_PLAN_APPROVAL']
                    assert persisted[0]['tables'][0]['replication']['delete_policy'] is None
                    assert not any(n in {'activate_replication_config','enable_replication','submit_replication'} for n,p in calls)
                    if mode=='odp_provision':
                        assert any(n=='submit_provisioning' for n,p in calls)
                        assert 'Ingestion remains disabled' in result['current_message']
                elif mode=='rebuild':
                    assert len([p for p,n in phases if p=='WAITING_FOR_DELETE_POLICY_APPROVAL'])==2
                    assert len([p for p,n in phases if p=='WAITING_FOR_WATERMARK_APPROVAL'])==2
                    assert all(t['replication']['watermark_column']=='AEDAT' for t in persisted[0]['tables'])
                else:assert not persisted
                if mode=='cancel_agent':assert any(n=='opencode_interrupt' for n,p in calls)
                assert not any(n in {'sap_datasource_context','sap_rebuild_graph','sap_agent_metadata','sap_view_graph','validate_sap_key'} for n,p in calls)
                history=await handle.fetch_history()
            await Replayer(workflows=[cls]).replay_workflow(history)
    asyncio.run(exercise())
