import asyncio,copy,json
from uuid import uuid4
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker,Replayer
from app.migration.opencode_view_plan import adapt_view_bundle,query_references
from app.migration.workflow_oracle_view import OpenCodeOracleViewMigrationWorkflow
from app.migration.contracts import MigrationInput

SOURCE=dict(migration_approach='ORACLE_VIEW',discovery_provider='OPENCODE',
 source_connection_name='Oracle Local',source_schema_name='ONT',source_object_name='V_TOP',
 fabric_workspace_id='00000000-0000-0000-0000-000000000001',
 fabric_lakehouse_id='00000000-0000-0000-0000-000000000002',fabric_schema_name='bronze')
GRAPH=dict(root='ONT.V_TOP',nodes={
 'ONT.T':dict(kind='TABLE',dependencies=[]),
 'ONT.V_BASE':dict(kind='VIEW',dependencies=['ONT.T'],oracle_sql='select ID from ONT.T'),
 'ONT.V_TOP':dict(kind='VIEW',dependencies=['ONT.V_BASE'],oracle_sql='select ID from ONT.V_BASE')},
 view_order=['ONT.V_BASE','ONT.V_TOP'],tables={'ONT.T':dict(columns=[dict(column_name='ID')],primary_key={'columns':[dict(name='ID')]})})
SOURCE['source_view_graph']=GRAPH
BUNDLE=dict(schema_version=1,source_schema='ONT',source_view='V_TOP',tables=[dict(
 schema_version=1,source_schema='ONT',source_table='T',target_table='T',
 columns=[dict(position=1,name='ID',source_type='NUMBER(9,0)',fabric_type='STRING',nullable=False,source_expression='TO_CHAR(ID)')],
 primary_key=['ID'],watermark_candidates=[],evidence=['verified metadata'],unresolved_decisions=[])],
 views=[dict(source_schema='ONT',source_view='V_TOP',target_name='V_TOP',spark_sql='SELECT ID FROM bronze.V_BASE',dependencies=['ONT.V_BASE'],translation_notes=['Direct']),
 dict(source_schema='ONT',source_view='V_BASE',target_name='V_BASE',spark_sql='WITH q AS (SELECT ID FROM bronze.T) SELECT ID FROM q',dependencies=['ONT.T'],translation_notes=['CTE'])],
 evidence=['ALL_DEPENDENCIES'],warnings=[],unresolved_decisions=[])

def test_bundle_preserves_mappings_and_orders_nested_views():
 r=adapt_view_bundle(json.dumps(BUNDLE),SOURCE,'Bronze')
 assert r['tables'][0]['columns'][0]['fabric_data_type']=='STRING'
 assert r['tables'][0]['replication']['ingestion_flag'] is False
 assert [v['view']['view_name'] for v in r['views']]==['V_BASE','V_TOP']
 assert r['views'][0]['dependencies'][0]['source_table_index']==0
 assert r['views'][1]['dependencies'][0]['depends_on_view_index']==0
 assert r['review']['oracle_view_definitions']['ONT.V_TOP']==GRAPH['nodes']['ONT.V_TOP']['oracle_sql']

@pytest.mark.parametrize('payload_expression',['CAST(NULL AS VARCHAR2(4000))','NULL','RAWTOHEX(ID)'])
def test_not_null_blob_allows_only_deferred_nullable_path(payload_expression):
 b=copy.deepcopy(BUNDLE);source=copy.deepcopy(SOURCE)
 column=b['tables'][0]['columns'][0]
 column.update(source_type='BLOB',fabric_type='STRING',nullable=True,target_name='ID_FILE_PATH',
  source_expression=payload_expression,transformation_notes='Attachment BLOB; source NOT NULL; nullable deferred path. Separate attachment process required.')
 b['tables'][0]['primary_key']=[]
 source['source_view_graph']['tables']['ONT.T'].update(primary_key={'columns':[]},
  columns=[dict(column_name='ID',data_type='BLOB',nullable=False)])
 b['views'][1]['spark_sql']='SELECT ID_FILE_PATH AS ID FROM bronze.T'
 if payload_expression=='RAWTOHEX(ID)':
  with pytest.raises(ValueError,match='nullability'):adapt_view_bundle(json.dumps(b),source,'Bronze')
 else:
  result=adapt_view_bundle(json.dumps(b),source,'Bronze')
  assert result['tables'][0]['columns'][0]['is_nullable'] is True
  assert result['tables'][0]['columns'][0]['source_expression']==payload_expression

@pytest.mark.parametrize('sql',['SELECT * FROM other.T','SELECT * FROM bronze.T; DROP TABLE bronze.T','DELETE FROM bronze.T','SELECT * FROM T','SELECT * FROM cat.bronze.T'])
def test_bad_sql_cannot_become_view_configuration(sql):
 b=copy.deepcopy(BUNDLE);b['views'][1]['spark_sql']=sql
 with pytest.raises(ValueError):adapt_view_bundle(json.dumps(b),SOURCE,'Bronze')

@pytest.mark.parametrize('change',['table','view','dependency','columns','key','unresolved','target'])
def test_incomplete_dependency_bundle_rejected(change):
 b=copy.deepcopy(BUNDLE)
 if change=='table':b['tables']=[]
 if change=='view':b['views']=b['views'][:1]
 if change=='dependency':b['views'][1]['dependencies']=[]
 if change=='columns':b['tables'][0]['columns'][0]['name']='WRONG'
 if change=='key':b['tables'][0]['primary_key']=[]
 if change=='unresolved':b['unresolved_decisions']=['Choose numeric domain']
 if change=='target':b['views'][0]['target_name']='Wrong'
 with pytest.raises(ValueError):adapt_view_bundle(json.dumps(b),SOURCE,'Bronze')

def test_view_requires_agent_discovery():
 MigrationInput.model_validate(SOURCE)
 with pytest.raises(ValueError):MigrationInput.model_validate({**SOURCE,'discovery_provider':'BUILTIN'})

async def exercise(cancel=False, transport_failure=False):
 persisted=[];calls=[]
 def fake(name,result=None):
  @activity.defn(name=name)
  async def run(payload):
   calls.append(name)
   if name=='opencode_prompt' and transport_failure:
    from temporalio.exceptions import ApplicationError
    raise ApplicationError('OpenCode POST transport failed (ReadTimeout); check server availability and worker connectivity',
                           type='OpenCodeTransportError')
   if name=='persist_runtime_plan':persisted.append(payload)
   return result
  return run
 result=adapt_view_bundle(json.dumps(BUNDLE),SOURCE,'Bronze')
 activities=[fake('create_migration_plan',str(uuid4())),fake('set_temporal_ids'),fake('validate_fabric',{'lakehouse_name':'Bronze'}),
 fake('oracle_view_graph',GRAPH),fake('opencode_create',{'session_id':'ses_view'}),fake('opencode_prompt'),
 fake('opencode_poll',{'status':'COMPLETED','text':json.dumps(BUNDLE)}),fake('opencode_validate',result),
 fake('transition_plan'),fake('check_oracle_view_targets',{'valid':True}),fake('approve_runtime_plan'),fake('persist_runtime_plan'),
 fake('fail_migration'),fake('opencode_interrupt')]
 async with await WorkflowEnvironment.start_local() as env:
  queue=str(uuid4())
  async with Worker(env.client,task_queue=queue,workflows=[OpenCodeOracleViewMigrationWorkflow],activities=activities):
   h=await env.client.start_workflow(OpenCodeOracleViewMigrationWorkflow.run,SOURCE,id=str(uuid4()),task_queue=queue)
   for _ in range(240):
    state=await h.query('get_state')
    if transport_failure and state.get('status')=='FAILED':break
    if state.get('phase')=='WAITING_FOR_PLAN_APPROVAL':break
    assert state.get('status')!='FAILED',state
    await asyncio.sleep(.05)
   if transport_failure:
    state=await h.result()
    assert state['last_error'].startswith('SUBMITTING_AGENT_PROMPT: OpenCode POST transport failed (ReadTimeout)')
    assert not persisted and calls.count('opencode_prompt')==1
    assert 'opencode_interrupt' in calls and 'fail_migration' in calls
    await Replayer(workflows=[OpenCodeOracleViewMigrationWorkflow]).replay_workflow(await h.fetch_history())
    return
   assert state['phase']=='WAITING_FOR_PLAN_APPROVAL'
   assert not persisted
   await h.signal('command',{'action':'cancel'} if cancel else {'action':'approve_plan','actor':'Reviewer'})
   state=await h.result()
   if cancel:
    assert state['status']=='CANCELLED' and not persisted
   else:
    assert state['status']=='PLANNED'
    assert len(persisted[0]['tables'])==1 and len(persisted[0]['views'])==2
    assert 'submit_provisioning' not in calls
   await Replayer(workflows=[OpenCodeOracleViewMigrationWorkflow]).replay_workflow(await h.fetch_history())

@pytest.mark.parametrize('cancel',[False,True])
def test_bundle_approval_cancellation_and_replay(cancel):asyncio.run(exercise(cancel))

def test_prompt_transport_failure_reports_safe_cause_without_saving_config():
 asyncio.run(exercise(transport_failure=True))

def test_view_output_names_and_order_are_preserved():
 assert query_references('SELECT ID, V FROM bronze.T',['ID','V'])=={'BRONZE.T'}
 with pytest.raises(ValueError):query_references('SELECT V, ID FROM bronze.T',['ID','V'])
 with pytest.raises(ValueError):query_references('SELECT * FROM bronze.T',['ID','V'])
 with pytest.raises(ValueError):query_references('SELECT ( FROM bronze.T')

def test_unsupported_timestamp_cast_is_a_review_blocker():
 with pytest.raises(ValueError,match='TIMESTAMP_NTZ'):
  query_references('SELECT CAST(ID AS TIMESTAMP_NTZ) AS ID FROM bronze.T')

@pytest.mark.parametrize('unsupported',['cycle','remote','function'])
def test_oracle_dependency_traversal_blocks_unsupported_graphs(unsupported):
 from types import SimpleNamespace
 from app.migration.oracle_view_source import inspect_view_graph
 class Cursor:
  def __enter__(self):return self
  def __exit__(self,*args):pass
  def execute(self,sql,**params):self.sql=sql
  def fetchall(self):
   if 'SELECT status' in self.sql:return [('VALID',)]
   if 'SELECT text' in self.sql:return [('SELECT ID FROM ONT.V',)]
   if 'SELECT column_name' in self.sql:return [('ID',)]
   if 'all_dependencies' in self.sql:
    return [('ONT','V','VIEW',None)] if unsupported=='cycle' else [('ONT','T','TABLE','REMOTE')] if unsupported=='remote' else [('ONT','FN','FUNCTION',None)]
   raise AssertionError(self.sql)
 class Connection:
  def __enter__(self):return self
  def __exit__(self,*args):pass
  def cursor(self):return Cursor()
 with pytest.raises(ValueError):inspect_view_graph(SimpleNamespace(connect=Connection),'ONT','V')
