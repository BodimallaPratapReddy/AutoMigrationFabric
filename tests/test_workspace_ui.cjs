const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('frontend/workspace.js','utf8');
const projection = source.slice(source.indexOf('function objects()'),source.indexOf('function renderObjects()'));

test('process status distinguishes running work, completed actions, and approval waits',()=>{
  const chat=fs.readFileSync('frontend/chat-workspace.js','utf8');
  const getStatus=vm.runInNewContext(chat.slice(chat.indexOf('export function processStatus'),chat.indexOf('export function workflowSteps')).replace('export ','')+';processStatus');
  const state={status:'RUNNING',phase:'WAITING_FOR_AGENT'};
  const events=[{engine:'OpenCode',title:'Inspect column definitions',status:'completed'}];
  assert.equal(getStatus(state,events).label,'AutoMigrate Agent is working');
  assert.equal(getStatus(state,events).detail,'Last activity: Inspect column definitions · completed');
  assert.equal(getStatus({...state,waiting_for_user:true},events).mode,'waiting');
  assert.equal(getStatus({...state,status:'COMPLETED'},events).mode,'finished');
  assert.equal(getStatus(null).mode,'loading');
  assert.equal(getStatus({...state,phase:'PROVISIONING',current_message:'Creating base tables'}).detail,'Creating base tables');
});

test('failed outcome shows the recorded activity cause instead of the wrapper error',()=>{
  const card={innerHTML:''};
  const context=vm.createContext({activeState:{},selected:{status:'FAILED',last_error:'DISCOVERING_SOURCE: ActivityError'},
    events:[{kind:'error',message:'OpenCode agent oracle-to-fabric is not available'}],
    $:()=>card,category:()=> 'error',statusName:()=> 'Needs attention',escape:x=>x});
  vm.runInContext(source.slice(source.indexOf('function renderOutcome()'),source.indexOf('function eventHTML('))+';renderOutcome();',context);
  assert.match(card.innerHTML,/OpenCode agent oracle-to-fabric is not available/);
  assert.doesNotMatch(card.innerHTML,/ActivityError/);
});

test('leaving the walkthrough restores live requests and approval controls',async()=>{
  const elements=new Map();
  const element=id=>{if(!elements.has(id))elements.set(id,{hidden:false,disabled:true,readOnly:true,value:'Preview only',textContent:'',remove(){this.removed=true;}});return elements.get(id);};
  const context=vm.createContext({URLSearchParams,window:{migrationPreview:{transform:s=>s}},
    location:{pathname:'/',search:'?preview=approvals'},history:{replaceState(){}},
    document:{querySelectorAll:()=>[]},$:element,control:{stop(){}},chatWorkspace:{close(){}},notice(){}});
  const navigation=source.slice(source.indexOf('function exitApprovalPreview('),source.indexOf("\ndocument.querySelectorAll('[data-page]')"));
  vm.runInContext(`let approvalPreview=true,previewStep=2,previewSnapshot={},previewControls=[['approve',{disabled:false,textContent:'Approve'}],['actor',{value:'Pratap',readOnly:false}]],page,selected; ${navigation}; showPage('new');`,context);
  assert.equal(context.window.migrationPreview,undefined);
  assert.equal(vm.runInContext('approvalPreview',context),false);
  assert.equal(element('approve').disabled,false);
  assert.equal(element('actor').value,'Pratap');
  assert.equal(element('actor').readOnly,false);
  assert.equal(element('approval-preview').removed,true);
});

test('approval preview blocks every mutation before any network request',async()=>{
  const html=fs.readFileSync('app/migration_ui.html','utf8');
  const request=html.slice(html.indexOf('    async function request('),html.indexOf("    $('connection-form').addEventListener"));
  let calls=0;
  const context=vm.createContext({URLSearchParams,window:{location:{search:'?preview=approvals'}},
    fetch:async()=>{calls++;return {ok:true,json:async()=>({})};}});
  vm.runInContext(request,context);
  for(const method of ['POST','PUT','PATCH','DELETE'])await assert.rejects(vm.runInContext(`request('/migrations/job/approve-plan',{method:'${method}'})`,context),/read-only approval preview/);
  assert.equal(calls,0);
  await vm.runInContext("request('/migrations/job')",context);
  assert.equal(calls,1);
});

test('source-named audit rows enrich target-named plans without doubling object counts',()=>{
  const context=vm.createContext({
    human:value=>value,
    selected:{},
    activeState:{review:{agent_plan:{tables:[{source_schema:'SAP',source_table:'VBAK',target_table:'VIEW_VBAK',columns:[{name:'ID'}]}],views:[{source_view:'VIEW',target_name:'VIEW'}]}}},
    activeAudit:{tables:[{source:'VBAK',target:'VIEW_VBAK',provisioning_status:'READY'}],batches:[{started_timestamp:'2026-10-13',objects:[{object_type:'TABLE',object_name:'VBAK',status:'SUCCEEDED'},{object_type:'VIEW',object_name:'VIEW',status:'SUCCEEDED'}]}]}
  });
  const objects=vm.runInContext(`${projection}; objects()`,context);
  assert.equal(objects.length,2);
  assert.equal(objects[0].target,'VIEW_VBAK');
  assert.equal(objects[0].columns[0].name,'ID');
  assert.equal(objects[0].status,'SUCCEEDED');
  assert.equal(objects[0].rows,undefined);
});

test('most recent execution evidence takes precedence over a prior failed batch',()=>{
  const context=vm.createContext({human:v=>v,selected:{},activeState:{},activeAudit:{batches:[
    {started_timestamp:'2026-10-13T12:00:00Z',objects:[{object_type:'TABLE',object_name:'ORDERS',status:'SUCCEEDED',rows_written:0}]},
    {started_timestamp:'2026-10-13T11:00:00Z',objects:[{object_type:'TABLE',object_name:'ORDERS',status:'FAILED'}]}
  ]}});
  const objects=vm.runInContext(`${projection}; objects()`,context);
  assert.equal(objects.length,1);
  assert.equal(objects[0].status,'SUCCEEDED');
  assert.equal(objects[0].rows,0);
});

test('no inventory is invented when discovery and audit have no objects',()=>{
  const context=vm.createContext({human:v=>v,selected:{},activeState:{status:'FAILED'},activeAudit:null});
  assert.equal(vm.runInContext(`${projection}; objects()`,context).length,0);
});

test('agent steps are grouped without swallowing approval decisions',()=>{
  const chat=fs.readFileSync('frontend/chat-workspace.js','utf8');
  const group=vm.runInNewContext(chat.slice(chat.indexOf('export function groupTimeline'),chat.indexOf('export function createChatWorkspace')).replace('export ','')+';groupTimeline');
  const grouped=group([{id:'a',kind:'agent'},{id:2,kind:'activity',activity_name:'opencode_poll'},{id:'b',kind:'agent'},{id:3,kind:'decision'},{id:'c',kind:'agent'}]);
  assert.equal(grouped.length,3);
  assert.equal(grouped[0].events.length,2);
  assert.equal(grouped[1].kind,'decision');
  assert.equal(grouped[2].events[0].id,'c');
});

test('chat automatically loads later history pages and retains approved decisions on refresh',async()=>{
  let calls=0;
  const context=vm.createContext({selected:{workflow_id:'job'},eventsLoading:false,lastEventRefresh:0,events:[],eventCursor:null,activeState:null,
    $:()=>({}),renderEvents(){},renderOutcome(){},empty(){},
    api:async()=> ++calls===1?{events:[{id:1,kind:'activity'}],next_cursor:'page2'}:calls===2?{events:[{id:251,kind:'decision'}],next_cursor:null}:{events:[{id:1,kind:'activity'}],next_cursor:null}});
  vm.runInContext(source.slice(source.indexOf('async function loadEvents('),source.indexOf("$('refresh-events').addEventListener")),context);
  await context.loadEvents();
  assert.equal(calls,2);
  assert.equal(context.events.find(e=>e.id===251).kind,'decision');
  await context.loadEvents();
  assert.equal(context.events.find(e=>e.id===251).kind,'decision');
});


test('repeated provisioning checks update one step and stop shining after completion',()=>{
  const chat=fs.readFileSync('frontend/chat-workspace.js','utf8');
  const project=vm.runInNewContext(chat.slice(chat.indexOf('export function workflowSteps'),chat.indexOf('export function groupTimeline')).replace('export ','')+';workflowSteps');
  const events=[{id:1,kind:'activity',activity_name:'get_provisioning_status'},
    {id:2,kind:'activity_status',scheduled_event_id:1,status:'completed'},
    {id:3,kind:'activity',activity_name:'get_provisioning_status'},
    {id:4,kind:'activity_status',scheduled_event_id:3,status:'completed'}];
  const running=project(events,{status:'RUNNING',phase:'WAITING_FOR_PROVISIONING'});
  assert.equal(running.length,1);assert.equal(running[0].checks,2);assert.equal(running[0].status,'running');
  assert.equal(project(events,{status:'COMPLETED'})[0].status,'completed');
  events.push({id:5,kind:'decision'},{id:6,kind:'activity',activity_name:'get_provisioning_status'});
  assert.equal(project(events,{status:'RUNNING'}).length,3);
});

test('elapsed duration advances in seconds and formats completed durations',()=>{
  const chat=fs.readFileSync('frontend/chat-workspace.js','utf8');
  const elapsed=vm.runInNewContext(chat.slice(chat.indexOf('export function elapsedLabel'),chat.indexOf('export function reviewIdentity')).replace('export ','')+';elapsedLabel');
  assert.equal(elapsed(1000,46000),'45s');
  assert.equal(elapsed(1000,151000),'2m 30s');
  assert.equal(elapsed(1000,3601000),'1h 0s');
  assert.equal(elapsed(1000,0),'0s');
  assert.equal(elapsed(NaN,1000),'');
});

test('Fabric execution is grouped after approval without absorbing discovery or decisions',()=>{
 const chat=fs.readFileSync('frontend/chat-workspace.js','utf8');
 const group=vm.runInNewContext(chat.slice(chat.indexOf('export function groupTimeline'),chat.indexOf('export function createChatWorkspace')).replace('export ','')+';groupTimeline');
 const events=[{id:1,kind:'activity',activity_name:'validate_fabric'}, {id:2,kind:'decision'},
 ...['approve_runtime_plan','persist_runtime_plan','validate_fabric','start_batch','submit_provisioning','get_provisioning_status','finish_provisioning','activate_replication_config'].map((name,i)=>({id:i+3,kind:'activity',activity_name:name})),{id:12,kind:'decision'}];
 const result=group(events);
 assert.equal(result.length,4);assert.equal(result[0].kind,'activity');assert.equal(result[1].kind,'decision');
 assert.equal(result[2].kind,'fabric-group');assert.equal(result[2].events.length,8);assert.equal(result[3].kind,'decision');
});

test('server pagination replaces rows, reuses previous cursors, and resets on size change',async()=>{
  const elements=new Map();
  const $=id=>{if(!elements.has(id))elements.set(id,{setAttribute(){}});return elements.get(id);};
  const urls=[];
  const context=vm.createContext({$,jobs:[],jobPageIndex:0,jobPageSize:20,jobPageCursors:[null],nextCursor:null,loadingJobs:false,
    renderJobs(){},notice(){},empty(){},api:async url=>{urls.push(url);return {jobs:[{workflow_id:url.includes('cursor')?'second':'first'}],next_cursor:url.includes('cursor')?null:'next-token'};}});
  vm.runInContext(source.slice(source.indexOf('function renderJobPagination()'),source.indexOf("$('refresh-jobs').addEventListener")),context);
  await context.loadJobs();assert.equal(urls[0],'/workspace/jobs?limit=20');
  await context.loadJobs(1);assert.equal(urls[1],'/workspace/jobs?limit=20&cursor=next-token');
  assert.equal(context.jobs.length,1);assert.equal(context.jobs[0].workflow_id,'second');assert.equal($('jobs-next').disabled,true);
  await context.loadJobs(0);assert.equal(urls[2],'/workspace/jobs?limit=20');assert.equal($('jobs-previous').disabled,true);
  await context.loadJobs(0,50);assert.equal(context.jobPageSize,50);assert.equal(context.jobPageIndex,0);assert.equal(urls[3],'/workspace/jobs?limit=50');
  context.api=async()=>{throw Error('unavailable');};await context.loadJobs(1);assert.equal(context.jobPageIndex,0);assert.equal(context.jobs[0].workflow_id,'first');
});
