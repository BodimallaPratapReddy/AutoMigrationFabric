const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const {test} = require('node:test');
const script = fs.readFileSync(require('node:path').join(__dirname, '../app/migration_ui.html'), 'utf8')
  .match(/<script>([\s\S]*?)<\/script>/)[1];
const code = script.slice(script.indexOf('    function renderAgent()'), script.indexOf('    async function refresh()'));
function harness() {
  const elements = new Map(), calls = [];
  function make() { return {dataset:{}, children:[], value:'', childElementCount:0, listeners:{},
    classList:{toggle(){}}, replaceChildren(){this.children=[];},
    append(...items){this.children.push(...items); this.childElementCount=this.children.length;},
    add(option){this.children.push(option);}, addEventListener(event, cb){this.listeners[event]=cb;},
    querySelectorAll(){return this.children.filter(x => x.dataset?.agentKey);}}; }
  function $(id) {if(!elements.has(id)) elements.set(id,make());return elements.get(id);}
  const context = vm.createContext({$, document:{createElement:make},
    Option:function(label,value){this.label=label;this.value=value;},
    request:async(route,opts)=>calls.push({route,body:JSON.parse(opts.body)}), refresh:async()=>{}});
  vm.runInContext(`let current={workflow_id:'wf',status:'RUNNING',agent_session_id:'ses',
    phase:'WAITING_FOR_AGENT_INPUT',agent_request:{id:'frm',kind:'FORM',fields:[
      {key:'q0',type:'string',title:'Type choice',custom:true,options:[{value:'STRING'}]}]}};${code}`,context);
  return {context,$,calls};
}

test('SQL correction review shows failure and both SQL versions before approval',()=>{
  const h=harness();
  vm.runInContext(`current.review={kind:'oracle_view_bundle',agent_plan:{tables:[],views:[]},correction:{
    attempt:1,maximum_attempts:2,proposed_version:2,failure:{message:'PARSE_SYNTAX_ERROR'},
    changes:[{name:'s.v',before:'SELECT broken',after:'SELECT corrected'}]}};renderOracleViewBundle()`,h.context);
  const text=h.$('view-bundle-wrap').children.map(c=>c.textContent).join('\n');
  assert.match(text,/SQL correction 1\/2/);
  assert.match(text,/plan version 2/);
  assert.match(text,/PARSE_SYNTAX_ERROR/);
  assert.match(text,/Previous Spark SQL\nSELECT broken/);
  assert.match(text,/Corrected Spark SQL\nSELECT corrected/);
});
test('agent form preserves typed answer on refresh and submits correlated reply',async()=>{
  const h=harness();
  vm.runInContext('renderAgent()',h.context);
  h.$('agent-fields').querySelectorAll()[0].value='STRING';
  vm.runInContext('renderAgent()',h.context);
  assert.equal(h.$('agent-fields').querySelectorAll()[0].value,'STRING');
  h.$('agent-actor').value='reviewer';
  await h.$('agent-form').listeners.submit({preventDefault(){}});
  assert.deepEqual(h.calls,[{route:'/migrations/wf/agent-reply',body:{approved_by:'reviewer',request_id:'frm',answer:{q0:'STRING'}}}]);
});
test('permission UI only offers reject or once',()=>{
  const h=harness();
  vm.runInContext("current.agent_request={id:'perm',kind:'PERMISSION',action:'read',resources:['metadata']};renderAgent()",h.context);
  const choices=h.$('agent-fields').querySelectorAll()[0].children.map(x=>x.value);
  assert.deepEqual(choices,['','reject','once']);
});

test('revision blockers are prominent and live progress is separate from the report',()=>{
  const h=harness();
  vm.runInContext("current.phase='WAITING_FOR_AGENT_REVISION';current.agent_validation_error='Verify watermark';current.agent_report='plan';renderAgent()",h.context);
  assert.match(h.$('agent-summary').textContent,/paused.*Verify watermark/);
  assert.equal(h.$('agent-validation').textContent,'Verify watermark');
  assert.equal(h.$('agent-heading').textContent,'Discovery needs attention');
  assert.match(h.$('agent-explanation').textContent,/Column mappings are not available/);
  assert.equal(h.$('agent-report-details').open,true);
  assert.equal(h.$('agent-submit').textContent,'Send instructions to agent');
  vm.runInContext("current.phase='WAITING_FOR_AGENT';current.agent_progress=[{tool:'oracle-sqlcl',status:'running'}];renderAgent()",h.context);
  assert.match(h.$('agent-summary').textContent,/oracle-sqlcl/);
  assert.equal(h.$('agent-progress').textContent,'oracle-sqlcl: running');
});
