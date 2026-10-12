// Exercises the installed native MCP dispatch seam without SAP writes or credentials.
const assert = require('node:assert/strict');
process.env.MCP_PROFILE_GATE = 'off';
const {AbapAdtServer} = require(process.env.SAP_MCP_INDEX);
const base = {name:'TEST',url:'https://example.invalid',user:'fixture',password:'fixture',authType:'basic',policy:{readOnly:true,allowFreeSql:true}};

async function fixture(policy, failures) {
  const server = new AbapAdtServer();
  server.systems = new Map([['TEST',{...base,policy}]]);
  server.setupNeeded=undefined;server.defaultDest='TEST';
  server.pool.clear();
  const dest = server.getDestination('TEST');
  let calls=0,auth=0,retries=0;
  server.reauthenticate=async()=>{auth++};
  dest.handlers.query.handle=async()=>{
    calls++;
    if (calls<=failures.length) throw failures[calls-1];
    return {status:'success'};
  };
  try {
    const result=await server.dispatch('runQuery',{destination:'TEST',sqlQuery:'SELECT ROLLNAME FROM DD04L'},()=>{retries++});
    return {calls,auth,retries,result};
  } catch(error) {return {calls,auth,retries,error};}
  finally {dest.httpsAgent?.destroy();}
}
const opaque=()=>Object.assign(new Error('Request failed with status code 400'),{status:400});
(async()=>{
  const policy=base.policy;
  const recovered=await fixture(policy,[opaque()]);
  assert.equal(recovered.calls,2,'opaque read-only failure must retry once');
  assert.equal(recovered.auth,1);assert.equal(recovered.retries,1);assert.ok(recovered.result);
  const bounded=await fixture(policy,[opaque(),opaque()]);
  assert.equal(bounded.calls,2);assert.equal(bounded.auth,1);assert.ok(bounded.error);
  const semantic=await fixture(policy,[Object.assign(new Error('Unknown column name REFTABLE'),{err:400,type:'ExceptionDataPreviewGeneral',namespace:'http://www.sap.com/adt/wda/dataPreview'})]);
  assert.equal(semantic.calls,1);assert.equal(semantic.auth,0);assert.ok(semantic.error);
  const mutable=await fixture({...policy,readOnly:false},[opaque()]);
  assert.equal(mutable.calls,1);assert.equal(mutable.auth,0);assert.ok(mutable.error);
  const server=new AbapAdtServer();
  const read=server.makeClient(base),write=server.makeClient({...base,policy:{readOnly:false}});
  assert.equal(read.adtClient.stateful,'stateless');assert.equal(write.adtClient.stateful,'stateful');
  read.httpsAgent?.destroy();write.httpsAgent?.destroy();
  console.log('Native MCP read-only session regression checks passed');
})().catch(error=>{console.error(error.message);process.exitCode=1});
