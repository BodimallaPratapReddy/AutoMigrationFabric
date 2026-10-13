const assert=require('node:assert/strict');
const path=require('node:path');
const {ObjectSourceHandlers}=require(path.join(path.dirname(process.env.SAP_MCP_INDEX),'handlers/ObjectSourceHandlers.js'));
async function read(url,classInclude=false) {
  const seen=[];
  const client={getObjectSource:async actual=>{
    seen.push(actual);
    return (classInclude || actual.endsWith('/source/main')) ? 'DEFINE process_line_item.\nEND-OF-DEFINITION.' : '<?xml version="1.0"?><adtcore:metadata/>';
  }};
  const handler=new ObjectSourceHandlers(client);
  const result=await handler.handle('getObjectSource',{objectSourceUrl:url});
  return {seen,payload:JSON.parse(result.content[0].text)};
}
(async()=>{
  const base='/sap/bc/adt/functions/groups/bwfid/includes/lbwfidd01';
  const explicit=await read(base+'/source/main');
  assert.equal(explicit.seen[0],base+'/source/main','function-group include source suffix must remain');
  assert.match(explicit.payload.source,/DEFINE process_line_item/);
  const bare=await read(base);
  assert.deepEqual(bare.seen,[base,base+'/source/main']);assert.match(bare.payload.source,/DEFINE/);
  const program='/sap/bc/adt/programs/includes/lbwfidtop/source/main';
  assert.equal((await read(program)).seen[0],program);
  const classUrl='/sap/bc/adt/oo/classes/zcl_test/includes/implementations';
  assert.equal((await read(classUrl+'/source/main',true)).seen[0],classUrl);
  console.log('Native MCP include-source regression checks passed');
})().catch(error=>{console.error(error.message);process.exitCode=1});
