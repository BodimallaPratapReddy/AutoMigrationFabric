'use strict';
const assert = require('node:assert/strict');
const root = '/home/pratap/.local/share/abap-mcp/native/node_modules';
const {QueryHandlers} = require(root + '/abap-adt-mcp/dist/handlers/QueryHandlers.js');
const {ADTClient, session_types} = require(root + '/abap-adt-api');

async function unit() {
  let calls = 0;
  const preview = {
    isStateful: false,
    async runQuery(sql, limit, decode) {
      assert.equal(limit, 2); assert.equal(decode, false); calls++;
      return {columns: [], values: []};
    },
    async tableContents(name, limit, decode, sql) {
      assert.equal(name, 'ROOSOURCE'); assert.equal(limit, 2);
      assert.equal(decode, false); calls++;
      return {columns: [], values: []};
    }
  };
  const repo = {stateful: 'stateful', statelessClone: preview,
    runQuery() {throw new Error('Repository query must never run');},
    tableContents() {throw new Error('Repository preview must never run');}};
  const handler = new QueryHandlers(repo);
  for (let i = 0; i < 45; i++) await handler.handle('runQuery', {sqlQuery: 'SELECT OLTPSOURCE FROM ROOSOURCE', rowNumber: 2});
  await handler.handle('tableContents', {ddicEntityName: 'ROOSOURCE', rowNumber: 2});
  assert.equal(calls, 46); assert.equal(repo.stateful, 'stateful');
  preview.runQuery = async () => {throw new Error('simulated SAP failure');};
  await assert.rejects(handler.handle('runQuery', {sqlQuery: 'SELECT X', rowNumber: 2}), /simulated SAP failure/);
  assert.equal(repo.stateful, 'stateful');
  await assert.rejects(new QueryHandlers({statelessClone: {isStateful: true}}).handle('runQuery', {sqlQuery: 'SELECT X'}), /independent stateless/);
  const client = new ADTClient('https://example.invalid', 'dummy', 'dummy', '800');
  client.stateful = session_types.stateful;
  assert.notEqual(client.statelessClone, client);
  assert.equal(client.statelessClone.isStateful, false);
  assert.equal(client.isStateful, true);
  console.log('PASS: both tools use isolated client; 45 queries; failure isolation; stateful clone refusal; actual library clone behavior.');
}

async function live() {
  const fs = require('node:fs');
  const https = require('node:https');
  const secret = JSON.parse(fs.readFileSync('/mnt/c/Users/bodim/Desktop/Projects/opencode/test-skill-ddic-2/secrets.json', 'utf8'));
  const agent = new https.Agent({rejectUnauthorized: false});
  const client = new ADTClient(new URL(secret.API_URL).origin, secret.SAP_USERID, secret.SAP_PASSWORD, '800', 'EN', {httpsAgent: agent, timeout: 25000});
  client.stateful = session_types.stateful;
  const handler = new QueryHandlers(client);
  const sql = "SELECT OLTPSOURCE, OBJVERS FROM ROOSOURCE WHERE OLTPSOURCE = '0FI_GL_4' AND OBJVERS = 'A'";
  try {
    await client.login();
    for (let i = 0; i < 45; i++) {
      const response = await handler.handle('runQuery', {sqlQuery: sql, rowNumber: 2, decode: false});
      const payload = JSON.parse(response.content[0].text);
      assert.equal(payload.status, 'success');
      if ((i+1) % 10 === 0) console.log('Live metadata queries passed:', i+1);
    }
    await handler.handle('tableContents', {ddicEntityName: 'ROOSOURCE', sqlQuery: sql, rowNumber: 2, decode: false});
    const registry = "SELECT ENHNAME, BADI_IMPL, ACTIVE FROM BADIIMPL_ENH WHERE BADI_NAME = 'FAGL_APPLICATION'";
    await handler.handle('runQuery', {sqlQuery: registry, rowNumber: 30, decode: false});
    const source = await client.getObjectSource('/sap/bc/adt/ddic/tables/roosource/source/main');
    assert.ok(source); assert.equal(client.isStateful, true);
    console.log('PASS live: 45 control SELECTs, table preview, BAdI registry SELECT, subsequent stateful repository source read. No result rows printed.');
  } finally {
    try {await client.statelessClone.logout();} catch {}
    try {await client.logout();} catch {}
    agent.destroy();
  }
}
(async () => {await unit(); if (process.argv.includes('--live')) await live();})().catch(err => {
  console.error('Validation failed:', err.message); process.exitCode = 1;
});
