const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { test } = require('node:test');

const html = fs.readFileSync(path.join(__dirname, '../app/migration_ui.html'), 'utf8');
const script = html.match(/<script>([\s\S]*?)<\/script>/)[1];
new vm.Script(script); // Check the entire browser script for syntax errors.
const request = script.slice(script.indexOf('    async function request('),
  script.indexOf("    $('connection-form').addEventListener"));
const polling = script.slice(script.indexOf('    async function refresh('),
  script.indexOf("    $('start-form').addEventListener"));

function harness(status, saved = 'missing-workflow') {
  const elements = new Map();
  const intervals = new Map();
  let counter = 0;
  let calls = 0;
  const storage = new Map([['migrationWorkflowId', saved]]);
  const element = id => {
    if (!elements.has(id)) {
      const classes = new Set();
      elements.set(id, { value: id === 'lookup' ? 'missing-workflow' : '',
        textContent: '', classList: { add: c => classes.add(c),
          remove: c => classes.delete(c), contains: c => classes.has(c) } });
    }
    return elements.get(id);
  };
  const context = vm.createContext({
    $: element,
    setMessage: message => { element('message').textContent = message; },
    localStorage: { getItem: key => storage.get(key), removeItem: key => storage.delete(key) },
    setInterval: callback => { const id = ++counter; intervals.set(id, callback); return id; },
    clearInterval: id => intervals.delete(id),
    fetch: async () => {
      calls++;
      return { ok: false, status, json: async () => ({detail: 'Workflow was not found'}) };
    }
  });
  vm.runInContext(`let current = {workflow_id:'previous-workflow'};
    let timer = null; let refreshInProgress = false; ${request} ${polling}`, context);
  return { context, element, intervals, storage, calls: () => calls };
}

for (const status of [404, 410]) {
  test(`HTTP ${status} stops polling and removes stale workflow controls`, async () => {
    const h = harness(status);
    await vm.runInContext('trackWorkflow()', h.context);
    assert.equal(h.calls(), 1);
    assert.equal(h.intervals.size, 0);
    assert.equal(h.storage.has('migrationWorkflowId'), false);
    assert.equal(vm.runInContext('current', h.context), null);
    assert.equal(h.element('review-section').classList.contains('hidden'), true);
    assert.equal(h.element('cancel').classList.contains('hidden'), true);
    assert.equal(vm.runInContext('refreshInProgress', h.context), false);
  });
}

test('temporary service failures continue polling', async () => {
  const h = harness(503);
  await vm.runInContext('trackWorkflow()', h.context);
  assert.equal(h.intervals.size, 1);
  assert.equal(h.storage.get('migrationWorkflowId'), 'missing-workflow');
  await [...h.intervals.values()][0]();
  assert.equal(h.calls(), 2);
});

test('missing lookup preserves a different saved workflow', async () => {
  const h = harness(404, 'another-workflow');
  await vm.runInContext('trackWorkflow()', h.context);
  assert.equal(h.storage.get('migrationWorkflowId'), 'another-workflow');
});

function lakehouseHarness(responder) {
  const elements = new Map();
  const timeouts = new Map();
  let counter = 0;
  const element = id => {
    if (!elements.has(id)) elements.set(id, {
      value: id === 'workspace' ? 'workspace-1' : '', textContent: '', disabled: false,
      options: [], classList: {add() {}, remove() {}},
      replaceChildren(...options) { this.options = options; },
      add(option) { this.options.push(option); }
    });
    return elements.get(id);
  };
  const context = vm.createContext({
    $: element, AbortController,
    Option: function(text, value) { this.text = text; this.value = value; },
    setTimeout: callback => { const id = ++counter; timeouts.set(id, callback); return id; },
    clearTimeout: id => timeouts.delete(id), fetch: responder
  });
  const loading = script.slice(script.indexOf('    async function loadLakehouses('),
    script.indexOf("    $('workspace').addEventListener"));
  vm.runInContext(`let lakehouses = []; let lakehouseRequestVersion = 0;
    let lakehouseRequestController = null; ${request} ${loading}`, context);
  return {context, element, timeouts};
}

test('stalled Lakehouse fetch exits loading and offers retry', async () => {
  const h = lakehouseHarness((path, options) => new Promise((resolve, reject) => {
    options?.signal.addEventListener('abort', () => {
      const error = new Error('aborted'); error.name = 'AbortError'; reject(error);
    });
  }));
  const pending = vm.runInContext('loadLakehouses()', h.context);
  assert.equal(h.timeouts.size, 1);
  [...h.timeouts.values()][0]();
  await pending;
  assert.equal(h.element('lakehouse').options[0].text, 'Lakehouses unavailable');
  assert.match(h.element('lakehouse-list-message').textContent, /timed out/i);
  assert.equal(h.element('lakehouse-retry').disabled, false);
  assert.equal(h.timeouts.size, 0);
});

test('successful Lakehouse fetch enables the dropdown and clears its timeout', async () => {
  const h = lakehouseHarness(async () => ({ok: true, json: async () => [
    {lakehouse_id:'lakehouse-1', lakehouse_name:'Bronze'}
  ]}));
  await vm.runInContext('loadLakehouses()', h.context);
  assert.equal(h.element('lakehouse').disabled, false);
  assert.equal(h.element('lakehouse').options[1].value, 'lakehouse-1');
  assert.equal(h.timeouts.size, 0);
});
