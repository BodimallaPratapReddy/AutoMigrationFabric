const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const {test} = require('node:test');

const script = fs.readFileSync(path.join(__dirname, '../app/migration_ui.html'), 'utf8')
  .match(/<script>([\s\S]*?)<\/script>/)[1];
const action = script.slice(script.indexOf('    async function sendAction('),
  script.indexOf("    $('approve').addEventListener"));
const rendering = script.slice(script.indexOf('    function renderPrimaryKeyFields('),
  script.indexOf('    const selectedSourceType'));

function harness(choice, selected) {
  const calls = [];
  let message = '';
  const elements = new Map();
  const makeElement = () => ({value: '', dataset: {}, checked: false, children: [],
    listeners: {}, addEventListener(event, callback) {this.listeners[event] = callback;},
    classList: {toggle() {}}, append(...children) {this.children.push(...children);},
    replaceChildren() {this.children = [];},
    querySelectorAll() {return selected.map(name => ({dataset: {primaryKeyColumn: name}}));}});
  const radios = ['fields', 'none'].map(value => ({value, checked: value === choice}));
  const element = id => {
    if (!elements.has(id)) elements.set(id, makeElement());
    return elements.get(id);
  };
  element('actor').value = 'reviewer';
  const context = vm.createContext({
    $: element,
    document: {querySelector: () => radios.find(r => r.checked),
      querySelectorAll: () => radios, createElement: makeElement, createTextNode: name => name},
    request: async (route, options) => calls.push({route, body: JSON.parse(options.body)}),
    setReviewMessage: value => {message = value;}, setTimeout() {}, refresh() {},
    showPrimaryKeyChoice() {}
  });
  vm.runInContext(`const current = {workflow_id:'test', phase:'WAITING_FOR_PRIMARY_KEY_APPROVAL',
    review:{primary_key:[],columns:[{name:'ID'},{name:'TENANT'}]}};
    ${action} ${rendering}`, context);
  return {context, calls, element, radios, message: () => message};
}

test('chosen composite key is submitted to the primary key endpoint', async () => {
  const h = harness('fields', ['ID', 'TENANT']);
  await vm.runInContext("sendAction('approve')", h.context);
  assert.deepEqual(h.calls, [{route:'/migrations/test/approve-primary-key',
    body:{approved_by:'reviewer',primary_key_columns:['ID','TENANT']}}]);
});

test('no key sends an explicit empty list', async () => {
  const h = harness('none', []);
  await vm.runInContext("sendAction('approve')", h.context);
  assert.deepEqual(h.calls[0].body.primary_key_columns, []);
});

for (const choice of [undefined, 'fields']) {
  test(`unconfirmed key decision (${choice}) is not sent`, async () => {
    const h = harness(choice, []);
    await vm.runInContext("sendAction('approve')", h.context);
    assert.equal(h.calls.length, 0);
    assert.ok(h.message());
  });
}

test('refreshing the same key review preserves the user selection', () => {
  const h = harness(undefined, []);
  vm.runInContext('renderPrimaryKeyFields()', h.context);
  const fields = h.element('primary-key-fields');
  assert.equal(fields.children.length, 2);
  assert.equal(fields.children[0].children[0].dataset.primaryKeyColumn, 'ID');
  fields.children[0].children[0].checked = true;
  h.radios[0].checked = true;
  vm.runInContext('renderPrimaryKeyFields()', h.context);
  assert.equal(fields.children[0].children[0].checked, true);
  assert.equal(h.radios[0].checked, true);
});

test('unique-index proposal fills all composite fields and still requires approval', () => {
  const h = harness(undefined, []);
  vm.runInContext(`current.review.primary_key_candidates = [{index_name:'UX_TENANT_ID',
    columns:['TENANT','ID'], reason:'Unique source index', warnings:['Check nulls']}];
    renderPrimaryKeyFields()`, h.context);
  const fields = h.element('primary-key-fields');
  fields.querySelectorAll = () => fields.children.map(label => label.children[0]);
  const proposal = h.element('primary-key-candidates').children[0];
  assert.match(proposal.children[0].textContent, /TENANT \+ ID/);
  assert.match(proposal.children[1].textContent, /Check nulls/);
  assert.ok(fields.children.every(label => !label.children[0].checked));
  proposal.children[0].listeners.click();
  assert.ok(fields.children.every(label => label.children[0].checked));
  assert.equal(h.radios[0].checked, true);
  assert.equal(h.calls.length, 0);
  vm.runInContext('renderPrimaryKeyFields()', h.context);
  assert.ok(fields.children.every(label => label.children[0].checked));
});

test('combined delete policy submits source values and reconciliation interval after confirmation', async () => {
  const h = harness(undefined, []);
  vm.runInContext(`current.phase = 'WAITING_FOR_DELETE_POLICY_APPROVAL';
    current.review.selected_watermark = 'UPDATED'`, h.context);
  h.element('delete-mode').value = 'SOFT_DELETE_AND_RECONCILE';
  h.element('delete-action').value = 'MARK';
  h.element('soft-delete-column').value = 'DELETED';
  h.element('soft-delete-predicate').value = 'VALUES';
  h.element('soft-delete-values').value = 'Y\n1';
  h.element('reconcile-interval').value = '1440';
  await vm.runInContext("sendAction('approve')", h.context);
  assert.equal(h.calls.length, 0);
  assert.match(h.message(), /watermark/);
  h.element('soft-delete-watermark-confirmed').checked = true;
  await vm.runInContext("sendAction('approve')", h.context);
  assert.deepEqual(h.calls, [{route:'/migrations/test/approve-delete-policy', body:{approved_by:'reviewer',
    delete_policy:{mode:'SOFT_DELETE_AND_RECONCILE',behavior:'MARK',soft_delete_column:'DELETED',
      soft_delete_predicate:'VALUES',soft_delete_values:['Y','1'],watermark_tracks_soft_delete:true,
      reconcile_interval_minutes:1440}}}]);
});

test('no-delete choice is explicit and malformed intervals are blocked', async () => {
  const h = harness(undefined, []);
  vm.runInContext("current.phase = 'WAITING_FOR_DELETE_POLICY_APPROVAL'", h.context);
  await vm.runInContext("sendAction('approve')", h.context);
  assert.equal(h.calls.length, 0);
  h.element('delete-mode').value = 'RECONCILE';
  h.element('delete-action').value = 'DELETE';
  h.element('reconcile-interval').value = '1.5';
  await vm.runInContext("sendAction('approve')", h.context);
  assert.equal(h.calls.length, 0);
  h.element('delete-mode').value = 'NONE';
  await vm.runInContext("sendAction('approve')", h.context);
  assert.deepEqual(h.calls[0].body.delete_policy, {mode:'NONE'});
});
