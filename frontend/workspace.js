import '@fluentui/web-components/button.js';
import {setTheme} from '@fluentui/web-components/theme/set-theme.js';
import {webLightTheme, webDarkTheme} from '@fluentui/tokens';
import hljs from 'highlight.js/lib/core';
import sql from 'highlight.js/lib/languages/sql';
import python from 'highlight.js/lib/languages/python';
import './workspace.css';
import {createChatWorkspace} from './chat-workspace.js';

hljs.registerLanguage('sql', sql);
hljs.registerLanguage('python', python);
const $ = id => document.getElementById(id);
const control = window.migrationControl;
let approvalPreview = new URLSearchParams(location.search).get('preview') === 'approvals';
let previewStep = 0, previewSnapshot = null, previewControls = null;
const escape = value => String(value ?? '—').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const human = value => String(value ?? 'Not reported').replace(/_/g, ' ').toLowerCase().replace(/^./, c => c.toUpperCase());
const date = value => value ? new Date(value).toLocaleString(undefined, {month:'short',day:'numeric',hour:'2-digit',minute:'2-digit'}) : 'Not reported';
const typeName = value => ({ORACLE_TABLE:'Oracle table',ORACLE_VIEW:'Oracle view',SAP_TABLE:'SAP table',SAP_VIEW:'SAP view',SAP_ODP:'SAP ODP DataSource',SAP_REBUILD:'SAP DataSource rebuild'}[value] || 'Migration');
let jobs = [], nextCursor = null, loadingJobs = false, page = 'overview', selected = null;
let jobPageIndex=0, jobPageSize=20, jobPageCursors=[null];
let activeState = null, activeAudit = null, events = [], eventCursor = null, opening = 0, wizardStep = 0;
let eventsLoading = false, lastEventRefresh = 0;
let agentEvents = [], agentTimelineMessage = '';
function conversationEvents(){return [...events,...agentEvents].sort((a,b)=>Date.parse(a.time)-Date.parse(b.time)||String(a.id).localeCompare(String(b.id)));}
function recordedDetails(value){
  if(value?.watermark_column===null)value={...value,watermark_column:'Full refresh (no watermark)'};
  if(!Array.isArray(value?.columns))return structured(value);
  const {columns,...rest}=value;
  return structured(rest)+`<h3>Column mappings</h3><div class="table-scroll"><table><thead><tr><th>Column</th><th>Source type</th><th>Fabric type</th></tr></thead><tbody>${columns.map(c=>`<tr><td>${escape(c.name||c.column_name)}</td><td>${escape(c.source_type||'Not recorded')}</td><td>${escape(c.fabric_type)}</td></tr>`).join('')}</tbody></table></div>`;
}
const chatWorkspace = createChatWorkspace({openActivity:()=>loadEvents(),renderDetails:recordedDetails});
const themeMedia = matchMedia('(prefers-color-scheme: dark)');

function theme() {
  const choice = $('theme-choice').value;
  const dark = choice === 'dark' || (choice === 'system' && themeMedia.matches);
  document.documentElement.dataset.theme = dark ? 'dark' : 'light';
  const accent = dark ? '#9bc9f0' : '#073d68';
  setTheme({...dark ? webDarkTheme : webLightTheme,
    colorBrandBackground: dark ? '#164d79' : accent,
    colorBrandBackgroundHover: dark ? '#205f91' : '#052e50',
    colorBrandBackgroundPressed: '#03243f',
    colorBrandForeground1: accent, colorCompoundBrandForeground1: accent,
    colorCompoundBrandBackground: accent, colorCompoundBrandStroke: accent});
}
try { $('theme-choice').value = localStorage.getItem('fabricTheme') || 'system'; } catch {}
theme();
$('theme-choice').addEventListener('change', () => { localStorage.setItem('fabricTheme', $('theme-choice').value); theme(); });
themeMedia.addEventListener('change', theme);

async function api(path) {
  const response = await fetch(path, {cache:'no-store', signal:AbortSignal.timeout(55000)});
  const data = await response.json();
  if (!response.ok) throw new Error(typeof data.detail === 'string' ? data.detail : 'The request could not be completed.');
  return data;
}
function notice(message) { $('workspace-notice').textContent = message; $('workspace-notice').hidden = !message; }
function empty(title, text='') { return `<div class="empty-state"><strong>${escape(title)}</strong><p>${escape(text)}</p></div>`; }
function category(job) {
  if (job.waiting_for_user && job.status === 'RUNNING') return 'approval';
  if (['FAILED','TIMED_OUT','TERMINATED'].includes(job.status)) return 'error';
  if (job.status === 'RUNNING') return 'running';
  return 'complete';
}
function statusName(job) {
  if (job.waiting_for_user && job.status === 'RUNNING') return 'Needs your approval';
  if (job.status === 'RUNNING') return 'In progress';
  return ({PLANNED:'Plan saved',COMPLETED:job.state_available ? 'Completed' : 'Workflow finished',
    PROVISIONED:'Structures prepared',FAILED:'Needs attention',CANCELLED:'Cancelled',CANCELED:'Cancelled',
    DUPLICATE_TARGET:'Already configured',CHANGE_REQUESTED:'Change recorded',REJECTED:'Not approved',
    TIMED_OUT:'Timed out',TERMINATED:'Stopped'}[job.status] || human(job.status));
}
function badge(job) { return `<span class="badge ${category(job)}">${escape(statusName(job))}</span>`; }
function phaseName(job) {
  const phase = job.phase || '';
  return ({WAITING_FOR_COLUMN_MAPPING_APPROVAL:'Review column mappings',WAITING_FOR_PRIMARY_KEY_APPROVAL:'Choose record identifiers',
    WAITING_FOR_WATERMARK_APPROVAL:'Choose an update method',WAITING_FOR_DELETE_POLICY_APPROVAL:'Choose delete handling',
    WAITING_FOR_TARGET_CHANGE_APPROVAL:'Review destination changes',WAITING_FOR_ANALYSIS_APPROVAL:'Review source analysis',
    WAITING_FOR_FABRIC_PLAN_APPROVAL:'Review Fabric plan',WAITING_FOR_PLAN_APPROVAL:'Review migration plan',
    WAITING_FOR_AGENT_INPUT:'Answer a discovery question',WAITING_FOR_AGENT_REVISION:'Review requested corrections',
    WAITING_FOR_AGENT:'Discovering source details'}[phase] || (phase ? human(phase) : 'Details not reported'));
}
function exitApprovalPreview() {
  if (!approvalPreview) return;
  approvalPreview = false;
  delete window.migrationPreview;
  previewStep = 0; previewSnapshot = null;
  $('approval-preview')?.remove();
  for (const [id, properties] of previewControls || []) Object.assign($(id), properties);
  previewControls = null;
}
function showPage(name, writeHash=true) {
  if (name !== 'detail') chatWorkspace.close(false);
  if (name !== 'detail') exitApprovalPreview();
  page = name;
  for (const id of ['overview','jobs','new','connections','detail']) $(`${id}-page`).hidden = id !== name;
  document.querySelectorAll('[data-page]').forEach(el => el.classList.toggle('active', el.dataset.page === (name==='detail' ? 'jobs' : name)));
  $('breadcrumb').textContent = ({overview:'Overview',jobs:'Migration jobs',new:'New migration',connections:'Connections',detail:'Migration details'})[name];
  if (writeHash) history.replaceState(null, '', name === 'detail' && selected ? `?workflow_id=${encodeURIComponent(selected.workflow_id)}${approvalPreview?'&preview=approvals':''}#detail` : `${location.pathname}#${name}`);
  if (name === 'connections') renderConnections();
  if (name !== 'detail') control.stop();
  notice('');
}
document.querySelectorAll('[data-page]').forEach(el => el.addEventListener('click', () => showPage(el.dataset.page)));
document.querySelectorAll('.start-new').forEach(el => el.addEventListener('click', () => {showPage('new'); showWizard(0);}));
$('view-all').addEventListener('click', () => showPage('jobs'));
$('back-jobs').addEventListener('click', () => showPage('jobs'));
window.addEventListener('hashchange', () => {if (['overview','jobs','new','connections'].includes(location.hash.slice(1))) showPage(location.hash.slice(1));});

function jobTable(list) {
  if (!list.length) return empty('No migrations to show', 'Start a migration or adjust your filters.');
  return `<table><thead><tr><th>Migration</th><th>Source</th><th>Scope</th><th>Status</th><th>Started</th></tr></thead><tbody>${list.map(job => `<tr>
    <td><button class="job-link" data-job="${escape(job.workflow_id)}">${escape(job.source_object_name || job.workflow_id)}</button><span class="cell-sub">${escape(typeName(job.migration_approach))}</span></td>
    <td>${escape(job.source_connection_name || 'Not reported')}<span class="cell-sub">${escape(job.fabric_schema_name ? `To ${job.fabric_schema_name}` : 'Destination not reported')}</span></td>
    <td>${job.table_count == null && job.view_count == null ? '<span class="cell-sub">Not reported yet</span>' : `${job.table_count ?? 0} tables · ${job.view_count ?? 0} views`}</td>
    <td>${badge(job)}<span class="cell-sub">${escape(phaseName(job))}</span></td><td><span class="cell-sub">${escape(date(job.started_at))}</span></td></tr>`).join('')}</tbody></table>`;
}
function renderJobs() {
  const metrics = [['running','In progress','Working through preparation'],['approval','Waiting for approval','Ready for your decision'],['error','Needs attention','Review what happened'],['complete','Finished','Inspect recorded outcomes']];
  $('metrics').innerHTML = metrics.map(([key,label,note]) => `<button class="metric" data-filter="${key}"><span class="metric-title"><span class="state-dot" style="background:var(--${key==='error'?'danger':key==='approval'?'warning':key==='complete'?'success':'accent'})"></span>${label}</span><span class="metric-value">${jobs.filter(j=>category(j)===key).length}</span><span class="metric-note">${note}</span></button>`).join('');
  const pending = jobs.filter(job=>category(job)==='approval');
  $('attention-count').textContent = pending.length;
  $('attention-list').innerHTML = pending.length ? pending.map(job=>`<div class="attention-row"><div><strong>${escape(job.source_object_name || job.workflow_id)}</strong><p>${escape(phaseName(job))}</p></div><fluent-button appearance="outline" data-job="${escape(job.workflow_id)}">Review</fluent-button></div>`).join('') : empty('You’re all caught up', 'No loaded jobs are waiting for your approval.');
  $('recent-jobs').innerHTML = jobTable(jobs.slice(0,6));
  $('nav-count').textContent = jobs.length;
  const query = $('job-search').value.trim().toLowerCase();
  const state = $('job-filter').value, source = $('source-filter').value, days = $('date-filter').value;
  const filtered = jobs.filter(job => (state==='all'||category(job)===state) &&
    (source==='all'||job.migration_approach?.startsWith(source)) &&
    (days==='all'||new Date(job.started_at).getTime() >= Date.now()-Number(days)*86400000) &&
    [job.source_object_name,job.workflow_id,job.source_connection_name,job.fabric_schema_name].join(' ').toLowerCase().includes(query));
  $('all-jobs').innerHTML = jobTable(filtered);
  $('results-count').textContent = `${filtered.length} of ${jobs.length} entries on page ${jobPageIndex+1}`;
  renderJobPagination();
  $('history-note').textContent = `Summary covers ${jobs.length} loaded jobs on page ${jobPageIndex+1}. Data loading is tracked separately.`;
  const known=jobs.filter(job=>job.table_count!=null||job.view_count!=null);
  $('scope-counts').innerHTML=`<div><strong>${known.length?known.reduce((n,j)=>n+(j.table_count||0),0):'—'}</strong><span>Tables in reported plans</span></div><div><strong>${known.length?known.reduce((n,j)=>n+(j.view_count||0),0):'—'}</strong><span>Views in reported plans</span></div>`;
  $('scope-coverage').textContent=`Scope reported by ${known.length} of ${jobs.length} loaded jobs. An object can appear in more than one job.`;
}
function renderJobPagination(){
  $('jobs-page-number').textContent=`Page ${jobPageIndex+1}`;
  $('jobs-previous').disabled=loadingJobs||jobPageIndex===0;
  $('jobs-next').disabled=loadingJobs||!nextCursor;
  $('jobs-page-size').disabled=loadingJobs;
  $('jobs-page-size').value=String(jobPageSize);
  $('refresh-jobs').disabled=loadingJobs;
  $('all-jobs').setAttribute('aria-busy',String(loadingJobs));
}
async function loadJobs(targetPage=jobPageIndex,requestedSize=jobPageSize) {
  if (loadingJobs) return;
  const resizing=requestedSize!==jobPageSize;
  if(resizing)targetPage=0;
  const cursor=resizing?null:jobPageCursors[targetPage];
  if(targetPage<0||(targetPage>0&&!cursor))return;
  loadingJobs=true;renderJobPagination();
  try {
    const result=await api(`/workspace/jobs?limit=${requestedSize}${cursor?`&cursor=${encodeURIComponent(cursor)}`:''}`);
    jobs=result.jobs;
    jobPageSize=requestedSize;jobPageIndex=targetPage;
    if(resizing)jobPageCursors=[null];
    jobPageCursors=jobPageCursors.slice(0,targetPage+1);
    nextCursor=result.next_cursor;
    if(nextCursor)jobPageCursors[targetPage+1]=nextCursor;
    renderJobs();notice('');
    $('last-updated').textContent=`Updated ${new Date().toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})} · ${jobs.length} jobs on page ${jobPageIndex+1}`;
  } catch(error){
    notice(error.message);
    $('last-updated').textContent=jobs.length?'Unable to load page · Showing previous results':'Job history is unavailable';
    if(!jobs.length){
      $('recent-jobs').innerHTML=$('all-jobs').innerHTML=empty('Unable to load jobs','Use Refresh to try again.');
      $('attention-list').innerHTML=empty('Approval status unavailable','Reconnect to job history to see pending decisions.');
    }
  } finally {loadingJobs=false;renderJobPagination();}
}
$('refresh-jobs').addEventListener('click',()=>loadJobs());
$('jobs-previous').addEventListener('click',()=>loadJobs(jobPageIndex-1));
$('jobs-next').addEventListener('click',()=>loadJobs(jobPageIndex+1));
$('jobs-page-size').addEventListener('change',()=>loadJobs(0,Number($('jobs-page-size').value)));
for (const id of ['job-search','job-filter','source-filter','date-filter']) $(id).addEventListener(id==='job-search'?'input':'change',renderJobs);
document.addEventListener('click',event=>{
  const target = event.target.closest('[data-job],[data-filter]');
  if (target?.dataset.job) openJob(target.dataset.job);
  if (target?.dataset.filter) { $('job-filter').value=target.dataset.filter; renderJobs(); showPage('jobs'); }
});

function wizardSummary() {
  const selectedText = id => $(id).selectedOptions[0]?.textContent || 'Not selected';
  $('start-summary').innerHTML = readable({Source:[$('schema').value,$('source').value].filter(Boolean).join('.'),
    Connection:selectedText('connection'),'Migration type':selectedText('approach'),Workspace:selectedText('workspace'),
    Lakehouse:selectedText('lakehouse'),'Destination':[$('fabric-schema').value,$('target').value||$('source').value].filter(Boolean).join('.')});
}
function showWizard(step) {
  wizardStep=step;
  ['source','destination','review'].forEach((name,index)=>$(`wizard-${name}`).hidden=index!==step);
  $('start-section').querySelector('h2').textContent=['Choose your source','Choose your destination','Review and start'][step];
  $('wizard-help').textContent=['Tell us what you would like to bring into Fabric.','Select where the approved structures should be prepared.','Discovery starts first. You will review the plan before it is applied.'][step];
  [...$('wizard-steps').children].forEach((el,index)=>el.classList.toggle('active',index===step));
  $('wizard-back').hidden=step===0; $('wizard-next').hidden=step===2; $('start-submit').hidden=step!==2;
  if(step===2) wizardSummary();
}
$('wizard-next').addEventListener('click',()=>{
  const fields = wizardStep===0 ? ['connection','approach','source',...($('schema-wrap').classList.contains('hidden')?[]:['schema'])] : ['workspace','lakehouse','fabric-schema'];
  for (const id of fields) { const field=$(id); if(!field.value.trim()) { field.setCustomValidity('Please complete this field.'); field.reportValidity();field.setCustomValidity('');return; } }
  showWizard(wizardStep+1);
});
$('wizard-back').addEventListener('click',()=>showWizard(wizardStep-1));
$('execution-mode').addEventListener('change',()=>{
  if($('execution-mode').value==='plan'){ $('notebook').dataset.previous=$('notebook').value; $('notebook').value=''; }
  else $('notebook').value=$('notebook').dataset.previous||'';
  $('advanced-settings').hidden=$('execution-mode').value==='plan';
});
$('start-form').addEventListener('submit',event=>{
  if(wizardStep!==2){event.preventDefault();event.stopImmediatePropagation();$('wizard-next').click();return;}
  if($('execution-mode').value==='prepare'&&!$('notebook').value.trim()){
    event.preventDefault();event.stopImmediatePropagation();$('advanced-settings').open=true;$('notebook').focus();
    $('start-error').textContent='Enter a provisioning notebook ID, or choose to save the plan only.';return;
  }
  $('start-error').textContent=''; $('start-submit').disabled=true;
},true);
new MutationObserver(()=>{
  if(page==='new') { $('start-error').textContent=$('message').textContent; $('start-submit').disabled=false; }
}).observe($('message'),{childList:true,subtree:true,characterData:true});

async function renderConnections() {
  $('connection-list').innerHTML=empty('Loading connections…');
  try {
    const list = await api('/connections');
    $('connection-list').innerHTML=list.length ? `<table><thead><tr><th>Connection</th><th>Source system</th><th>Status</th></tr></thead><tbody>${list.map(item=>`<tr><td><strong>${escape(item.connection_name)}</strong></td><td>${escape(item.source_type==='ORACLE'?'Oracle':'SAP ECC')}</td><td><span class="badge ${item.is_active?'complete':''}">${item.is_active?'Active':'Inactive'}</span></td></tr>`).join('')}</tbody></table>` : empty('No source connections yet','Add an Oracle or SAP connection to start your first migration.');
  }catch(error){$('connection-list').innerHTML=empty('Connections unavailable',error.message);}
}
$('add-connection').addEventListener('click',()=>$('new-connection').click());
$('new-connection').addEventListener('click',()=>{$('connection-dialog').showModal();$('connection-name').focus();});
$('close-connection').addEventListener('click',()=>$('connection-dialog').close());
new MutationObserver(()=>{if($('connection-section').classList.contains('hidden')&&$('connection-dialog').open){$('connection-dialog').close();if(page==='connections')renderConnections();}}).observe($('connection-section'),{attributes:true,attributeFilter:['class']});

function readable(values) {
  return `<div class="readable-grid">${Object.entries(values).filter(([,v])=>v!==undefined).map(([key,value])=>`<div><small>${escape(human(key))}</small>${escape(Array.isArray(value)?value.join(', '):value ?? 'None')}</div>`).join('')}</div>`;
}
function renderFacts(job) {
  const workspace = control.workspaces().find(w=>w.workspace_id===job.fabric_workspace_id)?.workspace_name || job.fabric_workspace_id;
  const facts = {'Source connection':job.source_connection_name,'Migration type':typeName(job.migration_approach),
    'Destination schema':job.fabric_schema_name,Workspace:workspace,'Requested by':job.requested_by,
    Started:date(job.started_at),'Data loading':job.state?.replication_status ? human(job.state.replication_status) : 'Not reported by pipeline'};
  $('job-facts').innerHTML=`<dl class="facts">${Object.entries(facts).map(([k,v])=>`<div class="facts-row"><dt>${escape(k)}</dt><dd>${escape(v||'Not reported')}</dd></div>`).join('')}</dl>`;
}
async function openJob(id) {
  const version=++opening;
  control.stop(); selected={workflow_id:id}; activeState=null;activeAudit=null;events=[];eventCursor=null;lastEventRefresh=0;
  agentEvents=[];agentTimelineMessage='';
  showPage('detail');selectTab('conversation');
  $('detail-title').textContent='Loading migration…';$('detail-subtitle').textContent=id;
  for(const section of ['agent-section','review-section','audit-section','provisioning-report-section']) $(section).classList.add('hidden');
  $('conversation-history').replaceChildren();$('outcome-card').innerHTML=empty('Loading job details…');
  try {
    const job=await api(`/workspace/jobs/${encodeURIComponent(id)}`);
    if(version!==opening)return;
    selected=job;
    update(job.state || {workflow_id:id,status:job.status,source_object_name:job.source_object_name,migration_approach:job.migration_approach},null);
    if(!approvalPreview) loadEvents();
    if(job.state_available) await control.open(id);
    else { $('cancel').classList.add('hidden');$('status').classList.add('hidden'); }
  }catch(error){if(version===opening){notice(error.message);$('outcome-card').innerHTML=empty('Job unavailable',error.message);}}
}
function selectTab(name) {
  if(name==='conversation')chatWorkspace.close(false);
  else chatWorkspace.open(name);
}
document.querySelectorAll('[data-tab]').forEach((button,index,buttons)=>{
  button.addEventListener('click',()=>selectTab(button.dataset.tab));
  button.addEventListener('keydown',event=>{if(['ArrowLeft','ArrowRight','Home','End'].includes(event.key)){
    event.preventDefault();const next=event.key==='Home'?0:event.key==='End'?buttons.length-1:(index+(event.key==='ArrowRight'?1:-1)+buttons.length)%buttons.length;
    buttons[next].focus();selectTab(buttons[next].dataset.tab);
  }});
});

const questions = {
  WAITING_FOR_COLUMN_MAPPING_APPROVAL:['How should these fields be stored in Fabric?','Review the suggested types and any warnings before continuing.'],
  WAITING_FOR_PRIMARY_KEY_APPROVAL:['Which fields uniquely identify each record?','Choose a stable identifier, or use a full refresh if there is no reliable key.'],
  WAITING_FOR_WATERMARK_APPROVAL:['How should future changes be picked up?','Choose a full refresh or a date/time field that changes whenever a record is updated.'],
  WAITING_FOR_DELETE_POLICY_APPROVAL:['What should happen when source records are deleted?','These settings tell the Fabric pipeline how to handle records that disappear or are marked as deleted.'],
  WAITING_FOR_TARGET_CHANGE_APPROVAL:['How should the existing destination change?','Review the differences and the effect on existing data before choosing an action.'],
  WAITING_FOR_ANALYSIS_APPROVAL:['Does this source analysis look right?','Review the discovered objects and the proposed extraction approach.'],
  WAITING_FOR_FABRIC_PLAN_APPROVAL:['Is this the right plan for Fabric?','Check the proposed structures and request changes if needed.'],
};
function update(state,audit) {
  if(selected && selected.workflow_id!==state.workflow_id)return;
  const reachedOutcome = activeState?.status === 'RUNNING' && state.status !== 'RUNNING';
  activeState=state;activeAudit=audit;
  selected={...selected,...state,state,state_available:true};
  $('detail-title').textContent=state.source_object_name||selected.workflow_id;
  $('detail-kicker').textContent=typeName(state.migration_approach);
  $('detail-subtitle').textContent=[selected.source_connection_name,selected.fabric_schema_name].filter(Boolean).join(' → ') || phaseName(state);
  $('detail-badge').innerHTML=state.phase==='WAITING_FOR_AGENT_REVISION'?'<span class="badge error">Discovery needs attention</span>':badge(selected);renderFacts(selected);
  const phase=state.phase||'';
  const stage=state.status!=='RUNNING'?4:/PROVISION|EXECUT|PREPAR/.test(phase)?3:state.waiting_for_user?(/PLAN|FABRIC/.test(phase)?2:1):0;
  const successful = ['COMPLETED','PROVISIONED','PLANNED'].includes(state.status);
  $('job-stages').innerHTML=['Discover','Review','Approve','Prepare structures','Outcome'].map((text,index)=>`<li class="${index===stage?'active':index<stage&&(state.status==='RUNNING'||successful)&&!(index===3&&state.status==='PLANNED')?'done':''}">${index+1} &nbsp; ${text}</li>`).join('');
  const review=state.review||{};
  const question=questions[phase]||['Ready to approve this migration plan?','Review the objects and settings below. Approval records your decision and continues this workflow.'];
  $('review-title').textContent=question[0];
  $('review-explanation').textContent=(review.view_base_table?`Table ${review.base_table_index} of ${review.base_table_count}: ${review.source}. `:'')+question[1];
  $('review-readable').innerHTML=reviewSummary(review);
  if(review.agent_plan?.tables && review.agent_plan?.views) renderBundle(review);
  if(state.status!=='RUNNING') $('agent-section').classList.add('hidden');
  if(!['WAITING_FOR_COLUMN_MAPPING_APPROVAL','WAITING_FOR_PRIMARY_KEY_APPROVAL','WAITING_FOR_DELETE_POLICY_APPROVAL','WAITING_FOR_WATERMARK_APPROVAL','WAITING_FOR_TARGET_CHANGE_APPROVAL'].includes(phase)) $('approve').textContent='Approve this plan';
  $('agent-summary').textContent=state.current_message||phaseName(state);
  // Existing mapping renderer supplies inline warning colors; theme them consistently.
  document.querySelectorAll('.mapping-grid tr[style]').forEach(row=>row.style.backgroundColor='var(--warning-bg)');
  renderObjects();renderScripts();renderOutcome();
  if(approvalPreview) renderPreview();
  else if((reachedOutcome||Date.now()-lastEventRefresh>20000)&&page==='detail')loadEvents();
  chatWorkspace.render(activeState,selected,conversationEvents(),approvalPreview,agentTimelineMessage);
}
function reviewSummary(review) {
  const result={};
  for(const [key,label] of [['source','Source object'],['description','Description'],['route','Extraction approach'],['load_method','Update method'],['write_strategy','Write behavior'],['selected_watermark','Change tracking field']]) if(review[key])result[label]=humanValue(review[key]);
  if(review.primary_key)result['Record identifier']=review.primary_key.join(' + ')||'No key selected';
  if(review.key_validation)result['Key check']=review.key_validation.valid===true?'Passed at time of check':'Not verified';
  let markup=readable(result);
  if(review.warnings?.length) markup+=`<details class="review-notes"><summary>Review ${review.warnings.length} important notes</summary><ul class="muted">${review.warnings.map(w=>`<li>${escape(typeof w==='string'?w:human(w.code||w.message))}</li>`).join('')}</ul></details>`;
  if(review.target_review){const selected=review.target_review.definitions?.find(x=>x.source_table_guid===review.target_review.selected_existing_guid);const diff=selected?.comparison;
    if(diff){markup+='<h3>Proposed changes</h3><table><thead><tr><th>Field</th><th>Before</th><th>After</th></tr></thead><tbody>';
    for(const c of diff.added||[])markup+=`<tr><td>${escape(c.name)}</td><td>Not present</td><td>${escape(c.fabric_type)}</td></tr>`;
    for(const c of diff.removed||[])markup+=`<tr><td>${escape(c.name)}</td><td>${escape(c.fabric_type)}</td><td>Remove</td></tr>`;
    for(const c of diff.changed||[])markup+=`<tr><td>${escape(c.before.name)}</td><td>${escape(c.before.fabric_type)}</td><td>${escape(c.after.fabric_type)}</td></tr>`;
    markup+='</tbody></table>';}
  }
  if(['WAITING_FOR_ANALYSIS_APPROVAL','WAITING_FOR_FABRIC_PLAN_APPROVAL'].includes(activeState?.phase)){
    const extra=Object.fromEntries(Object.entries(review).filter(([key])=>!['agent_plan','columns','target_review','source','description','warnings','route','load_method','write_strategy','selected_watermark','primary_key','key_validation'].includes(key)));
    markup+=structured(extra);
  }
  return markup;
}
function structured(value,depth=0){
  if(value==null)return '<span class="muted">Not supplied</span>';
  if(typeof value!=='object')return `<p>${escape(value)}</p>`;
  if(depth>5)return '<p class="muted">Additional detail is available in the technical plan.</p>';
  if(Array.isArray(value)){
    if(value.every(v=>typeof v!=='object'))return `<ul>${value.map(v=>`<li>${escape(v)}</li>`).join('')}</ul>`;
    return value.map((v,i)=>`<details><summary>${escape(v?.name||v?.object_name||v?.target_name||`Item ${i+1}`)}</summary>${structured(v,depth+1)}</details>`).join('');
  }
  return Object.entries(value).map(([key,item])=>typeof item==='object'?`<details ${depth===0?'open':''}><summary>${escape(human(key))}</summary>${structured(item,depth+1)}</details>`:
    /sql|script|code/.test(key)&&typeof item==='string'?`<h3>${escape(human(key))}</h3><pre><code>${escape(item)}</code></pre>`:readable({[key]:item})).join('');
}
function humanValue(value){return ({FULL:'Refresh all records',WATERMARK:'New and updated records',UPSERT:'Update matching records and add new ones',REPLACE:'Replace the table contents'}[value]||value);}
function renderBundle(review){
  const plan=review.agent_plan;
  let html=`<p>${plan.tables.length} base tables and ${plan.views.length} views are included in this plan.</p>`;
  if(review.correction){
    html+=`<h3>Corrected SQL · attempt ${escape(review.correction.attempt)}</h3><p class="muted">Only failed views are retried. Previously successful objects are reused.</p>`;
    for(const change of review.correction.changes||[])html+=`<details><summary>${escape(change.name)} · Compare SQL</summary><h3>Previous SQL</h3><pre><code class="hljs">${hljs.highlight(change.before||'',{language:'sql'}).value}</code></pre><h3>Proposed correction</h3><pre><code class="hljs">${hljs.highlight(change.after||'',{language:'sql'}).value}</code></pre></details>`;
  }
  for(const table of plan.tables){
    const source=[table.source_schema,table.source_table].filter(Boolean).join('.');
    const approved=review.table_reviews?.find(t=>t.source===source);
    html+=`<details class="object-detail"><summary>${escape(source)} → ${escape(table.target_table)} <span class="badge">${table.columns.length} fields</span></summary>${readable({'Record identifier':(approved?.primary_key||table.primary_key||[]).join(' + ')||'None','Update method':humanValue(approved?.load_method||'Not reviewed'),'Change tracking field':approved?.selected_watermark||'None','Delete handling':human(approved?.delete_policy?.mode||'Not reviewed'),'Database description':approved?.description||'Not supplied'})}<div class="table-scroll"><table><thead><tr><th>Field</th><th>Source type</th><th>Fabric type</th><th>Note</th></tr></thead><tbody>${table.columns.map(c=>`<tr><td>${escape(c.name)}</td><td>${escape(c.source_type)}</td><td>${escape(c.fabric_type)}</td><td>${escape(c.warning||c.description||'—')}</td></tr>`).join('')}</tbody></table></div></details>`;
  }
  for(const view of plan.views)html+=`<details class="object-detail"><summary>${escape(view.target_name)} <span class="badge">View</span></summary><p class="muted">Depends on: ${escape((view.dependencies||[]).join(', ')||'Not reported')}</p><pre><code class="hljs">${hljs.highlight(view.spark_sql||'',{language:'sql'}).value}</code></pre>${(view.translation_notes||[]).map(n=>`<p class="muted">${escape(n)}</p>`).join('')}</details>`;
  const el=$('view-bundle-wrap');
  // Do not collapse an open table each time the workflow polls.
  if(el.dataset.readableContent!==html||!el.querySelector('[data-readable-bundle]')){el.innerHTML=`<div data-readable-bundle>${html}</div>`;el.dataset.readableContent=html;}
}
function objects() {
  const review=activeState?.review||{}, plan=review.agent_plan||{};
  const items=new Map();
  const add=(item)=>{const key=`${item.type}:${item.target||item.source}`;items.set(key,{...items.get(key),...item});};
  for(const t of plan.tables||[])add({type:'Table',source:[t.source_schema,t.source_table].filter(Boolean).join('.'),target:t.target_table,columns:t.columns,key:t.primary_key,status:'Planned'});
  for(const v of plan.views||[])add({type:'View',source:[v.source_schema,v.source_view].filter(Boolean).join('.'),target:v.target_name,dependencies:v.dependencies,sql:v.spark_sql,status:'Planned'});
  if(!items.size&&review.columns) add({type:'Table',source:review.source||activeState.source_object_name,target:selected.fabric_target_name||activeState.source_object_name,columns:review.columns,key:review.primary_key,status:'Under review'});
  for(const t of activeAudit?.tables||[])add({type:'Table',source:t.source,target:t.target,status:human(t.provisioning_status),replication:t.replication});
  // Batch rows are execution evidence. Preserve every batch in Activity; latest evidence wins here.
  const batches=[...(activeAudit?.batches||[])].sort((a,b)=>new Date(a.started_timestamp)-new Date(b.started_timestamp));
  for(const batch of batches)for(const obj of batch.objects||[]){
    if(!['TABLE','VIEW'].includes(obj.object_type))continue;
    const type=obj.object_type==='TABLE'?'Table':'View';
    const match=[...items.values()].find(item=>item.type===type&&(item.target===obj.object_name||item.source===obj.object_name||item.source?.split('.').pop()===obj.object_name));
    add({type,target:match?.target||obj.object_name,status:human(obj.status),error:obj.error_message,rows:obj.rows_written});
  }
  for(const obj of activeState?.provisioning_report?.objects||[]){
    const type=obj.object_type==='TABLE'?'Table':'View';
    const shortName=obj.name?.split('.').pop();
    const match=[...items.values()].find(item=>item.type===type&&(item.target===obj.name||item.target===shortName));
    add({type,target:match?.target||obj.name,status:({SUCCESS:'Prepared',FAILED:'Failed',SKIPPED:'Skipped'}[obj.status]||human(obj.status)),
      error:obj.status==='FAILED'?obj.message:undefined,message:obj.message,rows:obj.rows_written});
  }
  return [...items.values()];
}
function renderObjects() {
  const list=objects();
  $('object-tab-count').textContent=list.length?`(${list.length})`:'';
  $('object-summary').innerHTML=`<span>${list.filter(o=>o.type==='Table').length} recorded tables</span><span>${list.filter(o=>o.type==='View').length} recorded views</span>`;
  $('object-list').innerHTML=list.length?list.map(obj=>`<details class="object-detail"><summary>${escape(obj.target||obj.source)} <span class="badge">${escape(obj.type)} · ${escape(obj.status)}</span></summary>${readable({'Source':obj.source||'Not reported','Destination':obj.target||'Not reported','Record identifier':obj.key?.join(' + '),'Dependencies':obj.dependencies?.join(', '),'Rows written':obj.rows??'Not reported','Result':obj.message,'Error':obj.error})}${obj.columns?.length?`<div class="table-scroll"><table><thead><tr><th>Field</th><th>Source type</th><th>Fabric type</th><th>Description</th></tr></thead><tbody>${obj.columns.map(c=>`<tr><td class="object-name">${escape(c.name)}</td><td>${escape(c.source_type)}</td><td>${escape(c.fabric_type)}</td><td>${escape(c.description||c.warning||'—')}</td></tr>`).join('')}</tbody></table></div>`:''}</details>`).join(''):empty('Object inventory is not available yet','Discovered objects and confirmed results appear here when the workflow reports them.');
}
function scripts() {
  const list=[], seen=new Set();
  function walk(value,path='',depth=0){if(!value||depth>10)return;
    if(Array.isArray(value)){value.forEach((v,i)=>walk(v,`${path} ${i+1}`,depth+1));return;}
    if(typeof value!=='object')return;
    for(const [key,item] of Object.entries(value)){
      const label=value.target_name||value.target_table||value.source||value.name||path;
      if(typeof item==='string'&&/sql$|sql_script|python_code|notebook_code/i.test(key)&&!seen.has(item)){
        seen.add(item);list.push({title:label||human(key),label:human(key),code:item,language:/python|notebook/.test(key)?'python':'sql'});
      }else if(/view_definitions$/.test(key)&&item&&typeof item==='object'){
        for(const [name,code] of Object.entries(item))if(typeof code==='string'&&!seen.has(code)){seen.add(code);list.push({title:name,label:'Source view SQL',code,language:'sql'});}
      }else if(typeof item==='object')walk(item,label||human(key),depth+1);
    }
  }
  walk(activeState?.review);return list;
}
function renderScripts() {
  const list=scripts();
  $('script-list').innerHTML=list.length?list.map((script,index)=>`<article class="code-block"><div class="code-heading"><h3>${escape(script.title)}</h3><fluent-button data-copy="${index}">Copy code</fluent-button></div><div class="code-label">${escape(script.label)} · Reported plan</div><pre><code class="hljs">${hljs.highlight(script.code,{language:script.language}).value}</code></pre></article>`).join(''):empty('No scripts reported for this stage','When the workflow supplies extraction SQL or view definitions, they appear here.');
}
$('script-list').addEventListener('click',async event=>{const button=event.target.closest('[data-copy]');if(!button)return;try{await navigator.clipboard.writeText(scripts()[Number(button.dataset.copy)].code);button.textContent='Copied';}catch{button.textContent='Select code to copy';}});
function renderOutcome() {
  if(!activeState)return;
  const job=selected;
  const recordedError = [...events].reverse().find(event=>(event.kind==='error'||event.kind==='activity_status')&&event.message)?.message;
  const error = category(job)==='error' ? (recordedError || job.last_error) : job.last_error;
  if(job.status==='RUNNING'){$('outcome-card').innerHTML='';return;}
  const text=job.status==='RUNNING' ? 'Preparation is in progress. You can leave this page and return to the job later.' :
    job.status==='PLANNED'?'Your approved configuration has been saved. Fabric structures have not been provisioned by this job.':
    category(job)==='error'?'This job needs attention. Review the recorded error and activity before starting another migration.':
    'Review the per-object results for confirmed changes. Data loading runs separately through the Fabric pipelines.';
  $('outcome-card').innerHTML=`<span class="eyebrow">${job.status==='RUNNING'?'CURRENT STEP':'JOB OUTCOME'}</span><h2 class="outcome-title">${escape(job.status==='RUNNING'?phaseName(job):statusName(job))}</h2><p class="muted">${text}</p>${error?`<details open><summary>What happened</summary><p class="error">${escape(error)}</p></details>`:''}${job.agent_blocker?.required_resolution?`<p>${escape(job.agent_blocker.required_resolution)}</p>`:''}`;
}
function eventHTML(event) {
  const details=event.details||{};
  return `<article class="conversation-event"><strong>${escape(event.title)}</strong><span class="event-time">${escape(date(event.time))}${event.actor?` · Recorded by ${escape(event.actor)}`:''}</span>${event.message?`<p class="muted">${escape(event.message)}</p>`:''}${Object.keys(details).length?`<details class="event-details"><summary>View recorded decision</summary>${renderDecision(details)}</details>`:''}</article>`;
}
function renderDecision(details) {
  let output='';
  for(const [key,value] of Object.entries(details)){
    if(Array.isArray(value)&&value.length&&typeof value[0]==='object') output+=`<p><strong>${escape(human(key))}</strong></p><div class="table-scroll"><table><thead><tr>${Object.keys(value[0]).map(k=>`<th>${escape(human(k))}</th>`).join('')}</tr></thead><tbody>${value.map(row=>`<tr>${Object.keys(value[0]).map(k=>`<td>${escape(row[k])}</td>`).join('')}</tr>`).join('')}</tbody></table></div>`;
    else if(value&&typeof value==='object'&&!Array.isArray(value)) output+=`<h3>${escape(human(key))}</h3>${readable(value)}`;
    else output+=readable({[key]:value});
  }return output;
}
function renderEvents(){
  $('activity-list').innerHTML=events.length?events.map(eventHTML).join(''):empty('No activity entries in this page','Use Load more events if additional history is available.');
  chatWorkspace.render(activeState,selected,conversationEvents(),approvalPreview,agentTimelineMessage);
  $('load-more-events').hidden=!eventCursor;
}
async function loadEvents(append=false){
  if(!selected||eventsLoading===selected.workflow_id)return;
  const id=selected.workflow_id;eventsLoading=id;lastEventRefresh=Date.now();
  $('load-more-events').disabled=true;
  try{let result=await api(`/workspace/jobs/${encodeURIComponent(id)}/events${append&&eventCursor?`?cursor=${encodeURIComponent(eventCursor)}`:''}`);
    if(selected?.workflow_id!==id)return;
    const merged=new Map(events.map(e=>[e.id,e]));result.events.forEach(e=>merged.set(e.id,e));
    const seenCursors=new Set();
    while(result.next_cursor&&!seenCursors.has(result.next_cursor)){
      seenCursors.add(result.next_cursor);
      result=await api(`/workspace/jobs/${encodeURIComponent(id)}/events?cursor=${encodeURIComponent(result.next_cursor)}`);
      if(selected?.workflow_id!==id)return;
      result.events.forEach(e=>merged.set(e.id,e));
    }
    events=[...merged.values()].sort((a,b)=>a.id-b.id);eventCursor=result.next_cursor;renderEvents();renderOutcome();
    if(activeState?.agent_session_id&&!approvalPreview){
      try{
        const timeline=await api(`/workspace/jobs/${encodeURIComponent(id)}/agent-timeline`);
        if(selected?.workflow_id!==id)return;
        agentEvents=timeline.events;agentTimelineMessage=timeline.truncated?'Showing the latest 500 AutoMigrate Agent messages.':timeline.message||'';
      }catch(error){if(selected?.workflow_id===id)agentTimelineMessage='AutoMigrate Agent activity is temporarily unavailable. Showing the last recorded updates.';}
      if(selected?.workflow_id===id)renderEvents();
    }
  }catch(error){if(selected?.workflow_id===id)$('activity-list').innerHTML=empty('Activity unavailable',error.message);}
  finally{if(eventsLoading===id)eventsLoading=false;$('load-more-events').disabled=false;}
}
$('refresh-events').addEventListener('click',()=>loadEvents());
$('load-more-events').addEventListener('click',()=>loadEvents(true));
$('download-summary').addEventListener('click',()=>{
  if(!selected)return;
  const text=[`Migration: ${selected.source_object_name||selected.workflow_id}`,`Job: ${selected.workflow_id}`,`Outcome: ${statusName(selected)}`,`Started: ${date(selected.started_at)}`,`Source: ${selected.source_connection_name||'Not reported'}`,
    '\nOBJECTS',...objects().map(o=>`${o.type}: ${o.source||'—'} → ${o.target||'—'} | ${o.status}`),
    '\nLOADED ACTIVITY',...events.map(e=>`${date(e.time)} | ${e.title}${e.actor?` | ${e.actor}`:''}`),
    eventCursor?'Additional activity is available in the app.':'', '\nData loading is tracked separately. History follows Temporal retention.'].join('\n');
  const url=URL.createObjectURL(new Blob([text],{type:'text/plain'}));const link=document.createElement('a');link.href=url;link.download=`migration-${selected.workflow_id}.txt`;link.click();setTimeout(()=>URL.revokeObjectURL(url),1000);
});
window.migrationWorkspace={update,started(id){selected={workflow_id:id};events=[];agentEvents=[];agentTimelineMessage="";eventCursor=null;showPage('detail');selectTab('conversation');$('start-submit').disabled=false;api(`/workspace/jobs/${encodeURIComponent(id)}`).then(job=>{if(selected?.workflow_id===id){selected={...selected,...job};renderFacts(selected);}}).catch(()=>{});}};
if(approvalPreview){
  window.migrationPreview={transform(state){
    previewSnapshot=structuredClone(state);
    const reviews=state.review?.table_reviews||[];
    if(state.migration_approach!=='ORACLE_VIEW'||!reviews.length)return state;
    const copy=structuredClone(state);
    copy.status='RUNNING';copy.waiting_for_user=true;copy.agent_session_id=null;
    copy.provisioning_report=null;copy.last_error=null;
    copy.phase=previewStep>0&&previewStep<=reviews.length?'WAITING_FOR_COLUMN_MAPPING_APPROVAL':'WAITING_FOR_PLAN_APPROVAL';
    if(copy.phase==='WAITING_FOR_COLUMN_MAPPING_APPROVAL')copy.review=structuredClone(reviews[previewStep-1]);
    return copy;
  }};
}
function renderPreview(){
  control.stop();
  if (!previewControls) previewControls = [
    ...['approve','reject','edit-columns','edit-primary-key','feedback-send','agent-submit','cancel'].map(id=>[id,{disabled:$(id).disabled,textContent:$(id).textContent}]),
    ['actor',{value:$('actor').value,readOnly:$('actor').readOnly}]
  ];
  let toolbar=$('approval-preview');
  if(!toolbar){toolbar=document.createElement('section');toolbar.id='approval-preview';toolbar.className='panel preview-toolbar';$('detail-page').prepend(toolbar);}
  const tables=previewSnapshot?.review?.table_reviews||[];
  const stages=['Base tables',...tables.map((t,i)=>`Table ${i+1}: column mappings`),'Final approval'];
  toolbar.innerHTML=`<div><span class="eyebrow">READ-ONLY WALKTHROUGH</span><h2>Oracle view approval flow</h2><p class="muted">Reconstructed from this completed job’s saved plan. No migration is running and no decisions will be submitted.</p></div><div class="preview-controls"><label for="preview-stage">Review stage</label><select id="preview-stage">${stages.map((s,i)=>`<option value="${i}" ${i===previewStep?'selected':''}>${escape(s)}</option>`).join('')}</select><button id="preview-back" class="secondary" ${previewStep===0?'disabled':''}>Previous</button><button id="preview-next" ${previewStep===stages.length-1?'disabled':''}>Next stage</button></div>`;
  const change=step=>{previewStep=step;control.refresh();};
  $('preview-stage').addEventListener('change',e=>change(Number(e.target.value)));
  $('preview-back').addEventListener('click',()=>change(previewStep-1));
  $('preview-next').addEventListener('click',()=>change(previewStep+1));
  for(const id of ['approve','reject','edit-columns','edit-primary-key','feedback-send','agent-submit','cancel'])$(id).disabled=true;
  $('actor').value='Preview only';$('actor').readOnly=true;
  $('detail-badge').innerHTML='<span class="badge approval">Approval preview · completed job</span>';
  [...$('job-stages').children].forEach((el,index)=>{el.classList.remove('done','active');if(index===(previewStep===stages.length-1?2:1))el.classList.add('active');});
  if(previewStep===0){$('review-title').textContent='These base tables support your Oracle view';$('review-explanation').textContent='Review the discovered dependencies. Next, each base table goes through column, key, update, and delete-policy review.';}
  if(previewStep===stages.length-1){$('review-title').textContent='Ready to approve the view and its base tables?';$('review-explanation').textContent='In a live job, this approval saves the reviewed bundle and continues to structure preparation when configured. Data loading remains separate.';$('approve').textContent='Approve this plan (preview only)';}
  else $('approve').textContent=previewStep===0?'Review base tables (preview only)':'Confirm column mappings (preview only)';
}
const linked=new URLSearchParams(location.search).get('workflow_id');
if(linked)openJob(linked);else showPage(['overview','jobs','new','connections'].includes(location.hash.slice(1))?location.hash.slice(1):'overview');
loadJobs();
setInterval(()=>{if(!document.hidden&&['overview','jobs'].includes(page))loadJobs();},30000);
