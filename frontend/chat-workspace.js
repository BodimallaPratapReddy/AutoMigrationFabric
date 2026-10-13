const escape = value => String(value ?? '').replace(/OpenCode/g, 'AutoMigrate Agent').replace(/[&<>"']/g, c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));

export function elapsedLabel(start,end=Date.now()) {
  if(!Number.isFinite(start)||!Number.isFinite(end))return '';
  const seconds=Math.max(0,Math.floor((end-start)/1000));
  const hours=Math.floor(seconds/3600),minutes=Math.floor(seconds%3600/60),remaining=seconds%60;
  return [hours?`${hours}h`:'',minutes?`${minutes}m`:'',`${remaining}s`].filter(Boolean).join(' ');
}

export function reviewIdentity(state) {
  return [state?.workflow_id, state?.phase, state?.plan_version, state?.analysis_version,
    state?.review?.base_table_index, state?.review?.source, state?.agent_request?.id].join(':');
}

export function processStatus(state,events=[],unavailable='') {
  if(!state)return {mode:'loading',label:'Connecting',detail:'Loading migration status…'};
  if(state.status!=='RUNNING')return {mode:'finished',label:state.status==='FAILED'?'Needs attention':'Finished',detail:state.last_error||'View the recorded outcome below.'};
  if(state.phase==='WAITING_FOR_AGENT_REVISION')return {mode:'waiting',label:'Discovery needs attention',detail:'No valid plan is available yet. Open the discovery report to review the blocker.'};
  if(state.waiting_for_user)return {mode:'waiting',label:'Waiting for you',detail:'Open the review card to answer or approve. Work resumes after your decision.'};
  const agent=/AGENT|SUBMITTING_AGENT/.test(state.phase||'');
  const latest=agent?[...events].reverse().find(e=>e.engine==='OpenCode'):null;
  return {mode:'running',label:agent?'AutoMigrate Agent is working':'Migration in progress',
    detail:unavailable || (latest?`${latest.status==='completed'?'Last activity: ':''}${latest.title}${latest.status?' · '+latest.status:''}`:state.current_message||String(state.phase||'Preparing migration').replaceAll('_',' ').toLowerCase())};
}

export function workflowSteps(events,state) {
  const lifecycle=new Map();
  for(const event of events)if(event.kind==='activity_status')lifecycle.set(String(event.scheduled_event_id),event);
  const result=[];
  for(const event of events){
    if(event.kind==='activity_status')continue;
    if(event.kind==='activity'&&/poll|set_temporal|transition|heartbeat|update.*status/.test(event.activity_name||''))continue;
    if(event.kind!=='activity'){result.push(event);continue;}
    const outcome=lifecycle.get(String(event.id));
    const step={...event,status:outcome?.status||event.status||'scheduled',completed_at:outcome?.time,checks:1};
    const previous=result.at(-1);
    if(previous?.kind==='activity'&&previous.activity_name===step.activity_name){
      previous.status=step.status;previous.completed_at=step.completed_at;previous.checks++;previous.lastCheck=event.time;
    }else result.push(step);
  }
  const last=result.filter(e=>e.kind==='activity').at(-1);
  if(last&&state?.status==='RUNNING'&&!state.waiting_for_user){
    if(['scheduled','started'].includes(last.status)||(/get_provisioning_status/.test(last.activity_name)&&/WAITING_FOR.*PROVISIONING/.test(state.phase||'')))last.status='running';
  }
  return result;
}

export function groupTimeline(events) {
  const groups=[];
  let fabricStarted=false;
  for(const event of events){
    if(event.kind==='activity'&&/poll|set_temporal|transition|heartbeat|update.*status/.test(event.activity_name||''))continue;
    if(event.kind==='decision'||event.kind==='agent')fabricStarted=false;
    if(event.kind==='activity'&&/^(approve_runtime_plan|persist_runtime_plan|persist_verified_rebuild_plan|save_migration_plan|start_batch|submit_provisioning|submit_replication)$/.test(event.activity_name||''))fabricStarted=true;
    if(event.kind==='activity'&&fabricStarted){
      let group=groups.at(-1);
      if(group?.kind!=='fabric-group'){group={kind:'fabric-group',id:'fabric-'+event.id,events:[]};groups.push(group);}
      group.events.push(event);continue;
    }
    if(event.kind==='agent'){
      let group=groups.at(-1);
      if(group?.kind!=='agent-group'){group={kind:'agent-group',id:'agent-'+event.id,events:[]};groups.push(group);}
      group.events.push(event);
    }else groups.push(event);
  }
  return groups;
}

export function createChatWorkspace({openActivity,renderDetails}) {
  const $ = id=>document.getElementById(id);
  let currentPane=null, identity='', returnFocus=null, signature='', records=[];
  const layout=document.querySelector('.detail-layout');
  layout.classList.add('chat-layout');
  const heading=$('detail-page').querySelector('.page-heading');
  heading.prepend($('back-jobs'));
  const navigation=document.createElement('button');navigation.type='button';navigation.id='toggle-workspace-nav';navigation.className='secondary';navigation.textContent='☰';navigation.setAttribute('aria-label','Toggle navigation');navigation.setAttribute('aria-expanded','false');heading.prepend(navigation);
  const backdrop=document.createElement('button');backdrop.type='button';backdrop.className='navigation-backdrop';backdrop.setAttribute('aria-label','Close navigation');document.body.append(backdrop);
  const setNavigation=expanded=>{document.body.classList.toggle('migration-nav-open',expanded);navigation.setAttribute('aria-expanded',String(expanded));};
  const closeNav=document.createElement('button');closeNav.type='button';closeNav.className='close-navigation';closeNav.textContent='✕';closeNav.setAttribute('aria-label','Close menu');document.querySelector('.sidebar').prepend(closeNav);closeNav.addEventListener('click',()=>{setNavigation(false);navigation.focus();});
  navigation.addEventListener('click',()=>setNavigation(!document.body.classList.contains('migration-nav-open')));
  backdrop.addEventListener('click',()=>{setNavigation(false);navigation.focus();});
  document.querySelector('.sidebar').addEventListener('click',event=>{if(event.target.closest('button,a'))setNavigation(false);});
  document.addEventListener('keydown',event=>{if(event.key==='Escape'&&document.body.classList.contains('migration-nav-open')){setNavigation(false);navigation.focus();}});
  const status=document.createElement('div');status.id='process-status';status.className='process-status';status.setAttribute('role','status');status.setAttribute('aria-live','polite');
  layout.querySelector('.detail-content').prepend(status);
  document.querySelector('.detail-tabs').hidden=true;
  $('job-stages').hidden=true;
  $('conversation-tab').removeAttribute('role');
  const thread=$('conversation-history');
  thread.classList.add('chat-thread');
  thread.setAttribute('aria-label','Migration conversation');
  const tickTimers=()=>{
    if($('detail-page').hidden)return;
    for(const timer of thread.querySelectorAll('[data-elapsed-start]'))timer.textContent='Running · '+elapsedLabel(Number(timer.dataset.elapsedStart));
  };
  setInterval(tickTimers,1000);
  const inspector=document.createElement('aside');
  inspector.id='migration-inspector';inspector.className='migration-inspector';inspector.inert=true;inspector.setAttribute('aria-hidden','true');
  inspector.setAttribute('aria-label','Migration review panel');
  inspector.innerHTML='<h2 id="inspector-title" class="visually-hidden" tabindex="-1">Review</h2><button type="button" id="close-inspector" aria-label="Close panel">✕</button><div id="inspector-body"></div>';
  layout.append(inspector);
  const record=document.createElement('section');record.id='recorded-request';record.className='panel';
  const panes={review:$('review-section'),agent:$('agent-section'),objects:$('objects-tab'),scripts:$('scripts-tab'),activity:$('activity-tab'),summary:$('job-summary'),record};
  const labels={review:'Review & approve',agent:'Assistant request',objects:'Tables & views',scripts:'SQL & scripts',activity:'Activity',summary:'Migration details',record:'Recorded details'};
  for(const [key,node] of Object.entries(panes)) {
    const wrapper=document.createElement('div');wrapper.dataset.inspectorPane=key;wrapper.hidden=true;
    node.hidden=false;node.removeAttribute('role');wrapper.append(node);$('inspector-body').append(wrapper);
  }
  const dock=document.createElement('div');dock.className='chat-dock';
  dock.innerHTML='<div class="chat-tools" aria-label="Migration workspace"><button data-panel="objects" type="button">▤ &nbsp; Tables & views</button><button data-panel="scripts" type="button">&lt;/&gt; &nbsp; SQL & scripts</button><button data-panel="activity" type="button">◷ &nbsp; Activity</button><button data-panel="summary" type="button">ⓘ &nbsp; Details</button></div><p id="chat-hint" role="status">Your migration assistant will guide you through each decision.</p>';
  $('conversation-tab').append(dock);
  function close(focus=true){
    inspector.inert=true;inspector.setAttribute('aria-hidden','true');layout.classList.remove('inspector-open');currentPane=null;
    if(focus&&returnFocus?.isConnected)returnFocus.focus({preventScroll:true});
  }
  function open(name){
    if(!panes[name])return;
    returnFocus=document.activeElement;currentPane=name;
    inspector.inert=false;inspector.removeAttribute('aria-hidden');layout.classList.add('inspector-open');
    for(const wrapper of $('inspector-body').children)wrapper.hidden=wrapper.dataset.inspectorPane!==name;
    $('inspector-body').scrollTop=0;
    $('inspector-title').textContent=labels[name];$('inspector-title').focus({preventScroll:true});
    if(name==='activity')openActivity();
  }
  $('close-inspector').addEventListener('click',()=>close());
  inspector.addEventListener('keydown',event=>{if(event.key==='Escape'){event.preventDefault();close();}});
  $('detail-page').addEventListener('click',event=>{
    const item=event.target.closest('[data-record]');
    if(item){
      const entry=records.find(e=>String(e.id)===item.dataset.record);if(!entry)return;
      record.innerHTML=`<span class="eyebrow">${entry.kind==='decision'?'RECORDED DECISION':'AGENT ACTIVITY'}</span><h2>${escape(entry.title)}</h2><p class="muted">${escape(new Date(entry.time).toLocaleString())}${entry.actor?' · '+escape(entry.actor):''}</p>${entry.message?`<p>${escape(entry.message)}</p>`:''}${entry.kind==='decision'?`<h3>Submitted decision</h3>${renderDetails(entry.details||{})}<h3>Review at this point</h3>${entry.review_snapshot?renderDetails(entry.review_snapshot):'<p class="muted">This older job recorded the submitted choices but did not save a full review snapshot. The current plan is not substituted here.</p>'}`:renderDetails(entry.details||{})}`;
      open('record');return;
    }
    const button=event.target.closest('[data-panel]');if(button)open(button.dataset.panel);
  });
  function render(state,job,events,preview=false,timelineMessage=''){
    records=events;
    const process=processStatus(state,events,timelineMessage);
    status.dataset.mode=process.mode;
    const statusHTML=`<span class="process-indicator" aria-hidden="true"></span><div><strong>${escape(preview?'Approval preview':process.label)}</strong><span>${escape(preview?'Read-only walkthrough. No migration is running.':process.detail)}</span></div>`;
    if(status.innerHTML!==statusHTML)status.innerHTML=statusHTML;
    const scroller=layout.querySelector('.detail-content');
    const followLatest=scroller.scrollHeight-scroller.scrollTop-scroller.clientHeight<100 || !signature;
    const visibleMessage=[...thread.children].find(el=>el.getBoundingClientRect().bottom>scroller.getBoundingClientRect().top);
    const anchor=visibleMessage?{id:visibleMessage.dataset.entry,top:visibleMessage.getBoundingClientRect().top}:null;
    const nextIdentity=reviewIdentity(state);
    const returningToChat=identity!==nextIdentity && ['review','agent'].includes(currentPane);
    if(returningToChat)close(false);
    identity=nextIdentity;
    const waiting=state?.status==='RUNNING'&&state.waiting_for_user;
    const agentWaiting=waiting && /AGENT/.test(state.phase);
    const revision=state?.phase==='WAITING_FOR_AGENT_REVISION';
    const title=revision?'Discovery needs attention before we can review columns.':agentWaiting?'I need your input to continue.':$('review-title').textContent;
    const key=JSON.stringify([state?.workflow_id,state?.phase,state?.status,waiting,title,events,job?.fabric_schema_name,preview,timelineMessage]);
    dock.querySelector('.chat-tools').hidden=!state || state.status==='RUNNING';
    $('chat-hint').textContent=waiting?'Waiting for you · Open the review card to continue.':state?.status==='RUNNING'?'Working on your migration · Updates appear here automatically.':'This conversation is saved with your migration.';
    if(key===signature)return;
    signature=key;
    const source=[job?.source_schema_name, state?.source_object_name].filter(Boolean).join('.')||'your source';
    const destination=job?.fabric_schema_name||'your selected destination';
    let messageKey='intro';
    const message=(role,body)=>`<article class="chat-message ${role}" data-entry="${escape(messageKey+role)}"><div class="chat-avatar" aria-hidden="true">${role==='user'?escape((job?.requested_by||'You')[0].toUpperCase()):'<img class="dn-color" src="/static/brand/dn-color.svg" alt=""><img class="dn-white" src="/static/brand/dn-white.svg" alt="">'}</div><div class="chat-message-body"><span class="chat-author">${role==='user'?escape(job?.requested_by||'You'):'AutoMigrate Agent'}</span>${body}</div></article>`;
    let html=message('user',`<p>Prepare a migration for <strong>${escape(source)}</strong> into <strong>${escape(destination)}</strong>.</p>`);
    html+=message('assistant',`<p>I’ll inspect the source, prepare the mappings, and bring each decision to you for review.</p>`);
    const clock=time=>new Date(time).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'});
    const card=(heading,subtitle,button,attributes,style='')=>`<div class="artifact-card ${style}"><span class="artifact-icon">${style==='approved-artifact'?'✓':'▤'}</span><span><strong>${escape(heading)}</strong><small>${escape(subtitle)}</small></span><button type="button" class="card-action" ${attributes}>${escape(button)}</button></div>`;
    const workflowLabels={validate_fabric:'Check the Fabric destination',create_migration_plan:'Create the migration draft',opencode_create:'Start the AutoMigrate Agent discovery session',opencode_prompt:'Send the discovery request to AutoMigrate Agent',opencode_validate:'Validate the agent’s proposal',validate_primary_key:'Validate the selected record identifiers',save_migration_plan:'Save the approved migration plan'};
    const expanded=new Map([...thread.querySelectorAll('details[data-group]')].map(el=>[el.dataset.group,el.open]));
    const grouped=groupTimeline(workflowSteps(events,state));
    const agentActive=state?.status==='RUNNING'&&!waiting&&/AGENT/.test(state.phase||'');
    const lastAgent=grouped.filter(e=>e.kind==='agent-group').at(-1);
    const lastFabric=grouped.filter(e=>e.kind==='fabric-group').at(-1);
    const fabricActive=!!lastFabric&&state?.status==='RUNNING'&&!waiting&&!agentActive&&grouped.at(-1)?.kind==='fabric-group';
    const agentStep=event=>`<div class="phase-step"><div>${escape(event.title)} ${event.status?`<small class="muted">· ${escape(event.status)}</small>`:''}<span class="timeline-time">${escape(clock(event.time))}</span></div>${event.message?`<p>${escape(event.message)}</p>`:''}${Object.keys(event.details||{}).length?`<button type="button" class="text-button" data-record="${escape(event.id)}">View ${event.details.sql?'SQL query':'action details'} →</button>`:''}</div>`;
    const timing=(steps,active)=>{
      const start=Date.parse(steps[0]?.time);
      if(!Number.isFinite(start))return active?'Starting…':'';
      if(active)return `<span data-elapsed-start="${start}">Running · ${elapsedLabel(start)}</span>`;
      const finish=steps.findLast(e=>e.title==='Migration proposal prepared');
      if(revision&&steps===lastAgent?.events)return 'Needs attention · No valid plan returned';
      if(finish)return `Completed in ${elapsedLabel(start,Date.parse(finish.completed_at||finish.time))}`;
      return `Recorded activity · ${elapsedLabel(start,Date.parse(steps.at(-1).time))}`;
    };
    const phase=(id,steps,active)=>`<details class="agent-phase ${active?'is-running':''}" data-group="${escape(id)}" ${expanded.get(id)??false?'open':''}><summary><span class="phase-orb" aria-hidden="true"></span><span><strong>${active?'AutoMigrate Agent is discovering the source and planning':'AutoMigrate Agent discovery and planning'}</strong><small>${timing(steps,active)} · ${steps.length} updates · expand or collapse</small></span></summary><div class="phase-steps">${steps.map(agentStep).join('')}${active?'<div class="phase-live">Working · New updates will appear here</div>':''}</div></details>`;
    for(const event of grouped){
      if(event.kind==='fabric-group'){
        messageKey=event.id;
        const active=fabricActive&&event===lastFabric;
        const steps=event.events;
        const last=steps.at(-1),start=Date.parse(steps[0].time);
        const failed=steps.some(e=>['failed','timed_out'].includes(e.status))||(event===lastFabric&&state?.status==='FAILED');
        const complete=!failed&&!active&&steps.every(e=>e.status==='completed');
        const label=active?`<span data-elapsed-start="${start}">Running · ${elapsedLabel(start)}</span>`:failed?'Needs attention':complete?`Completed in ${elapsedLabel(start,Date.parse(last.completed_at||last.time))}`:'Recorded activity';
        const content=steps.map(e=>agentStep({...e,title:workflowLabels[e.activity_name]||e.title,message:e.checks>1?`${e.checks} status checks`:''})).join('');
        html+=message('assistant',`<details class="agent-phase ${active?'is-running':''}" data-group="${escape(event.id)}" ${expanded.get(event.id)?'open':''}><summary><span class="phase-orb" aria-hidden="true"></span><span><strong>Migration to Fabric</strong><small>${label} · ${steps.length} steps · expand or collapse</small>${active?`<small>${escape(workflowLabels[last.activity_name]||last.title)}</small>`:''}</span></summary><div class="phase-steps">${content}</div></details>`);
        continue;
      }
      if(event.kind==='agent-group'){
        messageKey=event.id;
        html+=message('assistant',phase(event.id,event.events,agentActive&&event===lastAgent));
        continue;
      }
      messageKey=String(event.id);
      const time=`<span class="timeline-time">${escape(clock(event.time))}</span>`;
      if(event.kind==='activity' && !/poll|set_temporal|transition|heartbeat|update.*status/.test(event.activity_name||'')){
        html+=message('assistant',`<div class="timeline-step workflow-step ${event.status==='running'?'is-running':''}"><span class="engine-tag">Workflow</span>${time}<p>${escape(workflowLabels[event.activity_name]||event.title)} <small class="muted">· ${escape(event.status)}</small></p>${event.checks>1?`<small class="muted">${event.checks} status checks · Last checked ${escape(clock(event.lastCheck))}</small>`:''}</div>`);
      }
      if(event.kind==='agent'){
        html+=message('assistant',`<div class="timeline-step"><span class="engine-tag agent">AutoMigrate Agent</span>${time}<p>${escape(event.title)} ${event.status?`<small class="muted">· ${escape(event.status)}</small>`:''}</p>${event.message?`<p class="muted">${escape(event.message)}</p>`:''}${Object.keys(event.details||{}).length?`<button type="button" class="text-button" data-record="${escape(event.id)}">View ${event.details.sql?'SQL query':'action details'} →</button>`:''}</div>`);
      }
      if(event.kind==='decision'){
        const approved=/^approve_/.test(event.action||'');
        html+=message('user',`${time}${card(event.title,`${event.review_snapshot?.source||event.review_snapshot?.source_object_name||'Recorded review'} · ${event.actor||'Recorded decision'}`,approved?'View approved review':'View decision',`data-record="${escape(event.id)}"`,approved?'approved-artifact':'decision-artifact')}`);
      }
    }
    messageKey='current';
    if(state?.status==='RUNNING') {
      if(waiting){
        html+=message('assistant',`<p>${escape(title)}</p><p class="muted">${escape(revision?'The agent did not return a valid migration plan. Open the report to see what blocked discovery.':agentWaiting?'Open the request, choose your answer, and send it back.':$('review-explanation').textContent)}</p>${card(revision?'Discovery blocked':agentWaiting?'Your answer is needed':'Approval required',`${state?.review?.source||state?.source_object_name||'Migration plan'} · ${preview?'Read-only preview':'Waiting for you'}`,revision?'View discovery report':agentWaiting?'Answer request':'Review & approve',`data-panel="${agentWaiting?'agent':'review'}"`,'review-artifact')}`);
      } else if(agentActive){
        if(!lastAgent)html+=message('assistant',phase('agent-pending',[],true));
      } else if(!fabricActive) {
        html+=message('assistant',`<div class="working-label is-running"><span class="working-dot"></span>${escape(state.current_message||'I’m preparing your migration.')}</div>${timelineMessage?`<p class="muted">${escape(timelineMessage)}</p>`:''}`);
      }
    }
    thread.innerHTML=html;
    if(returningToChat){
      const latest=thread.lastElementChild;
      latest?.setAttribute('tabindex','-1');latest?.focus({preventScroll:true});
      scroller.scrollTo({top:scroller.scrollHeight,behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});
    }else if(followLatest)scroller.scrollTop=scroller.scrollHeight;
    else if(anchor){const kept=[...thread.children].find(el=>el.dataset.entry===anchor.id);if(kept)scroller.scrollTop+=kept.getBoundingClientRect().top-anchor.top;}
  }
  return {open,close,render};
}
