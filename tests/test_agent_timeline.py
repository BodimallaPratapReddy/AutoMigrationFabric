from app.migration.agent_timeline import project_messages


def test_nested_tool_calls_keep_actual_sql_and_ignore_reasoning_and_secrets():
    messages=[{'id':'m1','type':'assistant','time':{'created':1000},'content':[
        {'type':'reasoning','text':'private reasoning'},
        {'type':'tool','id':'p1','name':'execute','state':{'status':'completed',
          'input':{'code':'private wrapper code'},'content':[{'text':'raw database rows'}],
          'metadata':{'toolCalls':[{'tool':'oracle-sqlcl.sql_run','status':'completed',
            'input':{'sql':'SELECT column_name FROM all_tab_cols','password':'secret'}}]}}}]}]
    events=project_messages(messages)
    assert len(events)==1
    assert events[0]['title']=='Inspect column definitions'
    assert events[0]['details']=={'sql':'SELECT column_name FROM all_tab_cols'}
    assert events[0]['engine']=='OpenCode'
    assert all(value not in str(events) for value in ['private reasoning','private wrapper','raw database rows','secret'])


def test_timeline_chronology_uses_part_time_and_stable_ids():
    messages=[{'id':'later','type':'assistant','time':{'created':5000},'content':[
        {'type':'tool','id':'p','time':{'created':6000},'name':'read','state':{
            'status':'completed','input':{'path':'/references/number-profiling.md'}}}]},
        {'id':'earlier','type':'assistant','time':{'created':1000},'content':[
            {'type':'text','text':'Checking the source definitions.'}]}]
    events=project_messages(messages)
    assert events[0]['id'].startswith('agent:earlier:')
    assert events[1]['title']=='Read number-profiling.md'
    assert project_messages(messages)==events


def test_final_plan_is_a_delivery_event_not_a_json_chat_dump():
    events=project_messages([{'id':'m','type':'assistant','finish':'stop','time':{'created':1000},
        'content':[{'type':'text','text':'{"columns": ["many columns"]}'}]}])
    assert events[0]['title']=='Migration proposal prepared'
    assert 'columns' not in str(events)
