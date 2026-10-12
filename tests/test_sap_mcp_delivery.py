from copy import deepcopy
import pytest
from app.migration.sap_mcp_delivery import validate_part,assemble_table
from test_opencode_sap import META,PLAN


def fixture_part():
    source={'plan_guid':'plan-test','source_datasource_context':{'analysis_objects':{'VBAK':'TRANSPARENT_TABLE'}}}
    part={'kind':'SAP_REBUILD_TABLE_PART','bundle_id':'plan-test','destination':'ECC-LOCAL',
          'part_index':1,'part_count':1,'total_columns':len(PLAN['columns']),
          'metadata':deepcopy(META),'plan':{**deepcopy(PLAN),'target_table':'DS_TEST_VBAK'}}
    return source,part


def test_complete_multipart_table_preserves_exact_metadata_and_plan():
    source,part=fixture_part()
    count=len(part['plan']['columns']);assert count>1
    chunks={}
    for index in range(count):
        p=deepcopy(part);p.update(part_index=index+1,part_count=count)
        p['metadata']['schema']['columns']=[part['metadata']['schema']['columns'][index]]
        p['plan']['columns']=[part['plan']['columns'][index]]
        chunks[('VBAK',index+1)]=validate_part(p,source,'ECC-LOCAL')
    meta,plan=assemble_table('VBAK',chunks)
    from app.migration.opencode_sap_view_plan import SAPViewTable
    assert meta=={**part['metadata'],'semantic_metadata':{}}
    assert plan==SAPViewTable.model_validate(part['plan']).model_dump(mode='json')
    chunks.pop(('VBAK',count))
    with pytest.raises(ValueError,match='Incomplete'):assemble_table('VBAK',chunks)


def test_delivery_rejects_identity_mismatch_and_duplicate_columns():
    source,part=fixture_part()
    for key,value in [('bundle_id','other'),('destination','OTHER')]:
        bad=deepcopy(part);bad[key]=value
        with pytest.raises(ValueError,match='match this plan'):validate_part(bad,source,'ECC-LOCAL')
    part['metadata']['schema']['columns'][1]=deepcopy(part['metadata']['schema']['columns'][0])
    with pytest.raises(ValueError,match='unique'):validate_part(part,source,'ECC-LOCAL')


def test_delivery_rejects_cross_part_conflicts_and_total_mismatch():
    source,part=fixture_part();part['total_columns']+=1
    part=validate_part(part,source,'ECC-LOCAL')
    with pytest.raises(ValueError,match='count/duplicates'):assemble_table('VBAK',{('VBAK',1):part})
    _,part=fixture_part();part.update(part_count=2,total_columns=len(part['plan']['columns'])*2)
    other=deepcopy(part);other['part_index']=2;other['plan']['target_table']='OTHER'
    with pytest.raises(ValueError,match='headers differ'):assemble_table('VBAK',{('VBAK',1):part,('VBAK',2):other})


def test_final_manifest_assembles_all_tables_and_rejects_missing_dependency(monkeypatch):
    import json
    from app.migration import sap_mcp_delivery
    from test_opencode_datasource import GRAPH,BUNDLE
    source={'plan_guid':'plan-test','migration_approach':'SAP_REBUILD','datasource_stage':'PLAN',
            'source_connection_name':'SAP ECC Local',
            'source_datasource_context':{'analysis_objects':{n:'TRANSPARENT_TABLE' for n in GRAPH['tables']}}}
    monkeypatch.setenv('OPENCODE_SAP_SOURCE_CONNECTIONS','{"SAP ECC Local":"ECC-LOCAL"}')
    parts={}
    for table in BUNDLE['tables']:
        name=table['source_table']
        parts[(name,1)]={'part_index':1,'part_count':1,'total_columns':len(table['columns']),
            'metadata':deepcopy(GRAPH['tables'][name]),'plan':deepcopy(table)}
    monkeypatch.setattr(sap_mcp_delivery,'collect_parts',lambda *args:parts)
    manifest={'kind':'SAP_REBUILD_COMPLETE','bundle_id':'plan-test','destination':'ECC-LOCAL',
        'graph':{**deepcopy(GRAPH),'tables':list(GRAPH['tables'])},
        'plan':{k:deepcopy(v) for k,v in BUNDLE.items() if k!='tables'}}
    payload={'input':source,'session_id':'session','text':json.dumps(manifest)}
    result=json.loads(sap_mcp_delivery.handle_rebuild_delivery(payload)['text'])
    assert set(result['graph']['tables'])==set(GRAPH['tables'])
    assert result['plan']['tables']==BUNDLE['tables']
    manifest['graph']['tables'].pop();payload['text']=json.dumps(manifest)
    with pytest.raises(ValueError,match='every approved base table'):
        sap_mcp_delivery.handle_rebuild_delivery(payload)


def test_parts_can_be_built_in_native_execute_without_custom_source_adapter(monkeypatch):
    import json
    from contextlib import nullcontext
    from app.migration import opencode_activities
    from app.migration.sap_mcp_delivery import collect_parts
    source,part=fixture_part()
    response={'data':[{'type':'assistant','finish':'tool-calls','content':[{
        'type':'tool','name':'execute','state':{'status':'completed','content':[{
            'type':'text','text':json.dumps({'sap_rebuild_parts':[part]})}]}}]}],'cursor':{}}
    monkeypatch.setattr(opencode_activities,'_client',lambda:nullcontext(None))
    monkeypatch.setattr(opencode_activities,'_request',lambda *a,**kw:response)
    result=collect_parts('session',source,'ECC-LOCAL')
    assert result[('VBAK',1)]['plan']['columns']
    response['data'][0]['content'][0]['state']['status']='error'
    assert not collect_parts('session',source,'ECC-LOCAL')


def test_truncated_native_delivery_reads_only_bounded_configured_artifact(monkeypatch,tmp_path):
    import json
    from app.migration.sap_mcp_delivery import execute_output_texts
    _,part=fixture_part();text=json.dumps({'sap_rebuild_parts':[part]})
    artifact=tmp_path/'tool_test';artifact.write_text(text,encoding='utf8')
    monkeypatch.setenv('OPENCODE_TOOL_OUTPUT_ROOT',str(tmp_path))
    state={'metadata':{'truncated':True,'outputPath':'/native/tool-output/tool_test'},
           'content':[{'type':'text','text':text[:100]}]}
    assert execute_output_texts(state)==[text]
    state['content'][0]['text']='{\n  "part_count": 1,\n  "errs": [],\n  "sap_rebuild_parts": [\n'
    assert execute_output_texts(state)==[text]
    state['metadata']['outputPath']='../../secret.env'
    with pytest.raises(ValueError,match='Configure'):execute_output_texts(state)
    state['content'][0]['text']='Unrelated large SAP source output'
    assert execute_output_texts(state)==[]


def test_json_delivery_typo_does_not_accept_extra_data_or_missing_structure():
    from app.migration.opencode_sap_view_plan import parse_json
    assert parse_json('```json\n{"kind":"complete"}}\n```')=={'kind':'complete'}
    for text in ['{"kind":"complete"} {"second":true}','{"kind":"complete"','{"kind":"complete"}}}']:
        with pytest.raises(ValueError):parse_json(text)


def test_target_binding_preserves_alias_and_rejects_unrelated_schema():
    from app.migration.opencode_sap_view_plan import bind_sap_targets
    from app.migration.opencode_view_plan import query_references
    sql='SELECT BKPF.BELNR AS BELNR FROM SAP_TEST.BKPF'
    bound=bind_sap_targets(sql,{'BKPF':'SAP_TEST.DS_TEST_BKPF'},'SAP_TEST')
    assert query_references(bound,['BELNR'])=={'SAP_TEST.DS_TEST_BKPF'}
    assert 'BKPF.BELNR' in bound and 'AS BKPF' in bound
    wrong='SELECT BELNR FROM OTHER.BKPF'
    assert bind_sap_targets(wrong,{'BKPF':'SAP_TEST.DS_TEST_BKPF'},'SAP_TEST')==wrong


def test_native_final_manifest_is_read_only_from_the_current_prompt(monkeypatch):
    import hashlib,json
    from contextlib import nullcontext
    from app.migration import opencode_activities
    from app.migration.sap_mcp_delivery import collect_native_completion
    sid='session';revision=7;start='msg_'+hashlib.sha256(f'{sid}:{revision}'.encode()).hexdigest()[:26]
    final={'kind':'SAP_REBUILD_COMPLETE','bundle_id':'plan-test','destination':'ECC-LOCAL','graph':{},'plan':{}}
    tool={'type':'tool','name':'execute','state':{'status':'completed','content':[{'type':'text','text':json.dumps({'sap_rebuild_complete':final})}]}}
    response={'data':[{'id':'latest','content':[tool]},{'id':start,'content':[]}],'cursor':{}}
    monkeypatch.setattr(opencode_activities,'_client',lambda:nullcontext(None))
    monkeypatch.setattr(opencode_activities,'_request',lambda *a,**kw:response)
    assert collect_native_completion({'session_id':sid,'delivery_revision':revision})==final
    response['data'].reverse()
    assert collect_native_completion({'session_id':sid,'delivery_revision':revision}) is None
