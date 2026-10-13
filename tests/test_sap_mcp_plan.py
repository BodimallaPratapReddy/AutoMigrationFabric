from copy import deepcopy
import json
import pytest
from app.migration.sap_mcp_plan import adapt_mcp_sap,require_evidence,validate_graph,collect_mcp_evidence
from test_opencode_datasource import SOURCE,CONTEXT,ODP_PLAN,GRAPH,BUNDLE
from test_opencode_sap import META,PLAN


def test_extraction_route_rejects_narrative_before_database_write():
    from app.migration.sap_mcp_plan import validate_extraction_route
    assert validate_extraction_route({'route':'FUNCTION_MODULE'})=='FUNCTION_MODULE'
    with pytest.raises(ValueError,match='exact SAP extraction route'):
        validate_extraction_route({'route':'ROOSOURCE TYPE=TRAN, EXMETHOD=F1. Full refresh'})


def test_wide_bundle_parser_is_bounded_without_truncation():
    from app.migration.opencode_sap_view_plan import parse_json
    text=json.dumps({'evidence':'x'*300000})
    assert parse_json(text)['evidence']=='x'*300000
    with pytest.raises(ValueError,match='1.5 MB'):
        parse_json(json.dumps({'evidence':'x'*1500000}))


@pytest.fixture
def no_sap_sdk(monkeypatch):
    from app.migration import source_connections,oracle_view_targets
    monkeypatch.setenv('OPENCODE_SAP_SOURCE_CONNECTIONS','{"SAP ECC Local":"ECC-LOCAL"}')
    def forbidden(*a,**kw):raise AssertionError('Custom SAP adapter must never run')
    monkeypatch.setattr(source_connections,'sap_client',forbidden)
    monkeypatch.setattr(oracle_view_targets,'check_oracle_view_targets',lambda payload:None)


@pytest.mark.parametrize('approach',['SAP_TABLE','SAP_VIEW','SAP_ODP','SAP_REBUILD'])
def test_all_sap_mcp_modes_validate_without_custom_adapter(no_sap_sdk,approach):
    source={**SOURCE,'migration_approach':approach,'sap_mcp_discovery':True,'plan_guid':'00000000-0000-0000-0000-000000000001'}
    context=deepcopy(CONTEXT);context['datasource']['DATASOURCE_ID']='ZDS'
    context['analysis_objects']={'VBAK':'TRANSPARENT_TABLE','VBUK':'TRANSPARENT_TABLE'}
    if approach=='SAP_REBUILD':
        context['output_metadata']['schema']['columns']=[context['output_metadata']['schema']['columns'][1]]
        context['output_metadata']['schema']['primary_key_columns']=['VBELN']
    if approach=='SAP_TABLE':
        source['source_object_name']='VBAK';data=dict(destination='ECC-LOCAL',metadata=META,plan=PLAN)
    elif approach=='SAP_ODP':data=dict(destination='ECC-LOCAL',context=context,plan=ODP_PLAN)
    else:
        source['source_datasource_context']=context
        data=dict(destination='ECC-LOCAL',graph=GRAPH,plan=BUNDLE)
    records=[dict(tool='sap-abap-adt.getTable',destination='ECC-LOCAL',inputs=[n]) for n in ('ZDS','VBAK','VBUK')]
    result=adapt_mcp_sap(dict(input=source,text=json.dumps(data),lakehouse_name='Bronze'),records)
    assert result['review']['source_discovery']=='SAP_ABAP_ADT_MCP'
    assert result['review']['mcp_evidence']


def test_source_provenance_rejects_missing_object_wrong_destination_and_writes():
    calls=[dict(tool='sap-abap-adt.getTable',destination='ECC-LOCAL',inputs=['VBAK'])]
    assert require_evidence(calls,'ECC-LOCAL',['VBAK'])
    for destination,name in [('OTHER','VBAK'),('ECC-LOCAL','VBUK')]:
        with pytest.raises(ValueError):require_evidence(calls,destination,[name])
    with pytest.raises(ValueError):require_evidence([{**calls[0],'tool':'sap-abap-adt.updateTable'}],'ECC-LOCAL',['VBAK'])


def test_mcp_graph_rejects_cycles_and_incomplete_metadata():
    graph=deepcopy(GRAPH);graph['nodes']['ZDS']['dependencies']=['ZDS']
    with pytest.raises(ValueError):validate_graph(graph,'ZDS')
    graph=deepcopy(GRAPH);graph['tables']['VBAK']['schema']['columns']=[]
    with pytest.raises(ValueError):validate_graph(graph,'ZDS')


def test_odp_keeps_ddic_positions_after_hidden_fields_are_excluded(no_sap_sdk):
    context=deepcopy(CONTEXT);context['datasource']['DATASOURCE_ID']='ZDS'
    plan=deepcopy(ODP_PLAN)
    for field in context['output_metadata']['schema']['columns']:field['position']+=1
    for column in plan['columns']:column['position']+=1
    data=dict(destination='ECC-LOCAL',context=context,plan=plan)
    records=[dict(tool='sap-abap-adt.runQuery',destination='ECC-LOCAL',inputs=["SELECT * FROM ROOSOURCE WHERE OLTPSOURCE='ZDS'"])]
    payload=dict(input={**SOURCE,'sap_mcp_discovery':True,'plan_guid':'00000000-0000-0000-0000-000000000001'},text=json.dumps(data),lakehouse_name='Bronze')
    result=adapt_mcp_sap(payload,records)
    assert result['tables'][0]['columns'][0]['sno']==2
    plan['columns'][0]['position']=1;payload['text']=json.dumps(data)
    with pytest.raises(ValueError,match='position/datatype'):adapt_mcp_sap(payload,records)


def test_evidence_reads_paginated_opencode_tool_ledger(monkeypatch):
    from app.migration import opencode_activities
    from contextlib import nullcontext
    pages=[dict(data=[dict(content=[dict(type='tool',state=dict(metadata=dict(toolCalls=[dict(tool='sap-abap-adt.getTable',status='completed',input=dict(destination='ECC-LOCAL',tableName='VBAK'))])))])],cursor={'next':'next-page'}),dict(data=[],cursor={})]
    monkeypatch.setattr(opencode_activities,'_client',lambda:nullcontext(None))
    monkeypatch.setattr(opencode_activities,'_request',lambda *a,**kw:pages.pop(0))
    result=collect_mcp_evidence('ses_test')
    assert result[0]['destination']=='ECC-LOCAL' and 'VBAK' in result[0]['inputs'] and not pages


def test_datasource_output_preserves_verified_physical_structure_identity(no_sap_sdk):
    from app.migration.sap_mcp_plan import validate_datasource_output
    context=deepcopy(CONTEXT)
    context['datasource']['EXTRACT_STRUCTURE']='ZEXTRACT'
    context['output_metadata']['schema']['table_name']='ZEXTRACT'
    records=[dict(tool='sap-abap-adt.runQuery',destination='ECC-LOCAL',inputs=['ZEXTRACT'])]
    validate_datasource_output(context,'ZDS',records,'ECC-LOCAL')
    with pytest.raises(ValueError):validate_datasource_output(context,'ZDS',[],'ECC-LOCAL')
    context['output_metadata']['schema']['table_name']='OTHER'
    with pytest.raises(ValueError,match='verified extraction structure'):
        validate_datasource_output(context,'ZDS',records,'ECC-LOCAL')
