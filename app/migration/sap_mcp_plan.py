"""Validate MCP provenance and agent plans; never fetch SAP through custom SDKs."""
import json
import os
import re
from uuid import UUID
from urllib.parse import unquote
from temporalio import activity
from .sap_mcp_contracts import MCPTableProposal,MCPViewProposal,MCPODPProposal,MCPExtractorProposal,MCPKeyCheck,SAPMaterializedSource,materialized_names
from .opencode_sap_view_plan import parse_json,adapt_sap_view_bundle
from .opencode_sap_plan import adapt_sap_plan
from .opencode_sap_odp_plan import adapt_sap_odp_plan
from thirdparty.sap.utils import SAPTableSchema, SAPExtractionRoute
from thirdparty.sap.extraction import identifier
from thirdparty.configdb.repository import ConfigDBRepository,SAPAnalysisCreate,SAPAnalysisObjectCreate


def mapped_destination(source):
    try:bindings=json.loads(os.getenv('OPENCODE_SAP_SOURCE_CONNECTIONS','{}'))
    except json.JSONDecodeError:raise ValueError('OPENCODE_SAP_SOURCE_CONNECTIONS must be a JSON object') from None
    dest=bindings.get(source['source_connection_name']) if isinstance(bindings,dict) else None
    if not isinstance(dest,str) or not dest.strip():raise ValueError('Configure OPENCODE_SAP_SOURCE_CONNECTIONS for this source')
    return dest


def _strings(value):
    if isinstance(value,str):return [unquote(value)]
    if isinstance(value,dict):return [s for v in value.values() for s in _strings(v)]
    if isinstance(value,list):return [s for v in value for s in _strings(v)]
    return []


def collect_mcp_evidence(session_id):
    from .opencode_activities import _client,_path,_request
    records=[];cursor=None;seen=set()
    with _client() as client:
        for _ in range(300):
            response=_request(client,'GET',_path(session_id)+'/message',params={**({'cursor':cursor} if cursor else {}),'limit':2})
            for message in response.get('data',[]):
                for part in message.get('content',[]):
                    if part.get('type')!='tool':continue
                    state=part.get('state',{})
                    for call in state.get('metadata',{}).get('toolCalls',[]):
                        name=call.get('tool','')
                        if name.startswith('sap-abap-adt.') and call.get('status')=='completed':
                            args=call.get('input',{})
                            records.append(dict(tool=name,destination=args.get('destination'),inputs=_strings(args)))
            if not response.get('data'):break
            cursor=(response.get('cursor') or {}).get('next')
            if not cursor or cursor in seen:break
            seen.add(cursor)
        else:raise ValueError('SAP MCP evidence exceeds the bounded session history')
    return records


def require_evidence(records,destination,names):
    scoped=[r for r in records if r['destination']==destination and not any(x in r['tool'].lower() for x in ('create','delete','update','activate','write'))]
    if not scoped:raise ValueError('No completed source MCP calls on the mapped SAP destination')
    inputs='\n'.join(s for r in scoped for s in r['inputs']).upper()
    for name in names:
        identifier(name)
        url_match=any(s.upper().split('?')[0].rstrip('/').endswith('/'+name) for r in scoped for s in r['inputs'])
        if not url_match and not re.search(r'(?<![A-Z0-9_/$])'+re.escape(name)+r'(?![A-Z0-9_/$])',inputs):
            raise ValueError('Every source object needs completed sap-abap-adt discovery evidence: '+name)
    return [{'tool':r['tool'],'destination':r['destination']} for r in scoped]


def validate_metadata(metadata,name):
    schema=SAPTableSchema.model_validate(metadata['schema'])
    if schema.table_name!=name or not schema.columns or len(schema.columns)>1000:
        raise ValueError('MCP source metadata identity or column bounds mismatch')
    names=[c.fieldname for c in schema.columns];positions=[c.position for c in schema.columns]
    if len(set(names))!=len(names) or len(set(positions))!=len(positions):raise ValueError('MCP source columns/positions must be unique')
    if any(c not in names for c in schema.primary_key_columns):raise ValueError('MCP key fields must exist in the source metadata')
    semantics=metadata.get('semantic_metadata',{})
    for key,value in semantics.items():
        identifier(key)
        if not isinstance(value,dict) or not isinstance(value.get('domain'),str) or not isinstance(value.get('fixed_values'),list):
            raise ValueError('Provide linked Data Element/Domain metadata and fixed values')
    metadata['schema']=schema.model_dump(mode='json')
    if metadata.get('source_object_type') in {'CDS_VIEW','DB_VIEW'}:
        contract=SAPMaterializedSource.model_validate(metadata.get('materialized_source'))
        if contract.source_object_type!=metadata['source_object_type']:
            raise ValueError('Source view type differs from its connector contract')


def validate_datasource_output(context,source_name,records,destination):
    metadata=context['output_metadata']
    actual=metadata['schema']['table_name']
    structure=context['datasource'].get('EXTRACT_STRUCTURE')
    if actual not in {source_name,structure}:
        raise ValueError('Output metadata must identify the DataSource or its verified extraction structure')
    validate_metadata(metadata,actual)
    if actual!=source_name:
        require_evidence(records,destination,[actual])


def validate_extraction_route(context):
    try:
        return SAPExtractionRoute(context['route']).value
    except (KeyError, ValueError, TypeError):
        raise ValueError('context.route must be an exact SAP extraction route such as FUNCTION_MODULE; put extraction explanations in source_definitions or analysis evidence') from None


def validate_graph(graph,root):
    nodes=graph['nodes'];tables=graph['tables'];order=graph['view_order']
    if graph['root']!=root or not nodes or len(nodes)>64 or not tables:raise ValueError('MCP source graph identity/bounds mismatch')
    if set(nodes)!=set(tables)|set(order) or len(set(order))!=len(order) or not order or order[-1]!=root:
        raise ValueError('MCP graph must cover every table/view exactly once with the root last')
    done=set(tables)
    for name,metadata in tables.items():
        validate_metadata(metadata,name)
        if nodes[name]['kind']!='TABLE' or nodes[name]['dependencies']:raise ValueError('MCP base objects must be verified tables')
    for name in order:
        node=nodes[name];deps=node['dependencies']
        if node['kind']!='VIEW' or not deps or len(set(deps))!=len(deps) or not set(deps)<=done or not node.get('output_columns'):
            raise ValueError('MCP view dependency order/cycle or output fields are invalid')
        if not node.get('source_sql'):raise ValueError('Preserve source definition or extractor evidence for each view')
        done.add(name)


def adapt_mcp_sap(payload,records):
    source=payload['input'];dest=mapped_destination(source);data=parse_json(payload['text'])
    approach=source['migration_approach'];lakehouse=payload['lakehouse_name']
    if data.get('destination')!=dest:raise ValueError('Agent proposal uses a different SAP MCP destination')
    if source.get('datasource_stage')=='ANALYSIS':
        proposal=MCPExtractorProposal.model_validate(data);context=proposal.context;analysis=proposal.analysis
        context['route']=validate_extraction_route(context)
        if context['datasource']['DATASOURCE_ID']!=source['source_object_name']:
            raise ValueError('DataSource analysis identity differs from the request')
        if analysis.unresolved_items:
            raise ValueError('Resolve SAP DataSource analysis blockers before approval: '+
                '; '.join(analysis.unresolved_items)[:1500])
        groups=(analysis.source_tables,analysis.source_views,analysis.lookup_tables,analysis.configuration_tables)
        objects={o.name:o.object_type for group in groups for o in group if o.required}
        if not objects or len(objects)>63:raise ValueError('Extractor analysis requires bounded source dependencies')
        evidence=require_evidence(records,dest,[source['source_object_name'],*objects])
        materialized = context.get('materialized_sources', {})
        if not isinstance(materialized, dict):
            raise ValueError('materialized_sources must identify connector-readable source views')
        executed = '\n'.join(s for r in records if r['destination']==dest for s in r['inputs'])
        for name, raw in materialized.items():
            identifier(name)
            contract = SAPMaterializedSource.model_validate(raw)
            identifier(contract.entity_name)
            if objects.get(name)!=contract.source_object_type:
                raise ValueError('Materialized source type must match the approved CDS/database view identity')
            require_evidence(records,dest,[contract.entity_name])
            for query in contract.verification_queries:
                if (not query.lstrip().upper().startswith('SELECT ') or ';' in query or
                    not re.search(r'\b'+re.escape(name)+r'\b',query,re.I) or query not in executed):
                    raise ValueError('Materialized views require completed source MCP read-query evidence')
            materialized[name] = contract.model_dump(mode='json')
        validate_datasource_output(context,source['source_object_name'],records,dest)
        context['analysis_objects']=objects
        summary=dict(route=context['route'],analysis=analysis.model_dump(mode='json'),context=context,
            verified={'verified':[{'name':n,'object_type':t} for n,t in objects.items()]},
            warnings=analysis.warnings,mcp_evidence=evidence)
        db=ConfigDBRepository();version=source.get('analysis_version',1)
        aid=db.create_sap_analysis(SAPAnalysisCreate(plan_guid=UUID(source['plan_guid']),analysis_version=version,
            datasource_name=source['source_object_name'],root_object_type=context['route'],root_object_name=context['datasource']['EXTRACTOR'],
            summary=json.dumps(summary),llm_model='OpenCode:sap-datasource-to-fabric'))
        db.insert_sap_analysis_objects(aid,[SAPAnalysisObjectCreate(object_type=t,object_name=n,observed_from_scraper=False,
            interpreted_role='source',interpretation='Agent discovered through sap-abap-adt MCP',is_replication_required=True) for n,t in objects.items()])
        db.update_sap_analysis_status(aid,expected_status='DRAFT',new_status='WAITING_APPROVAL',expected_version=version)
        return {'analysis_guid':str(aid),**summary}
    if approach=='SAP_TABLE':
        proposal=MCPTableProposal.model_validate(data)
        validate_metadata(proposal.metadata,source['source_object_name'])
        evidence=require_evidence(records,dest,[source['source_object_name']])
        result=adapt_sap_plan(json.dumps(proposal.plan.model_dump()),{**source,'source_sap_metadata':proposal.metadata},lakehouse)
        result['review']['source_metadata']=proposal.metadata
    elif approach=='SAP_ODP':
        proposal=MCPODPProposal.model_validate(data)
        if proposal.context['datasource']['DATASOURCE_ID']!=source['source_object_name']:raise ValueError('ODP DataSource identity mismatch')
        validate_datasource_output(proposal.context,source['source_object_name'],records,dest)
        evidence=require_evidence(records,dest,[source['source_object_name']])
        result=adapt_sap_odp_plan(json.dumps(proposal.plan.model_dump()),{**source,'source_datasource_context':proposal.context},lakehouse)
        result['review']['source_metadata']=proposal.context['output_metadata']
    else:
        proposal=MCPViewProposal.model_validate(data);graph=proposal.graph
        validate_graph(graph,source['source_object_name'])
        evidence=require_evidence(records,dest,list(graph['nodes']))
        if approach=='SAP_REBUILD':
            approved=source['source_datasource_context']['analysis_objects']
            materialized=materialized_names(source['source_datasource_context'])
            for name,kind in approved.items():
                if name not in graph['nodes'] or graph['nodes'][name]['kind']!=('TABLE' if kind=='TRANSPARENT_TABLE' or name in materialized else 'VIEW'):
                    raise ValueError('Rebuild must include every approved extractor dependency')
                if name in materialized:
                    metadata=graph['tables'][name]
                    contract=source['source_datasource_context']['materialized_sources'][name]
                    if metadata.get('materialized_source')!=contract or metadata.get('source_object_type')!=kind:
                        raise ValueError('Preserve the approved materialized source type and connector contract')
            output=source['source_datasource_context']['output_metadata']['schema']['columns']
            if graph['nodes'][graph['root']]['output_columns']!=[c['fieldname'] for c in sorted(output,key=lambda c:c['position'])]:
                raise ValueError('Rebuild must preserve the approved DataSource output fields/order')
        result=adapt_sap_view_bundle(json.dumps(proposal.plan.model_dump()),{**source,'source_view_graph':graph},lakehouse)
        for review in result['review']['table_reviews']:review['source_metadata']=graph['tables'][review['source']]
        result['review']['mcp_graph']=graph
        if source.get('view_correction'):
            from copy import deepcopy
            from .view_correction import validate_correction
            from .sap_bundle_storage import expand_bundle
            previous=expand_bundle(source['view_correction']['previous'])
            if graph!=previous['review']['mcp_graph']:raise ValueError('SQL repair cannot rediscover/change the source graph')
            original=adapt_sap_view_bundle(json.dumps(previous['review']['agent_plan']),{**source,'source_view_graph':graph},lakehouse)
            if result['tables']!=original['tables']:raise ValueError('SQL repair cannot change base mappings or extraction SQL')
            result['tables']=deepcopy(previous['tables']);result['review']['table_reviews']=deepcopy(previous['review']['table_reviews'])
            validate_correction(previous,result,source['view_correction']['eligible'])
    # mcp_graph/source_metadata and agent_plan already retain the complete
    # facts and repair contract. A second full proposal has no readers and
    # unnecessarily duplicates wide DDIC bundles in Temporal payloads.
    result['review'].update(source_discovery='SAP_ABAP_ADT_MCP',mcp_evidence=evidence)
    result['review']['warnings'].append('Source facts were discovered by the agent through SAP MCP tools; validation checks schema, consistency and completed tool-call provenance, without a second custom SAP SDK lookup.')
    from .oracle_view_targets import check_oracle_view_targets
    if approach!='SAP_TABLE':check_oracle_view_targets({'bundle':result,'plan_guid':source['plan_guid']})
    if approach=='SAP_REBUILD':
        from .sap_bundle_storage import compact_bundle
        result=compact_bundle(result)
    return result


@activity.defn
def opencode_sap_key_check(payload):
    from .opencode_activities import opencode_prompt,opencode_poll
    source=payload['input'];columns=payload['columns']
    # A separate bounded turn in the same planning session; no custom SAP queries.
    opencode_prompt(dict(session_id=payload['session_id'],workflow_id=payload['workflow_id'],
        revision=payload['revision'],input={**source,'sap_mcp_discovery':True,'datasource_stage':'KEY_CHECK',
            'key_columns':columns,'key_metadata':payload['metadata'],'key_plan_columns':payload['plan_columns']}))
    import time
    for _ in range(100):
        if activity.is_cancelled():raise RuntimeError('SAP MCP key check cancelled')
        snapshot=opencode_poll({'session_id':payload['session_id']})
        if snapshot['status']=='COMPLETED':
            result=MCPKeyCheck.model_validate(parse_json(snapshot['text']))
            dest=mapped_destination(source)
            if result.destination!=dest or result.columns!=columns:raise ValueError('MCP key check identity mismatch')
            records=collect_mcp_evidence(payload['session_id']);require_evidence(records,dest,[source['source_object_name']])
            if result.valid:
                if result.status!='CHECKED' or result.scope!='FULL_TABLE' or result.has_nulls is not False or result.has_duplicates is not False:
                    raise ValueError('A valid key needs complete constraint/profile evidence')
                if result.method=='DDIC_PRIMARY_KEY_CONSTRAINT':
                    if payload['metadata'].get('source_object_type') in {'CDS_VIEW','DB_VIEW'}:
                        raise ValueError('CDS/view key annotations are not enforced uniqueness constraints; use MCP aggregate validation')
                    if columns!=payload['metadata']['schema']['primary_key_columns'] or any(c['column_name'] in columns and c.get('source_expression') for c in payload['plan_columns']):
                        raise ValueError('Converted/custom keys require actual MCP aggregate validation')
                else:
                    executed='\n'.join(s for r in records if r['destination']==dest for s in r['inputs']).upper()
                    if not result.validation_queries or any(q.upper() not in executed or 'COUNT' not in q.upper() or source['source_object_name'] not in q.upper() for q in result.validation_queries):
                        raise ValueError('Custom keys need executed source MCP aggregate queries')
            return result.model_dump(exclude={'destination'})
        if snapshot['status'] in {'FORM','PERMISSION','FAILED'}:
            return dict(valid=False,status='UNVERIFIED',scope='FULL_TABLE',columns=columns,
                error='MCP could not establish the key automatically; choose no key/full refresh or revise the agent proposal')
        time.sleep(2)
    return dict(valid=False,status='UNVERIFIED',scope='FULL_TABLE',columns=columns,error='SAP MCP key check exceeded its bounded wait')
