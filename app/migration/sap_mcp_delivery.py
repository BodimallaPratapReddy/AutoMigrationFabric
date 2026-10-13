"""Bounded multipart agent delivery; source access remains exclusively SAP MCP."""
import json
import os
import re
from pathlib import Path
from typing import Literal
from pydantic import Field
from .sap_mcp_contracts import MCPModel
from .opencode_sap_view_plan import SAPViewTable, parse_json
from thirdparty.sap.utils import SAPTableSchema
from .sap_mcp_contracts import extraction_names


class SAPRebuildTablePart(MCPModel):
    kind: Literal['SAP_REBUILD_TABLE_PART']
    bundle_id: str
    destination: str
    part_index: int = Field(ge=1, le=32)
    part_count: int = Field(ge=1, le=32)
    total_columns: int = Field(ge=1, le=1000)
    metadata: dict
    plan: SAPViewTable


def execute_output_texts(state):
    metadata=state.get('metadata',{})
    if metadata.get('truncated'):
        preview='\n'.join(o.get('text','') for o in state.get('content',[]) if o.get('type')=='text')
        if not re.search(r'(?m)^\s*"sap_rebuild_(?:parts|complete)"\s*:',preview[:8192]) and not re.match(r'^\s*\{\s*"sap_rebuild_(?:parts|complete)"\s*:',preview):return []
        root=os.getenv('OPENCODE_TOOL_OUTPUT_ROOT')
        name=str(metadata.get('outputPath','')).rsplit('/',1)[-1]
        if not root or not re.fullmatch(r'tool_[A-Za-z0-9_]+',name):
            raise ValueError('Configure OPENCODE_TOOL_OUTPUT_ROOT to read complete native OpenCode delivery artifacts')
        directory=Path(root).resolve();path=(directory/name).resolve()
        if path.parent!=directory:raise ValueError('OpenCode output artifact must stay inside its configured directory')
        try:
            with path.open('rb') as stream:data=stream.read(1_500_001)
        except OSError:raise ValueError('Complete native OpenCode output artifact is unavailable') from None
        if len(data)>1_500_000:raise ValueError('OpenCode delivery artifact exceeds 1.5 MB')
        try:return [data.decode('utf-8')]
        except UnicodeDecodeError:raise ValueError('OpenCode delivery artifact must be UTF-8 JSON') from None
    return [o.get('text','') for o in state.get('content',[]) if o.get('type')=='text']


def validate_part(data, source, destination):
    part=SAPRebuildTablePart.model_validate(data)
    if part.bundle_id!=source['plan_guid'] or part.destination!=destination:
        raise ValueError('SAP rebuild part must match this plan and mapped destination')
    if part.part_index>part.part_count:
        raise ValueError('SAP rebuild part index exceeds its part count')
    schema=SAPTableSchema.model_validate(part.metadata['schema'])
    if schema.table_name!=part.plan.source_table or schema.table_name not in source['source_datasource_context']['analysis_objects']:
        raise ValueError('SAP rebuild part must name an approved base table')
    if schema.table_name not in extraction_names(source['source_datasource_context']):
        raise ValueError('SAP rebuild parts must describe approved tables or connector-materialized views')
    contract=source['source_datasource_context'].get('materialized_sources',{}).get(schema.table_name)
    if contract and (part.metadata.get('materialized_source')!=contract or
                     part.metadata.get('source_object_type')!=contract['source_object_type']):
        raise ValueError('Materialized source parts must preserve the approved view type and read contract')
    if not 1<=len(schema.columns)<=100 or len(part.plan.columns)!=len(schema.columns):
        raise ValueError('Each SAP rebuild part must carry 1-100 matching metadata/plan columns')
    fields={f.fieldname:f for f in schema.columns}
    if len(fields)!=len(schema.columns) or len({c.name for c in part.plan.columns})!=len(fields):
        raise ValueError('SAP rebuild part columns must be unique')
    for c in part.plan.columns:
        f=fields.get(c.name)
        if f is None or c.position!=f.position or c.source_type!=f'{f.datatype}({f.leng},{f.decimals})':
            raise ValueError('SAP rebuild part columns must match DDIC metadata')
    return part.model_dump(mode='json')


def collect_parts(session_id, source, destination, start_revision=None):
    import hashlib
    from .opencode_activities import _client,_path,_request
    parts={};cursor=None;seen=set()
    start_id=('msg_'+hashlib.sha256(f'{session_id}:{start_revision}'.encode()).hexdigest()[:26]) if start_revision is not None else None
    with _client() as client:
        for _ in range(300):
            response=_request(client,'GET',_path(session_id)+'/message',params={**({'cursor':cursor} if cursor else {}),'limit':2})
            for message in response.get('data',[]):
                if start_id is not None and message.get('id')==start_id:return parts
                candidates=[]
                if message.get('type')=='assistant' and message.get('finish') in {'stop','end_turn'}:
                    text='\n'.join(p.get('text','') for p in message.get('content',[]) if p.get('type')=='text')
                    try:candidates.append(parse_json(text))
                    except (ValueError,TypeError):pass
                for item in message.get('content',[]):
                    state=item.get('state',{})
                    if item.get('type')!='tool' or item.get('name')!='execute' or state.get('status')!='completed':continue
                    for output in execute_output_texts(state):
                        try:batch=parse_json(output)
                        except (ValueError,TypeError):continue
                        if not isinstance(batch,dict) or 'sap_rebuild_parts' not in batch:continue
                        values=batch['sap_rebuild_parts']
                        if not isinstance(values,list) or len(values)>32:raise ValueError('SAP rebuild tool delivery batch exceeds 32 parts')
                        candidates.extend(values)
                for data in candidates:
                    if not isinstance(data,dict) or data.get('kind')!='SAP_REBUILD_TABLE_PART' or data.get('bundle_id')!=source['plan_guid']:continue
                    old_key=((data.get('plan') or {}).get('source_table'),data.get('part_index'))
                    if old_key in parts:continue
                    part=validate_part(data,source,destination)
                    key=(part['plan']['source_table'],part['part_index'])
                    # Newest-first API: corrected parts supersede earlier versions.
                    parts.setdefault(key,part)
            cursor=(response.get('cursor') or {}).get('next')
            if not response.get('data') or not cursor:break
            if cursor in seen:raise ValueError('SAP rebuild delivery history cursor repeated')
            seen.add(cursor)
        else:raise ValueError('SAP rebuild delivery exceeds bounded session history')
    return parts


def collect_native_completion(payload):
    import hashlib
    from .opencode_activities import _client,_path,_request
    sid=payload['session_id'];revision=payload.get('delivery_revision')
    start_id=('msg_'+hashlib.sha256(f'{sid}:{revision}'.encode()).hexdigest()[:26]) if revision is not None else None
    cursor=None
    with _client() as client:
        for _ in range(100):
            response=_request(client,'GET',_path(sid)+'/message',params={**({'cursor':cursor} if cursor else {}),'limit':2})
            for message in response.get('data',[]):
                if start_id and message.get('id')==start_id:return None
                for item in message.get('content',[]):
                    state=item.get('state',{})
                    if item.get('type')!='tool' or item.get('name')!='execute' or state.get('status')!='completed':continue
                    for text in execute_output_texts(state):
                        try:value=parse_json(text)
                        except (ValueError,TypeError):continue
                        if isinstance(value,dict) and isinstance(value.get('sap_rebuild_complete'),dict):
                            return value['sap_rebuild_complete']
            cursor=(response.get('cursor') or {}).get('next')
            if not response.get('data') or not cursor:break
    return None


def assemble_table(name, parts):
    import copy
    group=sorted((p for (table,_),p in parts.items() if table==name),key=lambda p:p['part_index'])
    if not group:raise ValueError('Missing SAP rebuild table parts: '+name)
    first=group[0];count=first['part_count'];total=first['total_columns']
    if [p['part_index'] for p in group]!=list(range(1,count+1)):
        raise ValueError('Incomplete SAP rebuild table parts: '+name)
    metadata=copy.deepcopy(first['metadata']);plan=copy.deepcopy(first['plan'])
    metadata['schema']['columns']=[];metadata['semantic_metadata']={};plan['columns']=[]
    for p in group:
        if p['part_count']!=count or p['total_columns']!=total:
            raise ValueError('SAP rebuild part totals differ: '+name)
        for k in ('primary_key','watermark_candidates','target_table','source_table'):
            if p['plan'][k]!=first['plan'][k]:raise ValueError('SAP rebuild part plan headers differ: '+name)
        for k in ('primary_key_columns','table_name'):
            if p['metadata']['schema'][k]!=first['metadata']['schema'][k]:raise ValueError('SAP rebuild metadata headers differ: '+name)
        if p['metadata'].get('watermark_candidates')!=first['metadata'].get('watermark_candidates') or p['metadata'].get('description')!=first['metadata'].get('description'):
            raise ValueError('SAP rebuild metadata headers differ: '+name)
        if any(p['metadata'].get(k)!=first['metadata'].get(k) for k in ('source_object_type','materialized_source')):
            raise ValueError('SAP rebuild materialized source headers differ: '+name)
        metadata['schema']['columns'].extend(p['metadata']['schema']['columns']);plan['columns'].extend(p['plan']['columns'])
        for k,v in p['metadata'].get('semantic_metadata',{}).items():
            if k in metadata['semantic_metadata'] and metadata['semantic_metadata'][k]!=v:raise ValueError('Conflicting SAP domain metadata: '+k)
            metadata['semantic_metadata'][k]=v
        for k in ('evidence','warnings','unresolved_decisions'):
            plan[k]=list(dict.fromkeys([*plan.get(k,[]),*p['plan'].get(k,[])]))
    cols=metadata['schema']['columns'];names=[c['fieldname'] for c in cols];positions=[c['position'] for c in cols]
    if len(cols)!=total or len(set(names))!=total or len(set(positions))!=total:
        raise ValueError('SAP rebuild column manifest count/duplicates mismatch: '+name)
    cols.sort(key=lambda c:c['position']);plan['columns'].sort(key=lambda c:c['position'])
    return metadata,plan


def handle_rebuild_delivery(payload):
    source=payload['input']
    if source.get('migration_approach')!='SAP_REBUILD' or source.get('datasource_stage')!='PLAN':return None
    if source.get('view_correction'):
        data=parse_json(payload['text'])
        if not isinstance(data,dict) or data.get('kind')!='SAP_REBUILD_VIEW_REPAIR':return None
        from .sap_bundle_storage import expand_bundle
        from .sap_mcp_plan import mapped_destination
        if set(data)!={'kind','bundle_id','destination','views'} or data['bundle_id']!=source['plan_guid'] or data['destination']!=mapped_destination(source):
            raise ValueError('SAP rebuild SQL repair identity/fields mismatch')
        previous=expand_bundle(source['view_correction']['previous'])
        return {'text':json.dumps({'destination':data['destination'],'graph':previous['review']['mcp_graph'],
            'plan':{**previous['review']['agent_plan'],'views':data['views']}},separators=(',',':'))}
    try:data=parse_json(payload['text'])
    except ValueError:
        from .sap_mcp_plan import mapped_destination
        dest=mapped_destination(source)
        data=collect_native_completion(payload)
        if data is None:
            current=collect_parts(payload['session_id'],source,dest,payload.get('delivery_revision'))
            if not current:return None
            data={'kind':'SAP_REBUILD_PARTS_READY','bundle_id':source['plan_guid'],'destination':dest}
    if isinstance(data,dict) and data.get('kind')=='SAP_REBUILD_COMPLETE_READY':
        data=collect_native_completion(payload)
        if data is None:raise ValueError('No completed native SAP rebuild manifest was found for this prompt')
    if not isinstance(data,dict) or data.get('kind') not in {'SAP_REBUILD_TABLE_PART','SAP_REBUILD_PARTS_READY','SAP_REBUILD_COMPLETE'}:return None
    from .sap_mcp_plan import mapped_destination
    dest=mapped_destination(source)
    if data.get('bundle_id')!=source['plan_guid'] or data.get('destination')!=dest:raise ValueError('Rebuild delivery identity mismatch')
    start=payload.get('delivery_start_revision')
    if start is None:start=source.get('agent_revision_start')
    parts=collect_parts(payload['session_id'],source,dest,start)
    if data['kind'] in {'SAP_REBUILD_TABLE_PART','SAP_REBUILD_PARTS_READY'}:
        if data['kind']=='SAP_REBUILD_TABLE_PART':
            part=validate_part(data,source,dest);parts[(part['plan']['source_table'],part['part_index'])]=part
        elif not parts:raise ValueError('No completed SAP rebuild parts were found in tool outputs')
        status={name:sorted(index for (table,index) in parts if table==name)
                for name,kind in source['source_datasource_context']['analysis_objects'].items()
                if name in extraction_names(source['source_datasource_context'])}
        return {'delivery_start_revision':start,'agent_continuation':'Extraction source part accepted (not approved or persisted). Continue with the next missing 1-100-column part for an approved table or connector-materialized source view; do not repeat accepted parts. Materialized CDS/database views belong in graph.tables as opaque extraction sources; only views explicitly selected for Spark reconstruction belong in graph.view_order. Preserve source_object_type and materialized_source. Accepted part indexes by source: '+json.dumps(status)+'. Once all required sources are complete, return SAP_REBUILD_COMPLETE with graph.tables as a list of those names and plan without tables. All approved policies remain unchanged.'}
    if set(data)!={'kind','bundle_id','destination','graph','plan'}:raise ValueError('Unexpected SAP rebuild completion fields')
    graph=data['graph'];plan=data['plan'];names=graph['tables']
    if not isinstance(names,list) or len(set(names))!=len(names):raise ValueError('Completion tables must list unique source names')
    expected=extraction_names(source['source_datasource_context'])
    if set(names)!=expected:raise ValueError('Completion must cover every approved base table')
    if 'tables' in plan:raise ValueError('Completion plan tables are assembled from accepted parts')
    metadata={};tables=[]
    for name in names:
        meta,table=assemble_table(name,parts);metadata[name]=meta;tables.append(table)
    return {'text':json.dumps({'destination':dest,'graph':{**graph,'tables':metadata},'plan':{**plan,'tables':tables}},separators=(',',':'))}
