"""SAP view bundles reuse strict SAP table mapping and Spark query validation."""
import json
import re
from pydantic import BaseModel, ConfigDict, Field
from .opencode_sap_plan import SAPAgentPlan, adapt_sap_plan
from .opencode_view_plan import query_references
from thirdparty.configdb.repository import FabricViewCreate, FabricViewDependencyCreate, FabricViewPlan


class SAPViewTable(SAPAgentPlan):
    target_table: str


class SAPView(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    source_view: str
    target_name: str
    spark_sql: str
    dependencies: list[str]
    translation_notes: list[str]


class SAPViewBundle(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)
    schema_version: int=1
    source_view: str
    tables: list[SAPViewTable]=Field(min_length=1,max_length=64)
    views: list[SAPView]=Field(min_length=1,max_length=64)
    evidence: list[str]=Field(min_length=1)
    warnings: list[str]=Field(default_factory=list)
    unresolved_decisions: list[str]


def parse_json(text):
    # Rebuilds include full DDIC metadata and plans for wide SAP base tables.
    # Stay below the 2 MB OpenCode/Temporal transport bound without truncation.
    if len(text.encode())>1_500_000: raise ValueError('SAP view plan exceeds 1.5 MB')
    blocks=re.findall(r'```json\s*(.*?)\s*```',text,re.S)
    if len(blocks)>1: raise ValueError('Return one SAP view JSON bundle')
    content=(blocks[0] if blocks else text).strip()
    try:return json.loads(content)
    except json.JSONDecodeError as exc:
        # A complete single object followed by one redundant closing brace is
        # a recoverable delivery typo. Never accept a second value or a partial
        # object; all strict proposal/schema/provenance checks still follow.
        if exc.msg=='Extra data':
            value,end=json.JSONDecoder().raw_decode(content)
            if isinstance(value,dict) and content[end:].strip()=='}':return value
        raise


def bind_sap_targets(sql,targets,schema):
    """Bind graph identities to the destination names explicitly in the plan."""
    import sqlglot
    from sqlglot import exp
    tree=sqlglot.parse_one(sql,read='spark');changed=False
    for table in tree.find_all(exp.Table):
        if table.catalog or table.db.upper()!=schema.upper() or table.name not in targets:continue
        original=table.name;target_schema,target=targets[original].split('.',1)
        if target==original:continue
        if not table.alias:table.set('alias',exp.TableAlias(this=exp.to_identifier(original)))
        table.set('this',exp.to_identifier(target));table.set('db',exp.to_identifier(target_schema));changed=True
    return tree.sql(dialect='spark') if changed else sql


def adapt_sap_view_bundle(text,source,lakehouse_name):
    bundle=SAPViewBundle.model_validate(parse_json(text))
    graph=source['source_view_graph']
    if bundle.schema_version!=1 or bundle.source_view!=graph['root']: raise ValueError('SAP view source/version mismatch')
    if bundle.unresolved_decisions: raise ValueError('Resolve SAP view decisions: '+'; '.join(bundle.unresolved_decisions)[:1500])
    table_map={t.source_table:t for t in bundle.tables}; view_map={v.source_view:v for v in bundle.views}
    if len(table_map)!=len(bundle.tables) or set(table_map)!=set(graph['tables']): raise ValueError('Include every verified SAP base table once')
    if len(view_map)!=len(bundle.views) or set(view_map)!=set(graph['view_order']): raise ValueError('Include every verified SAP view once')
    if view_map[graph['root']].target_name!=(source.get('fabric_target_name') or source['source_object_name']): raise ValueError('SAP root target mismatch')
    targets={}; tables=[]; reviews=[]; table_index={}
    def destination(name):
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',name): raise ValueError('Use ordinary Fabric target identifiers')
        target=f"{source['fabric_schema_name']}.{name}".upper()
        if target in targets.values(): raise ValueError('Duplicate SAP bundle target')
        return target
    for name,t in table_map.items():
        targets[name]=destination(t.target_table)
        result=adapt_sap_plan(json.dumps(t.model_dump(exclude={'target_table'})),
            {**source,'migration_approach':'SAP_TABLE','source_object_name':name,'source_schema_name':None,
             'fabric_target_name':t.target_table,'source_sap_metadata':graph['tables'][name]},lakehouse_name)
        table_index[name]=len(tables); tables.append(result['table_plan']); reviews.append(result['review'])
    for name,v in view_map.items(): targets[name]=destination(v.target_name)
    views=[]; view_index={}
    for sequence,name in enumerate(graph['view_order'],1):
        v=view_map[name]; deps=graph['nodes'][name]['dependencies']
        if len(v.dependencies)!=len(deps) or set(v.dependencies)!=set(deps): raise ValueError('SAP view dependencies mismatch')
        actual=query_references(v.spark_sql,graph['nodes'][name]['output_columns'])
        expected={targets[d] for d in deps}
        if actual!=expected:
            v.spark_sql=bind_sap_targets(v.spark_sql,targets,source['fabric_schema_name'])
        if query_references(v.spark_sql,graph['nodes'][name]['output_columns'])!=expected:
            raise ValueError('Spark view references differ from SAP dependency targets')
        dependencies=[]
        for number,dep in enumerate(v.dependencies,1):
            if dep in table_index:
                d=FabricViewDependencyCreate(dependency_type='SOURCE_TABLE',source_table_index=table_index[dep],dependency_sequence=number)
                extra={'source_table_index':table_index[dep]}
            elif dep in view_index:
                d=FabricViewDependencyCreate(dependency_type='FABRIC_VIEW',depends_on_view_index=view_index[dep],dependency_sequence=number)
                extra={'depends_on_view_index':view_index[dep]}
            else: raise ValueError('SAP view dependency order is invalid')
            dependencies.append(d.model_dump(mode='json')|extra)
        planned=FabricViewPlan(view=FabricViewCreate(fabric_workspace_id=source['fabric_workspace_id'],
            fabric_lakehouse_id=source['fabric_lakehouse_id'],fabric_lakehouse_name=lakehouse_name,
            fabric_schema_name=source['fabric_schema_name'],view_name=v.target_name,view_sql=v.spark_sql,
            creation_order=sequence,logic_description='\n'.join(v.translation_notes))).model_dump(mode='json')
        planned['dependencies']=dependencies; view_index[name]=len(views); views.append(planned)
    return {'tables':tables,'views':views,'review':{'kind':'sap_view_bundle','source':graph['root'],
        'discovery_provider':'OPENCODE','table_reviews':reviews,'agent_plan':bundle.model_dump(),
        'source_view_definitions':{n:graph['nodes'][n]['source_sql'] for n in graph['view_order']},
        'warnings':bundle.warnings+['Spark SQL is parsed; semantic equivalence and data ingestion require execution checks.'],
        'load_method':'PER_TABLE','write_strategy':'PER_TABLE','delete_policy':'PER_TABLE'}}
