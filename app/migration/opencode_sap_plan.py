"""SAP agent proposals are checked against independently read SAP DDIC metadata."""
import json
import re
from pydantic import BaseModel, ConfigDict, Field
from .opencode_plan import AgentColumn
from .mapping_types import normalize_fabric_type
from thirdparty.configdb.repository import SourceTableCreate, SourceTableColumnCreate, SourceTablePlan, ReplicationConfigCreate
from thirdparty.sap.extraction import build_extract_sql, recognized_conversion


class SAPAgentColumn(AgentColumn):
    description: str | None = Field(default=None, description='Original SAP DDIC field text, not an inferred explanation')
    etrm_description: None = None


class SAPAgentPlan(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    schema_version: int = 1
    source_table: str
    columns: list[SAPAgentColumn] = Field(min_length=1, max_length=1000)
    primary_key: list[str]
    watermark_candidates: list[str]
    description: str | None = None
    evidence: list[str] = Field(min_length=1)
    warnings: list[str] = Field(default_factory=list)
    unresolved_decisions: list[str]


def adapt_sap_plan(text: str, source: dict, lakehouse_name: str) -> dict:
    if len(text.encode()) > 256000:
        raise ValueError('SAP agent plan exceeds 256 KB')
    blocks = re.findall(r'```json\s*(.*?)\s*```',text,re.S)
    if len(blocks)>1:
        raise ValueError('Return exactly one SAP JSON plan')
    plan = SAPAgentPlan.model_validate(json.loads(blocks[0] if blocks else text))
    if plan.schema_version != 1 or plan.source_table != source['source_object_name']:
        raise ValueError('SAP source/version differs from the request')
    if plan.unresolved_decisions:
        raise ValueError('Resolve SAP decisions before approval: '+'; '.join(plan.unresolved_decisions)[:1500])
    metadata = source['source_sap_metadata']
    fields = {f['fieldname']: f for f in metadata['schema']['columns']}
    if len(plan.columns)!=len(fields) or {c.name for c in plan.columns}!=set(fields):
        raise ValueError('Include every independently verified SAP field exactly once')
    if plan.primary_key != metadata['schema']['primary_key_columns']:
        raise ValueError('SAP agent primary key differs from DDIC metadata')
    candidates = {c['field_name']: c for c in metadata['watermark_candidates']}
    if len(set(plan.watermark_candidates))!=len(plan.watermark_candidates) or not set(plan.watermark_candidates)<=set(candidates):
        raise ValueError('SAP watermark candidates must be independently verified date/timestamp fields')
    columns=[]
    for c in sorted(plan.columns,key=lambda c:c.position):
        original=fields[c.name]
        if c.position!=original['position'] or c.source_type!=f"{original['datatype']}({original['leng']},{original['decimals']})":
            raise ValueError('SAP source field position/datatype differs from DDIC metadata')
        if normalize_fabric_type(c.fabric_type)!=c.fabric_type:
            raise ValueError('SAP agent must return canonical Lakehouse types')
        conversion = recognized_conversion(c.source_expression or '', c.name)
        kind = conversion[0] if conversion else None
        semantic = metadata.get('semantic_metadata', {}).get(original.get('rollname'), {})
        temporal = original.get('rollname') in {'TIMESTAMP','TIMESTAMPL','UTC_TIMESTAMP','TZNTSTMPS','TZNTSTMPL'} or semantic.get('domain') in {'TIMESTAMP','TIMESTAMPL','TZNTSTMPS','TZNTSTMPL'}
        if original['datatype']=='DATS' and (c.fabric_type!='DATE' or kind!='DATE'):
            raise ValueError('SAP DATS requires DATE with the approved zero-date-to-NULL conversion')
        if original['datatype']=='TIMS' and (c.fabric_type!='STRING' or kind!='TIME'):
            raise ValueError('SAP TIMS requires HH:mm:ss STRING preserving zero time as midnight')
        if temporal and original['datatype']=='DEC' and original['decimals']>6:
            if original['decimals']!=7 or original['leng']!=21:
                raise ValueError('Nonstandard packed timestamp precision requires a reviewed exact format')
            if c.fabric_type!='STRING' or kind!='LONG_TIMESTAMP':
                raise ValueError(f'{plan.source_table}.{c.name}: Long SAP timestamps require exact seven-digit STRING conversion; Spark TIMESTAMP loses precision')
        elif temporal and original['datatype']=='DEC' and (c.fabric_type!='TIMESTAMP' or kind!='TIMESTAMP'):
            raise ValueError(f'{plan.source_table}.{c.name}: Short SAP timestamps require a verified TIMESTAMP conversion')
        if c.fabric_type=='BOOLEAN':
            if kind!='BOOLEAN' or original['datatype']!='CHAR' or original['leng']!=1:
                raise ValueError('SAP BOOLEAN requires a domain-verified character flag conversion')
            fixed={str(v).strip() for v in semantic.get('fixed_values', [])}
            if fixed != {conversion[1], conversion[2]}:
                raise ValueError('Boolean true/false mapping must match independently verified domain fixed values')
        if original['datatype']=='FLTP' and c.fabric_type!='DOUBLE' and not (c.fabric_type=='STRING' and kind=='STRING'):
            raise ValueError('SAP FLTP requires DOUBLE to preserve its floating-point precision')
        if original['datatype'] in {'NUMC','CLNT','LANG','CUKY','UNIT'} and c.fabric_type!='STRING':
            raise ValueError('SAP identifiers/initial date and time representations require STRING in Bronze')
        c.description=original['ddtext'] or None
        references = (f"SAP reference: {original['reftable']}.{original['reffield']}" if original.get('reftable') or original.get('reffield') else '')
        notes='\n'.join(n for n in (c.transformation_notes,references) if n) or None
        columns.append(SourceTableColumnCreate(sno=c.position,column_name=c.name,source_data_type=c.source_type,
            fabric_data_type=c.fabric_type,target_column_name=c.target_name,source_expression=c.source_expression,
            description=c.description,is_primary_key=c.name in plan.primary_key,is_nullable=False if c.name in plan.primary_key else None,
            is_watermark_candidate=c.name in plan.watermark_candidates,transformation_notes=notes))
    source_kind=metadata.get('source_object_type','TABLE')
    if source_kind not in {'TABLE','CDS_VIEW','DB_VIEW'}:
        raise ValueError('Unsupported SAP extraction source object type')
    if source_kind!='TABLE' and not metadata.get('materialized_source'):
        raise ValueError('SAP view extraction requires an approved connector read contract')
    runtime=SourceTablePlan(table=SourceTableCreate(connection_name=source['source_connection_name'],
        source_system_type='SAP_ECC',source_object_type=source_kind,source_table_name=plan.source_table,
        data_source_type='SAP_VIEW' if source_kind!='TABLE' else None,
        fabric_workspace_id=source['fabric_workspace_id'],fabric_lakehouse_id=source['fabric_lakehouse_id'],
        fabric_lakehouse_name=lakehouse_name,fabric_lakehouse_schema=source['fabric_schema_name'],
        fabric_table_name=source.get('fabric_target_name') or plan.source_table,description=metadata.get('description')),
        columns=columns,replication=ReplicationConfigCreate(incremental_method='FULL',write_strategy='REPLACE',
            primary_key_columns=plan.primary_key or None,ingestion_flag=False,pipeline_workspace_id=source['fabric_workspace_id']))
    runtime.table.extract_sql=build_extract_sql(runtime.table.model_dump(),[c.model_dump() for c in columns])
    return {'table_plan':runtime.model_dump(mode='json'),'review':dict(source=plan.source_table,discovery_provider='OPENCODE',
        columns=[dict(name=c.name,source_type=c.source_type,fabric_type=c.fabric_type,supported=True,description=c.description,
            warning=c.warning,target_name=c.target_name,source_expression=c.source_expression,transformation_notes=c.transformation_notes) for c in plan.columns],
        primary_key=plan.primary_key,primary_key_candidates=[],watermark_candidates=[dict(column_name=n,data_type=candidates[n]['datatype'],
            reason=candidates[n]['reason'],index_details=[]) for n in plan.watermark_candidates],load_method='FULL',write_strategy='REPLACE',
        warnings=plan.warnings,agent_plan=plan.model_dump(),extract_sql=runtime.table.extract_sql,
        extract_sql_dialect='SAP_NATIVE_SQL',description=metadata.get('description'),
        source_object_type=source_kind,materialized_source=metadata.get('materialized_source'))}
