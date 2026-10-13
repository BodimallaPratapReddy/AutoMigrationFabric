from copy import deepcopy
import json
from uuid import uuid4

import pytest

from app.migration.opencode_sap_plan import adapt_sap_plan
from app.migration.sap_mcp_contracts import SAPMaterializedSource, extraction_names
from app.migration.sap_mcp_delivery import validate_part
from thirdparty.configdb.repository import ApprovedRuntimePlan
from test_opencode_sap import META, PLAN, SOURCE


def contract(kind):
    return dict(source_object_type=kind, entity_name='I_SALES', access_method='SAP_APPLICATION_CONNECTOR',
                parameterless=True, client='100', client_semantics='SAP_SESSION_CLIENT',
                authorization_behavior='NO_CDS_DCL', authorization_evidence=['Verified CDS annotation NOT_REQUIRED'],
                verification_queries=['SELECT COUNT(*) FROM VBAK'])


@pytest.mark.parametrize('kind',['CDS_VIEW','DB_VIEW'])
def test_view_materializes_as_delta_table_with_source_kind_and_extract_sql(kind):
    metadata={**deepcopy(META),'source_object_type':kind,'materialized_source':contract(kind)}
    result=adapt_sap_plan(json.dumps(PLAN),{**SOURCE,'source_sap_metadata':metadata},'Bronze')
    runtime=result['table_plan']
    assert runtime['table']['source_object_type']==kind
    assert runtime['table']['source_table_name']=='VBAK'
    assert 'FROM "VBAK"' in runtime['table']['extract_sql']
    assert runtime['replication']['write_strategy']=='REPLACE'
    saved=ApprovedRuntimePlan.model_validate(dict(plan_guid=str(uuid4()),plan_version=1,tables=[runtime],views=[]))
    assert saved.tables[0].table.source_object_type==kind
    assert saved.tables[0].table.extract_sql==runtime['table']['extract_sql']


def test_materialized_cds_parts_require_approved_contract_and_cannot_change_type():
    from test_sap_mcp_delivery import fixture_part
    source,part=fixture_part()
    c=contract('CDS_VIEW')
    source['source_datasource_context'].update(analysis_objects={'VBAK':'CDS_VIEW'},materialized_sources={'VBAK':c})
    part['metadata'].update(source_object_type='CDS_VIEW',materialized_source=c)
    assert extraction_names(source['source_datasource_context'])=={'VBAK'}
    assert validate_part(part,source,'ECC-LOCAL')['metadata']['source_object_type']=='CDS_VIEW'
    part['metadata']['source_object_type']='TABLE'
    with pytest.raises(ValueError,match='preserve'):
        validate_part(part,source,'ECC-LOCAL')


def test_parameterized_or_unscoped_view_does_not_become_table_implicitly():
    with pytest.raises(ValueError):
        SAPMaterializedSource.model_validate({**contract('CDS_VIEW'),'parameterless':False})
    with pytest.raises(ValueError):
        SAPMaterializedSource.model_validate({**contract('CDS_VIEW'),'client_semantics':'ALL_CLIENTS'})
    source={'analysis_objects':{'VBAK':'TRANSPARENT_TABLE','I_SALES':'CDS_VIEW','ZVIEW':'DB_VIEW'}}
    assert extraction_names(source)=={'VBAK'}
