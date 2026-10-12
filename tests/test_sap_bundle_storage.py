from copy import deepcopy
import pytest
from app.migration.sap_bundle_storage import compact_bundle,expand_bundle,pack_json,unpack_json,graph_metadata


def test_large_metadata_roundtrip_preserves_frozen_repair_and_key_facts():
    graph={'tables':{'BSEG':{'schema':{'columns':['x'*500000]}}}}
    plan={'tables':[{'evidence':['y'*900000]}]}
    original={'tables':[], 'views':[], 'review':{'agent_plan':plan,'mcp_graph':graph,
        'table_reviews':[{'source':'BSEG','source_metadata':graph['tables']['BSEG'],'agent_plan':plan['tables'][0]}]}}
    compact=compact_bundle(deepcopy(original))
    assert 'agent_plan_packed' in compact['review'] and 'mcp_graph_packed' in compact['review']
    assert 'source_metadata' not in compact['review']['table_reviews'][0]
    restored=expand_bundle(compact)
    assert restored['review']['agent_plan']==plan and restored['review']['mcp_graph']==graph
    assert graph_metadata(compact,'BSEG')==graph['tables']['BSEG']
    assert 'mcp_graph_packed' in compact['review']  # Expansion does not mutate history.


def test_corrupt_compressed_metadata_fails_closed():
    packed=pack_json({'name':'BKPF'})
    packed['sha256']='0'*64
    with pytest.raises(ValueError,match='hash differs'):unpack_json(packed)
