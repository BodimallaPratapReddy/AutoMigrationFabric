"""Lossless bounded storage for wide SAP metadata inside Temporal payloads."""
import base64
import copy
import hashlib
import json
import zlib

MAX_EXPANDED_BYTES=8_000_000


def pack_json(value):
    raw=json.dumps(value,separators=(',',':'),ensure_ascii=False).encode('utf-8')
    if len(raw)>MAX_EXPANDED_BYTES:raise ValueError('SAP bundle metadata exceeds its expanded bound')
    return {'codec':'zlib-json-v1','sha256':hashlib.sha256(raw).hexdigest(),
            'data':base64.b64encode(zlib.compress(raw,9)).decode('ascii')}


def unpack_json(value):
    if value.get('codec')!='zlib-json-v1':raise ValueError('Unknown SAP bundle metadata codec')
    try:
        compressed=base64.b64decode(value['data'],validate=True)
        stream=zlib.decompressobj();raw=stream.decompress(compressed,MAX_EXPANDED_BYTES+1)
        if len(raw)>MAX_EXPANDED_BYTES or not stream.eof or stream.unused_data:
            raise ValueError('SAP bundle metadata exceeds its expanded bound or is incomplete')
        if hashlib.sha256(raw).hexdigest()!=value['sha256']:raise ValueError('SAP bundle metadata hash differs')
        return json.loads(raw)
    except (KeyError,zlib.error,UnicodeDecodeError) as exc:
        raise ValueError('SAP bundle metadata is invalid') from exc


def expand_bundle(bundle):
    review=bundle['review']
    if not any(k+'_packed' in review for k in ('agent_plan','mcp_graph')):return bundle
    result=copy.deepcopy(bundle)
    for key in ('agent_plan','mcp_graph'):
        packed=result['review'].pop(key+'_packed',None)
        if packed is not None:result['review'][key]=unpack_json(packed)
    return result


def compact_bundle(bundle):
    # Only the root agent plan and graph are used for SQL repair. Per-table
    # copies duplicate those facts and have no consumers in MCP workflows.
    for review in bundle['review']['table_reviews']:
        review.pop('agent_plan',None)
        review.pop('source_metadata',None)
    if len(json.dumps(bundle,separators=(',',':')).encode())>1_250_000:
        for key in ('agent_plan','mcp_graph'):
            if key in bundle['review']:
                bundle['review'][key+'_packed']=pack_json(bundle['review'].pop(key))
    if len(json.dumps(bundle).encode())>1_900_000:
        raise ValueError('Complete SAP runtime bundle exceeds the safe Temporal payload bound; no columns were truncated')
    return bundle


def graph_metadata(bundle,table):
    review=bundle['review']
    graph=review.get('mcp_graph')
    if graph is None:graph=unpack_json(review['mcp_graph_packed'])
    return graph['tables'][table]
