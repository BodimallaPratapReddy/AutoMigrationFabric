"""Bounded, independently verified classic SAP database-view dependency graph."""
from temporalio import activity
from .source_connections import sap_client
from .source_activities import sap_agent_metadata
from thirdparty.sap.extraction import identifier


@activity.defn
def sap_view_graph(payload: dict) -> dict:
    source=payload['input']; root=identifier(source['source_object_name'])
    nodes={}; tables={}; order=[]; active=set()
    with sap_client(source['source_connection_name']) as client:
        def visit(name, depth=0):
            name=identifier(name)
            if name in active: raise ValueError('SAP view dependency cycle')
            if name in nodes: return
            if depth>16 or len(nodes)>=64: raise ValueError('SAP view dependency graph exceeds bounds')
            active.add(name)
            kind=client.get_object_type(name).object_type
            if kind=='TRANSPARENT_TABLE':
                metadata=sap_agent_metadata({'input':{**source,'migration_approach':'SAP_TABLE','source_object_name':name}})
                nodes[name]={'kind':'TABLE','dependencies':[]}; tables[name]=metadata
            elif kind=='DB_VIEW':
                details=client.get_dbview_query_details(name)
                if not details.sqlQueryComplete or not details.sqlQuery.strip():
                    raise ValueError('SAP database view definition is incomplete; CDS/parameterized views require separate support')
                columns=client.read_metadata(name)
                deps=list(dict.fromkeys(t.tableName for t in details.tables))
                if not deps or not columns: raise ValueError('SAP view needs verified dependencies and output fields')
                nodes[name]={'kind':'VIEW','dependencies':deps,'source_sql':details.sqlQuery,
                    'output_columns':[c.fieldname for c in sorted(columns,key=lambda c:c.position)],
                    'column_metadata':[c.model_dump(mode='json') for c in columns]}
                for dependency in deps: visit(dependency,depth+1)
                order.append(name)
            else: raise ValueError('SAP view depends on an unsupported source object')
            active.remove(name)
        visit(root)
    if not order or order[-1]!=root: raise ValueError('Select a classic SAP database view, not a table')
    return {'root':root,'nodes':nodes,'tables':tables,'view_order':order}
