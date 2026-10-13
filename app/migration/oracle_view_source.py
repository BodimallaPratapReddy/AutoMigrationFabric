"""Read-only, bounded verification of an Oracle view's dependency closure."""

import json
from temporalio import activity
from .source_connections import oracle_client
from thirdparty.oracle.utils import _identifier


def inspect_view_graph(client, schema, name):
    root = (_identifier(schema, "schema"), _identifier(name, "view"))
    nodes, visiting, ordered = {}, set(), []
    with client.connect() as connection:
        connection.call_timeout = 60000
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION READ ONLY")

            def rows(sql, owner, name):
                cursor.execute(sql, owner=owner, name=name)
                return cursor.fetchall()

            def visit(owner, name, kind):
                key = f"{owner}.{name}"
                if key in visiting:
                    raise ValueError("Cyclic Oracle view dependencies are unsupported")
                if key in nodes:
                    return key
                if len(nodes) >= 64:
                    raise ValueError("View dependency graph exceeds 64 objects")
                visiting.add(key)
                node = {"schema": owner, "name": name, "kind": kind, "dependencies": []}
                nodes[key] = node
                if kind == "SYNONYM":
                    found = rows("SELECT table_owner, table_name, db_link FROM all_synonyms "
                                 "WHERE owner=:owner AND synonym_name=:name", owner, name)
                    if len(found) != 1 or found[0][2]:
                        raise ValueError("Unresolved or remote synonym is unsupported")
                    target_owner, target_name, _ = found[0]
                    types = rows("SELECT object_type FROM all_objects WHERE owner=:owner "
                                 "AND object_name=:name AND object_type IN ('TABLE','VIEW','SYNONYM')",
                                 target_owner, target_name)
                    if len(types) != 1:
                        raise ValueError("Synonym target is not a visible table or view")
                    node["dependencies"] = [visit(target_owner, target_name, types[0][0])]
                elif kind == "VIEW":
                    found = rows("SELECT text FROM all_views WHERE owner=:owner AND view_name=:name", owner, name)
                    if len(found) != 1 or not found[0][0]:
                        raise ValueError("Oracle view definition is unavailable")
                    node["oracle_sql"] = str(found[0][0])
                    node["output_columns"] = [r[0] for r in rows(
                        "SELECT column_name FROM all_tab_columns WHERE owner=:owner "
                        "AND table_name=:name ORDER BY column_id", owner, name)]
                    dependencies = rows("SELECT referenced_owner, referenced_name, referenced_type, "
                        "referenced_link_name FROM all_dependencies WHERE owner=:owner AND name=:name "
                        "AND type='VIEW' ORDER BY referenced_owner, referenced_name", owner, name)
                    for dep_owner, dep_name, dep_kind, link in dependencies:
                        if dep_owner == "SYS" and dep_name == "STANDARD":
                            continue
                        if link or dep_kind not in {"TABLE", "VIEW", "SYNONYM"}:
                            raise ValueError("Remote or PL/SQL view dependencies require a separate migration design")
                        node["dependencies"].append(visit(dep_owner, dep_name, dep_kind))
                    node["dependencies"] = list(dict.fromkeys(node["dependencies"]))
                    ordered.append(key)
                visiting.remove(key)
                return key

            found = rows("SELECT status FROM all_objects WHERE owner=:owner AND object_name=:name "
                         "AND object_type='VIEW'", *root)
            if len(found) != 1 or found[0][0] != "VALID":
                raise ValueError("Requested Oracle object is not a valid visible view")
            visit(*root, "VIEW")
    tables = {}
    for key, node in nodes.items():
        if node["kind"] == "TABLE":
            tables[key] = client.inspect_table(node["schema"], node["name"]).model_dump(mode="json")
    graph = {"root": ".".join(root), "nodes": nodes, "view_order": ordered, "tables": tables}
    if len(json.dumps(graph).encode()) > 256000:
        raise ValueError("Oracle view evidence exceeds 256 KB")
    return graph


@activity.defn
def oracle_view_graph(payload):
    source = payload["input"]
    return inspect_view_graph(oracle_client(source["source_connection_name"]),
                              source["source_schema_name"], source["source_object_name"])
