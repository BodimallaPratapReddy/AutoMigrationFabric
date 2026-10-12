"""Agent-authored view bundles, validated against a verified Oracle graph."""

import json
import re
from pydantic import BaseModel, ConfigDict, Field
from .opencode_plan import AgentPlan, adapt_plan, is_attachment_path_placeholder
from thirdparty.configdb.repository import FabricViewCreate, FabricViewDependencyCreate, FabricViewPlan


class AgentView(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    source_schema: str
    source_view: str
    target_name: str
    spark_sql: str
    dependencies: list[str] = Field(description="Exact SCHEMA.OBJECT source identities of direct tables/views; resolve synonyms")
    translation_notes: list[str]


class AgentViewTable(AgentPlan):
    target_table: str


class AgentViewBundle(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: int = 1
    source_schema: str
    source_view: str
    tables: list[AgentViewTable] = Field(max_length=64)
    views: list[AgentView] = Field(min_length=1, max_length=64)
    evidence: list[str] = Field(min_length=1)
    warnings: list[str] = Field(default_factory=list)
    unresolved_decisions: list[str]


def query_references(sql, output_columns=None):
    import sqlglot
    from sqlglot import exp
    from sqlglot.errors import SqlglotError
    from sqlglot.optimizer.scope import traverse_scope
    if ";" in sql or "--" in sql or "/*" in sql or len(sql.encode()) > 128000:
        raise ValueError("View SQL must be one bounded SELECT without comments or semicolons")
    try:
        expressions = sqlglot.parse(sql, read="spark")
    except SqlglotError:
        raise ValueError("View SQL could not be parsed as a Spark query") from None
    if len(expressions) != 1 or not isinstance(expressions[0], exp.Select):
        raise ValueError("View SQL must be a single Spark SELECT (WITH is supported)")
    tree = expressions[0]
    if output_columns is not None and [name.upper() for name in tree.named_selects] != [
            name.upper() for name in output_columns]:
        raise ValueError("Spark view must explicitly preserve Oracle output column names and order")
    if tree.find(exp.Into):
        raise ValueError("View SQL cannot write data")
    if any(dtype.this == exp.DataType.Type.TIMESTAMPNTZ for dtype in tree.find_all(exp.DataType)):
        raise ValueError("TIMESTAMP_NTZ expressions require unsupported runtime configuration; revise the view")
    references = set()
    for scope in traverse_scope(tree):
        for source in scope.sources.values():
            if isinstance(source, exp.Table):
                if not source.db or source.catalog or not isinstance(source.this, exp.Identifier):
                    raise ValueError("View tables must use explicit schema.table names in the selected Lakehouse")
                references.add(f"{source.db}.{source.name}".upper())
    return references


def adapt_view_bundle(text, source, lakehouse_name):
    if len(text.encode()) > 256000:
        raise ValueError("Agent view plan exceeds 256 KB")
    blocks = re.findall(r"```json\s*(.*?)\s*```", text, re.S)
    if len(blocks) > 1:
        raise ValueError("Return exactly one JSON view bundle")
    bundle = AgentViewBundle.model_validate(json.loads(blocks[0] if blocks else text))
    graph = source["source_view_graph"]
    if bundle.schema_version != 1 or f"{bundle.source_schema}.{bundle.source_view}" != graph["root"]:
        raise ValueError("View bundle source/version does not match the request")
    if bundle.unresolved_decisions:
        raise ValueError("Resolve view decisions: " + "; ".join(bundle.unresolved_decisions)[:1500])
    def identity(item):
        return f"{item.source_schema}.{getattr(item, 'source_table', None) or item.source_view}"
    table_map = {identity(item): item for item in bundle.tables}
    view_map = {identity(item): item for item in bundle.views}
    if len(table_map) != len(bundle.tables) or set(table_map) != set(graph["tables"]):
        raise ValueError("Bundle must include every verified base table exactly once")
    if len(view_map) != len(bundle.views) or set(view_map) != set(graph["view_order"]):
        raise ValueError("Bundle must include every verified view exactly once")
    if view_map[graph["root"]].target_name != (source.get("fabric_target_name") or source["source_object_name"]):
        raise ValueError("Root view target does not match the selected destination")
    targets, runtime_tables, table_reviews, table_index = {}, [], [], {}
    for key, item in table_map.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item.target_table):
            raise ValueError("Use ordinary target table identifiers")
        target = f"{source['fabric_schema_name']}.{item.target_table}".upper()
        if target in targets.values():
            raise ValueError("Duplicate target object name")
        targets[key] = target
        plan = item.model_dump(exclude={"target_table"})
        verified = graph["tables"][key]
        if "description" in verified:
            plan["description"] = verified["description"]
        actual = {c["column_name"] if "column_name" in c else c["name"] for c in verified["columns"]}
        if set(c.name for c in item.columns) != actual or len(item.columns) != len(actual):
            raise ValueError("Agent columns do not match verified Oracle table columns")
        metadata = {c["column_name"]: c for c in verified["columns"]}
        for column in item.columns:
            original = metadata[column.name]
            if "description" in original:
                next(c for c in plan["columns"] if c["name"] == column.name)["description"] = original["description"]
            deferred_attachment = (str(original.get("data_type", "")).upper() == "BLOB"
                and original.get("nullable") is False and is_attachment_path_placeholder(column))
            if ("id" in original and column.position != original["id"]
                    or original.get("nullable") is not None and column.nullable != original["nullable"]
                    and not deferred_attachment):
                raise ValueError("Agent column position/nullability differs from Oracle metadata")
        pk = [c["name"] for c in (verified.get("primary_key") or {}).get("columns", [])]
        if item.primary_key != pk:
            raise ValueError("Agent primary key does not match the Oracle constraint")
        result = adapt_plan(json.dumps(plan), {**source, "migration_approach": "ORACLE_TABLE",
            "source_schema_name": item.source_schema, "source_object_name": item.source_table,
            "fabric_target_name": item.target_table}, lakehouse_name)
        table_index[key] = len(runtime_tables)
        runtime_tables.append(result["table_plan"])
        table_reviews.append(result["review"])
    for key, item in view_map.items():
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", item.target_name):
            raise ValueError("Use ordinary target view identifiers")
        target = f"{source['fabric_schema_name']}.{item.target_name}".upper()
        if target in targets.values():
            raise ValueError("Duplicate target object name")
        targets[key] = target
    def resolve(key):
        node = graph["nodes"][key]
        return resolve(node["dependencies"][0]) if node["kind"] == "SYNONYM" else key
    runtime_views, view_index = [], {}
    for order, key in enumerate(graph["view_order"], 1):
        item = view_map[key]
        expected = {resolve(dep) for dep in graph["nodes"][key]["dependencies"]}
        if set(item.dependencies) != expected or len(item.dependencies) != len(expected):
            raise ValueError("Agent view dependencies do not match verified Oracle dependencies")
        output_columns = graph["nodes"][key].get("output_columns")
        if output_columns is None:
            import sqlglot
            output_columns = sqlglot.parse_one(graph["nodes"][key]["oracle_sql"], read="oracle").named_selects
            if "*" in output_columns:
                raise ValueError("Source view output columns must be verified from Oracle metadata")
        if query_references(item.spark_sql, output_columns) != {
                targets[dep] for dep in expected}:
            raise ValueError("Spark SQL references do not match declared target dependencies")
        deps = []
        for seq, dep in enumerate(item.dependencies, 1):
            if dep in table_index:
                deps.append(FabricViewDependencyCreate(dependency_type="SOURCE_TABLE",
                    source_table_index=table_index[dep], dependency_sequence=seq))
            elif dep in view_index:
                deps.append(FabricViewDependencyCreate(dependency_type="FABRIC_VIEW",
                    depends_on_view_index=view_index[dep], dependency_sequence=seq))
            else:
                raise ValueError("View dependency order is invalid")
        view_index[key] = len(runtime_views)
        runtime_views.append(FabricViewPlan(view=FabricViewCreate(
            fabric_workspace_id=source["fabric_workspace_id"], fabric_lakehouse_id=source["fabric_lakehouse_id"],
            fabric_lakehouse_name=lakehouse_name, fabric_schema_name=source["fabric_schema_name"],
            view_name=item.target_name, view_sql=item.spark_sql, creation_order=order,
            logic_description="\n".join(item.translation_notes)), dependencies=deps).model_dump(mode="json"))
        # Pydantic excludes transient indexes from dumps; retain them until transactional persistence.
        runtime_views[-1]["dependencies"] = [d.model_dump(mode="json") | {
            **({"source_table_index": d.source_table_index} if d.source_table_index is not None else {}),
            **({"depends_on_view_index": d.depends_on_view_index} if d.depends_on_view_index is not None else {})} for d in deps]
    return {"tables": runtime_tables, "views": runtime_views, "review": {
        "kind": "oracle_view_bundle", "discovery_provider": "OPENCODE", "source": graph["root"],
        "table_reviews": table_reviews, "agent_plan": bundle.model_dump(),
        "oracle_view_definitions": {key: graph["nodes"][key]["oracle_sql"] for key in graph["view_order"]},
        "load_method": "FULL", "write_strategy": "REPLACE", "delete_policy": "NONE",
        "warnings": bundle.warnings + ["Spark SQL is parsed locally; semantic equivalence and Fabric execution remain unverified.",
            "Base tables use full refresh; incremental loading and delete reconciliation are not enabled."]}}
