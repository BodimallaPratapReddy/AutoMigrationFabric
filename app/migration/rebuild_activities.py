"""SAP rebuild analysis, verification, and proposed Fabric plan activities."""

import json
from uuid import UUID

from temporalio import activity

from services.sap_analysis_service import build_datasource_rebuild_context
from thirdparty.configdb.repository import (
    ApprovedRuntimePlan, ConfigDBRepository, FabricViewCreate,
    FabricViewDependencyCreate, FabricViewPlan, SAPAnalysisCreate,
    SAPAnalysisObjectCreate,
)
from thirdparty.llm.client import MigrationAnalysisClient
from thirdparty.llm.models import (
    FabricImplementationPlan, FabricPlanRequest, IdentifiedSAPObject,
    SAPExtractorAnalysis, SAPExtractorAnalysisRequest,
)
from thirdparty.sap.utils import SAPClient, SAPExtractionRoute, SAPFieldMetadata, SAPTableSchema
from thirdparty.sap.verification import SAPObjectCandidate, verify_llm_identified_sap_objects

from .contracts import MigrationInput
from .plans import sap_table_plan
from .source_connections import sap_client


def _identified(name: str, kind: str, evidence: str) -> IdentifiedSAPObject:
    return IdentifiedSAPObject(name=name, object_type=kind, role="source", required=True,
                               evidence=[evidence], confidence="HIGH", notes=None)


def _deterministic_analysis(context, sap: SAPClient) -> tuple[SAPExtractorAnalysis, str | None]:
    route = context.extraction_resolution.route
    extractor = context.datasource.EXTRACTOR
    sql = None
    if route == SAPExtractionRoute.TABLE:
        names = [extractor]
    elif route == SAPExtractionRoute.DB_VIEW:
        if not context.dbview or not context.dbview.complete:
            raise ValueError("DB view source SQL or dependencies are incomplete")
        names = [table.tableName for table in context.dbview.details.tables]
        sql = context.dbview.details.sqlQuery
    elif route == SAPExtractionRoute.INFOSET_QUERY:
        if not context.infoset or not context.infoset.complete:
            raise ValueError("InfoSet SQL template or dependencies are incomplete")
        names = [table.tableName for table in context.infoset.details.tables]
        sql = context.infoset.details.sqlTemplate
    else:
        raise ValueError(f"Extraction route {route} is unsupported")
    tables = []
    views = []
    for name in names:
        kind = sap.get_object_type(name).object_type
        if kind == "TRANSPARENT_TABLE":
            tables.append(_identified(name, kind, f"SAP {route} metadata"))
        elif kind == "DB_VIEW":
            views.append(_identified(name, kind, f"SAP {route} metadata"))
        else:
            raise ValueError(f"Unverifiable SAP dependency: {name}")
    return SAPExtractorAnalysis(
        source_tables=tables, source_views=views,
        lookup_tables=[], configuration_tables=[], joins=[], filters=[],
        derived_fields=[], delta_logic=[], ignored_objects=[], unresolved_items=[],
        warnings=context.warnings), sql


@activity.defn(name="analyze_rebuild")
def analyze_rebuild(payload: dict) -> dict:
    input = MigrationInput.model_validate(payload["input"])
    version = payload["version"]
    db = ConfigDBRepository()
    with sap_client(input.source_connection_name) as sap:
        sap.test_connection()
        context = build_datasource_rebuild_context(sap, input.source_object_name)
        resolution = context.extraction_resolution
        if not resolution.supported:
            return {"failure_reason": f"{resolution.reason_code}: {resolution.reason}"}
        if resolution.route == SAPExtractionRoute.DB_VIEW and (
                not context.dbview or not context.dbview.complete):
            return {"failure_reason": "DB_VIEW_INCOMPLETE: source SQL or dependencies are incomplete"}
        if resolution.route == SAPExtractionRoute.INFOSET_QUERY and (
                not context.infoset or not context.infoset.complete):
            return {"failure_reason": "INFOSET_INCOMPLETE: SQL template or dependencies are incomplete"}
        llm = None
        if resolution.route == SAPExtractionRoute.FUNCTION_MODULE:
            if not context.abap or context.abap.complete is not True:
                return {"failure_reason": "ABAP_SCRAPE_INCOMPLETE: analysis cannot be approved"}
            request = SAPExtractorAnalysisRequest(
                plan_guid=UUID(payload["plan_guid"]), analysis_version=version,
                datasource=context.datasource, extract_structure_fields=context.extract_structure_schema,
                exposed_fields=context.datasource_fields, scrape=context.abap)
            llm = MigrationAnalysisClient()
            if payload.get("previous_analysis") and payload.get("feedback"):
                analysis = llm.revise_sap_extractor_analysis(
                    previous_analysis=SAPExtractorAnalysis.model_validate(payload["previous_analysis"]),
                    user_feedback=payload["feedback"], source_context=request)
            else:
                analysis = llm.analyze_sap_extractor(request)
            source_sql = None
        else:
            analysis, source_sql = _deterministic_analysis(context, sap)
            if payload.get("feedback"):
                raise ValueError("Feedback revision is only supported for function module analysis")
        candidates = [SAPObjectCandidate(name=item.name, object_type=item.object_type,
                                         evidence=item.evidence)
                      for group in (analysis.source_tables, analysis.source_views,
                                    analysis.lookup_tables, analysis.configuration_tables)
                      for item in group]
        verification = verify_llm_identified_sap_objects(sap, candidates)
        if verification.unresolved or analysis.unresolved_items or not verification.verified:
            return {"failure_reason": "SAP_OBJECT_VERIFICATION_FAILED: analysis contains unresolved or unverified source objects"}
    summary = {"route": str(resolution.route), "analysis": analysis.model_dump(mode="json"),
               "verified": verification.model_dump(mode="json"), "source_sql": source_sql,
               "warnings": context.warnings}
    analysis_id = db.create_sap_analysis(SAPAnalysisCreate(
        plan_guid=UUID(payload["plan_guid"]), analysis_version=version,
        datasource_name=input.source_object_name, root_object_type=str(resolution.route),
        root_object_name=context.datasource.EXTRACTOR,
        summary=json.dumps(summary), llm_model=llm.model if llm else None,
        phoenix_trace_id=llm.last_trace_id if llm else None))
    db.insert_sap_analysis_objects(analysis_id, [SAPAnalysisObjectCreate(
        object_type=item.object_type, object_name=item.name,
        observed_from_scraper=resolution.route != SAPExtractionRoute.FUNCTION_MODULE,
        interpreted_role="source", interpretation="Verified against SAP dictionary",
        is_replication_required=True) for item in verification.verified])
    db.update_sap_analysis_status(analysis_id, expected_status="DRAFT",
                                  new_status="WAITING_APPROVAL", expected_version=version)
    return {"analysis_guid": str(analysis_id), **summary}


@activity.defn(name="approve_analysis")
def approve_analysis(payload: dict) -> None:
    ConfigDBRepository().record_analysis_approval(
        UUID(payload["analysis_guid"]), approved_by=payload["approved_by"],
        expected_version=payload["version"])


@activity.defn(name="revise_analysis_version")
def revise_analysis_version(payload: dict) -> int:
    db = ConfigDBRepository()
    db.supersede_analysis(UUID(payload["analysis_guid"]))
    return db.update_analysis_version(UUID(payload["plan_guid"]),
                                      expected_version=payload["version"])


@activity.defn(name="generate_rebuild_plan")
def generate_rebuild_plan(payload: dict) -> dict:
    input = MigrationInput.model_validate(payload["input"])
    analysis = SAPExtractorAnalysis.model_validate(payload["analysis"])
    verified = payload["verified"]["verified"]
    mappings = {field["fieldname"]: field["datatype"]
                for item in verified for field in item["fields"]}
    existing = ConfigDBRepository().list_provisioned_target_tables(
        input.fabric_workspace_id, input.fabric_lakehouse_id)
    request = FabricPlanRequest(
        plan_guid=UUID(payload["plan_guid"]), plan_version=payload["version"],
        approved_analysis=analysis, target_workspace_id=input.fabric_workspace_id,
        target_lakehouse_name=payload["lakehouse_name"], existing_objects=existing,
        datatype_mappings=mappings,
        conventions=["Only verified SAP source objects may be replicated.",
                     "Views must be single SELECT queries with explicit dependencies."])
    llm = MigrationAnalysisClient()
    if payload.get("previous_plan") and payload.get("feedback"):
        result = llm.revise_fabric_plan(
            previous_plan=FabricImplementationPlan.model_validate(payload["previous_plan"]),
            user_feedback=payload["feedback"], source_context=request)
    else:
        result = llm.generate_fabric_plan(request)
    if result.unresolved_decisions:
        raise ValueError("Fabric proposal has unresolved decisions")
    return result.model_dump(mode="json")


@activity.defn(name="revise_plan_version")
def revise_plan_version(payload: dict) -> int:
    return ConfigDBRepository().update_migration_plan_version(
        UUID(payload["plan_guid"]), expected_version=payload["version"])


@activity.defn(name="persist_verified_rebuild_plan")
def persist_verified_rebuild_plan(payload: dict) -> dict:
    """Bind every replicated source to the approved, SAP-verified analysis."""
    input = MigrationInput.model_validate(payload["input"])
    db = ConfigDBRepository()
    guid = UUID(payload["plan_guid"])
    analysis_record = db.get_sap_analysis(UUID(payload["analysis_guid"]))
    if (analysis_record is None or analysis_record.plan_guid != guid
            or analysis_record.analysis_status != "APPROVED"):
        raise ValueError("Current SAP analysis is not approved")
    approved_names = {item.object_name.upper() for item in
                      db.list_sap_analysis_objects(analysis_record.analysis_guid)
                      if item.is_active and item.is_replication_required}
    summary = json.loads(analysis_record.summary or "{}")
    verified = {item["name"].upper(): item for item in
                summary.get("verified", {}).get("verified", [])
                if item["object_type"] == "TRANSPARENT_TABLE"}
    proposal = FabricImplementationPlan.model_validate(payload["proposal"])
    if proposal.unresolved_decisions:
        raise ValueError("Fabric proposal has unresolved decisions")
    tables = []
    table_index = {}
    used_targets = set()
    reused_targets = set()
    registered_targets = {name.upper() for name in db.list_provisioned_target_tables(
        input.fabric_workspace_id, input.fabric_lakehouse_id)}
    for item in proposal.tables:
        name = item.source_object.upper()
        target = item.target_table.upper()
        if target in used_targets:
            raise ValueError(f"Duplicate Fabric target table: {item.target_table}")
        used_targets.add(target)
        if name not in approved_names or name not in verified:
            raise ValueError(f"Unverified SAP source object: {name}")
        if item.action == "REUSE":
            if target not in registered_targets:
                raise ValueError(f"Reused Fabric table is not provisioned: {item.target_table}")
            reused_targets.add(target)
            continue
        if name in table_index:
            raise ValueError(f"Duplicate SAP source object: {name}")
        fields = [SAPFieldMetadata.model_validate(field) for field in verified[name]["fields"]]
        schema = SAPTableSchema(table_name=name, columns=fields,
                                primary_key_columns=verified[name]["primary_key_columns"])
        planned, _ = sap_table_plan(input.model_copy(update={
            "source_object_name": name, "fabric_target_name": item.target_table}),
            payload["lakehouse_name"], schema)
        table_index[name] = len(tables)
        table_index[target] = len(tables)
        tables.append(planned)
    views = []
    view_index = {}
    for item in sorted(proposal.views, key=lambda view: view.creation_order):
        sql = item.view_sql.strip()
        if not sql.upper().startswith("SELECT ") or ";" in sql or "--" in sql or "/*" in sql:
            raise ValueError(f"View {item.view_name} must be a single SELECT statement")
        dependencies = []
        for number, name in enumerate(item.dependencies, start=1):
            key = name.upper()
            if key in table_index:
                dependencies.append(FabricViewDependencyCreate(
                    dependency_type="SOURCE_TABLE", source_table_index=table_index[key],
                    dependency_sequence=number))
            elif key in view_index:
                dependencies.append(FabricViewDependencyCreate(
                    dependency_type="FABRIC_VIEW", depends_on_view_index=view_index[key],
                    dependency_sequence=number))
            elif key in reused_targets:
                dependencies.append(FabricViewDependencyCreate(
                    dependency_type="EXISTING_OBJECT", existing_object_name=name,
                    dependency_sequence=number))
            else:
                raise ValueError(f"Unverified Fabric view dependency: {name}")
        view_index[item.view_name.upper()] = len(views)
        views.append(FabricViewPlan(view=FabricViewCreate(
            fabric_workspace_id=input.fabric_workspace_id,
            fabric_lakehouse_id=input.fabric_lakehouse_id,
            fabric_lakehouse_name=payload["lakehouse_name"],
            fabric_schema_name=input.fabric_schema_name,
            view_name=item.view_name, view_sql=sql,
            creation_order=item.creation_order), dependencies=dependencies))
    if not tables and not views:
        raise ValueError("Fabric proposal contains no new table or view")
    runtime = ApprovedRuntimePlan(plan_guid=guid, plan_version=payload["version"],
                                  tables=tables, views=views)
    return db.persist_approved_runtime_plan(runtime).model_dump(mode="json")


ACTIVITIES = [analyze_rebuild, approve_analysis, revise_analysis_version,
              generate_rebuild_plan, revise_plan_version,
              persist_verified_rebuild_plan]
