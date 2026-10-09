"""Bounded Config DB operations. Workflow code never opens a DB connection."""

import json
from uuid import NAMESPACE_URL, UUID, uuid5

from temporalio import activity

from thirdparty.configdb.repository import (
    ApprovedRuntimePlan, BatchObjectRunCreate, BatchRunCreate, ConfigDBRepository,
    MigrationPlanCreate, PlanStatus,
)
from .target_review import compare_columns, compare_replication


def _db() -> ConfigDBRepository:
    return ConfigDBRepository()


@activity.defn(name="create_migration_plan")
def create_migration_plan(payload: dict) -> str:
    approach = payload["migration_approach"]
    system = "ORACLE" if approach == "ORACLE_TABLE" else "SAP_ECC"
    object_type = "TABLE" if approach in {"ORACLE_TABLE", "SAP_TABLE"} else "SAP_DATASOURCE"
    plan_id = uuid5(NAMESPACE_URL, f"fabric-migration:{payload['workflow_id']}")
    return str(_db().create_migration_plan(MigrationPlanCreate(
        source_connection_name=payload["source_connection_name"],
        source_system_type=system, source_object_type=object_type,
        source_object_name=payload["source_object_name"], migration_approach=approach),
        plan_guid=plan_id))


@activity.defn(name="set_temporal_ids")
def set_temporal_ids(payload: dict) -> None:
    _db().set_temporal_ids(UUID(payload["plan_guid"]), payload["workflow_id"], payload["run_id"])


@activity.defn(name="transition_plan")
def transition_plan(payload: dict) -> None:
    db = _db()
    guid = UUID(payload["plan_guid"])
    plan = db.get_migration_plan(guid)
    if plan is None:
        raise ValueError("migration plan was not found")
    expected, target = PlanStatus(payload["expected"]), PlanStatus(payload["target"])
    if plan.status == target:
        return
    db.update_migration_plan_status(guid, expected_status=expected,
                                    new_status=target, expected_plan_version=payload["version"])


@activity.defn(name="approve_runtime_plan")
def approve_runtime_plan(payload: dict) -> None:
    _db().record_plan_approval(UUID(payload["plan_guid"]),
                               approved_by=payload["approved_by"],
                               expected_plan_version=payload["version"])


@activity.defn(name="persist_runtime_plan")
def persist_runtime_plan(payload: dict) -> dict:
    result = _db().persist_approved_runtime_plan(ApprovedRuntimePlan.model_validate(payload))
    return result.model_dump(mode="json")


@activity.defn(name="review_existing_target")
def review_existing_target(payload: dict) -> dict:
    """Compare against all active Config DB records for the proposed target."""
    db = _db()
    table_plan = payload["table_plan"]
    target = table_plan["table"]
    matches = db.list_target_table_records(
        workspace_id=target["fabric_workspace_id"],
        lakehouse_id=target["fabric_lakehouse_id"],
        schema_name=target["fabric_lakehouse_schema"],
        table_name=target["fabric_table_name"],
        exclude_plan_guid=UUID(payload["plan_guid"]),
    )
    definitions = []
    for match in matches:
        columns = [column.model_dump(mode="json")
                   for column in db.list_source_table_columns(match.guid)]
        definition = {"source_table_guid": str(match.guid),
            "plan_guid": str(match.plan_guid),
            "provisioning_status": match.provisioning_status,
            "comparison": compare_columns(table_plan["columns"], columns,
                                          detailed=payload.get("review_replication", False))}
        if payload.get("review_replication"):
            for field in ("connection_name", "source_system_type", "source_object_type",
                          "source_schema_name", "source_table_name"):
                if str(getattr(match, field, None) or "").upper() != str(target.get(field) or "").upper():
                    raise ValueError("Existing target belongs to a different source; choose another Fabric target")
            config = db.get_replication_config(match.guid)
            definition["replication_comparison"] = compare_replication(
                table_plan["replication"], config.model_dump(mode="json") if config else None)
            definition["same"] = (definition["comparison"]["same"]
                                  and definition["replication_comparison"]["same"])
        definitions.append(definition)
    if payload.get("review_replication") and len(definitions) > 1:
        raise ValueError("Multiple active configurations exist for this target; reconcile them before changing it")
    return {"target": {"workspace_id": target["fabric_workspace_id"],
                       "lakehouse_id": target["fabric_lakehouse_id"],
                       "schema": target["fabric_lakehouse_schema"],
                       "table": target["fabric_table_name"]},
            "definitions": definitions,
            "basis": "CONFIG_DB_RECORDED_SCHEMA",
            "physical_schema_verified": False}


@activity.defn(name="record_target_change_request")
def record_target_change_request(payload: dict) -> None:
    _db().record_target_change_request(
        UUID(payload["plan_guid"]), expected_version=payload["version"],
        actor=payload["actor"], decision=payload["decision"],
        target_guid=UUID(payload["target_guid"]), review=payload["review"],
        proposed_plan=payload["proposed_plan"])


@activity.defn(name="validate_target_change_run")
def validate_target_change_run(payload: dict) -> dict:
    plan = _db().get_migration_plan(UUID(payload["plan_guid"]))
    if plan is None or plan.status != PlanStatus.CHANGE_REQUESTED:
        raise ValueError("approved target change was not found or is no longer pending")
    try:
        change = json.loads(plan.notes or "")["target_change"]
        target = change["review"]["target"]
        decision = change["decision"]
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError("approved target change details are invalid") from exc
    if decision not in {"REVISE_PLANNED", "ALTER_FUTURE", "ALTER_BACKFILL", "REPLACE_FULL"}:
        raise ValueError("approved target change decision is invalid")
    if (str(target.get("workspace_id", "")).lower() != payload["fabric_workspace_id"].lower()
            or str(target.get("lakehouse_id", "")).lower() != payload["fabric_lakehouse_id"].lower()):
        raise ValueError("Fabric target differs from the approved change")
    return {"decision": decision, "plan_version": plan.plan_version}


@activity.defn(name="finish_target_change")
def finish_target_change(payload: dict) -> None:
    result = payload["result"]
    objects = result.get("objects")
    if (result.get("status") != "SUCCESS" or result.get("tables_processed") != 1
            or result.get("views_processed") != 0 or not isinstance(objects, list)
            or len(objects) != 1 or objects[0].get("object_type") != "TABLE"
            or objects[0].get("status") != "SUCCESS"):
        raise ValueError("notebook did not confirm one successful target change")
    db = _db()
    guid = UUID(payload["plan_guid"])
    plan = db.get_migration_plan(guid)
    if plan is None:
        raise ValueError("target change plan was not found")
    if plan.status == PlanStatus.CHANGE_APPLIED:
        return
    db.update_migration_plan_status(guid, expected_status=PlanStatus.CHANGE_REQUESTED,
                                    new_status=PlanStatus.CHANGE_APPLIED,
                                    expected_plan_version=payload["version"])


@activity.defn(name="start_batch")
def start_batch(payload: dict) -> str:
    batch_id = uuid5(UUID(payload["plan_guid"]), f"batch:{payload['batch_type']}")
    db = _db()
    db.start_batch_run(BatchRunCreate(
        batch_type=payload["batch_type"], plan_guid=UUID(payload["plan_guid"]),
        trigger_type="TEMPORAL", temporal_workflow_id=payload["workflow_id"],
        temporal_run_id=payload["run_id"]), batch_run_id=batch_id)
    tables = db.list_source_tables_for_plan(UUID(payload["plan_guid"]))
    for table in tables:
        old = None
        if payload["batch_type"] == "REPLICATION":
            state = db.get_replication_state(table.guid)
            old = state.last_watermark_value if state else None
        db.start_batch_object_run(BatchObjectRunCreate(
            batch_run_id=batch_id, object_type="TABLE", object_guid=table.guid,
            object_name=table.source_table_name, old_watermark_value=old),
            batch_object_run_id=uuid5(batch_id, f"table:{table.guid}"))
    if payload["batch_type"] == "PROVISIONING":
        for view in db.list_fabric_views_for_plan(UUID(payload["plan_guid"])):
            db.start_batch_object_run(BatchObjectRunCreate(
                batch_run_id=batch_id, object_type="VIEW", object_guid=view.view_guid,
                object_name=view.view_name),
                batch_object_run_id=uuid5(batch_id, f"view:{view.view_guid}"))
    return str(batch_id)


@activity.defn(name="record_batch_job")
def record_batch_job(payload: dict) -> None:
    db = _db()
    batch_id = UUID(payload["batch_run_id"])
    db.set_batch_fabric_job_id(batch_id, payload["job_id"])
    for item in db.list_batch_object_runs(batch_id):
        db.set_batch_object_notebook_run_id(item.batch_object_run_id, payload["job_id"])


@activity.defn(name="finish_provisioning")
def finish_provisioning(payload: dict) -> None:
    db = _db()
    plan_guid = UUID(payload["plan_guid"])
    tables = db.list_source_tables_for_plan(plan_guid)
    views = db.list_fabric_views_for_plan(plan_guid)
    result = payload["result"]
    if result.get("tables_processed") is not None and result["tables_processed"] != len(tables):
        raise ValueError("Provisioning notebook table count differs from approved plan")
    if result.get("views_processed") is not None and result["views_processed"] != len(views):
        raise ValueError("Provisioning notebook view count differs from approved plan")
    for table in tables:
        db.update_source_table_provisioning_status(table.guid, status="PROVISIONED",
                                                   run_id=payload["job_id"])
    for view in views:
        db.update_view_provisioning_status(view.view_guid, status="PROVISIONED",
                                           run_id=payload["job_id"])
    batch_id = UUID(payload["batch_run_id"])
    for item in db.list_batch_object_runs(batch_id):
        if item.status == "RUNNING":
            db.complete_batch_object_run(item.batch_object_run_id)
        elif item.status != "SUCCEEDED":
            raise ValueError("provisioning object run is not successful")
    batch = db.get_batch_run(batch_id)
    if batch and batch.status == "RUNNING":
        db.complete_batch_run(batch_id, succeeded_objects=len(tables) + len(views),
                              failed_objects=0, total_objects=len(tables) + len(views))
    elif batch is None or batch.status != "SUCCEEDED":
        raise ValueError("provisioning batch is not successful")
    plan = db.get_migration_plan(plan_guid)
    if plan and plan.status != PlanStatus.PROVISIONED:
        db.update_migration_plan_status(plan_guid, expected_status=PlanStatus.PROVISIONING,
                                        new_status=PlanStatus.PROVISIONED,
                                        expected_plan_version=payload["version"])


@activity.defn(name="enable_replication")
def enable_replication(payload: dict) -> None:
    db = _db()
    for table in db.list_source_tables_for_plan(UUID(payload["plan_guid"])):
        db.set_replication_enabled(table.guid, True)
        state = db.get_replication_state(table.guid)
        if state and state.status != "RUNNING":
            db.mark_replication_started(table.guid)


@activity.defn(name="activate_replication_config")
def activate_replication_config(payload: dict) -> None:
    """Make provisioned tables eligible for the separately scheduled loader."""
    db = _db()
    for table in db.list_source_tables_for_plan(UUID(payload["plan_guid"])):
        if table.provisioning_status != "PROVISIONED":
            raise ValueError("Cannot activate replication before table provisioning")
        db.set_replication_enabled(table.guid, True)


@activity.defn(name="record_pipeline_job")
def record_pipeline_job(payload: dict) -> None:
    db = _db()
    batch_id = UUID(payload["batch_run_id"])
    db.set_batch_fabric_job_id(batch_id, payload["job_id"])
    for item in db.list_batch_object_runs(batch_id):
        db.set_batch_object_pipeline_run_id(item.batch_object_run_id, payload["job_id"])
    for table in db.list_source_tables_for_plan(UUID(payload["plan_guid"])):
        db.set_replication_pipeline_run_id(table.guid, payload["job_id"])


@activity.defn(name="finish_replication")
def finish_replication(payload: dict) -> None:
    db = _db()
    tables = db.list_source_tables_for_plan(UUID(payload["plan_guid"]))
    for table in tables:
        state = db.get_replication_state(table.guid)
        if state and state.status == "RUNNING":
            db.mark_replication_succeeded(table.guid, batch_run_id=UUID(payload["batch_run_id"]))
        elif state is None or state.status != "SUCCEEDED":
            raise ValueError("replication state is not successful")
    batch_id = UUID(payload["batch_run_id"])
    for item in db.list_batch_object_runs(batch_id):
        if item.status == "RUNNING":
            db.complete_batch_object_run(item.batch_object_run_id)
        elif item.status != "SUCCEEDED":
            raise ValueError("replication object run is not successful")
    batch = db.get_batch_run(batch_id)
    if batch and batch.status == "RUNNING":
        db.complete_batch_run(batch_id, succeeded_objects=len(tables),
                              failed_objects=0, total_objects=len(tables))
    elif batch is None or batch.status != "SUCCEEDED":
        raise ValueError("replication batch is not successful")


@activity.defn(name="fail_migration")
def fail_migration(payload: dict) -> None:
    db = _db()
    guid = UUID(payload["plan_guid"])
    phase = payload["phase"]
    message = payload["message"][:1000]
    batch_id = payload.get("batch_run_id")
    if batch_id:
        for item in db.list_batch_object_runs(UUID(batch_id)):
            if item.status == "RUNNING":
                if payload.get("cancelled"):
                    db.cancel_batch_object_run(item.batch_object_run_id, reason=message)
                else:
                    db.fail_batch_object_run(item.batch_object_run_id, message)
        try:
            if payload.get("cancelled"):
                db.cancel_batch_run(UUID(batch_id), reason=message)
            else:
                db.fail_batch_run(UUID(batch_id), message)
        except ValueError:
            pass
    if phase in {"PROVISIONING", "WAITING_FOR_PROVISIONING"}:
        for table in db.list_source_tables_for_plan(guid):
            db.update_source_table_provisioning_status(table.guid, status="FAILED", error=message)
    if phase in {"REPLICATING", "WAITING_FOR_REPLICATION"}:
        for table in db.list_source_tables_for_plan(guid):
            try:
                db.mark_replication_failed(table.guid, message)
            except ValueError:
                pass
    for attempt in range(3):
        plan = db.get_migration_plan(guid)
        if not plan or plan.status in {PlanStatus.FAILED, PlanStatus.CANCELLED}:
            break
        try:
            db.update_migration_plan_status(guid, expected_status=plan.status,
                new_status=PlanStatus.CANCELLED if payload.get("cancelled") else PlanStatus.FAILED,
                expected_plan_version=plan.plan_version)
            break
        except ValueError:
            # A timed-out synchronous save can finish between this read and update.
            if attempt == 2:
                raise


ACTIVITIES = [create_migration_plan, set_temporal_ids, transition_plan,
              approve_runtime_plan, persist_runtime_plan, review_existing_target,
              record_target_change_request, validate_target_change_run,
              finish_target_change, start_batch,
              record_batch_job, finish_provisioning, enable_replication,
              activate_replication_config, record_pipeline_job, finish_replication, fail_migration]
