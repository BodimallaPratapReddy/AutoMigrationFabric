"""Record partial provisioning and commit reviewed SQL revisions atomically."""
import hashlib
import json
from datetime import datetime, timezone
from uuid import UUID

from temporalio import activity
from thirdparty.configdb.repository import ConfigDBRepository
from .view_correction import validate_correction


@activity.defn
def record_view_attempt(payload: dict) -> dict:
    db = ConfigDBRepository()
    guid, batch_id = UUID(payload["plan_guid"]), UUID(payload["batch_run_id"])
    tables = db.list_source_tables_for_plan(guid)
    views = db.list_fabric_views_for_plan(guid)
    targets = {("TABLE", f"{t.fabric_lakehouse_schema}.{t.fabric_table_name}".casefold()): t for t in tables}
    targets.update({("VIEW", f"{v.fabric_schema_name}.{v.view_name}".casefold()): v for v in views})
    runs = db.list_batch_object_runs(batch_id)
    by_guid = {str(r.object_guid): r for r in runs}
    reports = payload["result"].get("objects") or []
    expected = {key for key, obj in targets.items()
                if str(obj.guid if key[0] == "TABLE" else obj.view_guid) in by_guid}
    actual = [(r["object_type"], r["name"].casefold()) for r in reports]
    if len(set(actual)) != len(actual) or set(actual) != expected:
        raise ValueError("Notebook report does not match this attempt's objects")
    for report, key in zip(reports, actual):
        obj = targets[key]
        obj_guid = obj.guid if key[0] == "TABLE" else obj.view_guid
        run = by_guid[str(obj_guid)]
        success = report["status"] == "SUCCESS"
        status = "PROVISIONED" if success else "FAILED"
        update = db.update_source_table_provisioning_status if key[0] == "TABLE" else db.update_view_provisioning_status
        update(obj_guid, status=status, run_id=payload["job_id"], error=None if success else (report.get("message") or "Skipped after failed prerequisite")[:1000])
        if run.status == "RUNNING":
            if success:
                db.complete_batch_object_run(run.batch_object_run_id)
            else:
                db.fail_batch_object_run(run.batch_object_run_id, (report.get("message") or "Dependency failed")[:1000])
        elif run.status != ("SUCCEEDED" if success else "FAILED"):
            raise ValueError("Attempt result conflicts with recorded object run")
    succeeded = sum(r["status"] == "SUCCESS" for r in reports)
    batch = db.get_batch_run(batch_id)
    if batch and batch.status == "RUNNING":
        db.complete_batch_run(batch_id, succeeded_objects=succeeded,
                              failed_objects=len(reports) - succeeded, total_objects=len(reports))
    elif not batch or batch.status != ("SUCCEEDED" if succeeded == len(reports) else "FAILED"):
        raise ValueError("Attempt result conflicts with recorded batch")
    return {"tables_processed": len(tables), "views_processed": len(views),
            "all_provisioned": all(t.provisioning_status == "PROVISIONED" for t in db.list_source_tables_for_plan(guid))
            and all(v.provisioning_status == "PROVISIONED" for v in db.list_fabric_views_for_plan(guid))}


@activity.defn
def persist_view_correction(payload: dict) -> int:
    changes = validate_correction(payload["previous"], payload["proposed"], payload["eligible"])
    if not str(payload.get("actor") or "").strip():
        raise ValueError("SQL correction requires a reviewer")
    version = payload["version"]
    audit = {"version": version + 1, "actor": payload["actor"], "changes": changes,
             "failure": payload["failure"], "previous_agent_plan": payload["previous"]["review"]["agent_plan"],
             "agent_plan": payload["proposed"]["review"]["agent_plan"]}
    fingerprint = hashlib.sha256(json.dumps(audit, sort_keys=True).encode()).hexdigest()
    db = ConfigDBRepository()
    with db.transaction() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT PlanVersion, Status, Notes FROM bronze_replication.MigrationPlans WITH (UPDLOCK, HOLDLOCK) WHERE PlanGUID = ?", (payload["plan_guid"],))
            row = cursor.fetchone()
            if row is None:
                raise ValueError("Plan no longer exists")
            try:
                notes = json.loads(row[2] or "{}")
                if not isinstance(notes, dict):
                    notes = {"previous_notes": row[2]}
            except ValueError:
                notes = {"previous_notes": row[2]}
            revisions = notes.setdefault("view_sql_revisions", [])
            if row[0] == version + 1 and revisions and revisions[-1].get("fingerprint") == fingerprint:
                return version + 1
            if row[0] != version or row[1] != "PROVISIONING":
                raise ValueError("Plan changed while SQL correction was reviewed")
            for change in changes:
                schema, name = change["name"].split(".")
                cursor.execute("UPDATE bronze_replication.FabricViews SET ViewSQL = ?, LogicDescription = ?, ProvisioningStatus = 'PENDING', ProvisioningError = NULL WHERE PlanGUID = ? AND FabricSchemaName = ? AND ViewName = ? AND ViewSQL = ? AND ProvisioningStatus IN ('FAILED','SKIPPED','PENDING')",
                    (change["after"], change["logic_description"], payload["plan_guid"], schema, name, change["before"]))
                if cursor.rowcount != 1:
                    raise ValueError("Failed view changed while SQL correction was reviewed")
            # Blocked dependents keep their original SQL and will be retried.
            cursor.execute("UPDATE bronze_replication.FabricViews SET ProvisioningStatus = 'PENDING', ProvisioningError = NULL WHERE PlanGUID = ? AND ProvisioningStatus IN ('FAILED','SKIPPED')", (payload["plan_guid"],))
            revisions.append({**audit, "approved_at": datetime.now(timezone.utc).isoformat(), "fingerprint": fingerprint})
            cursor.execute("UPDATE bronze_replication.MigrationPlans SET PlanVersion = ?, Status = 'READY_TO_PROVISION', ApprovedBy = ?, ApprovedTimestamp = SYSUTCDATETIME(), Notes = ?, UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ? AND PlanVersion = ? AND Status = 'PROVISIONING'",
                (version + 1, payload["actor"], json.dumps(notes), payload["plan_guid"], version))
            if cursor.rowcount != 1:
                raise ValueError("Plan changed during correction commit")
    return version + 1


ACTIVITIES = [record_view_attempt, persist_view_correction]
