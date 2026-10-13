import asyncio
import copy
import json
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer

from app.migration.view_correction import repairable_views, validate_correction
from app.migration.workflow_view_correction import OpenCodeOracleViewCorrectionWorkflow
from test_oracle_view_integration import SOURCE, GRAPH, BUNDLE, adapt_view_bundle


def report(code="PARSE_SYNTAX_ERROR", tables=True):
    objects = ([dict(object_type="TABLE", name="bronze.T", status="SUCCESS", duration_seconds=0)] if tables else [])
    objects += [dict(object_type="VIEW", name="bronze.V_BASE", status="FAILED", duration_seconds=0, error_code=code),
                dict(object_type="VIEW", name="bronze.V_TOP", status="SKIPPED", duration_seconds=0)]
    return dict(status="FAILED", objects=objects, message="View SQL failed")


@pytest.mark.parametrize("code", ["AUTHENTICATION_FAILED", "TABLE_OR_VIEW_NOT_FOUND", "UNKNOWN", "TIMEOUT"])
def test_operational_failure_is_not_sent_to_planner(code):
    assert repairable_views(report(code)) == []


def test_failed_table_prevents_sql_repair():
    failure = report()
    failure["objects"][0]["status"] = "FAILED"
    assert repairable_views(failure) == []


@pytest.mark.parametrize("change", ["table", "successful_view", "destination", "dependency", "unchanged"])
def test_sql_repair_boundaries(change):
    original = adapt_view_bundle(json.dumps(BUNDLE), SOURCE, "Bronze")
    revised = copy.deepcopy(original)
    if change == "table": revised["tables"][0]["columns"][0]["fabric_data_type"] = "BIGINT"
    if change == "successful_view": revised["views"][1]["view"]["view_sql"] += " WHERE ID IS NOT NULL"
    if change == "destination": revised["views"][0]["view"]["view_name"] = "OTHER"
    if change == "dependency": revised["views"][0]["dependencies"] = []
    with pytest.raises(ValueError):
        validate_correction(original, revised, ["bronze.V_BASE"])


async def exercise(mode, workflow_type=OpenCodeOracleViewCorrectionWorkflow):
    original = adapt_view_bundle(json.dumps(BUNDLE), SOURCE, "Bronze")
    revised = copy.deepcopy(original)
    revised["views"][0]["view"]["view_sql"] = "SELECT ID FROM bronze.T"
    calls, prompts, commits, attempts = [], [], [], []
    parse_count = 0
    validate_count = 0
    def fake(name, value=None):
        @activity.defn(name=name)
        async def run(payload):
            nonlocal parse_count, validate_count
            calls.append(name)
            if name == "opencode_prompt": prompts.append(payload)
            if name == "validate_oracle_key": return dict(valid=True, status="CHECKED", scope="FULL_TABLE", columns=payload["columns"])
            if name == "opencode_validate":
                validate_count += 1
                if validate_count == 1: return original
                next_plan = copy.deepcopy(revised)
                # Further SQL revisions remain distinguishable for bounded retries.
                next_plan["views"][0]["view"]["view_sql"] += " " * validate_count
                return next_plan
            if name == "parse_provisioning":
                parse_count += 1
                if parse_count == 1 or mode == "exhaust":
                    raise ApplicationError("SQL failed", report("AUTHENTICATION_FAILED" if mode == "operational" else "PARSE_SYNTAX_ERROR", parse_count == 1),
                                           type="FabricProvisioningLogicalFailure", non_retryable=True)
                return dict(status="SUCCESS", objects=[dict(object_type="VIEW", name="bronze.V_BASE", status="SUCCESS", duration_seconds=0), dict(object_type="VIEW", name="bronze.V_TOP", status="SUCCESS", duration_seconds=0)])
            if name == "record_view_attempt": attempts.append(payload); return {"all_provisioned": payload["result"]["status"] == "SUCCESS"}
            if name == "persist_view_correction": commits.append(payload); return payload["version"] + 1
            return value
        return run
    activities = [fake("create_migration_plan", str(uuid4())), fake("set_temporal_ids"),
        fake("validate_fabric", {"lakehouse_name": "Bronze"}), fake("oracle_view_graph", GRAPH),
        fake("opencode_create", {"session_id": "ses_repair"}), fake("opencode_prompt"),
        fake("opencode_poll", {"status": "COMPLETED", "text": json.dumps(BUNDLE)}), fake("opencode_validate"),
        fake("transition_plan"), fake("check_oracle_view_targets"), fake("approve_runtime_plan"),
        fake("persist_runtime_plan"), fake("start_batch", str(uuid4())),
        fake("submit_provisioning", {"job_instance_id": "job"}), fake("record_batch_job"),
        fake("get_provisioning_status", {"normalized_status": "SUCCEEDED"}), fake("parse_provisioning"),
        fake("record_view_attempt"), fake("persist_view_correction"), fake("activate_replication_config"),
        fake("opencode_interrupt"), fake("fail_migration"), fake("validate_oracle_key")]
    async with await WorkflowEnvironment.start_local() as env:
        queue = str(uuid4())
        async with Worker(env.client, task_queue=queue, workflows=[workflow_type], activities=activities):
            handle = await env.client.start_workflow(workflow_type.run,
                {**SOURCE, "provisioning_notebook_id": str(uuid4())}, id=str(uuid4()), task_queue=queue)
            approved = set()
            for _ in range(600):
                state = await handle.query("get_state")
                if state.get("status") in {"COMPLETED", "FAILED", "CANCELLED"}: break
                phase = state.get("phase")
                if phase in {"WAITING_FOR_COLUMN_MAPPING_APPROVAL", "WAITING_FOR_WATERMARK_APPROVAL", "WAITING_FOR_DELETE_POLICY_APPROVAL"} and phase not in approved:
                    approved.add(phase)
                    review = state['review']
                    command = dict(actor='Test reviewer')
                    if phase == 'WAITING_FOR_COLUMN_MAPPING_APPROVAL':
                        command.update(action='approve_columns', columns=[dict(name=c['name'], fabric_type=c['fabric_type']) for c in review['columns']])
                    elif phase == 'WAITING_FOR_WATERMARK_APPROVAL': command.update(action='approve_watermark', watermark_column=None)
                    else: command.update(action='approve_delete_policy', delete_policy={'mode':'NONE'})
                    await handle.signal('command', command)
                if state.get("phase") == "WAITING_FOR_PLAN_APPROVAL":
                    version = state.get("view_correction_attempt", 0)
                    if version not in approved:
                        approved.add(version)
                        action = "cancel" if version and mode == "cancel" else "reject_plan" if version and mode == "reject" else "approve_plan"
                        await handle.signal("command", {"action": action, "actor": "Test reviewer"})
                await asyncio.sleep(.025)
            else: pytest.fail("Workflow did not finish within test budget")
            state = await handle.result()
            if mode == "success":
                assert state["status"] == "COMPLETED" and state["plan_version"] == 2
                assert len(commits) == 1 and len(attempts) == 2
                assert [p.get("revision", 0) for p in prompts] == [0, 1]
                assert prompts[1]["session_id"] == prompts[0]["session_id"]
                assert prompts[1]["input"]["view_correction"]["failure"] == report()
                assert calls.count("persist_runtime_plan") == 1
            elif mode == "exhaust":
                assert state["status"] == "FAILED" and len(commits) == 2 and parse_count == 3
                assert [p.get("revision", 0) for p in prompts] == [0, 1, 2]
            else:
                assert state["status"] == ("CANCELLED" if mode == "cancel" else "FAILED")
                assert not commits and parse_count == 1
                assert "activate_replication_config" not in calls
                if mode == "operational": assert len(prompts) == 1
            await Replayer(workflows=[workflow_type]).replay_workflow(await handle.fetch_history())


@pytest.mark.parametrize("mode", ["success", "operational", "exhaust", "reject", "cancel"])
def test_durable_sql_correction_and_replay(mode):
    asyncio.run(exercise(mode))


def test_replication_review_retains_sql_repair_and_replays():
    from app.migration.workflow_view_replication import OpenCodeOracleViewReplicationWorkflow
    asyncio.run(exercise('success', OpenCodeOracleViewReplicationWorkflow))


def test_record_partial_success_preserves_tables_and_closes_failed_batch(monkeypatch):
    from types import SimpleNamespace as NS
    from app.migration import view_correction_activities as module
    table = NS(guid=uuid4(), fabric_lakehouse_schema="bronze", fabric_table_name="T", provisioning_status="PENDING")
    views = [NS(view_guid=uuid4(), fabric_schema_name="bronze", view_name=n, provisioning_status="PENDING") for n in ["V_BASE", "V_TOP"]]
    runs = [NS(object_guid=g, batch_object_run_id=uuid4(), status="RUNNING") for g in [table.guid, *(v.view_guid for v in views)]]
    class DB:
        def list_source_tables_for_plan(self, _): return [table]
        def list_fabric_views_for_plan(self, _): return views
        def list_batch_object_runs(self, _): return runs
        def update_source_table_provisioning_status(self, _, **kw): table.provisioning_status = kw["status"]
        def update_view_provisioning_status(self, guid, **kw): next(v for v in views if v.view_guid == guid).provisioning_status = kw["status"]
        def complete_batch_object_run(self, guid): next(r for r in runs if r.batch_object_run_id == guid).status = "SUCCEEDED"
        def fail_batch_object_run(self, guid, _): next(r for r in runs if r.batch_object_run_id == guid).status = "FAILED"
        def get_batch_run(self, _): return NS(status=self.status)
        def complete_batch_run(self, _, **counts): self.status = "FAILED"; self.counts = counts
        status = "RUNNING"
    db = DB()
    monkeypatch.setattr(module, "ConfigDBRepository", lambda: db)
    payload = dict(plan_guid=str(uuid4()), batch_run_id=str(uuid4()), job_id="job", result=report())
    for _ in range(2):
        assert module.record_view_attempt(payload)["all_provisioned"] is False
    assert table.provisioning_status == "PROVISIONED"
    assert [v.provisioning_status for v in views] == ["FAILED", "FAILED"]
    assert db.counts == dict(succeeded_objects=1, failed_objects=2, total_objects=3)
    payload["result"]["objects"].pop()
    with pytest.raises(ValueError, match="does not match"):
        module.record_view_attempt(payload)


def test_correction_commit_is_atomic_versioned_and_idempotent(monkeypatch):
    from contextlib import contextmanager
    from app.migration import view_correction_activities as module
    original = adapt_view_bundle(json.dumps(BUNDLE), SOURCE, "Bronze")
    revised = copy.deepcopy(original)
    revised["views"][0]["view"]["view_sql"] = "SELECT ID FROM bronze.T"
    class Cursor:
        row = [1, "PROVISIONING", "Existing audit note"]
        writes = []
        rowcount = 1
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def fetchone(self): return self.row
        def execute(self, sql, args):
            if sql.startswith("UPDATE"):
                self.writes.append((sql, args))
            if sql.startswith("UPDATE bronze_replication.MigrationPlans"):
                self.row = [args[0], "READY_TO_PROVISION", args[2]]
    cursor = Cursor()
    class DB:
        @contextmanager
        def transaction(self): yield self
        def cursor(self): return cursor
    monkeypatch.setattr(module, "ConfigDBRepository", DB)
    payload = dict(previous=original, proposed=revised, eligible=["bronze.V_BASE", "bronze.V_TOP"],
                   actor="Reviewer", version=1, plan_guid=str(uuid4()), failure=report())
    assert module.persist_view_correction(payload) == 2
    write_count = len(cursor.writes)
    assert module.persist_view_correction(payload) == 2
    assert len(cursor.writes) == write_count
    notes = json.loads(cursor.row[2])
    assert notes["previous_notes"] == "Existing audit note"
    assert notes["view_sql_revisions"][0]["approved_at"]
    assert notes["view_sql_revisions"][0]["changes"][0]["before"] == original["views"][0]["view"]["view_sql"]
    assert not any("SourceTables" in sql or "SourceTableColumns" in sql or "INSERT" in sql for sql, _ in cursor.writes)
    with pytest.raises(ValueError, match="changed"):
        module.persist_view_correction({**payload, "actor": "Different reviewer"})


def test_retry_batch_skips_provisioned_objects_and_uses_versioned_identity(monkeypatch):
    from types import SimpleNamespace as NS
    from app.migration import config_activities as module
    table = NS(guid=uuid4(), source_table_name="T", provisioning_status="PROVISIONED")
    views = [NS(view_guid=uuid4(), view_name=n, provisioning_status=s) for n, s in [("V_OK", "PROVISIONED"), ("V_BAD", "PENDING")]]
    class DB:
        batches = []
        objects = []
        def start_batch_run(self, _, *, batch_run_id): self.batches.append(batch_run_id)
        def list_source_tables_for_plan(self, _): return [table]
        def list_fabric_views_for_plan(self, _): return views
        def start_batch_object_run(self, run, **_): self.objects.append(run)
    db = DB()
    monkeypatch.setattr(module, "_db", lambda: db)
    payload = dict(plan_guid=str(uuid4()), workflow_id="wf", run_id="run", batch_type="PROVISIONING", pending_only=True, attempt_version=2)
    first = module.start_batch(payload)
    assert module.start_batch(payload) == first
    assert module.start_batch({**payload, "attempt_version": 3}) != first
    assert all(r.object_guid == views[1].view_guid and r.object_type == "VIEW" for r in db.objects)
