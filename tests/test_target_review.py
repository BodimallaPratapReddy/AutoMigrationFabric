"""Target collisions must stop before a second runtime configuration is saved."""

import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.migration.target_review import compare_columns
from app.migration.workflows import OracleTableMigrationWorkflow, SAPTableMigrationWorkflow
from app.migration import config_activities
from thirdparty.configdb.repository import PlanStatus


def test_column_comparison_reports_add_remove_and_type_change():
    before = [{"column_name": "A", "fabric_data_type": "INT", "is_nullable": False},
              {"column_name": "B", "fabric_data_type": "STRING", "is_nullable": True}]
    after = [{"column_name": "A", "fabric_data_type": "BIGINT", "is_nullable": False},
             {"column_name": "C", "fabric_data_type": "DATE", "is_nullable": True}]
    diff = compare_columns(after, before)
    assert diff["same"] is False
    assert [item["name"] for item in diff["added"]] == ["C"]
    assert [item["name"] for item in diff["removed"]] == ["B"]
    assert diff["changed"][0]["after"]["fabric_type"] == "BIGINT"


def test_change_completion_requires_one_successful_table(monkeypatch):
    guid = uuid4()
    calls = []
    class FakeDB:
        def get_migration_plan(self, plan_guid):
            return SimpleNamespace(status=PlanStatus.CHANGE_REQUESTED, plan_version=1)
        def update_migration_plan_status(self, plan_guid, **kwargs):
            calls.append((plan_guid, kwargs))
    monkeypatch.setattr(config_activities, "_db", FakeDB)
    payload = {"plan_guid": str(guid), "version": 1,
               "result": {"status": "SUCCESS", "tables_processed": 1,
                          "views_processed": 0, "objects": [{"object_type": "TABLE", "status": "SUCCESS"}]}}
    config_activities.finish_target_change(payload)
    assert calls[0][0] == guid
    assert calls[0][1]["new_status"] == PlanStatus.CHANGE_APPLIED
    with pytest.raises(ValueError, match="one successful"):
        config_activities.finish_target_change({**payload, "result": {
            **payload["result"], "tables_processed": 0}})


def test_change_run_uses_approved_target_and_decision(monkeypatch):
    guid = uuid4()
    notes = json.dumps({"target_change": {"decision": "ALTER_BACKFILL", "review": {
        "target": {"workspace_id": "w", "lakehouse_id": "l"}}}})
    class FakeDB:
        def get_migration_plan(self, plan_guid):
            return SimpleNamespace(status=PlanStatus.CHANGE_REQUESTED,
                                   plan_version=2, notes=notes)
    monkeypatch.setattr(config_activities, "_db", FakeDB)
    payload = {"plan_guid": str(guid), "fabric_workspace_id": "w", "fabric_lakehouse_id": "l"}
    assert config_activities.validate_target_change_run(payload) == {
        "decision": "ALTER_BACKFILL", "plan_version": 2}
    with pytest.raises(ValueError, match="differs"):
        config_activities.validate_target_change_run({**payload, "fabric_lakehouse_id": "other"})


@pytest.mark.parametrize("same,provisioned,decision,notebook,expected", [
    (True, False, None, False, "REJECTED"),
    (False, False, "REVISE_PLANNED", False, "CHANGE_REQUESTED"),
    (False, True, "ALTER_BACKFILL", False, "CHANGE_REQUESTED"),
    (False, True, "REPLACE_FULL", False, "CHANGE_REQUESTED"),
    (False, True, "ALTER_FUTURE", True, "COMPLETED"),
    (False, True, "ALTER_BACKFILL", True, "COMPLETED"),
    (False, True, "REPLACE_FULL", True, "COMPLETED"),
])
@pytest.mark.parametrize("approach", ["ORACLE_TABLE", "SAP_TABLE"])
def test_existing_target_change_execution(same, provisioned, decision, notebook, expected, approach):
    async def exercise():
        calls = []

        def make(name, value=None):
            @activity.defn(name=name)
            async def run(payload):
                calls.append((name, payload))
                return value
            return run

        existing = str(uuid4())
        review = {"definitions": [{"source_table_guid": existing,
            "plan_guid": str(uuid4()),
            "provisioning_status": "PROVISIONED" if provisioned else "PENDING",
            "comparison": {"same": same, "added": [], "removed": [], "changed": []}}],
            "target": {"schema": "bronze", "table": "ORDERS"},
            "physical_schema_verified": False}
        oracle = approach == "ORACLE_TABLE"
        workflow_class = OracleTableMigrationWorkflow if oracle else SAPTableMigrationWorkflow
        activities = [
            make("create_migration_plan", str(uuid4())), make("set_temporal_ids"),
            make("validate_fabric", {"lakehouse_name": "Bronze"}),
            make("discover_oracle" if oracle else "discover_sap_table", {"table_plan": {"table": {}, "columns": [
                {"column_name": "ID", "fabric_data_type": "INT"}],
                "replication": {"primary_key_columns": ["ID"]}},
                "review": {"columns": [{"name": "ID", "fabric_type": "INT"}],
                           "watermark_candidates": []}}),
            make("transition_plan"), make("review_existing_target", review),
            make("validate_oracle_key", {"valid": True, "status": "CHECKED", "columns": ["ID"],
                                        "scope": "FULL_TABLE", "has_nulls": False, "has_duplicates": False}),
            make("record_target_change_request"), make("approve_runtime_plan"),
            make("persist_runtime_plan"),
            make("validate_target_change_run", {"decision": decision, "plan_version": 1}),
            make("submit_provisioning", {"job_instance_id": "change-job"}),
            make("get_provisioning_status", {"normalized_status": "SUCCEEDED"}),
            make("parse_provisioning", {"status": "SUCCESS", "tables_processed": 1,
                                         "views_processed": 0, "objects": [{"object_type": "TABLE",
                                         "status": "SUCCESS"}]}),
            make("finish_target_change"),
            make("fail_migration"),
        ]
        queue = f"target-{uuid4()}"
        async with await WorkflowEnvironment.start_local() as env:
            async with Worker(env.client, task_queue=queue,
                              workflows=[workflow_class], activities=activities):
                handle = await env.client.start_workflow(workflow_class.run,
                    {"migration_approach": approach, "source_connection_name": "ORACLE" if oracle else "SAP",
                     "source_object_name": "ORDERS", "source_schema_name": "APP",
                     "fabric_workspace_id": str(uuid4()), "fabric_lakehouse_id": str(uuid4()),
                     "fabric_schema_name": "bronze",
                     "provisioning_notebook_id": str(uuid4()) if notebook else None,
                     "replication_pipeline_id": None},
                    id=str(uuid4()), task_queue=queue)
                approvals = [
                    ("WAITING_FOR_COLUMN_MAPPING_APPROVAL" if oracle else "WAITING_FOR_PLAN_APPROVAL",
                     {"action": "approve_columns" if oracle else "approve_plan",
                        "actor": "reviewer", "columns": [{"name": "ID", "fabric_type": "INT"}]}),
                    ("WAITING_FOR_WATERMARK_APPROVAL", {"action": "approve_watermark",
                        "actor": "reviewer", "watermark_column": None}),
                ]
                if oracle:
                    approvals.append(("WAITING_FOR_DELETE_POLICY_APPROVAL", {
                        "action": "approve_delete_policy", "actor": "reviewer", "delete_policy": {"mode": "NONE"}}))
                for phase, command in approvals:
                    for _ in range(100):
                        if (await handle.query("get_state"))["phase"] == phase:
                            break
                        await asyncio.sleep(.05)
                    else:
                        raise AssertionError(f"workflow did not reach {phase}")
                    await handle.signal("command", command)
                if decision:
                    for _ in range(100):
                        if (await handle.query("get_state"))["phase"] == "WAITING_FOR_TARGET_CHANGE_APPROVAL":
                            break
                        await asyncio.sleep(.05)
                    await handle.signal("command", {"action": "approve_target_change",
                        "actor": "reviewer", "decision": decision})
                result = await handle.result()
                assert result["status"] == expected
                assert not any(name == "persist_runtime_plan" for name, _ in calls)
                assert any(name == "submit_provisioning" for name, _ in calls) is notebook
                assert any(name == "finish_target_change" for name, _ in calls) is notebook
                if decision:
                    saved = [payload for name, payload in calls
                             if name == "record_target_change_request"]
                    assert saved[0]["decision"] == decision

    asyncio.run(exercise())
