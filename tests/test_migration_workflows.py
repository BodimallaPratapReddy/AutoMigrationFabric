"""Exercise Temporal waits, approval, and durable Fabric polling without live adapters."""

import asyncio
from uuid import uuid4

import pytest
from pydantic import ValidationError
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer

from app.migration.contracts import MigrationInput
from app.migration.workflows import (
    OracleTableMigrationWorkflow, SAPDataSourceRebuildWorkflow,
    SAPODPDataSourceMigrationWorkflow, SAPTableMigrationWorkflow,
)


def _activity(name, fn):
    async def run(payload):
        return fn(payload)
    return activity.defn(name=name)(run)


INPUT = {
    "migration_approach": "ORACLE_TABLE", "source_connection_name": "ORACLE-TEST",
    "source_object_name": "ORDERS", "source_schema_name": "APP",
    "fabric_workspace_id": "00000000-0000-0000-0000-000000000001",
    "fabric_lakehouse_id": "00000000-0000-0000-0000-000000000002",
    "fabric_schema_name": "bronze",
    "provisioning_notebook_id": "00000000-0000-0000-0000-000000000003",
    "replication_pipeline_id": "00000000-0000-0000-0000-000000000004",
}


def test_fabric_job_ids_are_optional():
    values = {key: value for key, value in INPUT.items()
              if key not in {"provisioning_notebook_id", "replication_pipeline_id"}}
    parsed = MigrationInput.model_validate(values)
    assert parsed.provisioning_notebook_id is None
    assert parsed.replication_pipeline_id is None


@pytest.mark.parametrize("field", ["fabric_workspace_id", "fabric_lakehouse_id",
                                  "provisioning_notebook_id", "replication_pipeline_id"])
def test_fabric_ids_reject_names_before_starting_workflow(field):
    with pytest.raises(ValidationError, match="Fabric ID must be a valid UUID"):
        MigrationInput.model_validate({**INPUT, field: "notebook_1"})


async def _exercise_plan_only(watermark: str | None = None, *, indexed: bool = True,
                              edit_again: bool = False, missing_key: bool = False,
                              manual_key: bool = True, delete_policy: dict | None = None) -> None:
    calls = []
    persisted = []
    guid = str(uuid4())

    def respond(name, result=None):
        def called(payload):
            calls.append(name)
            return result
        return _activity(name, called)

    def persist(payload):
        calls.append("persist_runtime_plan")
        persisted.append(payload)

    activities = [
        respond("create_migration_plan", guid), respond("set_temporal_ids"),
        respond("validate_fabric", {"lakehouse_name": "Bronze"}),
        respond("discover_oracle", {"table_plan": {"table": {}, "columns": [
                         {"column_name": "ORDER_ID", "fabric_data_type": "STRING"}],
                         "replication": {"primary_key_columns": None if missing_key else ["ORDER_ID"],
                                         "incremental_method": "FULL",
                                         "watermark_column": None,
                                         "watermark_column_data_type": None,
                                         "watermark_index_name": None}},
                         "review": {"source": "APP.ORDERS", "columns": [{
                             "name": "ORDER_ID", "source_type": "NUMBER",
                             "fabric_type": "STRING", "warning": None}],
                             "watermark_candidates": [{
                                 "column_name": "LAST_UPDATED_DATE", "data_type": "DATE",
                                 "index_details": ([{"index_name": "IX_UPDATED", "column_position": 1}]
                                                   if indexed else [])}]}}),
        respond("transition_plan"), respond("approve_runtime_plan"),
        respond("validate_oracle_key", {"valid": True, "status": "CHECKED", "columns": ["ORDER_ID"],
                                       "scope": "FULL_TABLE", "has_nulls": False, "has_duplicates": False}),
        respond("review_existing_target", {"definitions": []}),
        _activity("persist_runtime_plan", persist),
        respond("submit_provisioning"), respond("submit_replication"),
    ]
    queue = f"migration-test-{uuid4()}"
    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(environment.client, task_queue=queue,
                          workflows=[OracleTableMigrationWorkflow], activities=activities):
            payload = {**INPUT, "provisioning_notebook_id": None,
                       "replication_pipeline_id": None}
            handle = await environment.client.start_workflow(
                OracleTableMigrationWorkflow.run, payload, id=str(uuid4()), task_queue=queue)
            await _wait_for_phase(handle, "WAITING_FOR_COLUMN_MAPPING_APPROVAL")
            await handle.signal("command", {"action": "approve_columns", "actor": "reviewer",
                                            "columns": [{"name": "ORDER_ID", "fabric_type": "DECIMAL(18,0)"}]})
            if missing_key:
                await _wait_for_phase(handle, "WAITING_FOR_PRIMARY_KEY_APPROVAL")
                assert persisted == []
                await handle.signal("command", {"action": "approve_primary_key", "actor": "reviewer",
                    "primary_key_columns": ["ORDER_ID"] if manual_key else []})
            await _wait_for_phase(handle, "WAITING_FOR_WATERMARK_APPROVAL")
            assert persisted == []
            if edit_again:
                await handle.signal("command", {"action": "edit_columns"})
                await _wait_for_phase(handle, "WAITING_FOR_COLUMN_MAPPING_APPROVAL")
                await handle.signal("command", {"action": "approve_columns", "actor": "reviewer",
                                                "columns": [{"name": "ORDER_ID", "fabric_type": "BIGINT"}]})
                await _wait_for_phase(handle, "WAITING_FOR_WATERMARK_APPROVAL")
                assert persisted == []
            await handle.signal("command", {"action": "approve_watermark", "actor": "reviewer",
                                            "watermark_column": watermark})
            await _wait_for_phase(handle, "WAITING_FOR_DELETE_POLICY_APPROVAL")
            assert persisted == []
            await handle.signal("command", {"action": "approve_delete_policy", "actor": "reviewer",
                                            "delete_policy": delete_policy or {"mode": "NONE"}})
            result = await handle.result()
            assert persisted[0]["tables"][0]["replication"]["delete_policy"]["mode"] == (
                delete_policy or {"mode": "NONE"})["mode"]
            if delete_policy:
                assert persisted[0]["tables"][0]["replication"]["key_validation"]["valid"] is True
                await Replayer(workflows=[OracleTableMigrationWorkflow]).replay_workflow(await handle.fetch_history())
            if missing_key:
                assert result["review"]["primary_key_source"] == ("USER" if manual_key else "NONE")
                table = persisted[0]["tables"][0]
                assert table["replication"]["primary_key_columns"] == (["ORDER_ID"] if manual_key else None)
                assert table["columns"][0]["is_primary_key"] is manual_key
                await Replayer(workflows=[OracleTableMigrationWorkflow]).replay_workflow(
                    await handle.fetch_history())
            assert result["status"] == "PLANNED"
            assert result["phase"] == "READY_TO_PROVISION"
            assert "persist_runtime_plan" in calls
            assert "submit_provisioning" not in calls
            assert "submit_replication" not in calls
            assert persisted[0]["tables"][0]["columns"][0]["fabric_data_type"] == (
                "BIGINT" if edit_again else "DECIMAL(18,0)")
            if watermark:
                replication = persisted[0]["tables"][0]["replication"]
                assert replication["incremental_method"] == "WATERMARK"
                assert replication["watermark_column"] == watermark
                assert replication["watermark_column_data_type"] == "DATE"
                assert replication["watermark_index_name"] == ("IX_UPDATED" if indexed else None)
                assert replication["write_strategy"] == "UPSERT"
            else:
                replication = persisted[0]["tables"][0]["replication"]
                assert replication["incremental_method"] == "FULL"
                assert replication["watermark_column"] is None
                assert replication["watermark_column_data_type"] is None
                assert replication["watermark_index_name"] is None


def test_plan_only_skips_fabric_jobs():
    asyncio.run(_exercise_plan_only())


def test_reconciliation_configuration_is_persisted_without_loading_and_replays():
    asyncio.run(_exercise_plan_only("LAST_UPDATED_DATE", delete_policy={
        "mode": "RECONCILE", "behavior": "MARK", "reconcile_interval_minutes": 1440}))


def test_manual_primary_key_is_persisted_and_history_replays():
    asyncio.run(_exercise_plan_only("LAST_UPDATED_DATE", missing_key=True))


def test_explicit_no_primary_key_is_persisted_as_full_load():
    asyncio.run(_exercise_plan_only(missing_key=True, manual_key=False))


def test_oracle_watermark_choice_is_persisted_with_index():
    asyncio.run(_exercise_plan_only("LAST_UPDATED_DATE"))


def test_oracle_watermark_without_index_stores_null_index_name():
    asyncio.run(_exercise_plan_only("LAST_UPDATED_DATE", indexed=False))


def test_oracle_can_revisit_column_mappings_before_final_approval():
    asyncio.run(_exercise_plan_only(edit_again=True))


async def _wait_for_phase(handle, phase):
    for _ in range(100):
        state = await handle.query("get_state")
        if state.get("phase") == phase:
            return state
        await asyncio.sleep(.05)
    raise AssertionError(f"Workflow did not reach {phase}: {state}")


async def _wait_for_review_version(handle, phase, version):
    for _ in range(100):
        state = await handle.query("get_state")
        if state.get("phase") == phase and state.get("review", {}).get("version") == version:
            return state
        await asyncio.sleep(.05)
    raise AssertionError(f"Workflow did not reach {phase} review version {version}: {state}")


async def _exercise_standard(action: str, workflow_class=OracleTableMigrationWorkflow,
                             discovery_name="discover_oracle", approach="ORACLE_TABLE",
                             provisioning_status="SUCCEEDED") -> None:
    calls = []
    plan_guid = str(uuid4())
    cancel_sent = False

    def response(name, result=None):
        def called(payload):
            calls.append((name, payload))
            return result
        return _activity(name, called)

    def provision_status(payload):
        calls.append(("get_provisioning_status", payload))
        status = ("CANCELLED" if cancel_sent else "RUNNING") if action == "cancel_running" else provisioning_status
        return {"normalized_status": status, "retry_after_seconds": 2}

    def cancel_provision(payload):
        nonlocal cancel_sent
        calls.append(("cancel_provisioning", payload))
        cancel_sent = True
        return {}

    activities = [
        response("create_migration_plan", plan_guid), response("set_temporal_ids"),
        response("validate_fabric", {"lakehouse_name": "Bronze"}),
        response(discovery_name, {"table_plan": {"table": {}, "columns": [
                     {"column_name": "ORDER_ID", "fabric_data_type": "STRING"}],
                     "replication": {}},
                     "review": {"source": "APP.ORDERS", "columns": [{
                         "name": "ORDER_ID", "source_type": "NUMBER",
                         "fabric_type": "STRING", "warning": None}]}}),
        response("transition_plan"), response("approve_runtime_plan"),
        response("review_existing_target", {"definitions": []}),
        response("persist_runtime_plan"), response("start_batch", str(uuid4())),
        response("submit_provisioning", {"job_instance_id": "notebook-job"}),
        response("record_batch_job"), _activity("get_provisioning_status", provision_status),
        _activity("cancel_provisioning", cancel_provision),
        response("parse_provisioning", {"status": "SUCCESS"}),
        response("finish_provisioning"), response("activate_replication_config"),
        response("enable_replication"),
        response("submit_replication", {"job_instance_id": "pipeline-job"}),
        response("record_pipeline_job"), response("get_replication_status",
            {"normalized_status": "SUCCEEDED", "retry_after_seconds": None}),
        response("finish_replication"), response("fail_migration"),
    ]
    queue = f"migration-test-{uuid4()}"
    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(environment.client, task_queue=queue,
                          workflows=[workflow_class], activities=activities):
            handle = await environment.client.start_workflow(
                workflow_class.run, {**INPUT, "migration_approach": approach},
                id=str(uuid4()), task_queue=queue)
            oracle = approach == "ORACLE_TABLE"
            waiting = await _wait_for_phase(handle, "WAITING_FOR_COLUMN_MAPPING_APPROVAL" if oracle
                                            else "WAITING_FOR_PLAN_APPROVAL")
            assert waiting["plan_guid"] == plan_guid
            assert waiting["review"]["source"] == "APP.ORDERS"
            if oracle and action in {"approve_plan", "cancel_running"}:
                await handle.signal("command", {"action": "approve_columns", "actor": "reviewer",
                                                "columns": [{"name": "ORDER_ID", "fabric_type": "BIGINT"}]})
                await _wait_for_phase(handle, "WAITING_FOR_PRIMARY_KEY_APPROVAL")
                await handle.signal("command", {"action": "approve_primary_key", "actor": "reviewer",
                                                "primary_key_columns": []})
                await _wait_for_phase(handle, "WAITING_FOR_WATERMARK_APPROVAL")
            if approach == "SAP_TABLE" and action in {"approve_plan", "cancel_running"}:
                await handle.signal("command", {"action": "approve_plan", "actor": "reviewer"})
                await _wait_for_phase(handle, "WAITING_FOR_PRIMARY_KEY_APPROVAL")
                await handle.signal("command", {"action": "approve_primary_key", "actor": "reviewer",
                                                "primary_key_columns": []})
                await _wait_for_phase(handle, "WAITING_FOR_WATERMARK_APPROVAL")
            if action == "cancel_running":
                await handle.signal("command", {"action": "approve_watermark" if oracle or approach == "SAP_TABLE" else "approve_plan",
                                                "actor": "reviewer", "watermark_column": None})
                if oracle:
                    await _wait_for_phase(handle, "WAITING_FOR_DELETE_POLICY_APPROVAL")
                    await handle.signal("command", {"action": "approve_delete_policy", "actor": "reviewer",
                                                    "delete_policy": {"mode": "NONE"}})
                await _wait_for_phase(handle, "WAITING_FOR_PROVISIONING")
            selected_action = ("cancel" if action == "cancel_running" else
                               "reject_columns" if oracle and action == "reject_plan" else
                               "approve_watermark" if (oracle or approach == "SAP_TABLE") and action == "approve_plan" else action)
            await handle.signal("command", {"action": selected_action,
                                            "actor": "reviewer"})
            if oracle and action == "approve_plan":
                await _wait_for_phase(handle, "WAITING_FOR_DELETE_POLICY_APPROVAL")
                await handle.signal("command", {"action": "approve_delete_policy", "actor": "reviewer",
                                                "delete_policy": {"mode": "NONE"}})
            result = await handle.result()
            expected = {"approve_plan": "COMPLETED", "reject_plan": "REJECTED",
                        "cancel": "CANCELLED", "cancel_running": "CANCELLED"}[action]
            if action == "approve_plan" and provisioning_status != "SUCCEEDED":
                expected = "FAILED"
            assert result["status"] == expected
            assert (await handle.query("get_state"))["status"] == result["status"]
            assert any(name == "transition_plan" for name, _ in calls)
            if action in {"approve_plan", "cancel_running"}:
                assert any(name == "submit_provisioning" for name, _ in calls)
                if action == "approve_plan" and provisioning_status == "SUCCEEDED":
                    assert any(name == "activate_replication_config" for name, _ in calls)
                    assert result["provisioning_report"] == {"status": "SUCCESS"}
                assert not any(name == "submit_replication" for name, _ in calls)
            else:
                assert not any(name == "submit_provisioning" for name, _ in calls)
            if action in {"cancel", "cancel_running"}:
                assert any(name == "fail_migration" for name, _ in calls)
            if action == "cancel_running":
                assert cancel_sent
                assert not any(name == "submit_replication" for name, _ in calls)
            if expected == "FAILED":
                assert any(name == "fail_migration" for name, _ in calls)


def test_oracle_approval_and_execution():
    asyncio.run(_exercise_standard("approve_plan"))


def test_oracle_rejection_stops_execution():
    asyncio.run(_exercise_standard("reject_plan"))


def test_oracle_cancel_while_waiting():
    asyncio.run(_exercise_standard("cancel"))


def test_oracle_cancel_while_provisioning():
    asyncio.run(_exercise_standard("cancel_running"))


def test_sap_table_approval_and_execution():
    asyncio.run(_exercise_standard("approve_plan", SAPTableMigrationWorkflow,
                                   "discover_sap_table", "SAP_TABLE"))


def test_sap_odp_approval_and_execution():
    asyncio.run(_exercise_standard("approve_plan", SAPODPDataSourceMigrationWorkflow,
                                   "discover_sap_odp", "SAP_ODP"))


def test_oracle_provisioning_failure_stops_replication():
    asyncio.run(_exercise_standard("approve_plan", provisioning_status="FAILED"))


def test_oracle_does_not_start_a_pipeline_even_with_legacy_input_id():
    asyncio.run(_exercise_standard("approve_plan"))


async def _exercise_rebuild(replicate: bool, feedback: bool = False) -> None:
    calls = []
    plan_guid = str(uuid4())
    analysis_guid = str(uuid4())

    def response(name, result=None):
        def called(payload):
            calls.append(name)
            return result
        return _activity(name, called)

    analysis = {"analysis_guid": analysis_guid, "route": "FUNCTION_MODULE",
                "analysis": {"source_tables": [], "source_views": []},
                "verified": {"verified": [{"name": "BKPF", "object_type": "TRANSPARENT_TABLE"}]},
                "warnings": []}
    activities = [response("create_migration_plan", plan_guid), response("set_temporal_ids"),
                  response("validate_fabric", {"lakehouse_name": "Bronze"}),
                  response("analyze_rebuild", analysis), response("transition_plan"),
                  response("revise_analysis_version", 2), response("approve_analysis"),
                  response("generate_rebuild_plan",
                      {"tables": [{"source_object": "BKPF"}], "views": []}),
                  response("revise_plan_version", 2),
                  response("approve_runtime_plan"),
                  response("persist_verified_rebuild_plan",
                           {"source_table_guids": [str(uuid4())] if replicate else []}),
                  response("start_batch", str(uuid4())),
                  response("submit_provisioning", {"job_instance_id": "notebook-job"}),
                  response("record_batch_job"), response("get_provisioning_status",
                      {"normalized_status": "SUCCEEDED"}),
                  response("parse_provisioning", {"status": "SUCCESS"}),
                  response("finish_provisioning"), response("activate_replication_config"),
                  response("enable_replication"),
                  response("submit_replication", {"job_instance_id": "pipeline-job"}),
                  response("record_pipeline_job"), response("get_replication_status",
                      {"normalized_status": "SUCCEEDED"}),
                  response("finish_replication"), response("fail_migration")]
    queue = f"migration-test-{uuid4()}"
    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(environment.client, task_queue=queue,
                          workflows=[SAPDataSourceRebuildWorkflow], activities=activities):
            handle = await environment.client.start_workflow(
                SAPDataSourceRebuildWorkflow.run,
                {**INPUT, "migration_approach": "SAP_REBUILD"},
                id=str(uuid4()), task_queue=queue)
            state = await _wait_for_phase(handle, "WAITING_FOR_ANALYSIS_APPROVAL")
            assert state["review"]["verified_objects"][0]["name"] == "BKPF"
            if feedback:
                await handle.signal("command", {"action": "analysis_feedback",
                                                "actor": "reviewer", "message": "Check BKPF"})
                await _wait_for_review_version(handle, "WAITING_FOR_ANALYSIS_APPROVAL", 2)
            await handle.signal("command", {"action": "approve_analysis", "actor": "reviewer"})
            await _wait_for_phase(handle, "WAITING_FOR_FABRIC_PLAN_APPROVAL")
            if feedback:
                await handle.signal("command", {"action": "plan_feedback",
                                                "actor": "reviewer", "message": "Rename view"})
                await _wait_for_review_version(handle, "WAITING_FOR_FABRIC_PLAN_APPROVAL", 2)
            await handle.signal("command", {"action": "approve_plan", "actor": "reviewer"})
            result = await handle.result()
            assert result["status"] == "COMPLETED"
            assert calls.index("approve_analysis") < calls.index("generate_rebuild_plan")
            assert calls.index("approve_runtime_plan") < calls.index("persist_verified_rebuild_plan")
            assert "activate_replication_config" in calls
            assert "submit_replication" not in calls
            if feedback:
                assert "revise_analysis_version" in calls
                assert "revise_plan_version" in calls


def test_rebuild_requires_two_approvals():
    asyncio.run(_exercise_rebuild(True))


def test_rebuild_reuse_does_not_start_replication():
    asyncio.run(_exercise_rebuild(False))


def test_rebuild_feedback_versions_before_approval():
    asyncio.run(_exercise_rebuild(True, feedback=True))


async def _exercise_odp_unsupported() -> None:
    plan_guid = str(uuid4())
    queue = f"migration-test-{uuid4()}"
    async with await WorkflowEnvironment.start_local() as environment:
        async with Worker(environment.client, task_queue=queue,
                          workflows=[SAPODPDataSourceMigrationWorkflow], activities=[
                              _activity("create_migration_plan", lambda _: plan_guid),
                              _activity("set_temporal_ids", lambda _: None),
                              _activity("validate_fabric", lambda _: {"lakehouse_name": "Bronze"}),
                              _activity("discover_sap_odp", lambda _: {
                                  "failure_reason": "ODP_CAPABILITY_UNSUPPORTED: ODP is unavailable"}),
                              _activity("transition_plan", lambda _: None)]):
            result = await environment.client.execute_workflow(
                SAPODPDataSourceMigrationWorkflow.run,
                {**INPUT, "migration_approach": "SAP_ODP"},
                id=str(uuid4()), task_queue=queue)
            assert result["status"] == "FAILED"
            assert result["last_error"].startswith("ODP_CAPABILITY_UNSUPPORTED")


def test_odp_unsupported_has_clear_reason():
    asyncio.run(_exercise_odp_unsupported())
