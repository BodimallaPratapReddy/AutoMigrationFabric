"""Thin HTTP boundary for Temporal workflow control."""

import os
import asyncio
import re
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from temporalio.client import Client, WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from .contracts import (ColumnMappingApproval, Decision, DeletePolicyApproval, Feedback, MigrationInput,
                        MigrationState, PrimaryKeyApproval, TargetChangeApproval, WatermarkApproval)
from .primary_keys import validate_primary_key
from thirdparty.delete_policy import validate_delete_policy
from .migration_types import MigrationTypeOption, load_migration_types
from .mapping_types import normalize_fabric_type
from .target_review import allowed_target_decisions
from .worker import TASK_QUEUE
from .agent_forms import validate_reply
from .contracts import AgentReply, AgentFeedback
from .workflow_opencode import OpenCodeOracleMigrationWorkflow
from .workflow_view_replication import OpenCodeOracleViewReplicationWorkflow
from .workflow_sap_opencode import OpenCodeSAPTableMigrationWorkflow
from .workflow_sap_view import OpenCodeSAPViewMigrationWorkflow
from .workflow_sap_mcp import OpenCodeMCPSAPTableMigrationWorkflow, OpenCodeMCPSAPViewMigrationWorkflow
from .workflow_sap_datasource import OpenCodeSAPODPMigrationWorkflow, OpenCodeSAPDataSourceRebuildWorkflow
from thirdparty.configdb.repository import ConfigDBRepository
from thirdparty.configdb.utils import ConfigDB
from .workflows import (
    OracleTableMigrationWorkflow, SAPDataSourceRebuildWorkflow,
    SAPODPDataSourceMigrationWorkflow, SAPTableMigrationWorkflow,
)


router = APIRouter(prefix="/migrations", tags=["migrations"])


async def get_temporal_client() -> Client:
    return await Client.connect(os.getenv("TEMPORAL_ADDRESS", "localhost:7233"),
                                namespace=os.getenv("TEMPORAL_NAMESPACE", "default"))


TemporalClient = Annotated[Client, Depends(get_temporal_client)]


class StartedMigration(BaseModel):
    workflow_id: str
    run_id: str
    plan_guid: str | None = None


@router.get("/options", response_model=dict[str, list[MigrationTypeOption]])
def migration_options() -> dict[str, list[MigrationTypeOption]]:
    try:
        return load_migration_types()
    except (OSError, ValueError) as exc:
        raise HTTPException(503, "Migration type configuration is unavailable") from exc


async def _start(client: Client, request: MigrationInput, expected: str,
                 workflow_class: type, prefix: str) -> StartedMigration:
    if request.migration_approach != expected:
        raise HTTPException(422, f"migration_approach must be {expected}")
    if expected.startswith("SAP_") and request.discovery_provider != "OPENCODE":
        raise HTTPException(422,"New SAP migrations require OpenCode MCP discovery")
    if request.replication_pipeline_id is not None:
        raise HTTPException(422, "Replication pipelines are scheduled separately from migration")
    if (request.discovery_provider == 'OPENCODE' and expected in {'SAP_ODP','SAP_REBUILD'}
            and not request.fabric_target_name):
        request = request.model_copy(update={'fabric_target_name':'DS_'+re.sub(r'[^A-Za-z0-9_]','_',request.source_object_name)})
    try:
        options = load_migration_types()
    except (OSError, ValueError) as exc:
        raise HTTPException(503, "Migration type configuration is unavailable") from exc
    try:
        connection = await asyncio.to_thread(
            lambda: ConfigDBRepository().get_connection(request.source_connection_name))
    except Exception as exc:
        raise HTTPException(503, "Connection registry is unavailable") from exc
    if connection is None or not connection.is_active:
        raise HTTPException(422, "Select an active source connection")
    if expected not in {option.value for option in options.get(connection.source_type, [])}:
        raise HTTPException(422, "Migration type is not allowed for this source connection")
    try:
        workspaces = await asyncio.to_thread(
            lambda: ConfigDB().list_fabric_workspaces(active_only=True))
    except Exception as exc:
        raise HTTPException(503, "Fabric workspace registry is unavailable") from exc
    if not any(row.workspace_id.lower() == request.fabric_workspace_id.lower()
               for row in workspaces):
        raise HTTPException(422, "Select an active Fabric workspace")
    workflow_id = f"{prefix}-{uuid4()}"
    handle = await client.start_workflow(workflow_class.run,
        request.model_dump(mode="json"), id=workflow_id, task_queue=TASK_QUEUE)
    return StartedMigration(workflow_id=workflow_id, run_id=handle.result_run_id)


@router.post("/oracle-table", response_model=StartedMigration)
async def start_oracle(request: MigrationInput, client: TemporalClient) -> StartedMigration:
    workflow_class = (OpenCodeOracleMigrationWorkflow if request.discovery_provider == "OPENCODE"
                      else OracleTableMigrationWorkflow)
    return await _start(client, request, "ORACLE_TABLE", workflow_class, "oracle-table")


@router.post("/sap-table", response_model=StartedMigration)
async def start_sap_table(request: MigrationInput, client: TemporalClient) -> StartedMigration:
    workflow_class = OpenCodeMCPSAPTableMigrationWorkflow if request.discovery_provider == "OPENCODE" else SAPTableMigrationWorkflow
    return await _start(client, request, "SAP_TABLE", workflow_class, "sap-table")


@router.post("/oracle-view", response_model=StartedMigration)
async def start_oracle_view(request: MigrationInput, client: TemporalClient) -> StartedMigration:
    return await _start(client, request, "ORACLE_VIEW", OpenCodeOracleViewReplicationWorkflow, "oracle-view")


@router.post("/sap-view", response_model=StartedMigration)
async def start_sap_view(request: MigrationInput, client: TemporalClient) -> StartedMigration:
    return await _start(client, request, "SAP_VIEW", OpenCodeMCPSAPViewMigrationWorkflow, "sap-view")


@router.post("/sap-odp", response_model=StartedMigration)
async def start_sap_odp(request: MigrationInput, client: TemporalClient) -> StartedMigration:
    workflow_class = OpenCodeSAPODPMigrationWorkflow if request.discovery_provider == "OPENCODE" else SAPODPDataSourceMigrationWorkflow
    return await _start(client, request, "SAP_ODP", workflow_class, "sap-odp")


@router.post("/sap-rebuild", response_model=StartedMigration)
async def start_sap_rebuild(request: MigrationInput, client: TemporalClient) -> StartedMigration:
    workflow_class = OpenCodeSAPDataSourceRebuildWorkflow if request.discovery_provider == "OPENCODE" else SAPDataSourceRebuildWorkflow
    return await _start(client, request, "SAP_REBUILD", workflow_class, "sap-rebuild")


async def _state(client: Client, workflow_id: str) -> MigrationState:
    try:
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe()
        handle = client.get_workflow_handle(workflow_id, run_id=description.run_id)
        if description.status == WorkflowExecutionStatus.COMPLETED:
            result = await handle.result()
        elif description.status == WorkflowExecutionStatus.RUNNING:
            result = await handle.query("get_state")
        else:
            raise HTTPException(410, f"Workflow is {description.status.name.lower()}")
        return MigrationState.model_validate(result)
    except HTTPException:
        raise
    except RPCError as exc:
        if exc.status == RPCStatusCode.NOT_FOUND:
            raise HTTPException(404, "Workflow was not found") from exc
        if exc.status == RPCStatusCode.RESOURCE_EXHAUSTED:
            raise HTTPException(503, f"Workflow is waiting for a worker on the {TASK_QUEUE} task queue") from exc
        if exc.status == RPCStatusCode.FAILED_PRECONDITION:
            # A reset/replaying worker may temporarily reject a query. Recheck
            # the current execution before hiding a still-running workflow.
            latest = await client.get_workflow_handle(workflow_id).describe()
            if latest.status == WorkflowExecutionStatus.RUNNING:
                raise HTTPException(503, "Workflow status is temporarily unavailable; retrying") from exc
            raise HTTPException(410, "Workflow closed before a worker processed it") from exc
        raise HTTPException(503, "Workflow status is temporarily unavailable") from exc
    except Exception as exc:
        raise HTTPException(503, "Workflow status is temporarily unavailable") from exc


@router.get("/{workflow_id}", response_model=MigrationState)
async def get_migration(workflow_id: str, client: TemporalClient) -> MigrationState:
    return await _state(client, workflow_id)


@router.get("/{workflow_id}/audit")
async def get_migration_audit(workflow_id: str, client: TemporalClient) -> dict:
    state = await _state(client, workflow_id)
    if not state.plan_guid:
        return {"batches": [], "tables": []}

    def read() -> dict:
        db = ConfigDBRepository()
        guid = UUID(state.plan_guid)
        batches = db.list_batch_runs_for_plan(guid)
        tables = db.list_source_tables_for_plan(guid)
        table_status = []
        for table in tables:
            replication = db.get_replication_state(table.guid)
            table_status.append({"source": table.source_table_name,
                                 "target": table.fabric_table_name,
                                 "provisioning_status": table.provisioning_status,
                                 "replication": replication.model_dump(mode="json") if replication else None})
        return {"batches": [{**item.model_dump(mode="json"),
                             "objects": [entry.model_dump(mode="json") for entry in
                                         db.list_batch_object_runs(item.batch_run_id)]}
                            for item in batches],
                "tables": table_status}

    try:
        return await asyncio.to_thread(read)
    except Exception as exc:
        raise HTTPException(503, "Migration audit is unavailable") from exc


async def _command(client: Client, workflow_id: str, action: str,
                   actor: str | None = None, message: str | None = None,
                   watermark_column: str | None = None,
                   columns: list[dict] | None = None,
                   target_decision: str | None = None,
                   acknowledge_unsupported: bool = False,
                   primary_key_columns: list[str] | None = None,
                   delete_policy: dict | None = None) -> dict:
    state = await _state(client, workflow_id)
    phases = {
        "approve_plan": {"WAITING_FOR_PLAN_APPROVAL", "WAITING_FOR_FABRIC_PLAN_APPROVAL"},
        "reject_plan": {"WAITING_FOR_PLAN_APPROVAL", "WAITING_FOR_FABRIC_PLAN_APPROVAL"},
        "approve_columns": {"WAITING_FOR_COLUMN_MAPPING_APPROVAL"},
        "reject_columns": {"WAITING_FOR_COLUMN_MAPPING_APPROVAL"},
        "approve_watermark": {"WAITING_FOR_WATERMARK_APPROVAL"},
        "reject_watermark": {"WAITING_FOR_WATERMARK_APPROVAL"},
        "edit_columns": {"WAITING_FOR_WATERMARK_APPROVAL"},
        "approve_primary_key": {"WAITING_FOR_PRIMARY_KEY_APPROVAL"},
        "reject_primary_key": {"WAITING_FOR_PRIMARY_KEY_APPROVAL"},
        "approve_delete_policy": {"WAITING_FOR_DELETE_POLICY_APPROVAL"},
        "reject_delete_policy": {"WAITING_FOR_DELETE_POLICY_APPROVAL"},
        "edit_primary_key": {"WAITING_FOR_WATERMARK_APPROVAL"},
        "approve_target_change": {"WAITING_FOR_TARGET_CHANGE_APPROVAL"},
        "reject_target_change": {"WAITING_FOR_TARGET_CHANGE_APPROVAL"},
        "plan_feedback": {"WAITING_FOR_FABRIC_PLAN_APPROVAL"},
        "approve_analysis": {"WAITING_FOR_ANALYSIS_APPROVAL"},
        "reject_analysis": {"WAITING_FOR_ANALYSIS_APPROVAL"},
        "analysis_feedback": {"WAITING_FOR_ANALYSIS_APPROVAL"},
    }
    if action == "cancel":
        if state.status != "RUNNING":
            raise HTTPException(409, "Workflow is already terminal")
    elif state.phase not in phases[action]:
        raise HTTPException(409, f"{action} is unavailable in {state.phase}")
    if action in {"approve_primary_key", "edit_primary_key"}:
        review = state.review or {}
        if (state.migration_approach not in {"ORACLE_TABLE", "SAP_TABLE"} and not
                (state.migration_approach in {"ORACLE_VIEW", "SAP_VIEW", "SAP_REBUILD"} and review.get("view_base_table"))
                or (review.get("primary_key_source") == "DATABASE"
                    and (review.get("key_validation") or {}).get("valid") is not False)
                or (review.get("primary_key") and not review.get("primary_key_source"))):
            raise HTTPException(409, "Database primary keys cannot be replaced in this step")
        if action == "approve_primary_key":
            try:
                primary_key_columns = validate_primary_key(
                    primary_key_columns or [], [item["name"] for item in review.get("columns", [])])
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc
    if (action == "analysis_feedback" and state.review
            and state.review.get("route") != "FUNCTION_MODULE" and state.review.get("discovery_provider") != "OPENCODE"):
        raise HTTPException(409, "Analysis feedback is supported for function module routes")
    if action == "approve_columns":
        review_columns = (state.review or {}).get("columns", [])
        expected = {item["name"] for item in review_columns}
        names = [item["name"] for item in columns or []]
        if not expected or len(names) != len(expected) or len(set(names)) != len(names) or set(names) != expected:
            raise HTTPException(422, "Map every discovered column exactly once")
        try:
            columns = [{"name": item["name"],
                        "fabric_type": normalize_fabric_type(item["fabric_type"])}
                       for item in columns]
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        if (state.review or {}).get("discovery_provider") == "OPENCODE":
            proposed = {item["name"]: item["fabric_type"] for item in review_columns}
            if any(item["fabric_type"] != proposed[item["name"]] for item in columns):
                raise HTTPException(422, "Mappings must come from the agent plan; reject and revise the plan")
        if any(item.get("supported") is False for item in review_columns) and not acknowledge_unsupported:
            raise HTTPException(422, "Acknowledge unsupported Oracle mappings before approval")
    if watermark_column is not None:
        candidates = (state.review or {}).get("watermark_candidates", [])
        if (action not in {"approve_plan", "approve_watermark"}
                or (state.migration_approach not in {"ORACLE_TABLE", "SAP_TABLE"} and not
                    (state.migration_approach in {"ORACLE_VIEW", "SAP_VIEW", "SAP_REBUILD"} and (state.review or {}).get("view_base_table")))
                or watermark_column not in {item["column_name"] for item in candidates}):
            raise HTTPException(422, "Select a discovered date or timestamp watermark column")
        if not state.review.get("primary_key"):
            raise HTTPException(422, "A primary key is required for watermark upserts")
        if (state.review.get("key_validation_required")
                and (state.review.get("key_validation") or {}).get("valid") is not True):
            raise HTTPException(422, "Source key validation must succeed before watermark upserts")
    if action == "approve_target_change":
        review = (state.review or {}).get("target_review") or {}
        allowed = ({"ALTER_FUTURE", "ALTER_BACKFILL", "REPLACE_FULL"}
                   if review.get("is_provisioned_record") else {"REVISE_PLANNED"})
        if state.migration_approach == "SAP_TABLE" or (state.review or {}).get("delete_policy_required"):
            allowed = allowed_target_decisions(review)
        if target_decision not in allowed:
            raise HTTPException(422, "Choose an action valid for this target")
    if action == "approve_delete_policy":
        review = state.review or {}
        if state.migration_approach != "ORACLE_TABLE" and not (
                state.migration_approach in {"ORACLE_VIEW", "SAP_VIEW", "SAP_REBUILD"} and review.get("view_base_table")) and not (
                state.migration_approach == "SAP_TABLE" and review.get("delete_policy_required")):
            raise HTTPException(409, "Workflow does not support table delete-policy approval")
        try:
            delete_policy = validate_delete_policy(delete_policy,
                [item["name"] for item in review.get("columns", [])],
                review.get("primary_key") or [], review.get("selected_watermark"))
            if delete_policy["mode"] != "NONE" and (review.get("key_validation") or {}).get("valid") is not True:
                raise ValueError("Validate source key fields before enabling delete propagation")
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
    command = {"action": action, "actor": actor, "message": message,
               "watermark_column": watermark_column, "columns": columns}
    # Store the exact user-facing review with the decision, rather than reusing
    # the current (possibly different) review when opening history later.
    if action.startswith(('approve_', 'reject_')):
        review = state.review or {}
        command['review_snapshot'] = {key: review[key] for key in (
            'source', 'columns', 'primary_key', 'selected_watermark', 'load_method',
            'write_strategy', 'delete_policy', 'key_validation', 'warnings',
            'base_table_index', 'base_table_count', 'agent_plan') if key in review}
        command['review_snapshot']['phase'] = state.phase
        command['review_snapshot']['source_object_name'] = state.source_object_name
    if target_decision is not None:
        command["decision"] = target_decision
    if primary_key_columns is not None:
        command["primary_key_columns"] = primary_key_columns
    if delete_policy is not None:
        command["delete_policy"] = delete_policy
    await client.get_workflow_handle(workflow_id).signal("command", command)
    return {"accepted": True}


@router.post("/{workflow_id}/agent-reply")
async def reply_to_agent(workflow_id: str, reply: AgentReply, client: TemporalClient) -> dict:
    state = await _state(client, workflow_id)
    if state.status != "RUNNING" or state.phase != "WAITING_FOR_AGENT_INPUT" or not state.agent_request:
        raise HTTPException(409, "Workflow is not waiting for an agent answer")
    payload = {"action": "agent_reply", "actor": reply.approved_by,
               "request_id": reply.request_id, "answer": reply.answer, "decision": reply.decision,
               "review_snapshot": {"source_object_name": state.source_object_name,
                                   "phase": state.phase, "request": state.agent_request}}
    try:
        validate_reply(state.agent_request, payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from exc
    await client.get_workflow_handle(workflow_id).signal("command", payload)
    return {"accepted": True}


@router.post("/{workflow_id}/agent-feedback")
async def agent_feedback(workflow_id: str, feedback: AgentFeedback, client: TemporalClient) -> dict:
    state = await _state(client, workflow_id)
    if state.status != "RUNNING" or state.phase != "WAITING_FOR_AGENT_REVISION":
        raise HTTPException(409, "Workflow is not waiting for an agent revision")
    await client.get_workflow_handle(workflow_id).signal("command", {
        "action": "agent_feedback", "actor": feedback.submitted_by, "message": feedback.message})
    return {"accepted": True}


@router.post("/{workflow_id}/approve-plan")
async def approve_plan(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_plan", decision.approved_by,
                          decision.comment, decision.watermark_column)


@router.post("/{workflow_id}/approve-columns")
async def approve_columns(workflow_id: str, approval: ColumnMappingApproval,
                          client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_columns", approval.approved_by,
                          columns=[item.model_dump() for item in approval.columns],
                          acknowledge_unsupported=approval.acknowledge_unsupported)


@router.post("/{workflow_id}/reject-columns")
async def reject_columns(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_columns", decision.approved_by,
                          decision.comment)


@router.post("/{workflow_id}/approve-watermark")
async def approve_watermark(workflow_id: str, approval: WatermarkApproval,
                            client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_watermark", approval.approved_by,
                          watermark_column=approval.watermark_column)


@router.post("/{workflow_id}/approve-primary-key")
async def approve_primary_key(workflow_id: str, approval: PrimaryKeyApproval,
                              client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_primary_key", approval.approved_by,
                          primary_key_columns=approval.primary_key_columns)


@router.post("/{workflow_id}/approve-delete-policy")
async def approve_delete_policy(workflow_id: str, approval: DeletePolicyApproval,
                                client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_delete_policy", approval.approved_by,
                          delete_policy=approval.delete_policy)


@router.post("/{workflow_id}/reject-delete-policy")
async def reject_delete_policy(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_delete_policy", decision.approved_by, decision.comment)


@router.post("/{workflow_id}/reject-primary-key")
async def reject_primary_key(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_primary_key", decision.approved_by,
                          decision.comment)


@router.post("/{workflow_id}/edit-primary-key")
async def edit_primary_key(workflow_id: str, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "edit_primary_key")


@router.post("/{workflow_id}/reject-watermark")
async def reject_watermark(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_watermark", decision.approved_by,
                          decision.comment)


@router.post("/{workflow_id}/approve-target-change")
async def approve_target_change(workflow_id: str, approval: TargetChangeApproval,
                                client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_target_change",
                          approval.approved_by, target_decision=approval.decision)


@router.post("/{workflow_id}/reject-target-change")
async def reject_target_change(workflow_id: str, decision: Decision,
                               client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_target_change",
                          decision.approved_by, decision.comment)


@router.post("/{workflow_id}/edit-columns")
async def edit_columns(workflow_id: str, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "edit_columns")


@router.post("/{workflow_id}/reject-plan")
async def reject_plan(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_plan", decision.approved_by, decision.comment)


@router.post("/{workflow_id}/plan-feedback")
async def plan_feedback(workflow_id: str, feedback: Feedback, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "plan_feedback", feedback.submitted_by, feedback.message)


@router.post("/{workflow_id}/approve-analysis")
async def approve_analysis(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "approve_analysis", decision.approved_by, decision.comment)


@router.post("/{workflow_id}/reject-analysis")
async def reject_analysis(workflow_id: str, decision: Decision, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "reject_analysis", decision.approved_by, decision.comment)


@router.post("/{workflow_id}/analysis-feedback")
async def analysis_feedback(workflow_id: str, feedback: Feedback, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "analysis_feedback", feedback.submitted_by, feedback.message)


@router.post("/{workflow_id}/cancel")
async def cancel_migration(workflow_id: str, client: TemporalClient) -> dict:
    return await _command(client, workflow_id, "cancel")
