"""Read-only projections of Temporal jobs for the business workspace.

Visibility and history remain authoritative; unavailable state is never success.
No workflow or pipeline is started by these endpoints.
"""

import asyncio
import base64
import binascii
from datetime import timedelta
from typing import Annotated, Any

from fastapi import APIRouter, HTTPException, Query
from temporalio.client import WorkflowExecutionStatus
from temporalio.service import RPCError, RPCStatusCode

from .api import TemporalClient
from .worker import TASK_QUEUE
from .agent_timeline import project_messages

router = APIRouter(prefix="/workspace", tags=["workspace"])


def decode_cursor(cursor: str | None) -> bytes | None:
    try:
        return base64.b64decode(cursor, validate=True) if cursor else None
    except (ValueError, binascii.Error) as exc:
        raise HTTPException(422, "Invalid page cursor") from exc


def encode_cursor(token: bytes | None) -> str | None:
    return base64.b64encode(token).decode() if token else None


async def source_input(client, handle) -> dict:
    iterator = handle.fetch_history_events(page_size=1, rpc_timeout=timedelta(seconds=5))
    async for event in iterator:
        if event.HasField("workflow_execution_started_event_attributes"):
            values = await client.data_converter.decode(
                event.workflow_execution_started_event_attributes.input.payloads)
            return values[0] if values and isinstance(values[0], dict) else {}
        break
    return {}


async def job_projection(client, execution) -> dict[str, Any]:
    handle = client.get_workflow_handle(execution.id, run_id=execution.run_id)
    job = {"workflow_id": execution.id, "run_id": execution.run_id,
           "started_at": execution.start_time.isoformat(),
           "closed_at": execution.close_time.isoformat() if execution.close_time else None,
           "execution_status": execution.status.name if execution.status else "UNKNOWN",
           "status": execution.status.name if execution.status else "UNKNOWN",
           "state_available": False, "waiting_for_user": False}
    try:
        async with asyncio.timeout(5):
            source = await source_input(client, handle)
        # Explicit allowlist: never expose connection credentials or arbitrary inputs.
        for key in ("source_object_name", "source_schema_name", "migration_approach",
                    "source_connection_name", "fabric_workspace_id", "fabric_lakehouse_id",
                    "fabric_schema_name", "fabric_target_name", "requested_by"):
            job[key] = source.get(key)
    except Exception:
        job["input_unavailable"] = True
    try:
        async with asyncio.timeout(4):
            if execution.status == WorkflowExecutionStatus.COMPLETED:
                state = await handle.result()
            elif execution.status == WorkflowExecutionStatus.RUNNING:
                state = await handle.query("get_state")
            else:
                return job
        if isinstance(state, dict):
            job["state"] = state
            job["state_available"] = True
            for key in ("status", "phase", "waiting_for_user", "source_object_name",
                        "migration_approach", "current_message", "last_error", "provisioning_status"):
                if key in state:
                    job[key] = state[key]
    except Exception:
        job["state_message"] = "Detailed status is temporarily unavailable."
    return job


@router.get("/jobs")
async def list_jobs(client: TemporalClient,
                    cursor: Annotated[str | None, Query(max_length=16000)] = None,
                    limit: Annotated[int, Query(ge=1, le=50)] = 20) -> dict[str, Any]:
    token = decode_cursor(cursor)
    # Task queue is server configuration, never a user-supplied visibility expression.
    queue = TASK_QUEUE.replace("'", "''")
    iterator = client.list_workflows(f"TaskQueue = '{queue}'", page_size=limit,
                                     next_page_token=token, rpc_timeout=timedelta(seconds=8))
    try:
        await iterator.fetch_next_page()
        semaphore = asyncio.Semaphore(6)

        async def project(execution):
            async with semaphore:
                return await job_projection(client, execution)

        jobs = await asyncio.gather(*(project(item) for item in iterator.current_page or []))
    except Exception as exc:
        raise HTTPException(503, "Job history is unavailable. Check the Temporal connection and retry.") from exc
    # Heavy review/SQL payloads are requested only when opening a job.
    for job in jobs:
        state = job.pop("state", {})
        plan = (state.get("review") or {}).get("agent_plan") or {}
        job["table_count"] = len(plan["tables"]) if isinstance(plan.get("tables"), list) else None
        job["view_count"] = len(plan["views"]) if isinstance(plan.get("views"), list) else None
        if job["table_count"] is None and isinstance((state.get("review") or {}).get("columns"), list):
            job["table_count"] = 1
            job["view_count"] = 0
    return {"jobs": jobs, "next_cursor": encode_cursor(iterator.next_page_token),
            "history_note": "History follows this Temporal namespace's retention period."}


@router.get("/jobs/{workflow_id}")
async def get_job(workflow_id: str, client: TemporalClient) -> dict[str, Any]:
    try:
        execution = await client.get_workflow_handle(workflow_id).describe(rpc_timeout=timedelta(seconds=5))
        if execution.task_queue != TASK_QUEUE:
            raise HTTPException(404, "Migration job not found")
        return await job_projection(client, execution)
    except HTTPException:
        raise
    except RPCError as exc:
        raise HTTPException(404 if exc.status == RPCStatusCode.NOT_FOUND else 503,
                            "Migration job is unavailable") from exc


SIGNAL_LABELS = {
    "approve_columns": "Column mappings approved", "approve_primary_key": "Record identifiers confirmed",
    "approve_watermark": "Update method confirmed", "approve_delete_policy": "Delete handling confirmed",
    "approve_plan": "Migration plan approved", "approve_analysis": "Source analysis approved",
    "approve_target_change": "Destination change approved", "agent_reply": "Answer sent",
    "agent_feedback": "Revision requested", "plan_feedback": "Plan revision requested",
    "analysis_feedback": "Analysis revision requested", "cancel": "Cancellation requested",
}


async def event_projection(client, event) -> dict | None:
    kind = event.WhichOneof("attributes")
    if not kind:
        return None
    attrs = getattr(event, kind)
    item = {"id": event.event_id, "time": event.event_time.ToDatetime().isoformat() + "Z"}
    if kind == "workflow_execution_signaled_event_attributes":
        item.update(kind="decision", title=SIGNAL_LABELS.get(attrs.signal_name,
                    attrs.signal_name.replace("_", " ").capitalize()))
        values = await client.data_converter.decode(attrs.input.payloads)
        body = values[0] if values and isinstance(values[0], dict) else {}
        action = body.get("action") if attrs.signal_name == "command" else attrs.signal_name
        item["title"] = SIGNAL_LABELS.get(action, (action or attrs.signal_name).replace("_", " ").capitalize())
        item["actor"] = body.get("approved_by") or body.get("submitted_by") or body.get("actor")
        item["action"] = action
        item["review_snapshot"] = body.get("review_snapshot")
        item["details"] = {key: body[key] for key in (
            "comment", "message", "watermark_column", "primary_key_columns", "decision",
            "delete_policy", "columns", "answer") if key in body and (body[key] is not None or
                (key == 'watermark_column' and action == 'approve_watermark'))}
    elif kind == "workflow_execution_started_event_attributes":
        item.update(kind="milestone", title="Migration started")
    elif kind == "activity_task_scheduled_event_attributes":
        item.update(kind="activity", engine="Temporal", activity_name=attrs.activity_type.name,
                    title=attrs.activity_type.name.replace("_", " ").capitalize(), status="scheduled")
    elif kind in ("activity_task_started_event_attributes", "activity_task_completed_event_attributes",
                  "activity_task_failed_event_attributes", "activity_task_timed_out_event_attributes",
                  "activity_task_canceled_event_attributes"):
        status = kind.removeprefix("activity_task_").removesuffix("_event_attributes")
        item.update(kind="activity_status", scheduled_event_id=attrs.scheduled_event_id, status=status)
        if status in ("failed", "timed_out"):
            item["message"] = attrs.failure.message
    elif kind == "workflow_execution_completed_event_attributes":
        item.update(kind="milestone", title="Workflow finished", message="See the job outcome for the migration result.")
    elif kind in ("workflow_execution_failed_event_attributes", "workflow_execution_canceled_event_attributes",
                  "workflow_execution_terminated_event_attributes", "workflow_execution_timed_out_event_attributes"):
        item.update(kind="error", title=kind.removesuffix("_event_attributes").replace("_", " ").capitalize())
    else:
        return None
    return item


@router.get("/jobs/{workflow_id}/agent-timeline")
async def agent_timeline(workflow_id: str, client: TemporalClient) -> dict:
    job = await get_job(workflow_id, client)
    session_id = (job.get('state') or {}).get('agent_session_id')
    if not session_id:
        return {'events': [], 'available': False, 'message': 'No OpenCode session has been recorded yet.'}

    def read():
        from .opencode_activities import _client, _request, _path
        with _client() as connection:
            messages = _request(connection, 'GET', _path(session_id) + '/message').get('data', [])
        return {'events': project_messages(messages), 'available': True,
                'truncated': len(messages) > 500}

    try:
        return await asyncio.to_thread(read)
    except Exception as exc:
        raise HTTPException(503, 'OpenCode activity is temporarily unavailable. Workflow status remains available.') from exc


@router.get("/jobs/{workflow_id}/events")
async def get_events(workflow_id: str, client: TemporalClient,
                     cursor: Annotated[str | None, Query(max_length=16000)] = None) -> dict[str, Any]:
    token = decode_cursor(cursor)
    try:
        handle = client.get_workflow_handle(workflow_id)
        description = await handle.describe(rpc_timeout=timedelta(seconds=5))
        if description.task_queue != TASK_QUEUE:
            raise HTTPException(404, "Migration job not found")
        handle = client.get_workflow_handle(workflow_id, run_id=description.run_id)
        iterator = handle.fetch_history_events(page_size=250, next_page_token=token,
                                                rpc_timeout=timedelta(seconds=8))
        await iterator.fetch_next_page()
        events = [await event_projection(client, event) for event in iterator.current_page or []]
        return {"events": [event for event in events if event],
                "next_cursor": encode_cursor(iterator.next_page_token)}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(503, "Activity history is unavailable. Please retry.") from exc
