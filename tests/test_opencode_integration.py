import asyncio
import json
from uuid import uuid4
from unittest.mock import patch, AsyncMock

import httpx
import pytest
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker, Replayer

from app.migration.agent_forms import validate_reply
from app.migration.opencode_plan import adapt_plan
from app.migration import opencode_activities as oa
from app.migration.workflow_opencode import OpenCodeOracleMigrationWorkflow


INPUT = dict(migration_approach="ORACLE_TABLE", discovery_provider="OPENCODE",
    source_connection_name="ORACLE-TEST", source_schema_name="ONT", source_object_name="ORDERS",
    fabric_workspace_id="00000000-0000-0000-0000-000000000001",
    fabric_lakehouse_id="00000000-0000-0000-0000-000000000002", fabric_schema_name="bronze")
PLAN = dict(schema_version=1, source_schema="ONT", source_table="ORDERS",
    columns=[dict(position=1, name="ID", source_type="NUMBER(8,0)", fabric_type="STRING",
        nullable=False, source_expression="TO_CHAR(ID)", transformation_notes="Preserve identifier")],
    primary_key=["ID"], watermark_candidates=[], evidence=["ALL_TAB_COLUMNS, ALL_CONSTRAINTS"],
    unresolved_decisions=[])
FORM = dict(id="frm_test", kind="FORM", fields=[dict(key="q0", type="string",
    options=[dict(value="Preserve")], custom=False)])


@pytest.mark.parametrize("accepted", [True, False])
def test_prompt_reconciles_lost_response_without_resubmitting(monkeypatch, accepted):
    calls = []
    message_id = None
    monkeypatch.setenv("OPENCODE_SOURCE_CONNECTIONS", '{"ORACLE-TEST":"oracle_local"}')

    def handler(request):
        nonlocal message_id
        calls.append(request.method)
        if request.method == "POST":
            message_id = json.loads(request.content)["id"]
            assert request.extensions["timeout"]["read"] == 60
            raise httpx.ReadTimeout("secret URL must not appear", request=request)
        assert request.url.path.endswith("/message/" + message_id)
        return httpx.Response(200, json={"data": {"id": message_id, "type": "user"}}) if accepted else httpx.Response(404)

    monkeypatch.setattr(oa, "_client", lambda: httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)))
    payload = {"session_id": "ses_test", "input": INPUT}
    if accepted:
        oa.opencode_prompt(payload)
    else:
        with pytest.raises(Exception, match="ReadTimeout") as error:
            oa.opencode_prompt(payload)
        assert "secret URL" not in str(error.value)
    assert calls == ["POST", "GET"]


def test_agent_mapping_and_expression_are_preserved_without_old_mapper():
    with patch("thirdparty.datatype_mapping.map_oracle_table_columns", side_effect=AssertionError("Old mapper")):
        result = adapt_plan(json.dumps(PLAN), INPUT, "Bronze")
    column = result["table_plan"]["columns"][0]
    assert column["fabric_data_type"] == "STRING"  # Legacy NUMBER(8,0) would be INT.
    assert column["source_expression"] == "TO_CHAR(ID)"
    assert result["table_plan"]["replication"]["ingestion_flag"] is False


@pytest.mark.parametrize("change", [
    {"source_table": "OTHER"}, {"schema_version": 2}, {"unresolved_decisions": ["Choose ID mapping"]},
    {"primary_key": ["missing"]}, {"watermark_candidates": ["missing"]},
    {"columns": [{**PLAN["columns"][0], "fabric_type": "UNRESOLVED"}]},
    {"columns": PLAN["columns"] * 2},
])
def test_invalid_agent_plan_never_becomes_runtime_config(change):
    with pytest.raises(ValueError):
        adapt_plan(json.dumps({**PLAN, **change}), INPUT, "Bronze")


def test_stale_form_and_unoffered_answer_rejected():
    for payload in [dict(request_id="old", actor="user", answer={"q0": "Preserve"}),
                    dict(request_id="frm_test", actor="user", answer={"q0": "Other"})]:
        with pytest.raises(ValueError):
            validate_reply(FORM, payload)
    validate_reply(FORM, dict(request_id="frm_test", actor="user", answer={"q0": "Preserve"}))
    with pytest.raises(ValueError):
        validate_reply(dict(id="p", kind="PERMISSION"), dict(request_id="p", actor="u", decision="always"))


def test_poll_does_not_return_prior_turn_after_new_prompt(monkeypatch):
    messages = [{"type": "user"}, {"type": "idle", "outcome": "succeeded"},
                {"type": "assistant", "finish": "stop", "content": [{"type": "text", "text": "old"}]}]
    def handler(request):
        return httpx.Response(200, json={"data": messages if request.url.path.endswith('/message') else []})
    monkeypatch.setattr(oa, "_client", lambda: httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)))
    assert oa.opencode_poll({"session_id": "s"}) == {"status": "RUNNING"}


def test_recovered_session_does_not_duplicate_create(monkeypatch):
    monkeypatch.setenv("OPENCODE_DIRECTORY", "/project")
    monkeypatch.setenv("OPENCODE_SOURCE_CONNECTIONS", '{"ORACLE-TEST":"oracle_local"}')
    calls = []
    def handler(request):
        calls.append(request.method)
        return httpx.Response(200, json={"data": {"id": "existing"}})
    monkeypatch.setattr(oa, "_client", lambda: httpx.Client(base_url="http://test", transport=httpx.MockTransport(handler)))
    result = oa.opencode_create({"workflow_id": "wf", "input": INPUT})
    assert result["session_id"].startswith("ses_")
    assert calls == ["GET"]


def test_api_relays_answers_and_rejects_stale_requests(monkeypatch):
    from fastapi.testclient import TestClient
    from app.main import app
    from app.migration import api
    from app.migration.contracts import MigrationState
    state = MigrationState(workflow_id="test", migration_approach="ORACLE_TABLE",
        source_object_name="ORDERS", phase="WAITING_FOR_AGENT_INPUT", agent_request=FORM)
    monkeypatch.setattr(api, "_state", AsyncMock(return_value=state))
    class Client:
        signal = AsyncMock()
        def get_workflow_handle(self, workflow_id):
            return self
    client = Client()
    app.dependency_overrides[api.get_temporal_client] = lambda: client
    try:
        http = TestClient(app)
        body = dict(approved_by="reviewer", request_id="old", answer={"q0": "Preserve"})
        assert http.post('/migrations/test/agent-reply', json=body).status_code == 422
        assert client.signal.await_count == 0
        body["request_id"] = "frm_test"
        assert http.post('/migrations/test/agent-reply', json=body).status_code == 200
        assert client.signal.call_args.args[1]["answer"] == {"q0": "Preserve"}
        state.phase = "COMPLETED"
        assert http.post('/migrations/test/agent-reply', json=body).status_code == 409
    finally:
        app.dependency_overrides.clear()


def test_api_cannot_override_agent_mappings(monkeypatch):
    from fastapi.testclient import TestClient
    from app.main import app
    from app.migration import api
    from app.migration.contracts import MigrationState
    state = MigrationState(workflow_id="test", migration_approach="ORACLE_TABLE",
        source_object_name="ORDERS", phase="WAITING_FOR_COLUMN_MAPPING_APPROVAL",
        review=adapt_plan(json.dumps(PLAN), INPUT, "Bronze")["review"])
    monkeypatch.setattr(api, "_state", AsyncMock(return_value=state))
    app.dependency_overrides[api.get_temporal_client] = lambda: None
    try:
        response = TestClient(app).post('/migrations/test/approve-columns', json={
            "approved_by": "reviewer", "columns": [{"name": "ID", "fabric_type": "INT"}]})
        assert response.status_code == 422
    finally:
        app.dependency_overrides.clear()


async def _wait(handle, phase):
    for _ in range(240):
        state = await handle.query("get_state")
        if state.get("phase") == phase:
            return state
        if state.get("status") == "FAILED":
            raise AssertionError(state)
        await asyncio.sleep(.05)
    raise AssertionError(f"Did not reach {phase}: {state}")


async def _exercise(cancel=False, revision=False):
    persisted, calls = [], []
    poll_count = 0
    validation_count = 0
    def fake(name, result=None):
        @activity.defn(name=name)
        async def run(payload):
            calls.append(name)
            if name == "persist_runtime_plan":
                persisted.append(payload)
            return result
        return run
    @activity.defn(name="opencode_poll")
    async def poll(payload):
        nonlocal poll_count
        poll_count += 1
        return ({"status": "FORM", "request": FORM} if poll_count == 1 else
                {"status": "COMPLETED", "text": json.dumps(PLAN)})
    @activity.defn(name="opencode_validate")
    async def validate(payload):
        nonlocal validation_count
        validation_count += 1
        if revision and validation_count == 1:
            return {"validation_error": "Resolve identifier representation"}
        return adapt_plan(payload["text"], INPUT, "Bronze")
    activities = [fake("create_migration_plan", str(uuid4())), fake("set_temporal_ids"),
        fake("validate_fabric", {"lakehouse_name": "Bronze"}),
        fake("opencode_create", {"session_id": "ses_test"}), fake("opencode_prompt"), poll,
        fake("opencode_reply"), fake("opencode_interrupt"),
        validate,
        fake("transition_plan"), fake("approve_runtime_plan"), fake("persist_runtime_plan"),
        fake("fail_migration"), fake("review_existing_target", {"definitions": []}),
        fake("validate_oracle_key", {"valid": True, "status": "CHECKED", "columns": ["ID"], "scope": "FULL_TABLE"})]
    async with await WorkflowEnvironment.start_local() as env:
        queue = str(uuid4())
        async with Worker(env.client, task_queue=queue, workflows=[OpenCodeOracleMigrationWorkflow], activities=activities):
            handle = await env.client.start_workflow(OpenCodeOracleMigrationWorkflow.run, INPUT,
                id=str(uuid4()), task_queue=queue)
            await _wait(handle, "WAITING_FOR_AGENT_INPUT")
            assert not persisted
            if cancel:
                await handle.signal("command", {"action": "cancel"})
                result = await handle.result()
                assert result["status"] == "CANCELLED"
                assert "opencode_interrupt" in calls
                assert not persisted
            else:
                await handle.signal("command", dict(action="agent_reply", actor="user", request_id="frm_test", answer={"q0": "Preserve"}))
                if revision:
                    await _wait(handle, "WAITING_FOR_AGENT_REVISION")
                    assert not persisted
                    await handle.signal("command", dict(action="agent_feedback", actor="user", message="Preserve identifier as STRING"))
                await _wait(handle, "WAITING_FOR_COLUMN_MAPPING_APPROVAL")
                assert not persisted
                await handle.signal("command", dict(action="approve_columns", actor="user", columns=[dict(name="ID", fabric_type="STRING")]))
                await _wait(handle, "WAITING_FOR_WATERMARK_APPROVAL")
                await handle.signal("command", dict(action="approve_watermark", actor="user", watermark_column=None))
                await _wait(handle, "WAITING_FOR_DELETE_POLICY_APPROVAL")
                await handle.signal("command", dict(action="approve_delete_policy", actor="user", delete_policy={"mode": "NONE"}))
                result = await handle.result()
                assert result["status"] == "PLANNED"
                assert persisted[0]["tables"][0]["columns"][0]["fabric_data_type"] == "STRING"
                assert "discover_oracle" not in calls
                assert calls.count("opencode_prompt") == (2 if revision else 1)
            await Replayer(workflows=[OpenCodeOracleMigrationWorkflow]).replay_workflow(await handle.fetch_history())


@pytest.mark.parametrize("cancel", [False, True])
def test_durable_agent_form_approval_and_replay(cancel):
    asyncio.run(_exercise(cancel))


def test_incomplete_plan_waits_for_revision_before_persistence():
    asyncio.run(_exercise(revision=True))

def test_plan_blockers_are_visible_and_schema_errors_exclude_input():
    payload = dict(input=INPUT, lakehouse_name='Bronze')
    result = oa.opencode_validate({**payload, 'text': json.dumps({**PLAN, 'unresolved_decisions': ['Verify watermark behavior']})})
    assert 'Verify watermark behavior' in result['validation_error']
    result = oa.opencode_validate({**payload, 'text': json.dumps({**PLAN, 'columns': 'secret-input'})})
    assert 'columns: list_type' in result['validation_error']
    assert 'secret-input' not in result['validation_error']
    assert 'complete JSON plan' in oa.opencode_validate({**payload, 'text': 'Thanks'})['validation_error']


def test_revision_prompt_preserves_contract_and_destination(monkeypatch):
    monkeypatch.setenv('OPENCODE_SOURCE_CONNECTIONS', '{"ORACLE-TEST":"oracle_local"}')
    calls = []
    def handler(request):
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={})
    monkeypatch.setattr(oa, '_client', lambda: httpx.Client(base_url='http://test', transport=httpx.MockTransport(handler)))
    oa.opencode_prompt(dict(input=INPUT, lakehouse_name='Bronze', session_id='ses', revision=1, feedback='Please revise'))
    text = calls[0]['text']
    assert INPUT['fabric_lakehouse_id'] in text
    assert 'Bronze' in text and 'schema_version' in text
    assert 'complete JSON' in text and 'Reviewer feedback' in text
    assert 'Do not update files' in text
    assert 'Put deferred runtime reviews' in text
    assert 'use null descriptions' in text


def test_poll_exposes_only_tool_names_and_status(monkeypatch):
    messages = [{'type':'assistant', 'content':[
        {'type':'reasoning','text':'private reasoning'},
        {'type':'tool','name':'oracle-sqlcl','state':{'status':'running','input':{'secret':'hidden'}}}]}]
    def handler(request):
        return httpx.Response(200, json={'data':messages if request.url.path.endswith('/message') else []})
    monkeypatch.setattr(oa, '_client', lambda: httpx.Client(base_url='http://test', transport=httpx.MockTransport(handler)))
    assert oa.opencode_poll({'session_id':'s'}) == {'status':'RUNNING','progress':[{'tool':'oracle-sqlcl','status':'running'}]}


def test_poll_exposes_nested_mcp_status_without_arguments(monkeypatch):
    messages = [{'type':'assistant', 'content':[{'type':'tool','name':'execute',
        'state':{'status':'completed','metadata':{'toolCalls':[
            {'tool':'oracle-sqlcl.sql_run','status':'completed','input':{'sql':'private'}}]}}}]}]
    def handler(request):
        return httpx.Response(200, json={'data':messages if request.url.path.endswith('/message') else []})
    monkeypatch.setattr(oa, '_client', lambda: httpx.Client(base_url='http://test', transport=httpx.MockTransport(handler)))
    assert oa.opencode_poll({'session_id':'s'})['progress'] == [{'tool':'oracle-sqlcl.sql_run','status':'completed'}]
