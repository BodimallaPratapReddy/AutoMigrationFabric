"""Workspace projections use real outcomes, redact start inputs, and paginate."""
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

from fastapi.testclient import TestClient
from temporalio.client import WorkflowExecutionStatus
from temporalio.api.history.v1 import HistoryEvent, WorkflowExecutionSignaledEventAttributes
from temporalio.converter import DataConverter
import pytest

from app.main import app
from app.migration.api import get_temporal_client
from app.migration import workspace_api as workspace


class FakePage:
    current_page = []
    next_page_token = b"next-page"

    async def fetch_next_page(self):
        pass


class FakeClient:
    def __init__(self):
        self.page = FakePage()
        self.handle = SimpleNamespace(result=AsyncMock(return_value={
            "status": "FAILED", "source_object_name": "ORDERS", "phase": "FAILED"}),
            query=AsyncMock(side_effect=RuntimeError("Worker offline")))

    def list_workflows(self, query, **kwargs):
        self.query = query
        self.options = kwargs
        return self.page

    def get_workflow_handle(self, *args, **kwargs):
        return self.handle


def execution(status=WorkflowExecutionStatus.COMPLETED):
    return SimpleNamespace(id="oracle-table-test", run_id="run", status=status,
                           start_time=datetime(2026,10,13,tzinfo=timezone.utc), close_time=None)


@pytest.fixture
def api_client(monkeypatch):
    temporal=FakeClient()
    app.dependency_overrides[get_temporal_client]=lambda: temporal
    monkeypatch.setattr(workspace,"source_input",AsyncMock(return_value={
        "source_object_name":"ORDERS","source_connection_name":"Oracle",
        "migration_approach":"ORACLE_TABLE","password":"must-not-escape"}))
    try:
        yield TestClient(app),temporal
    finally:
        app.dependency_overrides.clear()


def test_list_preserves_failed_business_outcome_and_redacts_credentials(api_client):
    client,temporal=api_client
    temporal.page.current_page=[execution()]
    response=client.get('/workspace/jobs?limit=5')
    assert response.status_code==200
    job=response.json()['jobs'][0]
    assert job['status']=='FAILED'
    assert job['execution_status']=='COMPLETED'
    assert job['state_available'] is True
    assert 'state' not in job
    assert 'must-not-escape' not in response.text
    assert temporal.options['page_size']==5
    assert workspace.decode_cursor(response.json()['next_cursor'])==b'next-page'


def test_unavailable_worker_does_not_turn_running_job_into_success(api_client):
    client,temporal=api_client
    temporal.page.current_page=[execution(WorkflowExecutionStatus.RUNNING)]
    job=client.get('/workspace/jobs').json()['jobs'][0]
    assert job['status']=='RUNNING'
    assert job['state_available'] is False
    assert job['waiting_for_user'] is False
    assert job['state_message']


def test_terminated_jobs_remain_browsable_without_querying_worker(api_client):
    client,temporal=api_client
    temporal.page.current_page=[execution(WorkflowExecutionStatus.TERMINATED)]
    job=client.get('/workspace/jobs').json()['jobs'][0]
    assert job['status']=='TERMINATED'
    temporal.handle.query.assert_not_awaited()


def test_invalid_cursor_and_page_size_are_rejected(api_client):
    client,_=api_client
    assert client.get('/workspace/jobs?cursor=not!base64').status_code==422
    assert client.get('/workspace/jobs?limit=10000').status_code==422


def test_visibility_error_returns_retryable_service_failure(api_client):
    client,temporal=api_client
    temporal.page.fetch_next_page=AsyncMock(side_effect=RuntimeError('internal credentials'))
    response=client.get('/workspace/jobs')
    assert response.status_code==503
    assert 'internal credentials' not in response.text


def test_decision_event_includes_actor_and_selection_not_arbitrary_payload():
    converter=DataConverter.default
    payloads=asyncio.run(converter.encode([{'approved_by':'Reviewer','primary_key_columns':['ID'],
                                     'password':'must-not-escape'}]))
    event=HistoryEvent(event_id=8,workflow_execution_signaled_event_attributes=
        WorkflowExecutionSignaledEventAttributes(signal_name='approve_primary_key'))
    event.workflow_execution_signaled_event_attributes.input.payloads.extend(payloads)
    result=asyncio.run(workspace.event_projection(SimpleNamespace(data_converter=converter),event))
    assert result['actor']=='Reviewer'
    assert result['details']=={'primary_key_columns':['ID']}
    assert result['title']=='Record identifiers confirmed'


def test_command_signal_uses_approval_action_and_actor():
    converter=DataConverter.default
    payloads=asyncio.run(converter.encode([{'action':'approve_columns','actor':'Pratap','password':'private'}]))
    event=HistoryEvent(event_id=9,workflow_execution_signaled_event_attributes=
        WorkflowExecutionSignaledEventAttributes(signal_name='command'))
    event.workflow_execution_signaled_event_attributes.input.payloads.extend(payloads)
    result=asyncio.run(workspace.event_projection(SimpleNamespace(data_converter=converter),event))
    assert result['title']=='Column mappings approved'
    assert result['actor']=='Pratap'
    assert result['details']=={}

def test_pagination_defaults_to_twenty_and_passes_cursor_to_temporal(api_client):
    client, temporal = api_client
    assert client.get('/workspace/jobs').status_code == 200
    assert temporal.options['page_size'] == 20
    cursor = workspace.encode_cursor(b'page-two')
    response = client.get('/workspace/jobs', params={'limit': 50, 'cursor': cursor})
    assert response.status_code == 200
    assert temporal.options['page_size'] == 50
    assert temporal.options['next_page_token'] == b'page-two'
