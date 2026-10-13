import json

import httpx
import pytest
from temporalio.exceptions import ApplicationError

from app.migration import opencode_activities as oa


def setup(monkeypatch, handler):
    monkeypatch.setenv('OPENCODE_DIRECTORY', '/project')
    monkeypatch.setenv('OPENCODE_MODEL', 'old-model')
    monkeypatch.setattr(oa, '_prompt', lambda _: 'validated')
    monkeypatch.setattr(oa, '_client', lambda: httpx.Client(
        base_url='http://test', transport=httpx.MockTransport(handler)))


PAYLOAD = {'workflow_id': 'model-test', 'input': {
    'migration_approach': 'SAP_REBUILD', 'source_object_name': '0FI_GL_4'}}


def test_agent_catalog_not_ready_can_be_retried(monkeypatch):
    def handler(request):
        if request.url.path == '/api/agent':
            return httpx.Response(200, json={'data': []})
        return httpx.Response(404)
    setup(monkeypatch, handler)
    with pytest.raises(ApplicationError, match='not available') as caught:
        oa.opencode_create(PAYLOAD)
    assert not caught.value.non_retryable


def test_creation_uses_deployed_agent_model_not_global_default(monkeypatch):
    model = {'providerID': 'amazon-bedrock', 'id': 'new-model', 'variant': 'medium'}
    posted = []

    def handler(request):
        if request.url.path == '/api/agent':
            return httpx.Response(200, json={'data': [
                {'id': 'sap-datasource-rebuild-to-fabric', 'model': model}]})
        if request.method == 'GET':
            return httpx.Response(404)
        posted.append(json.loads(request.content))
        return httpx.Response(201, json={'data': {}})

    setup(monkeypatch, handler)
    oa.opencode_create(PAYLOAD)
    assert posted[0]['model'] == model
    assert posted[0]['agent'] == 'sap-datasource-rebuild-to-fabric'


@pytest.mark.parametrize(('status', 'non_retryable'), [(401, True), (503, False), (429, False)])
def test_lookup_failure_reports_status_and_preserves_retryability(monkeypatch, status, non_retryable):
    setup(monkeypatch, lambda _: httpx.Response(status))
    with pytest.raises(ApplicationError, match=f'HTTP {status}') as caught:
        oa.opencode_create(PAYLOAD)
    assert caught.value.non_retryable is non_retryable
