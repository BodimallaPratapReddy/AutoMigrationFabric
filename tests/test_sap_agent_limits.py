import json

import httpx
import pytest

from app.migration.sap_agent_limits import blocked_result
from app.migration.opencode_activities import opencode_validate


REPORT = dict(kind='SAP_AGENT_BLOCKED', destination='ECC-LOCAL', blocker='Unresolved currency behavior',
              attempts=5, attempted_actions=['Read source', 'Consulted ABAP docs'],
              documentation_references=['ABAP COND documentation'], required_resolution='Provide missing source include')


def test_terminal_blocker_does_not_discover_or_persist(monkeypatch):
    monkeypatch.setattr('app.migration.sap_mcp_plan.mapped_destination', lambda _: 'ECC-LOCAL')
    monkeypatch.setattr('app.migration.sap_mcp_delivery.handle_rebuild_delivery',
                        lambda _: pytest.fail('Terminal blocker must not assemble a plan'))
    result = opencode_validate(dict(input={'sap_mcp_discovery': True}, text=json.dumps(REPORT)))
    assert 'stopped after 5' in result['failure_reason']
    assert result['agent_blocker'] == REPORT


def test_terminal_blocker_requires_matching_destination_and_bounded_attempts():
    with pytest.raises(ValueError):
        blocked_result(REPORT, 'OTHER')
    with pytest.raises(ValueError):
        blocked_result({**REPORT, 'attempts': 6}, 'ECC-LOCAL')


@pytest.mark.parametrize(('approach', 'agent'), [
    ('SAP_ODP', 'sap-odp-to-fabric'),
    ('SAP_REBUILD', 'sap-datasource-rebuild-to-fabric'),
])
def test_new_sessions_use_dedicated_agents(monkeypatch, approach, agent):
    from app.migration import opencode_activities as oa
    monkeypatch.setenv('OPENCODE_DIRECTORY', '/project')
    monkeypatch.setenv('OPENCODE_SAP_DATASOURCE_AGENT', 'old-shared-agent')
    monkeypatch.delenv('OPENCODE_SAP_ODP_AGENT', raising=False)
    monkeypatch.delenv('OPENCODE_SAP_REBUILD_AGENT', raising=False)
    monkeypatch.setattr(oa, '_prompt', lambda _: 'verified prompt')
    posted = []

    def handler(request):
        if request.url.path == '/api/agent':
            return httpx.Response(200, json={'data': [{'id': agent, 'model': None}]})
        if request.method == 'GET':
            return httpx.Response(404)
        posted.append(json.loads(request.content))
        return httpx.Response(200, json={'data': {}})

    monkeypatch.setattr(oa, '_client', lambda: httpx.Client(base_url='http://test',
                         transport=httpx.MockTransport(handler)))
    oa.opencode_create({'workflow_id': 'test', 'input': {'migration_approach': approach,
                        'source_object_name': 'ZDS'}})
    assert posted[0]['agent'] == agent

