import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.migration import api, migration_types


class FakeRegistry:
    def get_connection(self, name):
        if name == "Oracle Local":
            return SimpleNamespace(source_type="ORACLE", is_active=True)
        if name == "SAP Local":
            return SimpleNamespace(source_type="SAP_ECC", is_active=True)
        return None


class FakeWorkspaces:
    def list_fabric_workspaces(self, *, active_only=False):
        assert active_only
        return [SimpleNamespace(workspace_id="5a590541-0088-465b-b8a3-d7609f270a5f")]


class FakeTemporal:
    def __init__(self):
        self.started = []

    async def start_workflow(self, *args, **kwargs):
        self.started.append((args, kwargs))
        return SimpleNamespace(result_run_id="run-1")


def _input(connection, approach):
    return {
        "source_connection_name": connection,
        "migration_approach": approach,
        "source_schema_name": "ONT" if approach == "ORACLE_TABLE" else None,
        "source_object_name": "ORDERS",
        "fabric_workspace_id": "5a590541-0088-465b-b8a3-d7609f270a5f",
        "fabric_lakehouse_id": "64d625a2-5ca0-432e-a6b7-522211c89352",
        "fabric_schema_name": "bronze",
    }


def test_options_and_start_use_connection_type(monkeypatch):
    temporal = FakeTemporal()
    monkeypatch.setattr(api, "ConfigDBRepository", FakeRegistry)
    monkeypatch.setattr(api, "ConfigDB", FakeWorkspaces)
    app.dependency_overrides[api.get_temporal_client] = lambda: temporal
    try:
        client = TestClient(app)
        options = client.get("/migrations/options")
        assert options.status_code == 200
        assert [item["value"] for item in options.json()["ORACLE"]] == ["ORACLE_TABLE"]
        assert [item["value"] for item in options.json()["SAP_ECC"]] == [
            "SAP_TABLE", "SAP_ODP", "SAP_REBUILD"]
        mismatch = client.post("/migrations/sap-table", json=_input("Oracle Local", "SAP_TABLE"))
        assert mismatch.status_code == 422
        assert temporal.started == []
        unsupported_workspace = client.post("/migrations/oracle-table", json={
            **_input("Oracle Local", "ORACLE_TABLE"),
            "fabric_workspace_id": "00000000-0000-0000-0000-000000000001"})
        assert unsupported_workspace.status_code == 422
        assert temporal.started == []
        legacy = client.post("/migrations/oracle-table", json={
            **_input("Oracle Local", "ORACLE_TABLE"),
            "replication_pipeline_id": "00000000-0000-0000-0000-000000000004"})
        assert legacy.status_code == 422
        assert temporal.started == []
        valid = client.post("/migrations/oracle-table", json=_input("Oracle Local", "ORACLE_TABLE"))
        assert valid.status_code == 200
        assert len(temporal.started) == 1
    finally:
        app.dependency_overrides.pop(api.get_temporal_client, None)


def test_config_cannot_enable_a_workflow_for_the_wrong_source(monkeypatch, tmp_path):
    path = tmp_path / "migration_types.json"
    path.write_text(json.dumps({"ORACLE": [{"value": "SAP_TABLE", "label": "SAP table"}]}),
                    encoding="utf-8")
    monkeypatch.setattr(migration_types, "CONFIG_PATH", path)
    with pytest.raises(ValueError, match="Invalid migration type option"):
        migration_types.load_migration_types()
