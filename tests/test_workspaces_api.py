from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.migration import workspaces_api


def test_workspace_dropdown_lists_active_registry_rows(monkeypatch):
    class FakeDB:
        def list_fabric_workspaces(self, *, active_only=False):
            assert active_only
            return [SimpleNamespace(
                workspace_name="Test_Gopi",
                workspace_id="5a590541-0088-465b-b8a3-d7609f270a5f")]

    monkeypatch.setattr(workspaces_api, "ConfigDB", FakeDB)
    response = TestClient(app).get("/workspaces")
    assert response.status_code == 200
    assert response.json() == [{
        "workspace_name": "Test_Gopi",
        "workspace_id": "5a590541-0088-465b-b8a3-d7609f270a5f"}]


def test_lakehouses_are_listed_only_for_an_active_workspace(monkeypatch):
    workspace_id = "5a590541-0088-465b-b8a3-d7609f270a5f"

    class FakeDB:
        def list_fabric_workspaces(self, *, active_only=False):
            assert active_only
            return [SimpleNamespace(workspace_id=workspace_id)]

    class FakeFabric:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def list_lakehouses(self, selected_workspace_id):
            assert selected_workspace_id == workspace_id
            return [SimpleNamespace(id="00000000-0000-0000-0000-000000000002",
                                    display_name="Silver"),
                    SimpleNamespace(id="64d625a2-5ca0-432e-a6b7-522211c89352",
                                    display_name="lh_bronze")]

    monkeypatch.setattr(workspaces_api, "ConfigDB", FakeDB)
    monkeypatch.setattr(workspaces_api, "FabricClient", FakeFabric)
    client = TestClient(app)
    response = client.get(f"/workspaces/{workspace_id}/lakehouses")
    assert response.status_code == 200
    assert response.json() == [
        {"lakehouse_name": "lh_bronze", "lakehouse_id": "64d625a2-5ca0-432e-a6b7-522211c89352"},
        {"lakehouse_name": "Silver", "lakehouse_id": "00000000-0000-0000-0000-000000000002"},
    ]
    assert client.get("/workspaces/00000000-0000-0000-0000-000000000001/lakehouses").status_code == 404
