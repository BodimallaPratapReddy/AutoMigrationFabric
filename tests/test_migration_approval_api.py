from fastapi.testclient import TestClient
from temporalio.client import WorkflowExecutionStatus

from app.main import app
from app.migration.api import get_temporal_client


class FakeHandle:
    def __init__(self):
        self.signals = []
        self.phase = "WAITING_FOR_COLUMN_MAPPING_APPROVAL"

    async def describe(self):
        class Description:
            status = WorkflowExecutionStatus.RUNNING
        return Description()

    async def query(self, name):
        assert name == "get_state"
        return {
            "workflow_id": "oracle-test", "migration_approach": "ORACLE_TABLE",
            "source_object_name": "ORDERS", "phase": self.phase,
            "status": "RUNNING", "waiting_for_user": True,
            "review": {"primary_key": ["ORDER_ID"],
                       "columns": [{"name": "ORDER_ID", "source_type": "NUMBER",
                                    "fabric_type": "STRING"}],
                       "watermark_candidates": [{
                "column_name": "LAST_UPDATED_DATE", "data_type": "DATE",
                "index_details": [{"index_name": "IX_UPDATED", "column_position": 1}]}]},
        }

    async def signal(self, name, payload):
        self.signals.append((name, payload))


class FakeClient:
    def __init__(self):
        self.handle = FakeHandle()

    def get_workflow_handle(self, workflow_id):
        assert workflow_id == "oracle-test"
        return self.handle


def test_delete_approval_validates_key_field_values_interval_and_watermark_confirmation():
    temporal = FakeClient()
    temporal.handle.phase = "WAITING_FOR_DELETE_POLICY_APPROVAL"
    original = temporal.handle.query
    async def delete_review(name):
        state = await original(name)
        state["review"].update(key_validation={"valid": True, "columns": ["ORDER_ID"]},
                               selected_watermark="LAST_UPDATED_DATE")
        state["review"]["columns"].append({"name": "DELETED", "fabric_type": "STRING"})
        return state
    temporal.handle.query = delete_review
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    policy = {"mode": "SOFT_DELETE_AND_RECONCILE", "behavior": "MARK",
              "soft_delete_column": "DELETED", "soft_delete_values": ["Y"],
              "watermark_tracks_soft_delete": True, "reconcile_interval_minutes": 1440}
    try:
        client = TestClient(app)
        route = "/migrations/oracle-test/approve-delete-policy"
        body = {"approved_by": "reviewer", "delete_policy": policy}
        for change in [{"soft_delete_column": "MISSING"}, {"soft_delete_values": []},
                       {"watermark_tracks_soft_delete": False}, {"reconcile_interval_minutes": -1}]:
            assert client.post(route, json={**body, "delete_policy": {**policy, **change}}).status_code == 422
        assert temporal.handle.signals == []
        assert client.post(route, json=body).status_code == 200
        saved = temporal.handle.signals[-1][1]["delete_policy"]
        assert saved["reconcile_require_complete_snapshot"] is True
        assert saved["soft_delete_values"] == ["Y"]
        temporal.handle.phase = "WAITING_FOR_WATERMARK_APPROVAL"
        assert client.post(route, json=body).status_code == 409
    finally:
        app.dependency_overrides.clear()


def test_failed_database_key_check_allows_an_explicit_no_key_decision():
    temporal = FakeClient()
    temporal.handle.phase = "WAITING_FOR_PRIMARY_KEY_APPROVAL"
    original = temporal.handle.query
    async def failed_review(name):
        state = await original(name)
        state["review"].update(primary_key_source="DATABASE", key_validation={"valid": False})
        return state
    temporal.handle.query = failed_review
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        response = TestClient(app).post("/migrations/oracle-test/approve-primary-key", json={
            "approved_by": "reviewer", "primary_key_columns": []})
        assert response.status_code == 200
    finally:
        app.dependency_overrides.clear()


def test_manual_primary_key_api_validates_columns_and_explicit_no_key():
    temporal = FakeClient()
    original = temporal.handle.query
    async def no_key_review(name):
        state = await original(name)
        state["review"]["primary_key"] = []
        state["review"]["columns"].append({"name": "TENANT", "fabric_type": "STRING"})
        return state
    temporal.handle.query = no_key_review
    temporal.handle.phase = "WAITING_FOR_PRIMARY_KEY_APPROVAL"
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        client = TestClient(app)
        body = {"approved_by": "reviewer", "primary_key_columns": ["ORDER_ID", "TENANT"]}
        assert client.post("/migrations/oracle-test/approve-primary-key", json=body).status_code == 200
        assert temporal.handle.signals[-1][1]["primary_key_columns"] == body["primary_key_columns"]
        for keys in [["UNKNOWN"], ["ORDER_ID", "ORDER_ID"], [""]]:
            assert client.post("/migrations/oracle-test/approve-primary-key", json={
                **body, "primary_key_columns": keys}).status_code == 422
        assert len(temporal.handle.signals) == 1
        assert client.post("/migrations/oracle-test/approve-primary-key", json={
            **body, "primary_key_columns": []}).status_code == 200
        assert temporal.handle.signals[-1][1]["primary_key_columns"] == []
        assert client.post("/migrations/oracle-test/approve-primary-key", json={
            "approved_by": "reviewer"}).status_code == 422
        temporal.handle.phase = "WAITING_FOR_WATERMARK_APPROVAL"
        assert client.post("/migrations/oracle-test/edit-primary-key").status_code == 200
        assert client.post("/migrations/oracle-test/approve-primary-key", json=body).status_code == 409
    finally:
        app.dependency_overrides.clear()


def test_database_primary_key_cannot_be_overridden():
    temporal = FakeClient()
    temporal.handle.phase = "WAITING_FOR_WATERMARK_APPROVAL"
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        assert TestClient(app).post("/migrations/oracle-test/edit-primary-key").status_code == 409
        assert temporal.handle.signals == []
    finally:
        app.dependency_overrides.clear()


def test_sap_watermark_approval_accepts_only_discovered_columns_and_requires_key():
    temporal = FakeClient()
    original = temporal.handle.query
    has_key = True
    async def sap_review(name):
        state = await original(name)
        state["migration_approach"] = "SAP_TABLE"
        state["phase"] = "WAITING_FOR_WATERMARK_APPROVAL"
        state["review"]["primary_key"] = ["MANDT", "VBELN"] if has_key else []
        state["review"]["watermark_candidates"] = [{"column_name": "AEDAT", "data_type": "DATS"}]
        return state
    temporal.handle.query = sap_review
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        client = TestClient(app)
        body = {"approved_by": "reviewer", "watermark_column": "AEDAT"}
        assert client.post("/migrations/oracle-test/approve-watermark", json=body).status_code == 200
        assert temporal.handle.signals[-1][1]["watermark_column"] == "AEDAT"
        assert client.post("/migrations/oracle-test/approve-watermark", json={
            **body, "watermark_column": "ERZET"}).status_code == 422
        has_key = False
        assert client.post("/migrations/oracle-test/approve-watermark", json=body).status_code == 422
        assert client.post("/migrations/oracle-test/approve-watermark", json={
            **body, "watermark_column": None}).status_code == 200
    finally:
        app.dependency_overrides.clear()


def test_oracle_mapping_and_watermark_are_separate_approvals():
    temporal = FakeClient()
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        client = TestClient(app)
        response = client.post("/migrations/oracle-test/approve-columns", json={
            "approved_by": "reviewer", "columns": [{"name": "ORDER_ID", "fabric_type": "decimal(18,0)"}]})
        assert response.status_code == 200
        assert temporal.handle.signals == [("command", {
            "action": "approve_columns", "actor": "reviewer", "message": None,
            "watermark_column": None,
            "columns": [{"name": "ORDER_ID", "fabric_type": "DECIMAL(18,0)"}]})]
        invalid = client.post("/migrations/oracle-test/approve-columns", json={
            "approved_by": "reviewer", "columns": [{"name": "ORDER_ID", "fabric_type": "EXEC SQL"}]})
        assert invalid.status_code == 422
        assert len(temporal.handle.signals) == 1
        temporal.handle.phase = "WAITING_FOR_WATERMARK_APPROVAL"
        back = client.post("/migrations/oracle-test/edit-columns")
        assert back.status_code == 200
        assert temporal.handle.signals[-1][1]["action"] == "edit_columns"
        selected = client.post("/migrations/oracle-test/approve-watermark", json={
            "approved_by": "reviewer", "watermark_column": "LAST_UPDATED_DATE"})
        assert selected.status_code == 200
        assert temporal.handle.signals[-1][1]["watermark_column"] == "LAST_UPDATED_DATE"
        invalid = client.post("/migrations/oracle-test/approve-watermark", json={
            "approved_by": "reviewer", "watermark_column": "NOT_A_CANDIDATE"})
        assert invalid.status_code == 422
        assert len(temporal.handle.signals) == 3
        full = client.post("/migrations/oracle-test/approve-watermark", json={
            "approved_by": "reviewer", "watermark_column": None})
        assert full.status_code == 200
        assert temporal.handle.signals[-1][1]["watermark_column"] is None
    finally:
        app.dependency_overrides.clear()


def test_unsupported_oracle_mapping_requires_acknowledgement():
    temporal = FakeClient()
    original_query = temporal.handle.query

    async def review_with_unsupported(name):
        state = await original_query(name)
        state["review"]["columns"][0]["supported"] = False
        return state

    temporal.handle.query = review_with_unsupported
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        client = TestClient(app)
        body = {"approved_by": "reviewer", "columns": [
            {"name": "ORDER_ID", "fabric_type": "STRING"}]}
        response = client.post("/migrations/oracle-test/approve-columns", json=body)
        assert response.status_code == 422
        assert temporal.handle.signals == []
        response = client.post("/migrations/oracle-test/approve-columns", json={
            **body, "acknowledge_unsupported": True})
        assert response.status_code == 200
        assert temporal.handle.signals[0][1]["action"] == "approve_columns"
    finally:
        app.dependency_overrides.clear()


def test_target_change_choice_is_limited_to_existing_target_state():
    temporal = FakeClient()
    original_query = temporal.handle.query

    async def target_review(name):
        state = await original_query(name)
        state["phase"] = "WAITING_FOR_TARGET_CHANGE_APPROVAL"
        state["review"]["target_review"] = {"is_provisioned_record": False}
        return state

    temporal.handle.query = target_review
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        client = TestClient(app)
        invalid = client.post("/migrations/oracle-test/approve-target-change", json={
            "approved_by": "reviewer", "decision": "REPLACE_FULL"})
        assert invalid.status_code == 422
        assert temporal.handle.signals == []
        valid = client.post("/migrations/oracle-test/approve-target-change", json={
            "approved_by": "reviewer", "decision": "REVISE_PLANNED"})
        assert valid.status_code == 200
        assert temporal.handle.signals[-1][1]["decision"] == "REVISE_PLANNED"
    finally:
        app.dependency_overrides.clear()


def test_completed_workflow_status_uses_stored_result_without_replay():
    temporal = FakeClient()

    async def completed_description():
        class Description:
            status = WorkflowExecutionStatus.COMPLETED
        return Description()

    async def completed_result():
        return {"workflow_id": "oracle-test", "migration_approach": "ORACLE_TABLE",
                "source_object_name": "ORDERS", "phase": "FAILED", "status": "FAILED",
                "waiting_for_user": False, "last_error": "CREATED: ActivityError"}

    async def should_not_query(name):
        raise AssertionError("Closed workflows should not be queried")

    temporal.handle.describe = completed_description
    temporal.handle.result = completed_result
    temporal.handle.query = should_not_query
    app.dependency_overrides[get_temporal_client] = lambda: temporal
    try:
        response = TestClient(app).get("/migrations/oracle-test")
        assert response.status_code == 200
        assert response.json()["last_error"] == "CREATED: ActivityError"
    finally:
        app.dependency_overrides.clear()
