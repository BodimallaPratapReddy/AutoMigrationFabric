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
