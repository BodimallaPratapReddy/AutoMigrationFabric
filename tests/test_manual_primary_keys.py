"""User-declared keys, no-key decisions, and compatibility with prior workflows."""
import asyncio
from types import SimpleNamespace

import pytest

from app.migration import workflow_base


@pytest.mark.parametrize("approach", ["ORACLE_TABLE", "SAP_TABLE"])
@pytest.mark.parametrize("selected", [["ID", "TENANT"], []])
def test_key_decision_precedes_watermark_and_is_persisted(monkeypatch, approach, selected):
    monkeypatch.setattr(workflow_base.workflow, "info", lambda: SimpleNamespace(
        workflow_id="test", run_id="run"))
    monkeypatch.setattr(workflow_base.workflow, "patched", lambda name:
                        name != "oracle-key-validation-delete-policy-v1")

    class Harness(workflow_base.MigrationWorkflowBase):
        def __init__(self):
            super().__init__()
            self.phases = []
            self.persisted = None

        async def _gate(self, phase, actions):
            self.phases.append(phase)
            assert self.persisted is None
            if phase == "WAITING_FOR_PRIMARY_KEY_APPROVAL":
                return {"action": "approve_primary_key", "actor": "reviewer",
                        "primary_key_columns": selected}
            if phase == "WAITING_FOR_COLUMN_MAPPING_APPROVAL":
                return {"action": "approve_columns", "actor": "reviewer", "columns": [
                    {"name": name, "fabric_type": kind} for name, kind in
                    [("ID", "BIGINT"), ("TENANT", "STRING"), ("UPDATED", "TIMESTAMP")]]}
            if phase == "WAITING_FOR_PLAN_APPROVAL":
                return {"action": "approve_plan", "actor": "reviewer"}
            assert phase == "WAITING_FOR_WATERMARK_APPROVAL"
            assert self.state["review"]["primary_key"] == selected
            return {"action": "approve_watermark", "actor": "reviewer",
                    "watermark_column": "UPDATED" if selected else None}

        async def _activity(self, name, payload, **kwargs):
            if name == "create_migration_plan": return "plan"
            if name == "validate_fabric": return {"lakehouse_name": "Bronze"}
            if name.startswith("discover_"):
                columns = [{"column_name": name, "fabric_data_type": kind,
                            "is_primary_key": False} for name, kind in
                           [("ID", "BIGINT"), ("TENANT", "STRING"), ("UPDATED", "TIMESTAMP")]]
                return {"table_plan": {"columns": columns, "replication": {
                    "primary_key_columns": None, "incremental_method": "FULL",
                    "write_strategy": "REPLACE"}}, "review": {"primary_key": [],
                    "columns": [{"name": c["column_name"], "fabric_type": c["fabric_data_type"]}
                                for c in columns], "watermark_candidates": [
                        {"column_name": "UPDATED", "data_type": "TIMESTAMP", "index_details": []}]}}
            if name == "review_existing_target": return {"definitions": []}
            if name == "persist_runtime_plan": self.persisted = payload

    harness = Harness()
    result = asyncio.run(harness._run({"migration_approach": approach,
        "source_object_name": "T", "provisioning_notebook_id": None}, "discover_test"))
    assert result["status"] == "PLANNED"
    assert harness.phases[1:] == ["WAITING_FOR_PRIMARY_KEY_APPROVAL", "WAITING_FOR_WATERMARK_APPROVAL"]
    table = harness.persisted["tables"][0]
    assert table["replication"]["primary_key_columns"] == (selected or None)
    assert [c["column_name"] for c in table["columns"] if c["is_primary_key"]] == selected
    assert table["replication"]["incremental_method"] == ("WATERMARK" if selected else "FULL")
    assert table["replication"]["write_strategy"] == ("UPSERT" if selected else "REPLACE")
    assert result["review"]["primary_key_source"] == ("USER" if selected else "NONE")
    assert result["review"]["primary_key_approved_by"] == "reviewer"


@pytest.mark.parametrize("invalid", [["UNKNOWN"], ["ID", "ID"], [""], "ID", [1]])
def test_invalid_direct_key_signal_does_not_fail_or_advance_workflow(monkeypatch, invalid):
    class Harness(workflow_base.MigrationWorkflowBase):
        async def _gate(self, phase, actions):
            return {"action": "approve_primary_key", "actor": "reviewer",
                    "primary_key_columns": self.commands.pop(0)}
    h = Harness()
    h.commands = [invalid, ["ID"]]
    h.state = {"review": {}}
    plan = {"table_plan": {"columns": [{"column_name": "ID"}], "replication": {}}}
    asyncio.run(h._choose_primary_key(plan))
    assert h.commands == []
    assert h.state["review"]["primary_key"] == ["ID"]
    assert "primary_key_error" not in h.state["review"]


def test_existing_watermark_wait_can_open_key_step(monkeypatch):
    monkeypatch.setattr(workflow_base.workflow, "patched", lambda name: True)
    class Harness(workflow_base.MigrationWorkflowBase):
        async def _gate(self, phase, actions):
            self.phases.append(phase)
            if len(self.phases) == 1: return {"action": "edit_primary_key"}
            if phase == "WAITING_FOR_PRIMARY_KEY_APPROVAL":
                return {"action": "approve_primary_key", "primary_key_columns": ["ID"], "actor": "reviewer"}
            return {"action": "approve_watermark", "watermark_column": "UPDATED"}
    h = Harness()
    h.phases = []
    h.state = {"review": {"primary_key": []}}
    plan = {"table_plan": {"columns": [{"column_name": "ID"}], "replication": {}}}
    result = asyncio.run(h._watermark_gate(plan, review_key=False))
    assert result["watermark_column"] == "UPDATED"
    assert h.phases == ["WAITING_FOR_WATERMARK_APPROVAL", "WAITING_FOR_PRIMARY_KEY_APPROVAL",
                        "WAITING_FOR_WATERMARK_APPROVAL"]
