"""SAP metadata, approval sequencing, and persisted watermark contract."""
import asyncio
from types import SimpleNamespace

import pytest

from app.migration import source_activities, workflow_base
from thirdparty.sap.utils import SAPFieldMetadata, is_sap_watermark_field


def field(kind, rollname="", length=8, scale=0):
    return SAPFieldMetadata(fieldname="VALUE", datatype=kind, rollname=rollname,
        leng=length, decimals=scale, position=1, keyflag="", checktable="",
        reftable="", reffield="", ddtext="")


@pytest.mark.parametrize("metadata,expected", [
    (field("DATS"), True), (field("TIMS", length=6), False),
    (field("CHAR"), False), (field("DEC", length=21, scale=7), False),
    (field("DEC", "TZNTSTMPL", 21, 7), True),
    (field("DEC", "TIMESTAMP", 15), True), (field("UTCLONG"), True),
])
def test_watermark_eligibility_uses_types_and_timestamp_data_elements(metadata, expected):
    assert is_sap_watermark_field(metadata) is expected


@pytest.mark.parametrize("watermark", ["AEDAT", None])
@pytest.mark.parametrize("patched", [True, False])
def test_sap_mapping_then_watermark_then_persistence(monkeypatch, watermark, patched):
    monkeypatch.setattr(workflow_base.workflow, "info", lambda: SimpleNamespace(
        workflow_id="sap-test", run_id="run-test"))
    monkeypatch.setattr(workflow_base.workflow, "patched", lambda name: patched)
    class Harness(workflow_base.MigrationWorkflowBase):
        def __init__(self):
            super().__init__()
            self.phases = []
            self.persisted = None
            self.persistence_options = None
        async def _gate(self, phase, actions):
            self.phases.append(phase)
            assert self.persisted is None
            return {"action": "approve_watermark" if "WATERMARK" in phase else "approve_plan",
                    "actor": "reviewer", "watermark_column": watermark if "WATERMARK" in phase else None}
        async def _activity(self, name, payload, **options):
            if name == "create_migration_plan":
                return "plan-guid"
            if name == "validate_fabric":
                return {"lakehouse_name": "Bronze"}
            if name == "discover_sap_table":
                return {"table_plan": {"columns": [], "replication": {
                    "primary_key_columns": ["MANDT", "VBELN"], "incremental_method": "FULL",
                    "write_strategy": "REPLACE"}}, "review": {"columns": [],
                    "watermark_candidates": [{"column_name": "AEDAT", "data_type": "DATS",
                                              "index_details": []}]}}
            if name == "persist_runtime_plan":
                self.persisted = payload
                self.persistence_options = options
            if name == "review_existing_target":
                return {"definitions": []}
    harness = Harness()
    result = asyncio.run(harness._run({"migration_approach": "SAP_TABLE",
        "source_object_name": "VBAK", "provisioning_notebook_id": None}, "discover_sap_table"))
    assert result["status"] == "PLANNED"
    assert harness.phases == (["WAITING_FOR_PLAN_APPROVAL", "WAITING_FOR_WATERMARK_APPROVAL"]
                              if patched else ["WAITING_FOR_PLAN_APPROVAL"])
    config = harness.persisted["tables"][0]["replication"]
    if patched and watermark:
        assert config["incremental_method"] == "WATERMARK"
        assert config["watermark_column"] == "AEDAT"
        assert config["watermark_column_data_type"] == "DATS"
        assert config["watermark_index_name"] is None
        assert config["write_strategy"] == "UPSERT"
    else:
        assert config["incremental_method"] == "FULL"
        assert config["write_strategy"] == "REPLACE"
    if patched:
        assert harness.persistence_options["timeout"] == 180
        assert harness.persistence_options["retry"].maximum_attempts == 3


def test_sap_discovery_normalizes_candidate_contract(monkeypatch):
    from thirdparty.sap.utils import SAPTableSchema, SAPWatermarkCandidate
    class Client:
        def __enter__(self): return self
        def __exit__(self, *_): pass
        def test_connection(self): pass
        def table_exists(self, _): return True
        def get_table_schema(self, _):
            return SAPTableSchema(table_name="T", columns=[field("DATS")], primary_key_columns=[])
        def get_watermark_candidates(self, _):
            return [SAPWatermarkCandidate(field_name="VALUE", datatype="DATS", reason="SAP date")]
    monkeypatch.setattr(source_activities, "sap_client", lambda _: Client())
    result = source_activities.discover_sap_table({"input": {
        "migration_approach": "SAP_TABLE", "source_connection_name": "SAP",
        "source_object_name": "T", "fabric_workspace_id": "00000000-0000-0000-0000-000000000001",
        "fabric_lakehouse_id": "00000000-0000-0000-0000-000000000002", "fabric_schema_name": "bronze"},
        "lakehouse_name": "Bronze"})
    candidate = result["review"]["watermark_candidates"][0]
    assert candidate["column_name"] == "VALUE"
    assert candidate["data_type"] == "DATS"
    assert candidate["index_details"] == []
    assert result["table_plan"]["columns"][0]["is_watermark_candidate"] is True
