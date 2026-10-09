"""SAP target identity, replication-only changes, and safe action selection."""
from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.migration import config_activities
from app.migration.target_review import compare_columns, compare_replication, allowed_target_decisions


def test_sap_comparison_matches_notebook_metadata_rules():
    before = [{"column_name": "MANDT", "fabric_data_type": "VARCHAR(3)",
               "source_data_type": "CLNT(3,0)", "is_primary_key": True,
               "is_nullable": None, "description": "Client"}]
    after = [{**before[0], "fabric_data_type": "STRING", "is_nullable": False}]
    assert compare_columns(after, before, detailed=True)["same"]
    for key, value in [("source_data_type", "CHAR(3,0)"), ("is_primary_key", False),
                       ("description", "Client identifier")]:
        assert not compare_columns([{**after[0], key: value}], before, detailed=True)["same"]


@pytest.mark.parametrize("field", ["watermark_column", "watermark_column_data_type", "incremental_method"])
def test_watermark_definition_changes_require_reset(field):
    comparison = compare_replication({field: "NEW"}, {field: "OLD"})
    review = {"is_provisioned_record": True, "selected_existing_guid": "existing",
              "definitions": [{"source_table_guid": "existing", "comparison": {"changed": []},
                                "replication_comparison": comparison}]}
    assert allowed_target_decisions(review) == {"ALTER_BACKFILL", "REPLACE_FULL"}


def test_key_changes_require_replacement_and_key_order_is_irrelevant():
    assert compare_replication({"primary_key_columns": ["MANDT", "VBELN"]},
                               {"primary_key_columns": ["vbeln", "mandt"]})["same"]
    comparison = compare_replication({"primary_key_columns": ["ID"]}, {"primary_key_columns": ["OLD"]})
    review = {"is_provisioned_record": True, "selected_existing_guid": "existing",
              "definitions": [{"source_table_guid": "existing", "comparison": {"changed": []},
                                "replication_comparison": comparison}]}
    assert allowed_target_decisions(review) == {"REPLACE_FULL"}
    review["is_provisioned_record"] = False
    assert allowed_target_decisions(review) == {"REVISE_PLANNED"}


def test_existing_sap_target_review_detects_watermark_only_changes(monkeypatch):
    guid = uuid4()
    target = dict(connection_name="SAP", source_system_type="SAP_ECC", source_object_type="TABLE",
                  source_schema_name=None, source_table_name="VBAK", fabric_workspace_id="w",
                  fabric_lakehouse_id="l", fabric_lakehouse_schema="s", fabric_table_name="VBAK")
    column = {"column_name": "AEDAT", "fabric_data_type": "VARCHAR(8)", "source_data_type": "DATS(8,0)"}
    existing = {"incremental_method": "FULL", "write_strategy": "REPLACE", "primary_key_columns": ["ID"]}
    class DB:
        def list_target_table_records(self, **_):
            return [SimpleNamespace(**target, guid=guid, plan_guid=uuid4(), provisioning_status="PROVISIONED")]
        def list_source_table_columns(self, _):
            return [SimpleNamespace(model_dump=lambda **_: column)]
        def get_replication_config(self, _):
            return SimpleNamespace(model_dump=lambda **_: existing)
    monkeypatch.setattr(config_activities, "_db", DB)
    result = config_activities.review_existing_target({"plan_guid": str(uuid4()), "review_replication": True,
        "table_plan": {"table": target, "columns": [column], "replication": {
            **existing, "incremental_method": "WATERMARK", "write_strategy": "UPSERT", "watermark_column": "AEDAT"}}})
    definition = result["definitions"][0]
    assert definition["comparison"]["same"]
    assert not definition["same"]
    assert not definition["replication_comparison"]["same"]
    assert not result["physical_schema_verified"]


def test_notebook_rejects_stale_replication_and_future_watermark_changes():
    import runpy
    module = runpy.run_path('tests/test_provisioning_notebook.py')["load_notebook"]()
    change = SimpleNamespace(approved_replication={"before": {"watermark_column": "AEDAT"}})
    module.check_replication_approval(change, {"WatermarkColumn": "aedat"})
    with pytest.raises(module.TargetStateError, match="changed since approval"):
        module.check_replication_approval(change, {"WatermarkColumn": "ERDAT"})
    with pytest.raises(module.UnsafeChangeError, match="reset state"):
        module.check_watermark_stability({"WatermarkColumn": "AEDAT"}, {"watermark_column": "ERDAT"})


@pytest.mark.parametrize('conflict', ['different_source', 'multiple_records'])
def test_sap_target_review_rejects_ambiguous_or_unrelated_targets(monkeypatch, conflict):
    from unittest.mock import Mock
    target = dict(connection_name='SAP', source_system_type='SAP_ECC', source_object_type='TABLE',
                  source_schema_name=None, source_table_name='VBAK', fabric_workspace_id='w',
                  fabric_lakehouse_id='l', fabric_lakehouse_schema='s', fabric_table_name='T')
    record = SimpleNamespace(**target, guid=uuid4(), plan_guid=uuid4(), provisioning_status='PROVISIONED')
    db = Mock()
    if conflict == 'different_source':
        record.connection_name = 'OTHER_SAP'
    db.list_target_table_records.return_value = [record] * (2 if conflict == 'multiple_records' else 1)
    db.list_source_table_columns.return_value = []
    db.get_replication_config.return_value = None
    monkeypatch.setattr(config_activities, '_db', lambda: db)
    with pytest.raises(ValueError):
        config_activities.review_existing_target({'plan_guid': str(uuid4()), 'review_replication': True,
            'table_plan': {'table': target, 'columns': [], 'replication': {}}})
