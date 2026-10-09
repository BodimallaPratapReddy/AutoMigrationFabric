"""Approval-time profiling and persisted delete policy; no ingestion in this app."""
import asyncio
import json
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.migration import workflow_base
from app.migration.source_activities import validate_oracle_key
from thirdparty.delete_policy import validate_delete_policy
from thirdparty.configdb.repository import ConfigDBRepository, ReplicationConfigCreate, ReplicationConfigRecord
from thirdparty.oracle.utils import OracleClient, OracleConnectionSettings, OracleTableColumn


KEY = {"valid": True, "status": "CHECKED", "columns": ["ID", "TENANT"],
       "scope": "FULL_TABLE", "checked_at": "2026-10-08T00:00:00+00:00",
       "has_nulls": False, "has_duplicates": False}
COMBINED = {"mode": "SOFT_DELETE_AND_RECONCILE", "behavior": "MARK",
            "soft_delete_column": "DELETED", "soft_delete_predicate": "VALUES",
            "soft_delete_values": ["Y"], "watermark_tracks_soft_delete": True,
            "reconcile_interval_minutes": 1440}


@pytest.mark.parametrize("nulls,duplicates", [(False, False), (True, False), (False, True), (True, True)])
def test_exact_composite_key_checks_use_one_read_only_snapshot(nulls, duplicates):
    client = OracleClient(OracleConnectionSettings(username="u", password="p", dsn="test"))
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.side_effect = [(1,) if nulls else None, (1,) if duplicates else None]
    columns = [OracleTableColumn(ID=i, COLUMN_NAME=name, DATATYPE="NUMBER", DESC=None)
               for i, name in enumerate(["ID", "TENANT"], 1)]
    with patch.object(client, "get_table_schema", return_value=columns), \
         patch.object(client, "connect", return_value=connection):
        result = client.validate_replication_key("APP", "ORDERS", ["ID", "TENANT"])
    assert result["valid"] is (not nulls and not duplicates)
    assert result["has_nulls"] is nulls
    assert result["has_duplicates"] is duplicates
    sql = [call.args[0] for call in cursor.execute.call_args_list]
    assert sql[0] == "SET TRANSACTION READ ONLY"
    assert '"ID" IS NULL OR "TENANT" IS NULL' in sql[1]
    assert 'GROUP BY "ID", "TENANT" HAVING COUNT(*) > 1' in sql[2]
    assert result["scope"] == "FULL_TABLE"
    connection.rollback.assert_called_once()
    connection.close.assert_called_once()


def test_source_validation_errors_are_unverified_and_do_not_leak_credentials():
    input = {"migration_approach": "ORACLE_TABLE", "source_connection_name": "test",
             "source_object_name": "ORDERS", "source_schema_name": "APP",
             "fabric_workspace_id": str(uuid4()), "fabric_lakehouse_id": str(uuid4()),
             "fabric_schema_name": "bronze"}
    with patch("app.migration.source_activities.oracle_client", side_effect=RuntimeError("secret password")):
        result = validate_oracle_key({"input": input, "columns": ["ID"]})
    assert result["valid"] is False
    assert result["status"] == "UNVERIFIED"
    assert "secret" not in str(result)


@pytest.mark.parametrize("mode", ["SOFT_DELETE", "RECONCILE", "SOFT_DELETE_AND_RECONCILE"])
@pytest.mark.parametrize("behavior", ["MARK", "DELETE"])
def test_policy_modes_and_actions(mode, behavior):
    policy = validate_delete_policy({**COMBINED, "mode": mode, "behavior": behavior},
                                    ["ID", "TENANT", "DELETED"], ["ID", "TENANT"], "UPDATED")
    config = ReplicationConfigCreate(primary_key_columns=KEY["columns"], key_validation=KEY,
                                    delete_policy=policy)
    assert config.delete_policy["mode"] == mode
    if mode != "SOFT_DELETE":
        assert config.delete_policy["reconcile_require_complete_snapshot"] is True
        assert config.delete_policy["reconcile_require_consistent_snapshot"] is True


@pytest.mark.parametrize("override", [{"soft_delete_column": "UNKNOWN"}, {"soft_delete_values": []},
    {"watermark_tracks_soft_delete": False}, {"reconcile_interval_minutes": 0},
    {"reconcile_interval_minutes": True}, {"reconcile_interval_minutes": 15.5},
    {"mode": "CDC"}, {"behavior": "BAD"}])
def test_invalid_delete_configuration_cannot_be_approved(override):
    with pytest.raises(ValueError):
        validate_delete_policy({**COMBINED, **override}, ["ID", "TENANT", "DELETED"], KEY["columns"], "UPDATED")


def test_no_key_allows_only_none_and_timestamp_predicate_has_no_values():
    assert validate_delete_policy({"mode": "NONE"}, ["ID"], [], None) == {"mode": "NONE"}
    with pytest.raises(ValueError, match="validated key"):
        validate_delete_policy(COMBINED, ["DELETED"], [], None)
    policy = validate_delete_policy({**COMBINED, "mode": "SOFT_DELETE", "soft_delete_predicate": "NOT_NULL"},
                                    ["DELETED"], ["ID"], "UPDATED")
    assert policy["soft_delete_values"] == []
    with pytest.raises(ValidationError, match="validated primary key"):
        ReplicationConfigCreate(primary_key_columns=["ID"], delete_policy=policy)


def test_configdb_persists_and_reads_json_policy_with_bound_parameters():
    policy = validate_delete_policy(COMBINED, ["ID", "TENANT", "DELETED"], KEY["columns"], "UPDATED")
    config = ReplicationConfigCreate(primary_key_columns=KEY["columns"], key_validation=KEY, delete_policy=policy)
    db = ConfigDBRepository("fake")
    connection = MagicMock()
    with patch.object(db, "connect", return_value=connection):
        db.create_replication_config(uuid4(), config)
    cursor = connection.cursor.return_value.__enter__.return_value
    sql, params = cursor.execute.call_args.args
    assert "DeletePolicy" in sql and "KeyValidation" in sql
    parsed = [json.loads(value) for value in params if isinstance(value, str) and value.startswith('{')]
    assert policy in parsed and KEY in parsed
    connection.commit.assert_called_once()
    row = {**config.model_dump(by_alias=True), "SourceTableGUID": str(uuid4()), "ReplicationConfigGUID": str(uuid4())}
    row["DeletePolicy"] = json.dumps(policy)
    row["KeyValidation"] = json.dumps(KEY)
    row["PrimaryKeyColumns"] = json.dumps(KEY["columns"])
    cursor.description = [(name,) for name in row]
    cursor.fetchall.return_value = [tuple(row.values())]
    loaded = db._rows(cursor, ReplicationConfigRecord)[0]
    assert loaded.delete_policy == policy
    assert loaded.key_validation == KEY


def test_failed_key_check_requires_another_decision_before_watermark():
    class Harness(workflow_base.MigrationWorkflowBase):
        async def _activity(self, name, payload, **kwargs):
            assert name == "validate_oracle_key"
            return {**KEY, "valid": False, "has_duplicates": True}
        async def _gate(self, phase, actions):
            self.phases.append(phase)
            if phase == "WAITING_FOR_PRIMARY_KEY_APPROVAL":
                assert "duplicate" in self.state["review"]["primary_key_error"]
                return {"action": "approve_primary_key", "actor": "reviewer", "primary_key_columns": []}
            return {"action": "approve_watermark", "actor": "reviewer", "watermark_column": None}
    h = Harness()
    h._oracle_key_checks = True
    h.state = {"review": {"primary_key": KEY["columns"], "primary_key_source": "DATABASE"}}
    h.phases = []
    plan = {"table_plan": {"columns": [{"column_name": name} for name in KEY["columns"]],
                           "replication": {"primary_key_columns": KEY["columns"]}}}
    asyncio.run(h._watermark_gate(plan, review_key=True))
    assert h.phases == ["WAITING_FOR_PRIMARY_KEY_APPROVAL", "WAITING_FOR_WATERMARK_APPROVAL"]
    assert plan["table_plan"]["replication"]["primary_key_columns"] is None


def test_direct_delete_signal_is_validated_before_plan_can_advance():
    class Harness(workflow_base.MigrationWorkflowBase):
        async def _gate(self, phase, actions):
            assert phase == "WAITING_FOR_DELETE_POLICY_APPROVAL"
            return {"action": "approve_delete_policy", "actor": "reviewer", "delete_policy": self.policies.pop(0)}
    h = Harness()
    h.state = {"review": {}}
    h.policies = [{**COMBINED, "watermark_tracks_soft_delete": False}, COMBINED]
    plan = {"table_plan": {"columns": [{"column_name": name} for name in ["ID", "TENANT", "DELETED"]],
        "replication": {"primary_key_columns": KEY["columns"], "key_validation": KEY, "watermark_column": "UPDATED"}}}
    asyncio.run(h._delete_policy_gate(plan))
    assert not h.policies
    assert plan["table_plan"]["replication"]["delete_policy"]["reconcile_interval_minutes"] == 1440
    assert "delete_policy_error" not in h.state["review"]
