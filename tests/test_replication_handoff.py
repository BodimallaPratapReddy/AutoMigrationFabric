from types import SimpleNamespace
from uuid import uuid4

import pytest

from app.migration import config_activities


class FakeConfigDB:
    def __init__(self, status="PROVISIONED"):
        self.table = SimpleNamespace(guid=uuid4(), provisioning_status=status)
        self.enabled = []

    def list_source_tables_for_plan(self, _plan_guid):
        return [self.table]

    def set_replication_enabled(self, guid, value):
        self.enabled.append((guid, value))

    def mark_replication_started(self, _guid):
        raise AssertionError("structure migration must not start replication")


def test_handoff_enables_config_without_starting_a_load(monkeypatch):
    db = FakeConfigDB()
    monkeypatch.setattr(config_activities, "_db", lambda: db)
    config_activities.activate_replication_config({"plan_guid": str(uuid4())})
    assert db.enabled == [(db.table.guid, True)]


def test_handoff_requires_a_provisioned_table(monkeypatch):
    db = FakeConfigDB(status="PENDING")
    monkeypatch.setattr(config_activities, "_db", lambda: db)
    with pytest.raises(ValueError, match="before table provisioning"):
        config_activities.activate_replication_config({"plan_guid": str(uuid4())})
    assert db.enabled == []


def test_failure_status_reconciles_a_late_persistence_commit(monkeypatch):
    from unittest.mock import Mock
    from thirdparty.configdb.repository import PlanStatus
    db = Mock()
    db.get_migration_plan.side_effect = [
        SimpleNamespace(status=PlanStatus.APPROVED, plan_version=1),
        SimpleNamespace(status=PlanStatus.READY_TO_PROVISION, plan_version=1)]
    db.update_migration_plan_status.side_effect = [ValueError("plan status or version changed"), None]
    monkeypatch.setattr(config_activities, "_db", lambda: db)
    config_activities.fail_migration({"plan_guid": str(uuid4()),
        "phase": "PERSISTING_RUNTIME_CONFIG", "message": "Activity timed out"})
    assert db.update_migration_plan_status.call_count == 2
    assert db.update_migration_plan_status.call_args.kwargs["expected_status"] == PlanStatus.READY_TO_PROVISION
