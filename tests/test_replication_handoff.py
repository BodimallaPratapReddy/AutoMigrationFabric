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
