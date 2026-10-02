from types import SimpleNamespace

import pytest

from app.migration import source_connections


class FakeRepository:
    def __init__(self, record):
        self.record = record

    def get_connection(self, name):
        return self.record if self.record.connection_name == name else None


def record(source_type, details):
    return SimpleNamespace(connection_name="chosen", source_type=source_type,
                           is_active=True, connection_details=details)


def test_oracle_uses_selected_database_credentials(monkeypatch):
    selected = record("ORACLE", {"dsn": "db.example.com/ORCL",
                                 "username": "scott", "password": "tiger"})
    monkeypatch.setattr(source_connections, "ConfigDBRepository", lambda: FakeRepository(selected))
    monkeypatch.delenv("ORACLE_CONNECTION_NAME", raising=False)
    client = source_connections.oracle_client("chosen")
    assert client.settings.username == "scott"
    assert client.settings.password.get_secret_value() == "tiger"
    assert client.settings.dsn == "db.example.com/ORCL"


def test_sap_uses_selected_database_credentials(monkeypatch):
    selected = record("SAP_ECC", {"host": "sap.example.com", "https_port": 443,
                                  "sap_client": "100", "username": "sapuser",
                                  "password": "sappass"})
    monkeypatch.setattr(source_connections, "ConfigDBRepository", lambda: FakeRepository(selected))
    monkeypatch.delenv("SAP_CONNECTION_NAME", raising=False)
    class CapturingSAP:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
    monkeypatch.setattr(source_connections, "SAPClient", CapturingSAP)
    assert source_connections.sap_client("chosen").kwargs == {
        "host": "sap.example.com", "port": 443, "sap_client": "100",
        "username": "sapuser", "password": "sappass"}


def test_incomplete_stored_credentials_do_not_fall_back_to_env(monkeypatch):
    selected = record("ORACLE", {"dsn": "db", "username": "scott"})
    monkeypatch.setattr(source_connections, "ConfigDBRepository", lambda: FakeRepository(selected))
    monkeypatch.setenv("ORACLE_CONNECTION_NAME", "chosen")
    with pytest.raises(ValueError, match="Connection password is missing"):
        source_connections.oracle_client("chosen")
