from types import SimpleNamespace

from fastapi.testclient import TestClient

from app.main import app
from app.migration import connections_api


class FakeConnections:
    def __init__(self):
        self.records = {}

    def get_connection(self, name):
        return self.records.get(name)

    def list_connections(self):
        return list(self.records.values())

    def insert_connection(self, value):
        self.records[value.connection_name] = SimpleNamespace(
            connection_name=value.connection_name,
            source_type=value.source_type, is_active=value.is_active,
            connection_details=value.connection_details)


def test_create_and_select_connections_without_exposing_details(monkeypatch):
    db = FakeConnections()
    monkeypatch.setattr(connections_api, "ConfigDBRepository", lambda: db)
    client = TestClient(app)

    oracle = {"source_type": "ORACLE", "connection_name": "ORACLE_DEV",
              "dsn": "db.example.com:1521/ORCL", "username": "scott",
              "password": "tiger", "tags": ["dev"]}
    response = client.post("/connections", json=oracle)
    assert response.status_code == 201
    assert response.json() == {"connection_name": "ORACLE_DEV",
                               "source_type": "ORACLE", "is_active": True}
    assert db.records["ORACLE_DEV"].connection_details == {
        "dsn": oracle["dsn"], "username": "scott", "password": "tiger"}

    sap = {"source_type": "SAP_ECC", "connection_name": "SAP_DEV",
           "host": "sap.example.com", "port": 443, "sap_client": "100",
           "username": "sapuser", "password": "sappass"}
    assert client.post("/connections", json=sap).status_code == 201
    assert db.records["SAP_DEV"].connection_details["password"] == "sappass"
    listed = client.get("/connections")
    assert listed.status_code == 200
    assert {item["connection_name"] for item in listed.json()} == {"ORACLE_DEV", "SAP_DEV"}
    assert all("connection_details" not in item for item in listed.json())
    assert client.post("/connections", json=oracle).status_code == 409


def test_connection_form_requires_credentials_and_complete_endpoint(monkeypatch):
    db = FakeConnections()
    monkeypatch.setattr(connections_api, "ConfigDBRepository", lambda: db)
    client = TestClient(app)
    assert client.post("/connections", json={
        "source_type": "ORACLE", "connection_name": "BAD",
        "dsn": "db.example.com:1521/ORCL", "password": "secret"}).status_code == 422
    assert client.post("/connections", json={
        "source_type": "SAP_ECC", "connection_name": "BAD",
        "host": "sap.example.com", "port": 443,
        "username": "sapuser", "password": "sappass"}).status_code == 422
    assert client.post("/connections", json={
        "source_type": "ORACLE", "connection_name": "BAD",
        "dsn": "user/password@db.example.com:1521/ORCL",
        "username": "scott", "password": "tiger"}).status_code == 422
    assert db.records == {}
