"""Bounded, read-only adapter smoke checks using configured environment values.

Run manually. This script prints only success or exception class names so
connection strings, credentials, and remote error bodies stay out of logs.
"""

from __future__ import annotations

import os

from dotenv import load_dotenv

from thirdparty.configdb.repository import ConfigDBRepository
from thirdparty.fabric.client import FabricClient
from thirdparty.oracle.utils import OracleClient
from thirdparty.sap.utils import SAPClient


def check(name: str, function) -> bool:
    try:
        function()
        print(f"{name}: OK")
        return True
    except Exception as exc:
        print(f"{name}: {type(exc).__name__}")
        return False


def configdb() -> None:
    repository = ConfigDBRepository()
    with repository.connect() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT TOP (1) PlanGUID FROM bronze_replication.MigrationPlans")
            cursor.fetchone()


def fabric() -> None:
    workspace_id = os.environ["FABRIC_WORKSPACE_ID"]
    with FabricClient() as client:
        client.get_workspace(workspace_id)
        lakehouse_id = os.getenv("FABRIC_LAKEHOUSE_ID")
        if lakehouse_id:
            client.get_lakehouse(workspace_id, lakehouse_id)


def oracle() -> None:
    OracleClient().test_connection()


def sap() -> None:
    with SAPClient(timeout=15.0) as client:
        client.test_connection()


if __name__ == "__main__":
    load_dotenv()
    results = [check(label, action) for label, action in
               (("ConfigDB", configdb), ("Fabric", fabric),
                ("Oracle", oracle), ("SAP", sap))]
    raise SystemExit(0 if all(results) else 1)
