"""Resolve source credentials inside activities, outside Temporal payloads."""

import os

from thirdparty.configdb.repository import ConfigDBRepository, DBConnectionRecord
from thirdparty.oracle.utils import OracleClient, OracleConnectionSettings
from thirdparty.sap.utils import SAPClient


def get_source_connection(name: str, source_type: str) -> DBConnectionRecord:
    record = ConfigDBRepository().get_connection(name)
    if record is None or not record.is_active or record.source_type.upper() != source_type:
        raise ValueError(f"Active {source_type} connection {name} was not found")
    return record


def _has_stored_credentials(record: DBConnectionRecord) -> bool:
    details = record.connection_details
    if "username" not in details and "password" not in details:
        return False
    if not isinstance(details.get("username"), str) or not details["username"]:
        raise ValueError("Connection username is missing")
    if not isinstance(details.get("password"), str) or not details["password"]:
        raise ValueError("Connection password is missing")
    return True


def oracle_client(name: str) -> OracleClient:
    record = get_source_connection(name, "ORACLE")
    if not _has_stored_credentials(record):
        if os.getenv("ORACLE_CONNECTION_NAME") != name:
            raise ValueError("ORACLE_CONNECTION_NAME must match the selected connection")
        return OracleClient()
    details = record.connection_details
    settings = OracleConnectionSettings.model_validate({
        "username": details["username"], "password": details["password"],
        "dsn": details.get("dsn"), "host": details.get("host"),
        "serviceName": details.get("serviceName"), "port": details.get("port", 1521),
        "clientPath": os.getenv("ORACLE_CLIENT_PATH") or None,
    })
    return OracleClient(settings)


def sap_client(name: str) -> SAPClient:
    record = get_source_connection(name, "SAP_ECC")
    if not _has_stored_credentials(record):
        if os.getenv("SAP_CONNECTION_NAME") != name:
            raise ValueError("SAP_CONNECTION_NAME must match the selected connection")
        return SAPClient()
    details = record.connection_details
    return SAPClient(host=details.get("host"), port=details.get("https_port"),
                     sap_client=details.get("sap_client"),
                     username=details["username"], password=details["password"])
