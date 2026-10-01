"""Authentication helpers for the Microsoft Fabric REST API."""

import os
import warnings
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TypedDict
from uuid import UUID

import httpx
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

from .exceptions import FabricAuthenticationError


FABRIC_API_URL = "https://api.fabric.microsoft.com/v1/"
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


class Lakehouse(TypedDict):
    id: str
    name: str


class _ServicePrincipalAuth(httpx.Auth):
    def __init__(self, credential: ClientSecretCredential) -> None:
        self._credential = credential

    def auth_flow(self, request: httpx.Request) -> Iterator[httpx.Request]:
        # Azure Identity caches tokens and renews them when needed.
        token = _get_fabric_token(self._credential)
        request.headers["Authorization"] = f"Bearer {token.token}"
        yield request


def _get_fabric_token(credential: ClientSecretCredential):
    try:
        return credential.get_token(FABRIC_SCOPE)
    except Exception as exc:
        # SDK exception text can include credential or tenant details.
        raise FabricAuthenticationError("Unable to authenticate with Microsoft Fabric") from exc


@contextmanager
def connect_to_fabric() -> Iterator[httpx.Client]:
    """Open an authenticated, synchronous client for Fabric REST API calls.

    Reads FABRIC_TENANT_ID, FABRIC_CLIENT_ID, and FABRIC_CLIENT_SECRET from
    the environment or a local .env file. Authentication errors surface when
    entering the context; endpoint authorization errors surface on requests.
    """
    load_dotenv()
    names = ("FABRIC_TENANT_ID", "FABRIC_CLIENT_ID", "FABRIC_CLIENT_SECRET")
    values = {name: os.getenv(name) for name in names}
    missing = [name for name, value in values.items() if not value]
    if missing:
        raise ValueError(f"Missing Fabric configuration: {', '.join(missing)}")

    try:
        credential = ClientSecretCredential(
            tenant_id=values["FABRIC_TENANT_ID"],
            client_id=values["FABRIC_CLIENT_ID"],
            client_secret=values["FABRIC_CLIENT_SECRET"],
        )
    except Exception as exc:
        raise FabricAuthenticationError("Unable to authenticate with Microsoft Fabric") from exc
    try:
        _get_fabric_token(credential)
        with httpx.Client(
            base_url=FABRIC_API_URL,
            auth=_ServicePrincipalAuth(credential),
            timeout=30.0,
        ) as client:
            yield client
    finally:
        credential.close()


def list_lakehouses(workspace_id: str) -> list[Lakehouse]:
    """Deprecated compatibility wrapper; use FabricClient.list_lakehouses."""
    warnings.warn("Use FabricClient.list_lakehouses instead", DeprecationWarning, stacklevel=2)
    try:
        workspace_id = str(UUID(workspace_id))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("workspace_id must be a valid UUID") from exc

    path = f"workspaces/{workspace_id}/lakehouses"
    lakehouses: list[Lakehouse] = []
    continuation_token: str | None = None

    with connect_to_fabric() as client:
        while True:
            params = (
                {"continuationToken": continuation_token}
                if continuation_token is not None
                else None
            )
            response = client.get(path, params=params)
            response.raise_for_status()
            data = response.json()
            lakehouses.extend(
                {"id": item["id"], "name": item["displayName"]}
                for item in data["value"]
            )
            continuation_token = data.get("continuationToken")
            if not continuation_token:
                return lakehouses
