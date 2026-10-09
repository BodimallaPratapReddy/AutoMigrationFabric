"""List active Fabric workspaces available to the migration form."""

import asyncio
from uuid import UUID

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from thirdparty.configdb.utils import ConfigDB
from thirdparty.fabric.client import FabricClient


router = APIRouter(prefix="/workspaces", tags=["workspaces"])
LAKEHOUSE_REQUEST_TIMEOUT = 25.0


class WorkspaceOption(BaseModel):
    workspace_name: str
    workspace_id: str


class LakehouseOption(BaseModel):
    lakehouse_name: str
    lakehouse_id: str


@router.get("", response_model=list[WorkspaceOption])
async def list_workspaces() -> list[WorkspaceOption]:
    try:
        rows = await asyncio.to_thread(
            lambda: ConfigDB().list_fabric_workspaces(active_only=True))
    except Exception as exc:
        raise HTTPException(503, "Fabric workspace registry is unavailable") from exc
    return [WorkspaceOption(workspace_name=row.workspace_name, workspace_id=row.workspace_id)
            for row in rows]


@router.get("/{workspace_id}/lakehouses", response_model=list[LakehouseOption])
async def list_lakehouses(workspace_id: UUID) -> list[LakehouseOption]:
    try:
        return await asyncio.wait_for(
            _list_lakehouses(workspace_id), timeout=LAKEHOUSE_REQUEST_TIMEOUT)
    except TimeoutError as exc:
        raise HTTPException(504, "Loading Lakehouses timed out. Check connectivity to Microsoft Entra and Fabric, then retry.") from exc


async def _list_lakehouses(workspace_id: UUID) -> list[LakehouseOption]:
    identifier = str(workspace_id)
    try:
        rows = await asyncio.to_thread(
            lambda: ConfigDB().list_fabric_workspaces(active_only=True))
    except Exception as exc:
        raise HTTPException(503, "Fabric workspace registry is unavailable") from exc
    if not any(row.workspace_id.lower() == identifier.lower() for row in rows):
        raise HTTPException(404, "Active Fabric workspace was not found")

    def fetch():
        with FabricClient() as client:
            return client.list_lakehouses(identifier)

    try:
        lakehouses = await asyncio.to_thread(fetch)
    except Exception as exc:
        raise HTTPException(502, "Fabric Lakehouses are unavailable for this workspace") from exc
    return [LakehouseOption(lakehouse_name=item.display_name, lakehouse_id=item.id)
            for item in sorted(lakehouses, key=lambda item: (item.display_name.lower(), item.id))]
