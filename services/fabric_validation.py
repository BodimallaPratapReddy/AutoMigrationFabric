"""One-shot validation of configured Fabric execution targets."""

from pydantic import BaseModel

from thirdparty.fabric.client import FabricClient
from thirdparty.fabric.models import FabricItem, FabricWorkspace, Lakehouse


class FabricTargetValidation(BaseModel):
    workspace: FabricWorkspace
    lakehouse: Lakehouse
    notebook: FabricItem
    pipeline: FabricItem | None = None


def validate_fabric_target(
    client: FabricClient, *, workspace_id: str, lakehouse_id: str,
    notebook_id: str, pipeline_id: str | None = None,
) -> FabricTargetValidation:
    return FabricTargetValidation(
        workspace=client.get_workspace(workspace_id),
        lakehouse=client.get_lakehouse(workspace_id, lakehouse_id),
        notebook=client.get_notebook(workspace_id, notebook_id),
        pipeline=client.get_pipeline(workspace_id, pipeline_id) if pipeline_id else None,
    )
