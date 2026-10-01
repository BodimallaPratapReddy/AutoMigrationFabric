"""Project contract for a completed Fabric provisioning notebook run."""

import json
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from thirdparty.fabric.models import FabricJobInstance, FabricJobStatus


class ProvisioningNotebookError(Exception):
    """A provisioning run cannot be accepted as successful."""


class ProvisioningJobFailedError(ProvisioningNotebookError):
    pass


class ProvisioningCancelledError(ProvisioningNotebookError):
    pass


class ProvisioningResultError(ProvisioningNotebookError):
    pass


class ProvisioningLogicalFailure(ProvisioningNotebookError):
    def __init__(self, result: "ProvisioningNotebookResult") -> None:
        super().__init__("Provisioning notebook reported FAILED")
        self.result = result


class ProvisioningNotebookResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: Literal["SUCCESS", "FAILED"]
    plan_guid: UUID
    tables_processed: int | None = Field(default=None, ge=0, strict=True)
    views_processed: int | None = Field(default=None, ge=0, strict=True)
    message: str | None = None


def parse_provisioning_notebook_result(
    job: FabricJobInstance, *, expected_plan_guid: UUID | str,
) -> ProvisioningNotebookResult:
    """Accept a successful run only when its structured result matches the plan."""
    if job.normalized_status == FabricJobStatus.FAILED:
        raise ProvisioningJobFailedError("Fabric provisioning job failed")
    if job.normalized_status == FabricJobStatus.CANCELLED:
        raise ProvisioningCancelledError("Fabric provisioning job was cancelled")
    if job.normalized_status != FabricJobStatus.SUCCEEDED:
        raise ProvisioningResultError("Fabric provisioning job is not successful")
    if not job.exit_value:
        raise ProvisioningResultError("Fabric provisioning job has no exit value")
    try:
        result = ProvisioningNotebookResult.model_validate(json.loads(job.exit_value))
    except (ValueError, ValidationError) as exc:
        raise ProvisioningResultError("Fabric provisioning job has an invalid exit value") from exc
    try:
        expected = UUID(str(expected_plan_guid))
    except (TypeError, ValueError, AttributeError) as exc:
        raise ValueError("expected_plan_guid must be a valid UUID") from exc
    if result.plan_guid != expected:
        raise ProvisioningResultError("Fabric provisioning result has a different plan_guid")
    if result.status == "FAILED":
        raise ProvisioningLogicalFailure(result)
    return result
