"""Validated values returned by the Fabric integration."""

from datetime import datetime
from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class _FabricModel(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")


class FabricWorkspace(_FabricModel):
    id: str
    display_name: str = Field(alias="displayName")


class Lakehouse(_FabricModel):
    id: str
    display_name: str = Field(alias="displayName")


class FabricItem(_FabricModel):
    id: str
    display_name: str = Field(alias="displayName")
    type: str
    workspace_id: str | None = Field(default=None, alias="workspaceId")


class FabricJobStatus(StrEnum):
    NOT_STARTED = "NOT_STARTED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"
    UNKNOWN = "UNKNOWN"


def is_terminal_job_status(status: FabricJobStatus) -> bool:
    return status in {FabricJobStatus.SUCCEEDED, FabricJobStatus.FAILED, FabricJobStatus.CANCELLED}


class FabricConnectionStatus(_FabricModel):
    success: bool
    accessible_workspace_count: int | None = None


class FabricJobSubmission(_FabricModel):
    job_instance_id: str
    location: str
    retry_after_seconds: int | None = None


class FabricCancellationSubmission(_FabricModel):
    location: str | None = None
    retry_after_seconds: int | None = None


class FabricJobParameter(_FabricModel):
    value: str | bool | int | float
    type: Literal["Text", "Boolean", "Number"]

    @model_validator(mode="after")
    def check_value_type(self) -> "FabricJobParameter":
        valid = ((self.type == "Text" and isinstance(self.value, str))
                 or (self.type == "Boolean" and isinstance(self.value, bool))
                 or (self.type == "Number" and isinstance(self.value, (int, float))
                     and not isinstance(self.value, bool)))
        if not valid:
            raise ValueError("parameter value does not match its Fabric type")
        return self


class FabricJobFailureReason(_FabricModel):
    error_code: str | None = Field(default=None, alias="errorCode")
    message: str | None = None
    request_id: str | None = Field(default=None, alias="requestId")


class FabricJobInstance(_FabricModel):
    id: str
    item_id: str | None = Field(default=None, alias="itemId")
    job_type: str | None = Field(default=None, alias="jobType")
    invoke_type: str | None = Field(default=None, alias="invokeType")
    status: str
    normalized_status: FabricJobStatus
    start_time_utc: datetime | None = Field(default=None, alias="startTimeUtc")
    end_time_utc: datetime | None = Field(default=None, alias="endTimeUtc")
    failure_reason: FabricJobFailureReason | None = Field(default=None, alias="failureReason")
    exit_value: str | None = Field(default=None, alias="exitValue")
    root_activity_id: str | None = Field(default=None, alias="rootActivityId")
    retry_after_seconds: int | None = None


def normalize_job_status(value: str) -> FabricJobStatus:
    return {
        "notstarted": FabricJobStatus.NOT_STARTED,
        "queued": FabricJobStatus.NOT_STARTED,
        "inprogress": FabricJobStatus.RUNNING,
        "running": FabricJobStatus.RUNNING,
        "completed": FabricJobStatus.SUCCEEDED,
        "succeeded": FabricJobStatus.SUCCEEDED,
        "failed": FabricJobStatus.FAILED,
        "cancelled": FabricJobStatus.CANCELLED,
        "canceled": FabricJobStatus.CANCELLED,
    }.get(value.replace("_", "").replace(" ", "").lower(), FabricJobStatus.UNKNOWN)
