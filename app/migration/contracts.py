"""Small, stable payloads shared by the API and Temporal workflows."""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field, field_validator, model_validator


Approach = Literal["ORACLE_TABLE", "SAP_TABLE", "SAP_ODP", "SAP_REBUILD"]


class MigrationInput(BaseModel):
    migration_approach: Approach
    source_connection_name: str = Field(min_length=1)
    source_object_name: str = Field(min_length=1)
    source_schema_name: str | None = None
    fabric_workspace_id: str = Field(min_length=1)
    fabric_lakehouse_id: str = Field(min_length=1)
    fabric_schema_name: str = Field(min_length=1)
    fabric_target_name: str | None = None
    provisioning_notebook_id: str | None = Field(default=None, min_length=1)
    # Kept in the request model so legacy callers receive an explicit 422 at the API.
    replication_pipeline_id: str | None = Field(default=None, min_length=1)
    requested_by: str | None = None

    @field_validator("fabric_workspace_id", "fabric_lakehouse_id",
                     "provisioning_notebook_id", "replication_pipeline_id")
    @classmethod
    def validate_fabric_id(cls, value: str | None) -> str | None:
        if value is not None:
            try:
                UUID(value)
            except ValueError as exc:
                raise ValueError("Fabric ID must be a valid UUID") from exc
        return value

    @model_validator(mode="after")
    def validate_source(self) -> "MigrationInput":
        if self.migration_approach == "ORACLE_TABLE" and not self.source_schema_name:
            raise ValueError("Oracle table migrations require source_schema_name")
        return self


class Decision(BaseModel):
    approved_by: str = Field(min_length=1)
    comment: str | None = None
    watermark_column: str | None = None


class ColumnMappingChoice(BaseModel):
    name: str = Field(min_length=1)
    fabric_type: str = Field(min_length=1)


class ColumnMappingApproval(BaseModel):
    approved_by: str = Field(min_length=1)
    columns: list[ColumnMappingChoice] = Field(min_length=1)
    acknowledge_unsupported: bool = False


class WatermarkApproval(BaseModel):
    approved_by: str = Field(min_length=1)
    watermark_column: str | None


class TargetChangeApproval(BaseModel):
    approved_by: str = Field(min_length=1)
    decision: Literal["REVISE_PLANNED", "ALTER_FUTURE", "ALTER_BACKFILL", "REPLACE_FULL"]


class Feedback(BaseModel):
    submitted_by: str = Field(min_length=1)
    message: str = Field(min_length=1)


class MigrationState(BaseModel):
    workflow_id: str
    migration_approach: Approach
    source_object_name: str
    phase: str = "CREATED"
    status: str = "RUNNING"
    plan_guid: str | None = None
    plan_version: int = 1
    analysis_version: int = 1
    waiting_for_user: bool = False
    current_message: str | None = None
    last_error: str | None = None
    review: dict | None = None
    fabric_job_instance_id: str | None = None
    pipeline_job_instance_id: str | None = None
    provisioning_status: str | None = None
    provisioning_report: dict | None = None
    replication_status: str | None = None
    batch_run_id: str | None = None
