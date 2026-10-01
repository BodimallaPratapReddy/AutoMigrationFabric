"""Typed repository for the current bronze_replication configuration schema.

Runtime rows are written only through ``persist_approved_runtime_plan`` after
the caller has recorded approval on the existing MigrationPlans row.
"""

from __future__ import annotations

import json
from hashlib import sha256
from contextlib import contextmanager
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Iterator, Literal
from uuid import UUID, uuid4, uuid5

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from .utils import ConfigDB

if TYPE_CHECKING:
    from mssql_python.connection import Connection


class DBRow(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class PlanStatus(StrEnum):
    DRAFT = "DRAFT"
    WAITING_ANALYSIS_APPROVAL = "WAITING_ANALYSIS_APPROVAL"
    ANALYSIS_APPROVED = "ANALYSIS_APPROVED"
    WAITING_PLAN_APPROVAL = "WAITING_PLAN_APPROVAL"
    APPROVED = "APPROVED"
    READY_TO_PROVISION = "READY_TO_PROVISION"
    PROVISIONING = "PROVISIONING"
    PROVISIONED = "PROVISIONED"
    FAILED = "FAILED"
    REJECTED = "REJECTED"
    SUPERSEDED = "SUPERSEDED"


_SECRET_KEYS = {"password", "passwd", "client_secret", "secret", "access_token", "token"}


class DBConnectionCreate(DBRow):
    source_type: str = Field(alias="SourceType", min_length=1)
    connection_name: str = Field(alias="ConnectionName", min_length=1)
    connection_details: dict[str, JsonValue] = Field(alias="ConnectionDetails")
    connection_tags: list[str] | None = Field(default=None, alias="ConnectionTags")
    is_active: bool = Field(default=True, alias="IsActive")

    @model_validator(mode="after")
    def no_plaintext_secrets(self) -> "DBConnectionCreate":
        def inspect(value: object) -> None:
            if isinstance(value, dict):
                for key, child in value.items():
                    if key.lower() in _SECRET_KEYS:
                        raise ValueError("connection details must use external secret references")
                    inspect(child)
            elif isinstance(value, list):
                for child in value:
                    inspect(child)
        inspect(self.connection_details)
        return self


class DBConnectionRecord(DBConnectionCreate):
    id: int = Field(alias="Id")


class MigrationPlanRecord(DBRow):
    plan_guid: UUID = Field(alias="PlanGUID")
    source_connection_name: str = Field(alias="SourceConnectionName")
    source_system_type: str = Field(alias="SourceSystemType")
    source_object_type: str = Field(alias="SourceObjectType")
    source_object_name: str = Field(alias="SourceObjectName")
    migration_approach: str = Field(alias="MigrationApproach")
    analysis_version: int = Field(alias="AnalysisVersion")
    plan_version: int = Field(alias="PlanVersion")
    runtime_plan_hash: str | None = Field(alias="RuntimePlanHash")
    status: PlanStatus = Field(alias="Status")
    approved_by: str | None = Field(alias="ApprovedBy")
    approved_timestamp: datetime | None = Field(alias="ApprovedTimestamp")
    temporal_workflow_id: str | None = Field(alias="TemporalWorkflowId")
    temporal_run_id: str | None = Field(alias="TemporalRunId")
    notes: str | None = Field(alias="Notes")


class MigrationPlanCreate(DBRow):
    source_connection_name: str = Field(alias="SourceConnectionName", min_length=1)
    source_system_type: str = Field(alias="SourceSystemType", min_length=1)
    source_object_type: str = Field(alias="SourceObjectType", min_length=1)
    source_object_name: str = Field(alias="SourceObjectName", min_length=1)
    migration_approach: str = Field(alias="MigrationApproach", min_length=1)
    notes: str | None = Field(default=None, alias="Notes")


class SAPAnalysisCreate(DBRow):
    plan_guid: UUID = Field(alias="PlanGUID")
    analysis_version: int = Field(alias="AnalysisVersion", gt=0)
    datasource_name: str = Field(alias="DataSourceName", min_length=1)
    root_object_type: str | None = Field(default=None, alias="RootObjectType")
    root_object_name: str | None = Field(default=None, alias="RootObjectName")
    analysis_status: str = Field(default="DRAFT", alias="AnalysisStatus")
    summary: str | None = Field(default=None, alias="Summary")
    llm_model: str | None = Field(default=None, alias="LLMModel")
    phoenix_trace_id: str | None = Field(default=None, alias="PhoenixTraceId")


class SAPAnalysisRecord(SAPAnalysisCreate):
    analysis_guid: UUID = Field(alias="AnalysisGUID")
    approved_by: str | None = Field(default=None, alias="ApprovedBy")
    approved_timestamp: datetime | None = Field(default=None, alias="ApprovedTimestamp")


class SAPAnalysisObjectCreate(DBRow):
    object_type: str = Field(alias="ObjectType", min_length=1)
    object_name: str = Field(alias="ObjectName", min_length=1)
    parent_object_name: str | None = Field(default=None, alias="ParentObjectName")
    dependency_type: str | None = Field(default=None, alias="DependencyType")
    dependency_depth: int | None = Field(default=None, alias="DependencyDepth", ge=0)
    observed_from_scraper: bool = Field(default=True, alias="ObservedFromScraper")
    interpreted_role: str | None = Field(default=None, alias="InterpretedRole")
    interpretation: str | None = Field(default=None, alias="Interpretation")
    proposed_action: str | None = Field(default=None, alias="ProposedAction")
    user_decision: str | None = Field(default=None, alias="UserDecision")
    user_decision_notes: str | None = Field(default=None, alias="UserDecisionNotes")
    is_replication_required: bool | None = Field(default=None, alias="IsReplicationRequired")
    is_active: bool = Field(default=True, alias="IsActive")


class SAPAnalysisObjectRecord(SAPAnalysisObjectCreate):
    analysis_object_guid: UUID = Field(alias="AnalysisObjectGUID")
    analysis_guid: UUID = Field(alias="AnalysisGUID")


class SourceTableCreate(DBRow):
    connection_name: str = Field(alias="ConnectionName", min_length=1)
    source_system_type: str = Field(alias="SourceSystemType", min_length=1)
    source_object_type: str = Field(alias="SourceObjectType", min_length=1)
    source_table_name: str = Field(alias="SourceTableName", min_length=1)
    fabric_workspace_id: str = Field(alias="FabricWorkspaceId", min_length=1)
    fabric_lakehouse_name: str = Field(alias="FabricLakehouseName", min_length=1)
    fabric_lakehouse_schema: str = Field(alias="FabricLakehouseSchema", min_length=1)
    fabric_table_name: str = Field(alias="FabricTableName", min_length=1)
    source_schema_name: str | None = Field(default=None, alias="SourceSchemaName")
    parent_source_object_guid: UUID | None = Field(default=None, alias="ParentSourceObjectGUID")
    parent_source_index: int | None = Field(default=None, ge=0, exclude=True)
    data_source_type: str | None = Field(default=None, alias="DataSourceType")
    application_component: str | None = Field(default=None, alias="ApplicationComponent")
    delta: str | None = Field(default=None, alias="Delta")
    replication_method: str | None = Field(default=None, alias="ReplicationMethod")
    object_role: str | None = Field(default=None, alias="ObjectRole")
    fabric_workspace_name: str | None = Field(default=None, alias="FabricWorkspaceName")
    fabric_lakehouse_id: str | None = Field(default=None, alias="FabricLakehouseId")
    is_active: bool = Field(default=True, alias="IsActive")

    @model_validator(mode="after")
    def one_parent_reference(self) -> "SourceTableCreate":
        if self.parent_source_object_guid is not None and self.parent_source_index is not None:
            raise ValueError("use either parent GUID or parent plan index")
        return self


class SourceTableColumnCreate(DBRow):
    sno: int = Field(alias="Sno", gt=0)
    column_name: str = Field(alias="ColumnName", min_length=1)
    source_data_type: str = Field(alias="SourceDataType", min_length=1)
    fabric_data_type: str = Field(alias="FabricDataType", min_length=1)
    target_column_name: str | None = Field(default=None, alias="TargetColumnName")
    description: str | None = Field(default=None, alias="Description")
    is_primary_key: bool = Field(default=False, alias="IsPrimaryKey")
    is_nullable: bool | None = Field(default=None, alias="IsNullable")
    is_watermark_candidate: bool = Field(default=False, alias="IsWatermarkCandidate")
    is_selected: bool = Field(default=True, alias="IsSelected")
    source_expression: str | None = Field(default=None, alias="SourceExpression")
    transformation_notes: str | None = Field(default=None, alias="TransformationNotes")


class SourceTableRecord(SourceTableCreate):
    guid: UUID = Field(alias="GUID")
    plan_guid: UUID = Field(alias="PlanGUID")
    provisioning_status: str = Field(alias="ProvisioningStatus")
    provisioning_error: str | None = Field(alias="ProvisioningError")
    last_provisioning_run_id: str | None = Field(alias="LastProvisioningRunId")
    provisioned_timestamp: datetime | None = Field(alias="ProvisionedTimestamp")


class SourceTableColumnRecord(SourceTableColumnCreate):
    source_table_column_guid: UUID = Field(alias="SourceTableColumnGUID")
    guid: UUID = Field(alias="GUID")


class ReplicationConfigCreate(DBRow):
    incremental_method: Literal["FULL", "WATERMARK", "SAP_ODP_DELTA", "CDC"] = Field(default="FULL", alias="IncrementalMethod")
    write_strategy: Literal["APPEND", "UPSERT", "SCD1", "SCD2", "REPLACE"] = Field(default="APPEND", alias="WriteStrategy")
    watermark_column: str | None = Field(default=None, alias="WatermarkColumn")
    watermark_column_data_type: str | None = Field(default=None, alias="WatermarkColumnDataType")
    primary_key_columns: list[str] | None = Field(default=None, alias="PrimaryKeyColumns")
    merge_key_columns: list[str] | None = Field(default=None, alias="MergeKeyColumns")
    effective_from_column: str | None = Field(default=None, alias="EffectiveFromColumn")
    effective_to_column: str | None = Field(default=None, alias="EffectiveToColumn")
    current_flag_column: str | None = Field(default=None, alias="CurrentFlagColumn")
    max_row_fetch: int = Field(default=0, alias="MaxRowFetch", ge=0)
    ingestion_flag: bool = Field(default=True, alias="IngestionFlag")
    pipeline_workspace_id: str | None = Field(default=None, alias="PipelineWorkspaceId")
    pipeline_item_id: str | None = Field(default=None, alias="PipelineItemId")

    @model_validator(mode="after")
    def check_strategy(self) -> "ReplicationConfigCreate":
        if self.incremental_method == "WATERMARK" and not (self.watermark_column and self.watermark_column_data_type):
            raise ValueError("WATERMARK requires column and datatype")
        if self.write_strategy in {"UPSERT", "SCD1", "SCD2"} and not (self.merge_key_columns or self.primary_key_columns):
            raise ValueError("merge strategy requires key columns")
        return self


class ReplicationConfigRecord(ReplicationConfigCreate):
    replication_config_guid: UUID = Field(alias="ReplicationConfigGUID")
    source_table_guid: UUID = Field(alias="SourceTableGUID")


class ReplicationStateRecord(DBRow):
    source_table_guid: UUID = Field(alias="SourceTableGUID")
    last_watermark_value: str | None = Field(alias="LastWatermarkValue")
    last_successful_batch_run_id: UUID | None = Field(alias="LastSuccessfulBatchRunId")
    last_pipeline_run_id: str | None = Field(alias="LastPipelineRunId")
    last_run_started_timestamp: datetime | None = Field(alias="LastRunStartedTimestamp")
    last_run_completed_timestamp: datetime | None = Field(alias="LastRunCompletedTimestamp")
    last_successful_timestamp: datetime | None = Field(alias="LastSuccessfulTimestamp")
    rows_read: int | None = Field(alias="RowsRead")
    rows_written: int | None = Field(alias="RowsWritten")
    status: str = Field(alias="Status")
    error_message: str | None = Field(alias="ErrorMessage")


class SourceTablePlan(DBRow):
    table: SourceTableCreate
    columns: list[SourceTableColumnCreate] = Field(min_length=1)
    replication: ReplicationConfigCreate

    @model_validator(mode="after")
    def unique_columns(self) -> "SourceTablePlan":
        positions = [column.sno for column in self.columns]
        names = [column.column_name.upper() for column in self.columns]
        if len(set(positions)) != len(positions) or len(set(names)) != len(names):
            raise ValueError("table column positions and names must be unique")
        return self


class FabricViewCreate(DBRow):
    fabric_workspace_id: str = Field(alias="FabricWorkspaceId", min_length=1)
    fabric_lakehouse_name: str = Field(alias="FabricLakehouseName", min_length=1)
    fabric_schema_name: str = Field(alias="FabricSchemaName", min_length=1)
    view_name: str = Field(alias="ViewName", min_length=1)
    view_sql: str = Field(alias="ViewSQL", min_length=1)
    fabric_workspace_name: str | None = Field(default=None, alias="FabricWorkspaceName")
    fabric_lakehouse_id: str | None = Field(default=None, alias="FabricLakehouseId")
    view_type: str = Field(default="SQL_VIEW", alias="ViewType")
    logic_description: str | None = Field(default=None, alias="LogicDescription")
    creation_order: int = Field(default=1, alias="CreationOrder", gt=0)
    is_active: bool = Field(default=True, alias="IsActive")


class FabricViewDependencyCreate(DBRow):
    dependency_type: Literal["SOURCE_TABLE", "FABRIC_VIEW", "EXISTING_OBJECT"] = Field(alias="DependencyType")
    source_table_guid: UUID | None = Field(default=None, alias="SourceTableGUID")
    depends_on_view_guid: UUID | None = Field(default=None, alias="DependsOnViewGUID")
    existing_object_name: str | None = Field(default=None, alias="ExistingObjectName")
    dependency_sequence: int = Field(default=1, alias="DependencySequence", gt=0)
    source_table_index: int | None = Field(default=None, ge=0, exclude=True)
    depends_on_view_index: int | None = Field(default=None, ge=0, exclude=True)

    @model_validator(mode="after")
    def check_reference(self) -> "FabricViewDependencyCreate":
        references = (self.source_table_guid, self.source_table_index,
                      self.depends_on_view_guid, self.depends_on_view_index,
                      self.existing_object_name)
        expected = {"SOURCE_TABLE": {0, 1}, "FABRIC_VIEW": {2, 3},
                    "EXISTING_OBJECT": {4}}[self.dependency_type]
        selected = [index for index, value in enumerate(references) if value is not None]
        if len(selected) != 1 or selected[0] not in expected:
            raise ValueError("dependency must contain exactly its matching reference")
        return self


class FabricViewRecord(FabricViewCreate):
    view_guid: UUID = Field(alias="ViewGUID")
    plan_guid: UUID = Field(alias="PlanGUID")
    provisioning_status: str = Field(alias="ProvisioningStatus")
    provisioning_error: str | None = Field(alias="ProvisioningError")
    last_provisioning_run_id: str | None = Field(alias="LastProvisioningRunId")
    provisioned_timestamp: datetime | None = Field(alias="ProvisionedTimestamp")


class FabricViewDependencyRecord(FabricViewDependencyCreate):
    view_dependency_guid: UUID = Field(alias="ViewDependencyGUID")
    view_guid: UUID = Field(alias="ViewGUID")


class FabricViewPlan(DBRow):
    view: FabricViewCreate
    dependencies: list[FabricViewDependencyCreate] = Field(default_factory=list)


class BatchRunCreate(DBRow):
    batch_type: Literal["PROVISIONING", "REPLICATION", "VALIDATION"] = Field(alias="BatchType")
    plan_guid: UUID | None = Field(default=None, alias="PlanGUID")
    trigger_type: str | None = Field(default=None, alias="TriggerType")
    temporal_workflow_id: str | None = Field(default=None, alias="TemporalWorkflowId")
    temporal_run_id: str | None = Field(default=None, alias="TemporalRunId")
    fabric_job_instance_id: str | None = Field(default=None, alias="FabricJobInstanceId")


class BatchObjectRunCreate(DBRow):
    batch_run_id: UUID = Field(alias="BatchRunId")
    object_type: Literal["TABLE", "VIEW", "NOTEBOOK", "PIPELINE"] = Field(alias="ObjectType")
    object_guid: UUID | None = Field(default=None, alias="ObjectGUID")
    object_name: str | None = Field(default=None, alias="ObjectName")
    fabric_pipeline_run_id: str | None = Field(default=None, alias="FabricPipelineRunId")
    fabric_notebook_run_id: str | None = Field(default=None, alias="FabricNotebookRunId")
    old_watermark_value: str | None = Field(default=None, alias="OldWatermarkValue")


class BatchRunRecord(DBRow):
    batch_run_id: UUID = Field(alias="BatchRunId")
    plan_guid: UUID | None = Field(alias="PlanGUID")
    batch_type: str = Field(alias="BatchType")
    trigger_type: str | None = Field(alias="TriggerType")
    temporal_workflow_id: str | None = Field(alias="TemporalWorkflowId")
    temporal_run_id: str | None = Field(alias="TemporalRunId")
    fabric_job_instance_id: str | None = Field(alias="FabricJobInstanceId")
    started_timestamp: datetime = Field(alias="StartedTimestamp")
    completed_timestamp: datetime | None = Field(alias="CompletedTimestamp")
    status: str = Field(alias="Status")
    total_objects: int | None = Field(alias="TotalObjects")
    succeeded_objects: int | None = Field(alias="SucceededObjects")
    failed_objects: int | None = Field(alias="FailedObjects")
    error_message: str | None = Field(alias="ErrorMessage")


class BatchObjectRunRecord(DBRow):
    batch_object_run_id: UUID = Field(alias="BatchObjectRunId")
    batch_run_id: UUID = Field(alias="BatchRunId")
    object_guid: UUID | None = Field(alias="ObjectGUID")
    object_type: str = Field(alias="ObjectType")
    object_name: str | None = Field(alias="ObjectName")
    fabric_pipeline_run_id: str | None = Field(alias="FabricPipelineRunId")
    fabric_notebook_run_id: str | None = Field(alias="FabricNotebookRunId")
    started_timestamp: datetime = Field(alias="StartedTimestamp")
    completed_timestamp: datetime | None = Field(alias="CompletedTimestamp")
    rows_read: int | None = Field(alias="RowsRead")
    rows_written: int | None = Field(alias="RowsWritten")
    old_watermark_value: str | None = Field(alias="OldWatermarkValue")
    new_watermark_value: str | None = Field(alias="NewWatermarkValue")
    status: str = Field(alias="Status")
    error_message: str | None = Field(alias="ErrorMessage")


class ApprovedRuntimePlan(DBRow):
    plan_guid: UUID
    plan_version: int = Field(gt=0)
    tables: list[SourceTablePlan] = Field(default_factory=list)
    views: list[FabricViewPlan] = Field(default_factory=list)

    @model_validator(mode="after")
    def check_dependency_order(self) -> "ApprovedRuntimePlan":
        for index, planned in enumerate(self.tables):
            parent = planned.table.parent_source_index
            if parent is not None and parent >= index:
                raise ValueError("source table parent must appear earlier in the plan")
        for index, planned in enumerate(self.views):
            for dependency in planned.dependencies:
                if (dependency.source_table_index is not None
                        and dependency.source_table_index >= len(self.tables)):
                    raise ValueError("source table dependency index is out of range")
                if dependency.depends_on_view_index is not None:
                    earlier = dependency.depends_on_view_index
                    if earlier >= len(self.views) or earlier == index:
                        raise ValueError("view dependency index is invalid")
                    if self.views[earlier].view.creation_order >= planned.view.creation_order:
                        raise ValueError("dependent view must have an earlier creation order")
        return self


class PersistedRuntimePlan(DBRow):
    plan_guid: UUID
    source_table_guids: list[UUID]
    view_guids: list[UUID]


class SourceTableRuntime(DBRow):
    table: SourceTableRecord
    columns: list[SourceTableColumnRecord]
    replication: ReplicationConfigRecord


class FabricViewRuntime(DBRow):
    view: FabricViewRecord
    dependencies: list[FabricViewDependencyRecord]


class ApprovedPlanRuntimeConfig(DBRow):
    plan: MigrationPlanRecord
    tables: list[SourceTableRuntime]
    views: list[FabricViewRuntime]


def _sql_value(value: object) -> object:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (list, dict)):
        return json.dumps(value, separators=(",", ":"))
    return value


def _runtime_fingerprint(plan: ApprovedRuntimePlan) -> str:
    payload = plan.model_dump(mode="json")
    # Plan-local references are excluded from SQL serialization, but must be
    # included in the idempotency hash.
    payload["dependency_indices"] = [
        [(item.source_table_index, item.depends_on_view_index)
         for item in view.dependencies]
        for view in plan.views
    ]
    payload["parent_source_indices"] = [item.table.parent_source_index for item in plan.tables]
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return sha256(encoded).hexdigest()


_SAP_ANALYSIS_COLUMNS = (
    "AnalysisGUID, PlanGUID, AnalysisVersion, DataSourceName, RootObjectType, "
    "RootObjectName, AnalysisStatus, Summary, LLMModel, PhoenixTraceId, "
    "ApprovedBy, ApprovedTimestamp"
)
_BATCH_RUN_COLUMNS = (
    "BatchRunId, PlanGUID, BatchType, TriggerType, TemporalWorkflowId, "
    "TemporalRunId, FabricJobInstanceId, StartedTimestamp, CompletedTimestamp, "
    "Status, TotalObjects, SucceededObjects, FailedObjects, ErrorMessage"
)
_BATCH_OBJECT_RUN_COLUMNS = (
    "BatchObjectRunId, BatchRunId, ObjectGUID, ObjectType, ObjectName, "
    "FabricPipelineRunId, FabricNotebookRunId, StartedTimestamp, "
    "CompletedTimestamp, RowsRead, RowsWritten, OldWatermarkValue, "
    "NewWatermarkValue, Status, ErrorMessage"
)


class ConfigDBRepository(ConfigDB):
    """Current-schema writes with explicit transaction boundaries."""

    @contextmanager
    def transaction(self) -> Iterator["Connection"]:
        connection = self.connect()
        try:
            if getattr(connection, "autocommit", False) is True:
                raise RuntimeError("Config DB transaction requires autocommit disabled")
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    @staticmethod
    def _insert(cursor: object, table: str, values: dict[str, object]) -> None:
        # The caller supplies table and column names exclusively from fixed model
        # definitions. Values remain bound parameters.
        columns = list(values)
        sql = (f"INSERT INTO bronze_replication.{table} (" + ", ".join(columns)
               + ") VALUES (" + ", ".join("?" for _ in columns) + ")")
        cursor.execute(sql, tuple(_sql_value(values[name]) for name in columns))

    @staticmethod
    def _rows(cursor: object, model: type[DBRow]) -> list[DBRow]:
        names = [column[0] for column in cursor.description]
        records = []
        for row in cursor.fetchall():
            values = dict(zip(names, row, strict=True))
            for key in ("ConnectionDetails", "ConnectionTags", "PrimaryKeyColumns", "MergeKeyColumns"):
                if key in values and values[key] is not None:
                    values[key] = json.loads(values[key])
            records.append(model.model_validate(values))
        return records

    def list_connections(self, *, active_only: bool = True) -> list[DBConnectionRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT Id, SourceType, ConnectionName, ConnectionDetails, ConnectionTags, IsActive "
                    "FROM bronze_replication.DBConnections "
                    "WHERE (? = 0 OR IsActive = 1) ORDER BY ConnectionName", (int(active_only),))
                return self._rows(cursor, DBConnectionRecord)

    def get_connection(self, connection_name: str) -> DBConnectionRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT Id, SourceType, ConnectionName, ConnectionDetails, ConnectionTags, IsActive "
                    "FROM bronze_replication.DBConnections WHERE ConnectionName = ?",
                    (connection_name,))
                rows = self._rows(cursor, DBConnectionRecord)
                return rows[0] if rows else None

    def insert_connection(self, value: DBConnectionCreate) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "DBConnections", value.model_dump(by_alias=True))

    def update_connection(self, value: DBConnectionCreate) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.DBConnections SET SourceType = ?, ConnectionDetails = ?, "
                    "ConnectionTags = ?, IsActive = ?, UpdatedTimestamp = SYSUTCDATETIME() "
                    "WHERE ConnectionName = ?",
                    (value.source_type, _sql_value(value.connection_details),
                     _sql_value(value.connection_tags), value.is_active, value.connection_name))
                if cursor.rowcount != 1:
                    raise ValueError("connection does not exist")

    def get_migration_plan(self, plan_guid: UUID) -> MigrationPlanRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT PlanGUID, SourceConnectionName, SourceSystemType, SourceObjectType, "
                    "SourceObjectName, MigrationApproach, AnalysisVersion, PlanVersion, RuntimePlanHash, Status, "
                    "ApprovedBy, ApprovedTimestamp, TemporalWorkflowId, TemporalRunId, Notes "
                    "FROM bronze_replication.MigrationPlans WHERE PlanGUID = ?", (str(plan_guid),))
                rows = self._rows(cursor, MigrationPlanRecord)
                return rows[0] if rows else None

    def update_migration_plan_status(
        self, plan_guid: UUID, *, expected_status: PlanStatus, new_status: PlanStatus,
        expected_plan_version: int,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.MigrationPlans SET Status = ?, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ? "
                    "AND Status = ? AND PlanVersion = ?",
                    (new_status, str(plan_guid), expected_status, expected_plan_version))
                if cursor.rowcount != 1:
                    raise ValueError("plan status or version changed")

    def record_plan_approval(self, plan_guid: UUID, *, approved_by: str,
                             expected_plan_version: int) -> None:
        if not approved_by.strip():
            raise ValueError("approved_by must be nonempty")
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.MigrationPlans SET Status = 'APPROVED', "
                    "ApprovedBy = ?, ApprovedTimestamp = SYSUTCDATETIME(), "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ? "
                    "AND Status = 'WAITING_PLAN_APPROVAL' AND PlanVersion = ?",
                    (approved_by, str(plan_guid), expected_plan_version))
                if cursor.rowcount != 1:
                    raise ValueError("plan cannot be approved at this version")

    def set_temporal_ids(self, plan_guid: UUID, workflow_id: str,
                         run_id: str) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.MigrationPlans SET TemporalWorkflowId = ?, "
                    "TemporalRunId = ?, UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ?",
                    (workflow_id, run_id, str(plan_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("plan does not exist")

    def update_migration_plan_version(self, plan_guid: UUID, *,
                                      expected_version: int) -> int:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.MigrationPlans SET PlanVersion = PlanVersion + 1, "
                    "Status = 'DRAFT', ApprovedBy = NULL, ApprovedTimestamp = NULL, "
                    "RuntimePlanHash = NULL, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ? "
                    "AND PlanVersion = ? AND Status NOT IN "
                    "('READY_TO_PROVISION', 'PROVISIONING', 'PROVISIONED')",
                    (str(plan_guid), expected_version))
                if cursor.rowcount != 1:
                    raise ValueError("plan version changed or is already provisioning")
        return expected_version + 1

    def update_analysis_version(self, plan_guid: UUID, *, expected_version: int) -> int:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.MigrationPlans SET "
                    "AnalysisVersion = AnalysisVersion + 1, Status = 'DRAFT', "
                    "ApprovedBy = NULL, ApprovedTimestamp = NULL, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ? "
                    "AND AnalysisVersion = ? AND Status NOT IN "
                    "('READY_TO_PROVISION', 'PROVISIONING', 'PROVISIONED')",
                    (str(plan_guid), expected_version))
                if cursor.rowcount != 1:
                    raise ValueError("analysis version changed or plan is already provisioning")
        return expected_version + 1

    def create_sap_analysis(self, analysis: SAPAnalysisCreate) -> UUID:
        guid = uuid4()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "SAPAnalysis", {
                    "AnalysisGUID": guid, **analysis.model_dump(by_alias=True, exclude_none=True)})
        return guid

    def get_sap_analysis(self, analysis_guid: UUID) -> SAPAnalysisRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {_SAP_ANALYSIS_COLUMNS} "
                    "FROM bronze_replication.SAPAnalysis WHERE AnalysisGUID = ?",
                    (str(analysis_guid),))
                rows = self._rows(cursor, SAPAnalysisRecord)
                return rows[0] if rows else None

    def get_sap_analysis_for_plan(
        self, plan_guid: UUID, *, analysis_version: int | None = None,
    ) -> SAPAnalysisRecord | None:
        if analysis_version is not None and analysis_version < 1:
            raise ValueError("analysis_version must be positive")
        with self.connect() as connection:
            with connection.cursor() as cursor:
                sql = (f"SELECT TOP (1) {_SAP_ANALYSIS_COLUMNS} "
                       "FROM bronze_replication.SAPAnalysis WHERE PlanGUID = ?")
                params: tuple[object, ...] = (str(plan_guid),)
                if analysis_version is not None:
                    sql += " AND AnalysisVersion = ?"
                    params += (analysis_version,)
                sql += " ORDER BY AnalysisVersion DESC, AnalysisGUID DESC"
                cursor.execute(sql, params)
                rows = self._rows(cursor, SAPAnalysisRecord)
                return rows[0] if rows else None

    def list_sap_analyses_for_plan(self, plan_guid: UUID) -> list[SAPAnalysisRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {_SAP_ANALYSIS_COLUMNS} FROM bronze_replication.SAPAnalysis "
                    "WHERE PlanGUID = ? ORDER BY AnalysisVersion DESC, AnalysisGUID DESC",
                    (str(plan_guid),))
                return self._rows(cursor, SAPAnalysisRecord)

    def update_sap_analysis_status(
        self, analysis_guid: UUID, *, expected_status: str,
        new_status: str, expected_version: int,
    ) -> None:
        allowed = {("DRAFT", "WAITING_APPROVAL"),
                   ("WAITING_APPROVAL", "DRAFT")}
        if (expected_status, new_status) not in allowed:
            raise ValueError("unsupported analysis status transition")
        if expected_version < 1:
            raise ValueError("expected_version must be positive")
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.SAPAnalysis SET AnalysisStatus = ?, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE AnalysisGUID = ? "
                    "AND AnalysisStatus = ? AND AnalysisVersion = ?",
                    (new_status, str(analysis_guid), expected_status, expected_version))
                if cursor.rowcount != 1:
                    raise ValueError("analysis status or version changed")

    def insert_sap_analysis_objects(self, analysis_guid: UUID,
                                    objects: list[SAPAnalysisObjectCreate]) -> list[UUID]:
        identifiers = [uuid4() for _ in objects]
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                for guid, item in zip(identifiers, objects, strict=True):
                    self._insert(cursor, "SAPAnalysisObjects", {
                        "AnalysisObjectGUID": guid, "AnalysisGUID": analysis_guid,
                        **item.model_dump(by_alias=True, exclude_none=True),
                    })
        return identifiers

    def list_sap_analysis_objects(self, analysis_guid: UUID) -> list[SAPAnalysisObjectRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT AnalysisObjectGUID, AnalysisGUID, ObjectType, ObjectName, "
                    "ParentObjectName, DependencyType, DependencyDepth, ObservedFromScraper, "
                    "InterpretedRole, Interpretation, ProposedAction, UserDecision, "
                    "UserDecisionNotes, IsReplicationRequired, IsActive "
                    "FROM bronze_replication.SAPAnalysisObjects WHERE AnalysisGUID = ? "
                    "ORDER BY DependencyDepth, ObjectName", (str(analysis_guid),))
                return self._rows(cursor, SAPAnalysisObjectRecord)

    def record_analysis_approval(self, analysis_guid: UUID, *, approved_by: str,
                                 expected_version: int) -> None:
        if not approved_by.strip():
            raise ValueError("approved_by must be nonempty")
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.SAPAnalysis SET AnalysisStatus = 'APPROVED', "
                    "ApprovedBy = ?, ApprovedTimestamp = SYSUTCDATETIME(), "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE AnalysisGUID = ? "
                    "AND AnalysisVersion = ? AND AnalysisStatus = 'WAITING_APPROVAL'",
                    (approved_by, str(analysis_guid), expected_version))
                if cursor.rowcount != 1:
                    raise ValueError("analysis cannot be approved at this version")

    def supersede_analysis(self, analysis_guid: UUID) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.SAPAnalysis SET AnalysisStatus = 'SUPERSEDED', "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE AnalysisGUID = ? "
                    "AND AnalysisStatus <> 'SUPERSEDED'", (str(analysis_guid),))
                if cursor.rowcount != 1:
                    raise ValueError("analysis does not exist or is already superseded")

    def start_batch_run(self, run: BatchRunCreate) -> UUID:
        guid = uuid4()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "BatchRuns", {
                    "BatchRunId": guid, **run.model_dump(by_alias=True, exclude_none=True)})
        return guid

    def get_batch_run(self, batch_run_id: UUID) -> BatchRunRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {_BATCH_RUN_COLUMNS} FROM bronze_replication.BatchRuns "
                    "WHERE BatchRunId = ?", (str(batch_run_id),))
                rows = self._rows(cursor, BatchRunRecord)
                return rows[0] if rows else None

    def list_batch_runs_for_plan(
        self, plan_guid: UUID, *, limit: int = 100,
    ) -> list[BatchRunRecord]:
        if type(limit) is not int or limit < 1:
            raise ValueError("limit must be a positive integer")
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT TOP ({limit}) {_BATCH_RUN_COLUMNS} "
                    "FROM bronze_replication.BatchRuns WHERE PlanGUID = ? "
                    "ORDER BY StartedTimestamp DESC, BatchRunId DESC",
                    (str(plan_guid),))
                return self._rows(cursor, BatchRunRecord)

    def _set_running_run_id(
        self, *, table: Literal["BatchRuns", "BatchObjectRuns"],
        key_column: Literal["BatchRunId", "BatchObjectRunId"],
        value_column: Literal["FabricJobInstanceId", "FabricNotebookRunId", "FabricPipelineRunId"],
        row_id: UUID, run_id: str,
    ) -> None:
        if not isinstance(run_id, str) or not run_id.strip():
            raise ValueError("run ID must be nonempty")
        run_id = run_id.strip()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE bronze_replication.{table} SET {value_column} = ? "
                    f"WHERE {key_column} = ? AND Status = 'RUNNING' "
                    f"AND {value_column} IS NULL", (run_id, str(row_id)))
                if cursor.rowcount == 1:
                    return
                cursor.execute(
                    f"SELECT {value_column} FROM bronze_replication.{table} "
                    f"WHERE {key_column} = ?", (str(row_id),))
                row = cursor.fetchone()
                if row is None or row[0] != run_id:
                    raise ValueError("run record is missing or has a different run ID")

    def set_batch_fabric_job_id(
        self, batch_run_id: UUID, fabric_job_instance_id: str,
    ) -> None:
        self._set_running_run_id(
            table="BatchRuns", key_column="BatchRunId",
            value_column="FabricJobInstanceId", row_id=batch_run_id,
            run_id=fabric_job_instance_id)

    def cancel_batch_run(self, batch_run_id: UUID, *, reason: str | None = None) -> None:
        self._cancel_running_run(
            table="BatchRuns", key_column="BatchRunId", row_id=batch_run_id,
            reason=reason)

    def _cancel_running_run(
        self, *, table: Literal["BatchRuns", "BatchObjectRuns"],
        key_column: Literal["BatchRunId", "BatchObjectRunId"],
        row_id: UUID, reason: str | None,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"UPDATE bronze_replication.{table} SET Status = 'CANCELLED', "
                    "CompletedTimestamp = SYSUTCDATETIME(), "
                    "ErrorMessage = COALESCE(?, ErrorMessage) "
                    f"WHERE {key_column} = ? AND Status = 'RUNNING'",
                    (reason, str(row_id)))
                if cursor.rowcount == 1:
                    return
                cursor.execute(
                    f"SELECT Status FROM bronze_replication.{table} "
                    f"WHERE {key_column} = ?", (str(row_id),))
                row = cursor.fetchone()
                if row is None or row[0] != "CANCELLED":
                    raise ValueError("run is missing or cannot be cancelled")

    def complete_batch_run(self, batch_run_id: UUID, *, succeeded_objects: int,
                           failed_objects: int, total_objects: int) -> None:
        if min(succeeded_objects, failed_objects, total_objects) < 0 or succeeded_objects + failed_objects > total_objects:
            raise ValueError("invalid batch object counts")
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.BatchRuns SET Status = ?, "
                    "CompletedTimestamp = SYSUTCDATETIME(), TotalObjects = ?, "
                    "SucceededObjects = ?, FailedObjects = ? "
                    "WHERE BatchRunId = ? AND Status = 'RUNNING'",
                    ("FAILED" if failed_objects else "SUCCEEDED", total_objects,
                     succeeded_objects, failed_objects, str(batch_run_id)))
                if cursor.rowcount != 1:
                    raise ValueError("batch run is not running")

    def fail_batch_run(self, batch_run_id: UUID, error_message: str) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.BatchRuns SET Status = 'FAILED', "
                    "CompletedTimestamp = SYSUTCDATETIME(), ErrorMessage = ? "
                    "WHERE BatchRunId = ? AND Status = 'RUNNING'",
                    (error_message, str(batch_run_id)))
                if cursor.rowcount != 1:
                    raise ValueError("batch run is not running")

    def start_batch_object_run(self, run: BatchObjectRunCreate) -> UUID:
        guid = uuid4()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "BatchObjectRuns", {
                    "BatchObjectRunId": guid, **run.model_dump(by_alias=True, exclude_none=True)})
        return guid

    def list_batch_object_runs(
        self, batch_run_id: UUID,
    ) -> list[BatchObjectRunRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {_BATCH_OBJECT_RUN_COLUMNS} "
                    "FROM bronze_replication.BatchObjectRuns WHERE BatchRunId = ? "
                    "ORDER BY StartedTimestamp, BatchObjectRunId",
                    (str(batch_run_id),))
                return self._rows(cursor, BatchObjectRunRecord)

    def set_batch_object_notebook_run_id(
        self, batch_object_run_id: UUID, notebook_run_id: str,
    ) -> None:
        self._set_running_run_id(
            table="BatchObjectRuns", key_column="BatchObjectRunId",
            value_column="FabricNotebookRunId", row_id=batch_object_run_id,
            run_id=notebook_run_id)

    def set_batch_object_pipeline_run_id(
        self, batch_object_run_id: UUID, pipeline_run_id: str,
    ) -> None:
        self._set_running_run_id(
            table="BatchObjectRuns", key_column="BatchObjectRunId",
            value_column="FabricPipelineRunId", row_id=batch_object_run_id,
            run_id=pipeline_run_id)

    def cancel_batch_object_run(
        self, batch_object_run_id: UUID, *, reason: str | None = None,
    ) -> None:
        self._cancel_running_run(
            table="BatchObjectRuns", key_column="BatchObjectRunId",
            row_id=batch_object_run_id, reason=reason)

    def complete_batch_object_run(self, run_id: UUID, *, rows_read: int | None = None,
                                  rows_written: int | None = None,
                                  new_watermark_value: str | None = None) -> None:
        if any(value is not None and value < 0 for value in (rows_read, rows_written)):
            raise ValueError("row counts must be nonnegative")
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.BatchObjectRuns SET Status = 'SUCCEEDED', "
                    "CompletedTimestamp = SYSUTCDATETIME(), RowsRead = ?, RowsWritten = ?, "
                    "NewWatermarkValue = ? WHERE BatchObjectRunId = ? AND Status = 'RUNNING'",
                    (rows_read, rows_written, new_watermark_value, str(run_id)))
                if cursor.rowcount != 1:
                    raise ValueError("batch object run is not running")

    def fail_batch_object_run(self, run_id: UUID, error_message: str) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.BatchObjectRuns SET Status = 'FAILED', "
                    "CompletedTimestamp = SYSUTCDATETIME(), ErrorMessage = ? "
                    "WHERE BatchObjectRunId = ? AND Status = 'RUNNING'",
                    (error_message, str(run_id)))
                if cursor.rowcount != 1:
                    raise ValueError("batch object run is not running")

    def create_replication_config(self, source_table_guid: UUID,
                                  config: ReplicationConfigCreate) -> UUID:
        guid = uuid4()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "ReplicationConfig", {
                    "ReplicationConfigGUID": guid, "SourceTableGUID": source_table_guid,
                    **config.model_dump(by_alias=True),
                })
        return guid

    def get_replication_config(self, source_table_guid: UUID) -> ReplicationConfigRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ReplicationConfigGUID, SourceTableGUID, IncrementalMethod, "
                    "WatermarkColumn, WatermarkColumnDataType, WriteStrategy, PrimaryKeyColumns, "
                    "MergeKeyColumns, EffectiveFromColumn, EffectiveToColumn, CurrentFlagColumn, "
                    "MaxRowFetch, IngestionFlag, PipelineWorkspaceId, PipelineItemId "
                    "FROM bronze_replication.ReplicationConfig WHERE SourceTableGUID = ?",
                    (str(source_table_guid),))
                rows = self._rows(cursor, ReplicationConfigRecord)
                return rows[0] if rows else None

    def update_replication_config(self, source_table_guid: UUID,
                                  config: ReplicationConfigCreate) -> None:
        values = config.model_dump(by_alias=True)
        assignments = ", ".join(f"{name} = ?" for name in values)
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationConfig SET " + assignments +
                    ", UpdatedTimestamp = SYSUTCDATETIME() WHERE SourceTableGUID = ?",
                    tuple(_sql_value(value) for value in values.values()) + (str(source_table_guid),))
                if cursor.rowcount != 1:
                    raise ValueError("replication config does not exist")

    def set_replication_enabled(self, source_table_guid: UUID, enabled: bool) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationConfig SET IngestionFlag = ?, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE SourceTableGUID = ?",
                    (enabled, str(source_table_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("replication config does not exist")

    def create_replication_state(self, source_table_guid: UUID) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "ReplicationState", {"SourceTableGUID": source_table_guid})

    def get_replication_state(self, source_table_guid: UUID) -> ReplicationStateRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT SourceTableGUID, LastWatermarkValue, LastSuccessfulBatchRunId, "
                    "LastPipelineRunId, LastRunStartedTimestamp, LastRunCompletedTimestamp, "
                    "LastSuccessfulTimestamp, RowsRead, RowsWritten, Status, ErrorMessage "
                    "FROM bronze_replication.ReplicationState WHERE SourceTableGUID = ?",
                    (str(source_table_guid),))
                rows = self._rows(cursor, ReplicationStateRecord)
                return rows[0] if rows else None

    def update_watermark(
        self, source_table_guid: UUID, watermark_value: str | None,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationState SET "
                    "LastWatermarkValue = ?, UpdatedTimestamp = SYSUTCDATETIME() "
                    "WHERE SourceTableGUID = ?",
                    (watermark_value, str(source_table_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("replication state does not exist")

    def set_replication_pipeline_run_id(
        self, source_table_guid: UUID, pipeline_run_id: str,
    ) -> None:
        if not isinstance(pipeline_run_id, str) or not pipeline_run_id.strip():
            raise ValueError("pipeline_run_id must be nonempty")
        pipeline_run_id = pipeline_run_id.strip()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationState SET LastPipelineRunId = ?, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE SourceTableGUID = ? "
                    "AND Status = 'RUNNING' AND LastPipelineRunId IS NULL",
                    (pipeline_run_id, str(source_table_guid)))
                if cursor.rowcount == 1:
                    return
                cursor.execute(
                    "SELECT LastPipelineRunId FROM bronze_replication.ReplicationState "
                    "WHERE SourceTableGUID = ?", (str(source_table_guid),))
                row = cursor.fetchone()
                if row is None or row[0] != pipeline_run_id:
                    raise ValueError("replication state is missing or has a different pipeline run ID")

    def mark_replication_started(self, source_table_guid: UUID,
                                 pipeline_run_id: str | None = None) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationState SET Status = 'RUNNING', "
                    "LastPipelineRunId = ?, LastRunStartedTimestamp = SYSUTCDATETIME(), "
                    "ErrorMessage = NULL, UpdatedTimestamp = SYSUTCDATETIME() "
                    "WHERE SourceTableGUID = ? AND Status <> 'RUNNING'",
                    (pipeline_run_id, str(source_table_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("replication is already running or state is missing")

    def mark_replication_succeeded(self, source_table_guid: UUID, *,
                                   batch_run_id: UUID | None = None,
                                   last_watermark_value: str | None = None,
                                   rows_read: int | None = None,
                                   rows_written: int | None = None) -> None:
        if any(value is not None and value < 0 for value in (rows_read, rows_written)):
            raise ValueError("row counts must be nonnegative")
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationState SET Status = 'SUCCEEDED', "
                    "LastSuccessfulBatchRunId = COALESCE(?, LastSuccessfulBatchRunId), "
                    "LastWatermarkValue = COALESCE(?, LastWatermarkValue), "
                    "RowsRead = ?, RowsWritten = ?, LastRunCompletedTimestamp = SYSUTCDATETIME(), "
                    "LastSuccessfulTimestamp = SYSUTCDATETIME(), ErrorMessage = NULL, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE SourceTableGUID = ? AND Status = 'RUNNING'",
                    (str(batch_run_id) if batch_run_id else None, last_watermark_value,
                     rows_read, rows_written, str(source_table_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("replication is not running")

    def mark_replication_failed(self, source_table_guid: UUID, error_message: str) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.ReplicationState SET Status = 'FAILED', "
                    "ErrorMessage = ?, LastRunCompletedTimestamp = SYSUTCDATETIME(), "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE SourceTableGUID = ? AND Status = 'RUNNING'",
                    (error_message, str(source_table_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("replication is not running")

    _TABLE_COLUMNS = (
        "GUID, PlanGUID, ConnectionName, SourceSystemType, SourceObjectType, "
        "SourceSchemaName, SourceTableName, ParentSourceObjectGUID, DataSourceType, "
        "ApplicationComponent, Delta, ReplicationMethod, ObjectRole, FabricWorkspaceName, "
        "FabricWorkspaceId, FabricLakehouseName, FabricLakehouseId, FabricLakehouseSchema, "
        "FabricTableName, ProvisioningStatus, ProvisioningError, LastProvisioningRunId, "
        "ProvisionedTimestamp, IsActive"
    )

    def get_source_table(self, source_table_guid: UUID) -> SourceTableRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {self._TABLE_COLUMNS} FROM bronze_replication.SourceTables WHERE GUID = ?",
                    (str(source_table_guid),))
                rows = self._rows(cursor, SourceTableRecord)
                return rows[0] if rows else None

    def list_source_tables_for_plan(self, plan_guid: UUID) -> list[SourceTableRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {self._TABLE_COLUMNS} FROM bronze_replication.SourceTables "
                    "WHERE PlanGUID = ? ORDER BY CreatedTimestamp, GUID", (str(plan_guid),))
                return self._rows(cursor, SourceTableRecord)

    def list_source_table_columns(self, source_table_guid: UUID) -> list[SourceTableColumnRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT SourceTableColumnGUID, GUID, Sno, ColumnName, TargetColumnName, "
                    "Description, SourceDataType, FabricDataType, IsPrimaryKey, IsNullable, "
                    "IsWatermarkCandidate, IsSelected, SourceExpression, TransformationNotes "
                    "FROM bronze_replication.SourceTableColumns WHERE GUID = ? ORDER BY Sno",
                    (str(source_table_guid),))
                return self._rows(cursor, SourceTableColumnRecord)

    def update_source_table_provisioning_status(
        self, source_table_guid: UUID, *, status: Literal["PENDING", "IN_PROGRESS", "PROVISIONED", "FAILED", "SKIPPED"],
        run_id: str | None = None, error: str | None = None,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.SourceTables SET ProvisioningStatus = ?, "
                    "LastProvisioningRunId = ?, ProvisioningError = ?, "
                    "ProvisionedTimestamp = CASE WHEN ? = 'PROVISIONED' THEN SYSUTCDATETIME() "
                    "ELSE ProvisionedTimestamp END, UpdatedTimestamp = SYSUTCDATETIME() "
                    "WHERE GUID = ?",
                    (status, run_id, error, status, str(source_table_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("source table does not exist")

    _VIEW_COLUMNS = (
        "ViewGUID, PlanGUID, FabricWorkspaceName, FabricWorkspaceId, "
        "FabricLakehouseName, FabricLakehouseId, FabricSchemaName, ViewName, ViewType, "
        "ViewSQL, LogicDescription, CreationOrder, IsActive, ProvisioningStatus, "
        "ProvisioningError, LastProvisioningRunId, ProvisionedTimestamp"
    )

    def get_fabric_view(self, view_guid: UUID) -> FabricViewRecord | None:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {self._VIEW_COLUMNS} FROM bronze_replication.FabricViews WHERE ViewGUID = ?",
                    (str(view_guid),))
                rows = self._rows(cursor, FabricViewRecord)
                return rows[0] if rows else None

    def list_fabric_views_for_plan(self, plan_guid: UUID) -> list[FabricViewRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"SELECT {self._VIEW_COLUMNS} FROM bronze_replication.FabricViews "
                    "WHERE PlanGUID = ? ORDER BY CreationOrder, ViewGUID", (str(plan_guid),))
                return self._rows(cursor, FabricViewRecord)

    def list_fabric_view_dependencies(self, view_guid: UUID) -> list[FabricViewDependencyRecord]:
        with self.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT ViewDependencyGUID, ViewGUID, DependencyType, SourceTableGUID, "
                    "DependsOnViewGUID, ExistingObjectName, DependencySequence "
                    "FROM bronze_replication.FabricViewDependencies WHERE ViewGUID = ? "
                    "ORDER BY DependencySequence", (str(view_guid),))
                return self._rows(cursor, FabricViewDependencyRecord)

    def update_view_provisioning_status(
        self, view_guid: UUID, *, status: Literal["PENDING", "IN_PROGRESS", "PROVISIONED", "FAILED", "SKIPPED"],
        run_id: str | None = None, error: str | None = None,
    ) -> None:
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "UPDATE bronze_replication.FabricViews SET ProvisioningStatus = ?, "
                    "LastProvisioningRunId = ?, ProvisioningError = ?, "
                    "ProvisionedTimestamp = CASE WHEN ? = 'PROVISIONED' THEN SYSUTCDATETIME() "
                    "ELSE ProvisionedTimestamp END, UpdatedTimestamp = SYSUTCDATETIME() "
                    "WHERE ViewGUID = ?",
                    (status, run_id, error, status, str(view_guid)))
                if cursor.rowcount != 1:
                    raise ValueError("view does not exist")

    def get_approved_plan_runtime_config(self, plan_guid: UUID) -> ApprovedPlanRuntimeConfig:
        plan = self.get_migration_plan(plan_guid)
        if plan is None or plan.status not in {"READY_TO_PROVISION", "PROVISIONING", "PROVISIONED"}:
            raise ValueError("approved runtime plan does not exist")
        tables = []
        for table in self.list_source_tables_for_plan(plan_guid):
            replication = self.get_replication_config(table.guid)
            if replication is None:
                raise RuntimeError(f"approved table {table.guid} has no replication config")
            tables.append(SourceTableRuntime(
                table=table, columns=self.list_source_table_columns(table.guid),
                replication=replication))
        views = [FabricViewRuntime(
            view=view, dependencies=self.list_fabric_view_dependencies(view.view_guid))
            for view in self.list_fabric_views_for_plan(plan_guid)]
        return ApprovedPlanRuntimeConfig(plan=plan, tables=tables, views=views)

    def create_migration_plan(self, plan: MigrationPlanCreate) -> UUID:
        guid = uuid4()
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                self._insert(cursor, "MigrationPlans", {
                    "PlanGUID": guid, **plan.model_dump(by_alias=True, exclude_none=True)})
        return guid

    def persist_approved_runtime_plan(self, plan: ApprovedRuntimePlan) -> PersistedRuntimePlan:
        plan.check_dependency_order()
        fingerprint = _runtime_fingerprint(plan)
        table_ids = [uuid5(plan.plan_guid, f"table:{plan.plan_version}:{index}")
                     for index in range(len(plan.tables))]
        view_ids: list[UUID] = []
        with self.transaction() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT Status, PlanVersion, ApprovedBy, ApprovedTimestamp, RuntimePlanHash "
                    "FROM bronze_replication.MigrationPlans WITH (UPDLOCK, HOLDLOCK) "
                    "WHERE PlanGUID = ?", (str(plan.plan_guid),))
                row = cursor.fetchone()
                if row is None or row[1] != plan.plan_version or not row[2] or not row[3]:
                    raise ValueError("plan must be approved at the requested version")
                if row[0] == "READY_TO_PROVISION":
                    if row[4] != fingerprint:
                        raise ValueError("retry payload differs from persisted runtime plan")
                    return PersistedRuntimePlan(
                        plan_guid=plan.plan_guid,
                        source_table_guids=[uuid5(plan.plan_guid, f"table:{plan.plan_version}:{i}")
                                            for i in range(len(plan.tables))],
                        view_guids=[uuid5(plan.plan_guid, f"view:{plan.plan_version}:{i}")
                                    for i in range(len(plan.views))],
                    )
                if row[0] != "APPROVED" or row[4] is not None:
                    raise ValueError("plan is not awaiting runtime persistence")
                for table_id, source in zip(table_ids, plan.tables, strict=True):
                    table_values = source.table.model_dump(by_alias=True, exclude_none=True)
                    if source.table.parent_source_index is not None:
                        table_values["ParentSourceObjectGUID"] = table_ids[source.table.parent_source_index]
                    self._insert(cursor, "SourceTables", {
                        "GUID": table_id, "PlanGUID": plan.plan_guid,
                        **table_values,
                    })
                    for column_index, column in enumerate(source.columns):
                        self._insert(cursor, "SourceTableColumns", {
                            "SourceTableColumnGUID": uuid5(table_id, f"column:{column_index}"),
                            "GUID": table_id,
                            **column.model_dump(by_alias=True, exclude_none=True),
                        })
                    self._insert(cursor, "ReplicationConfig", {
                        "ReplicationConfigGUID": uuid5(table_id, "replication-config"),
                        "SourceTableGUID": table_id,
                        **source.replication.model_dump(by_alias=True, exclude_none=True),
                    })
                    self._insert(cursor, "ReplicationState", {"SourceTableGUID": table_id})
                for index, derived in enumerate(plan.views):
                    view_id = uuid5(plan.plan_guid, f"view:{plan.plan_version}:{index}")
                    view_ids.append(view_id)
                    self._insert(cursor, "FabricViews", {
                        "ViewGUID": view_id, "PlanGUID": plan.plan_guid,
                        **derived.view.model_dump(by_alias=True, exclude_none=True),
                    })
                for view_id, derived in zip(view_ids, plan.views, strict=True):
                    for dependency_index, dependency in enumerate(derived.dependencies):
                        values = dependency.model_dump(by_alias=True, exclude_none=True)
                        if dependency.source_table_index is not None:
                            try:
                                values["SourceTableGUID"] = table_ids[dependency.source_table_index]
                            except IndexError as exc:
                                raise ValueError("source table dependency index is out of range") from exc
                        if dependency.depends_on_view_index is not None:
                            try:
                                values["DependsOnViewGUID"] = view_ids[dependency.depends_on_view_index]
                            except IndexError as exc:
                                raise ValueError("view dependency index is out of range") from exc
                        if dependency.source_table_guid is not None:
                            cursor.execute("SELECT 1 FROM bronze_replication.SourceTables "
                                           "WHERE GUID = ? AND PlanGUID = ?",
                                           (str(dependency.source_table_guid), str(plan.plan_guid)))
                            if cursor.fetchone() is None:
                                raise ValueError("source table dependency belongs to another plan")
                        if dependency.depends_on_view_guid is not None:
                            cursor.execute("SELECT 1 FROM bronze_replication.FabricViews "
                                           "WHERE ViewGUID = ? AND PlanGUID = ?",
                                           (str(dependency.depends_on_view_guid), str(plan.plan_guid)))
                            if cursor.fetchone() is None:
                                raise ValueError("view dependency belongs to another plan")
                        self._insert(cursor, "FabricViewDependencies", {
                            "ViewDependencyGUID": uuid5(view_id, f"dependency:{dependency_index}"),
                            "ViewGUID": view_id,
                            **values,
                        })
                cursor.execute(
                    "UPDATE bronze_replication.MigrationPlans SET Status = 'READY_TO_PROVISION', "
                    "RuntimePlanHash = ?, "
                    "UpdatedTimestamp = SYSUTCDATETIME() WHERE PlanGUID = ? AND Status = 'APPROVED' "
                    "AND PlanVersion = ? AND RuntimePlanHash IS NULL",
                    (fingerprint, str(plan.plan_guid), plan.plan_version))
                if cursor.rowcount != 1:
                    raise RuntimeError("plan changed during persistence")
        return PersistedRuntimePlan(plan_guid=plan.plan_guid,
                                    source_table_guids=table_ids, view_guids=view_ids)
