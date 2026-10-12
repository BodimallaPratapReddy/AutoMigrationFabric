"""Validate the agent's plan and adapt its fields; never infer datatype mappings."""

import json
import re

from pydantic import BaseModel, ConfigDict, Field

from .contracts import MigrationInput
from .mapping_types import normalize_fabric_type
from thirdparty.oracle.extraction import attach_extract_sql
from thirdparty.configdb.repository import (
    SourceTableCreate, SourceTableColumnCreate, SourceTablePlan, ReplicationConfigCreate,
)


class AgentColumn(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    position: int = Field(gt=0)
    name: str = Field(min_length=1, max_length=128)
    source_type: str = Field(min_length=1, max_length=128)
    fabric_type: str = Field(min_length=1, max_length=128)
    nullable: bool
    target_name: str | None = None
    source_expression: str | None = None
    description: str | None = Field(default=None, description="Original ALL_COL_COMMENTS text only; null if absent, never ETRM or inferred text")
    etrm_description: str | None = Field(default=None, description="Detailed description from the matching ETRM column documentation; null if unavailable")
    transformation_notes: str | None = None
    warning: str | None = None


class AgentPlan(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    schema_version: int = 1
    source_schema: str
    source_table: str
    description: str | None = Field(default=None, description="Original ALL_TAB_COMMENTS text only; null if absent")
    etrm_description: str | None = Field(default=None, description="Detailed table description from matching ETRM documentation; null if unavailable")
    columns: list[AgentColumn] = Field(min_length=1, max_length=1000)
    primary_key: list[str] = Field(description="Declared enabled database primary key in key order; empty if absent")
    watermark_candidates: list[str] = Field(description="Source date/timestamp candidates; not approved change tracking")
    evidence: list[str] = Field(min_length=1)
    unresolved_decisions: list[str]
    warnings: list[str] = Field(default_factory=list)
    unique_keys: list[dict] = Field(default_factory=list)
    indexes: list[dict] = Field(default_factory=list)
    partitioning: dict = Field(default_factory=dict)
    table_statistics: dict = Field(default_factory=dict)


def is_attachment_path_placeholder(column: AgentColumn) -> bool:
    """Recognize the approved nullable path exception, without inferring mappings."""
    return (column.source_type.strip().upper() == "BLOB"
            and column.fabric_type.strip().upper() == "STRING" and column.nullable
            and bool(column.target_name and column.target_name.upper().endswith("_FILE_PATH"))
            and bool(column.transformation_notes)
            and bool(re.fullmatch(r"NULL|CAST\s*\(\s*NULL\s+AS\s+VARCHAR2\s*\(\s*4000\s*\)\s*\)",
                                  (column.source_expression or "").strip(), re.I)))


def adapt_plan(text: str, input: dict, lakehouse_name: str) -> dict:
    if len(text.encode()) > 256_000:
        raise ValueError("Agent plan exceeds 256 KB")
    blocks = re.findall(r"```json\s*(.*?)\s*```", text, flags=re.S)
    if len(blocks) > 1:
        raise ValueError("Return exactly one JSON plan")
    plan = AgentPlan.model_validate(json.loads(blocks[0] if blocks else text))
    request = MigrationInput.model_validate(input)
    if plan.schema_version != 1:
        raise ValueError("Unsupported agent plan version")
    if (plan.source_schema != request.source_schema_name
            or plan.source_table != request.source_object_name):
        raise ValueError("Agent source does not match the requested source")
    if plan.unresolved_decisions:
        raise ValueError("Resolve agent decisions before reviewing runtime configuration: "
                         + "; ".join(plan.unresolved_decisions)[:1500])
    names = [column.name for column in plan.columns]
    targets = [column.target_name or column.name for column in plan.columns]
    if len(set(name.upper() for name in targets)) != len(targets):
        raise ValueError("Agent target column names must be unique")
    for selection in (plan.primary_key, plan.watermark_candidates):
        if len(selection) != len(set(selection)) or not set(selection) <= set(names):
            raise ValueError("Agent key/watermark references unknown or duplicate columns")
    columns = []
    for column in sorted(plan.columns, key=lambda item: item.position):
        # This checks the supported type vocabulary, not source-to-target mapping policy.
        if normalize_fabric_type(column.fabric_type) != column.fabric_type:
            raise ValueError("Agent must return canonical Fabric types")
        columns.append(SourceTableColumnCreate(
            sno=column.position, column_name=column.name, source_data_type=column.source_type,
            fabric_data_type=column.fabric_type, is_nullable=column.nullable,
            target_column_name=column.target_name, source_expression=column.source_expression,
            transformation_notes=column.transformation_notes, description=column.description,
            etrm_description=column.etrm_description,
            is_primary_key=column.name in plan.primary_key,
            is_watermark_candidate=column.name in plan.watermark_candidates))
    runtime = SourceTablePlan(
        table=SourceTableCreate(connection_name=request.source_connection_name,
            source_system_type="ORACLE", source_object_type="TABLE",
            source_schema_name=plan.source_schema, source_table_name=plan.source_table,
            description=plan.description, etrm_description=plan.etrm_description,
            fabric_workspace_id=request.fabric_workspace_id,
            fabric_lakehouse_id=request.fabric_lakehouse_id, fabric_lakehouse_name=lakehouse_name,
            fabric_lakehouse_schema=request.fabric_schema_name,
            fabric_table_name=request.fabric_target_name or request.source_object_name),
        columns=columns, replication=ReplicationConfigCreate(incremental_method="FULL",
            write_strategy="REPLACE", primary_key_columns=plan.primary_key or None,
            ingestion_flag=False, pipeline_workspace_id=request.fabric_workspace_id))
    extract_sql = attach_extract_sql(runtime)
    return {"table_plan": runtime.model_dump(mode="json"), "review": {
        "extract_sql": extract_sql,
        "description": plan.description, "etrm_description": plan.etrm_description,
        "source": f"{plan.source_schema}.{plan.source_table}", "discovery_provider": "OPENCODE",
        "agent_plan": plan.model_dump(), "columns": [dict(name=c.name, source_type=c.source_type,
            fabric_type=c.fabric_type, warning=c.warning, supported=True,
            description=c.description, etrm_description=c.etrm_description,
            target_name=c.target_name, source_expression=c.source_expression,
            transformation_notes=c.transformation_notes) for c in plan.columns],
        "primary_key": plan.primary_key, "primary_key_candidates": [],
        "unique_keys": plan.unique_keys, "indexes": plan.indexes,
        "partitioning": plan.partitioning, "table_statistics": plan.table_statistics,
        "watermark_candidates": [dict(column_name=c.name, data_type=c.source_type,
            index_details=[]) for c in plan.columns if c.name in plan.watermark_candidates],
        "warnings": plan.warnings, "load_method": "FULL", "write_strategy": "REPLACE"}}
