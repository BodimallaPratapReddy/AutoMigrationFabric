"""Request and response contracts for model-generated migration candidates."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from thirdparty.sap.utils import (
    ABAPScrapeAssessment, SAPDatasourceDetails, SAPDatasourceField,
    SAPFieldMetadata,
)


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SAPExtractorAnalysisRequest(StrictModel):
    plan_guid: UUID
    analysis_version: int = Field(gt=0)
    datasource: SAPDatasourceDetails
    extract_structure_fields: list[SAPFieldMetadata]
    exposed_fields: list[SAPDatasourceField]
    scrape: ABAPScrapeAssessment


class IdentifiedSAPObject(StrictModel):
    name: str
    object_type: Literal["TRANSPARENT_TABLE", "DB_VIEW"]
    role: str
    required: bool
    evidence: list[str]
    confidence: Literal["LOW", "MEDIUM", "HIGH"]
    notes: str | None


class JoinDefinition(StrictModel):
    left_object: str
    right_object: str
    condition: str
    evidence: list[str]


class FilterDefinition(StrictModel):
    expression: str
    evidence: list[str]


class DerivedFieldDefinition(StrictModel):
    target_field: str
    logic: str
    evidence: list[str]


class SAPExtractorAnalysis(StrictModel):
    source_tables: list[IdentifiedSAPObject]
    source_views: list[IdentifiedSAPObject]
    lookup_tables: list[IdentifiedSAPObject]
    configuration_tables: list[IdentifiedSAPObject]
    joins: list[JoinDefinition]
    filters: list[FilterDefinition]
    derived_fields: list[DerivedFieldDefinition]
    delta_logic: list[str]
    ignored_objects: list[IdentifiedSAPObject]
    unresolved_items: list[str]
    warnings: list[str]


class FabricPlanRequest(StrictModel):
    plan_guid: UUID
    plan_version: int = Field(gt=0)
    approved_analysis: SAPExtractorAnalysis
    target_workspace_id: str
    target_lakehouse_name: str
    existing_objects: list[str]
    datatype_mappings: dict[str, str]
    conventions: list[str]


class FabricTableProposal(StrictModel):
    source_object: str
    target_table: str
    action: Literal["REPLICATE", "REUSE"]
    incremental_method: Literal["FULL", "WATERMARK", "SAP_ODP_DELTA", "CDC"]
    evidence: list[str]


class FabricViewProposal(StrictModel):
    view_name: str
    view_sql: str
    dependencies: list[str]
    creation_order: int
    evidence: list[str]


class FabricImplementationPlan(StrictModel):
    tables: list[FabricTableProposal]
    views: list[FabricViewProposal]
    unresolved_decisions: list[str]
    warnings: list[str]
