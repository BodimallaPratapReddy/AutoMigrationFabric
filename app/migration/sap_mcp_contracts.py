"""Agent-discovered SAP facts and proposals. No SAP network adapter is used here."""
from pydantic import BaseModel, ConfigDict, Field
from .opencode_sap_plan import SAPAgentPlan
from .opencode_sap_view_plan import SAPViewBundle
from thirdparty.llm.models import SAPExtractorAnalysis
from typing import Literal


class MCPModel(BaseModel):
    model_config=ConfigDict(extra='forbid',strict=True)


class SAPMaterializedSource(MCPModel):
    source_object_type: Literal['CDS_VIEW', 'DB_VIEW']
    entity_name: str = Field(min_length=1, max_length=30)
    access_method: Literal['SAP_APPLICATION_CONNECTOR']
    parameterless: Literal[True]
    client: str = Field(pattern=r'^\d{3}$')
    client_semantics: Literal['SAP_SESSION_CLIENT']
    authorization_behavior: Literal['NO_CDS_DCL', 'CONNECTOR_PRESERVES_CDS_AUTHORIZATION']
    authorization_evidence: list[str] = Field(min_length=1, max_length=10)
    verification_queries: list[str] = Field(min_length=1, max_length=10)


def materialized_names(context):
    return set(context.get('materialized_sources', {}))


def extraction_names(context):
    return {name for name,kind in context['analysis_objects'].items()
            if kind=='TRANSPARENT_TABLE' or name in materialized_names(context)}


class MCPTableProposal(MCPModel):
    destination: str
    metadata: dict
    plan: SAPAgentPlan


class MCPViewProposal(MCPModel):
    destination: str
    graph: dict
    plan: SAPViewBundle


class MCPODPProposal(MCPModel):
    destination: str
    context: dict
    plan: SAPAgentPlan


class MCPExtractorProposal(MCPModel):
    destination: str
    context: dict
    analysis: SAPExtractorAnalysis


class MCPKeyCheck(MCPModel):
    destination: str
    columns: list[str]
    valid: bool
    status: str
    scope: str
    has_nulls: bool | None = None
    has_duplicates: bool | None = None
    method: str
    validation_queries: list[str] = Field(default_factory=list)
    error: str | None = None
