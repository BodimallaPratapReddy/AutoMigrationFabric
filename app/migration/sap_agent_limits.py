"""Bounded SAP agent failure reports; no source or target side effects."""
from typing import Literal

from pydantic import Field

from .sap_mcp_contracts import MCPModel


MAX_SAP_AGENT_RETRIES = 5


class SAPAgentBlocked(MCPModel):
    kind: Literal['SAP_AGENT_BLOCKED']
    destination: str
    blocker: str = Field(min_length=1, max_length=2000)
    attempts: int = Field(ge=1, le=MAX_SAP_AGENT_RETRIES)
    attempted_actions: list[str] = Field(min_length=1, max_length=5)
    documentation_references: list[str] = Field(max_length=10)
    required_resolution: str = Field(min_length=1, max_length=2000)


def blocked_result(data, destination):
    report = SAPAgentBlocked.model_validate(data)
    if report.destination != destination:
        raise ValueError('SAP blocker report must match the mapped destination')
    return {
        'failure_reason': f'SAP agent stopped after {report.attempts} unsuccessful attempts: '
                          f'{report.blocker}. Required resolution: {report.required_resolution}',
        'agent_blocker': report.model_dump(mode='json'),
    }
