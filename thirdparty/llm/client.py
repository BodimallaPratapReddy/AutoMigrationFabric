"""OpenAI structured-output adapter for unapproved migration candidates."""

from __future__ import annotations

import os
from contextlib import nullcontext
from contextvars import ContextVar
from time import monotonic
from typing import Any, TypeVar

from pydantic import BaseModel, ValidationError

from .models import (
    FabricImplementationPlan, FabricPlanRequest, SAPExtractorAnalysis,
    SAPExtractorAnalysisRequest,
)
from .tracing import configure_phoenix_tracing

Output = TypeVar("Output", bound=BaseModel)


class ModelOutputError(RuntimeError):
    pass


class MigrationAnalysisClient:
    """One model boundary; results are proposals until verified and approved."""

    prompt_version = "migration-analysis-v1"
    _trace_id: ContextVar[str | None] = ContextVar("migration_trace_id", default=None)

    def __init__(self, *, model: str | None = None, client: Any = None,
                 tracer: Any = None) -> None:
        self.model = model or os.getenv("MIGRATION_ANALYSIS_MODEL")
        if not self.model:
            raise ValueError("MIGRATION_ANALYSIS_MODEL is required")
        if client is None:
            from openai import OpenAI
            client = OpenAI()
        self._client = client
        if tracer is None:
            try:
                configure_phoenix_tracing()
                from opentelemetry import trace
                tracer = trace.get_tracer("fabric.migration_analysis")
            except ImportError:
                pass
        self._tracer = tracer

    @property
    def last_trace_id(self) -> str | None:
        return self._trace_id.get()

    def _parse(self, operation: str, context: BaseModel, result_type: type[Output],
               instruction: str) -> Output:
        span_manager = (self._tracer.start_as_current_span(operation)
                        if self._tracer else nullcontext())
        with span_manager as span:
            self._trace_id.set(None)
            if span:
                span.set_attribute("migration.operation", operation)
                span.set_attribute("migration.model", self.model)
                span.set_attribute("migration.prompt_version", self.prompt_version)
                for name in ("plan_guid", "analysis_version", "plan_version"):
                    if hasattr(context, name):
                        span.set_attribute(f"migration.{name}", str(getattr(context, name)))
            started = monotonic()
            try:
                response = self._client.responses.parse(
                    model=self.model,
                    input=[{"role": "system", "content": instruction},
                           {"role": "user", "content": context.model_dump_json()}],
                    text_format=result_type,
                    store=False,
                )
                if getattr(response, "status", "completed") != "completed":
                    if span:
                        span.set_attribute("migration.result_status", "incomplete")
                    raise ModelOutputError("Model response did not complete")
                parsed = getattr(response, "output_parsed", None)
                if parsed is None:
                    if span:
                        span.set_attribute("migration.result_status", "missing_output")
                    raise ModelOutputError("Model returned no structured output")
                output = result_type.model_validate(parsed)
                if span:
                    usage = getattr(response, "usage", None)
                    for name in ("input_tokens", "output_tokens", "total_tokens"):
                        value = getattr(usage, name, None)
                        if isinstance(value, int):
                            span.set_attribute(f"migration.{name}", value)
                    context_value = getattr(span, "get_span_context", lambda: None)()
                    trace_id = getattr(context_value, "trace_id", 0)
                    if isinstance(trace_id, int) and trace_id:
                        self._trace_id.set(f"{trace_id:032x}")
                    span.set_attribute("migration.result_status", "success")
                return output
            except ValidationError as exc:
                if span:
                    span.set_attribute("migration.result_status", "invalid_output")
                raise ModelOutputError("Model output failed schema validation") from exc
            except ModelOutputError:
                raise
            except Exception:
                if span:
                    span.set_attribute("migration.result_status", "error")
                raise
            finally:
                if span:
                    span.set_attribute("migration.latency_seconds", monotonic() - started)

    def analyze_sap_extractor(self, request: SAPExtractorAnalysisRequest) -> SAPExtractorAnalysis:
        return self._parse(
            "analyze_sap_extractor", request, SAPExtractorAnalysis,
            "Analyze the SAP extractor from supplied evidence only. Cite concrete evidence "
            "for each object, join, filter, and derived field. Flag incomplete scraping and "
            "uncertain facts. Return a structured proposal; do not execute changes.",
        )

    def revise_sap_extractor_analysis(
        self, *, previous_analysis: SAPExtractorAnalysis, user_feedback: str,
        source_context: SAPExtractorAnalysisRequest,
    ) -> SAPExtractorAnalysis:
        if not user_feedback.strip():
            raise ValueError("user_feedback must be nonempty")
        class RevisionContext(BaseModel):
            source: SAPExtractorAnalysisRequest
            previous: SAPExtractorAnalysis
            feedback: str
        return self._parse(
            "revise_sap_extractor_analysis",
            RevisionContext(source=source_context, previous=previous_analysis,
                            feedback=user_feedback), SAPExtractorAnalysis,
            "Revise the unapproved SAP analysis candidate using user feedback and source "
            "evidence. Preserve evidence and flag unresolved claims. Return a new candidate.",
        )

    def generate_fabric_plan(self, request: FabricPlanRequest) -> FabricImplementationPlan:
        return self._parse(
            "generate_fabric_plan", request, FabricImplementationPlan,
            "Propose Fabric table replication and view definitions from the approved SAP "
            "analysis and target context. Use only supplied evidence. Mark unresolved "
            "decisions. SQL is a proposal requiring review and approval.",
        )

    def revise_fabric_plan(
        self, *, previous_plan: FabricImplementationPlan, user_feedback: str,
        source_context: FabricPlanRequest,
    ) -> FabricImplementationPlan:
        if not user_feedback.strip():
            raise ValueError("user_feedback must be nonempty")
        class RevisionContext(BaseModel):
            source: FabricPlanRequest
            previous: FabricImplementationPlan
            feedback: str
        return self._parse(
            "revise_fabric_plan",
            RevisionContext(source=source_context, previous=previous_plan,
                            feedback=user_feedback), FabricImplementationPlan,
            "Revise the unapproved Fabric plan candidate using user feedback. Preserve "
            "evidence, do not execute SQL, and return a new candidate.",
        )
