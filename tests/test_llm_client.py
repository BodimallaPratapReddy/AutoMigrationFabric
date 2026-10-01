import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from thirdparty.llm.client import MigrationAnalysisClient, ModelOutputError
from thirdparty.llm.models import SAPExtractorAnalysis


class LLMClientTests(unittest.TestCase):
    def test_structured_parse_does_not_trace_prompt_or_secrets(self):
        response = SimpleNamespace(status="completed", output_parsed={
            "source_tables": [], "source_views": [], "lookup_tables": [],
            "configuration_tables": [], "joins": [], "filters": [],
            "derived_fields": [], "delta_logic": [], "ignored_objects": [],
            "unresolved_items": [], "warnings": [],
        })
        transport = SimpleNamespace(responses=SimpleNamespace(parse=Mock(return_value=response)))
        span = Mock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer = Mock()
        tracer.start_as_current_span.return_value = span
        client = MigrationAnalysisClient(model="test-model", client=transport, tracer=tracer)
        context = SAPExtractorAnalysis.model_validate(response.output_parsed)
        output = client._parse("test", context, SAPExtractorAnalysis, "instruction")
        self.assertEqual(output, context)
        self.assertFalse(transport.responses.parse.call_args.kwargs["store"])
        self.assertIs(transport.responses.parse.call_args.kwargs["text_format"], SAPExtractorAnalysis)
        traced = str(span.set_attribute.call_args_list)
        self.assertNotIn("instruction", traced)
        self.assertNotIn("source_tables", traced)

    def test_missing_structured_output_fails_closed(self):
        transport = SimpleNamespace(responses=SimpleNamespace(parse=Mock(
            return_value=SimpleNamespace(status="completed", output_parsed=None))))
        client = MigrationAnalysisClient(model="test-model", client=transport)
        context = SAPExtractorAnalysis(
            source_tables=[], source_views=[], lookup_tables=[],
            configuration_tables=[], joins=[], filters=[], derived_fields=[],
            delta_logic=[], ignored_objects=[], unresolved_items=[], warnings=[])
        with self.assertRaises(ModelOutputError):
            client._parse("test", context, SAPExtractorAnalysis, "instruction")
