import unittest
from unittest.mock import Mock, patch

from thirdparty.sap.utils import (
    SAPClient, SAPDatasourceDetails, SAPExtractionRoute, SAPObjectTypeInfo,
    SAPSQLQueryResponse, resolve_datasource_extraction_details,
)
from thirdparty.sap.verification import SAPObjectCandidate, verify_llm_identified_sap_objects
from services.sap_analysis_service import build_datasource_rebuild_context


class RemainingSAPGapsTests(unittest.TestCase):
    def client(self):
        with patch.dict("os.environ", {
            "SAP_HOST": "sap.example.test", "SAP_HTTPS_PORT": "443", "SAP_USER": "test",
            "SAP_PASSWORD": "secret", "SAP_CLIENT": "100",
        }), patch("thirdparty.sap.utils.load_dotenv"):
            return SAPClient()

    def details(self, method="V"):
        return SAPDatasourceDetails(
            DATASOURCE_ID="DS", OBJVERS="A", TYPE="TRAN", APPLICATION="SD",
            EXTRACT_STRUCTURE="S", EXTRACTOR="T", EXTRACTION_METHOD=method,
            DELTA="", DATASOURCE_NAME=None)

    def test_unknown_odp_and_explicit_unsupported_routes(self):
        with self.client() as client:
            with patch.object(client, "execute_sql_query", return_value=SAPSQLQueryResponse(
                success=True, rows=[], row_count=0, max_rows=1)):
                result = client.get_odp_capability("DS")
        self.assertIsNone(result.odp_capable)
        self.assertFalse(result.metadata_found)
        append = resolve_datasource_extraction_details(self.details("A"))
        self.assertEqual(append.route, SAPExtractionRoute.DATASOURCE_APPEND)
        self.assertEqual(append.reason_code, "DATASOURCE_APPEND_NOT_SUPPORTED")
        clazz = resolve_datasource_extraction_details(self.details("CA"))
        self.assertEqual(clazz.reason_code, "CLASS_BASED_NOT_SUPPORTED")

    def test_table_and_datasource_existence(self):
        with self.client() as client:
            with patch.object(client, "get_object_type", return_value=SAPObjectTypeInfo(
                object_name="V", object_type="DB_VIEW")):
                self.assertTrue(client.object_exists("V"))
                self.assertFalse(client.table_exists("V"))
            with patch.object(client, "get_datasource_details", return_value=None):
                self.assertFalse(client.datasource_exists("MISSING"))

    def test_verification_normalizes_deduplicates_and_checks_view_source(self):
        client = Mock()
        client.get_object_type.side_effect = lambda name: SAPObjectTypeInfo(
            object_name=name, object_type="DB_VIEW" if name == "V" else "TRANSPARENT_TABLE")
        client.read_metadata.return_value = []
        client.get_dbview_query_details.return_value = {
            "view": "V", "sqlQuery": "SELECT * FROM T", "sqlQueryComplete": True,
            "tables": [{"tableName": "T", "position": 1}], "filters": [],
        }
        from thirdparty.sap.utils import SAPDBViewQueryDetails
        client.get_dbview_query_details.return_value = SAPDBViewQueryDetails.model_validate(
            client.get_dbview_query_details.return_value)
        candidates = [SAPObjectCandidate(name="V", object_type="VIEW", evidence=["source"]),
                      SAPObjectCandidate(name="V", object_type="DB_VIEW", evidence=["source"])]
        result = verify_llm_identified_sap_objects(client, candidates)
        self.assertEqual(result.unresolved, [])
        self.assertEqual({item.name for item in result.verified}, {"V", "T"})
        self.assertEqual(next(item for item in result.verified if item.name == "V").source_objects, ["T"])
        self.assertEqual(client.get_dbview_query_details.call_count, 1)

    def test_context_builder_uses_typed_route_and_schema(self):
        client = Mock()
        client.get_datasource_details.return_value = self.details("A")
        client.read_metadata.return_value = []
        client.get_datasource_fields.return_value = []
        context = build_datasource_rebuild_context(client, "DS")
        self.assertFalse(context.extraction_resolution.supported)
        self.assertEqual(context.extraction_resolution.reason_code, "DATASOURCE_APPEND_NOT_SUPPORTED")
        client.abap_code_scraper.assert_not_called()

    def test_invalid_scraper_type_fails_before_request(self):
        with self.client() as client:
            with patch.object(client._http, "get") as get:
                with self.assertRaises(ValueError):
                    client.abap_code_scraper("ZROOT", "INVALID")
                get.assert_not_called()

    def test_assess_view_and_infoset_require_complete_sql_and_verified_sources(self):
        from thirdparty.sap.utils import SAPDBViewQueryDetails, SAPInfoSetQueryDetails
        with self.client() as client:
            view = SAPDBViewQueryDetails.model_validate({
                "view": "V", "sqlQuery": "SELECT * FROM T", "sqlQueryComplete": False,
                "tables": [{"tableName": "T", "position": 1}], "filters": []})
            infoset = SAPInfoSetQueryDetails.model_validate({
                "infoset": "I", "sqlTemplate": None, "sqlTemplateComplete": None,
                "tables": [{"tableName": "T", "sourceType": "TABLE", "usage": "SOURCE"}]})
            with patch.object(client, "get_dbview_query_details", return_value=view), \
                 patch.object(client, "get_infoset_query_details", return_value=infoset), \
                 patch.object(client, "get_object_type", return_value=SAPObjectTypeInfo(
                     object_name="T", object_type="TRANSPARENT_TABLE")):
                self.assertFalse(client.assess_dbview("V").complete)
                self.assertFalse(client.assess_infoset_query("I").complete)

    def test_watermark_candidates_are_proposals(self):
        from thirdparty.sap.utils import SAPFieldMetadata, SAPTableSchema
        fields = [SAPFieldMetadata(fieldname=name, position=i, keyflag="", rollname="",
                                   datatype=kind, leng=8, decimals=0, checktable="",
                                   reftable="", reffield="", ddtext="")
                  for i, (name, kind) in enumerate([("AEDAT", "DATS"), ("NAME", "CHAR")])]
        with self.client() as client:
            with patch.object(client, "table_exists", return_value=True), \
                 patch.object(client, "get_table_schema", return_value=SAPTableSchema(
                     table_name="T", columns=fields, primary_key_columns=[])):
                candidates = client.get_watermark_candidates("T")
        self.assertEqual([item.field_name for item in candidates], ["AEDAT"])
        self.assertIsNone(candidates[0].indexed)


if __name__ == "__main__":
    unittest.main()
