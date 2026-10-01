import unittest
from unittest.mock import Mock, patch

from thirdparty.sap.utils import (
    ABAPScraperResponse, SAPClient, SAPDatasourceDetails, SAPExtractionRoute,
    SAPObjectTypeInfo, SAPSQLQueryResponse, assess_abap_scrape,
    resolve_datasource_extraction,
)


class SAPAdapterAdditionsTests(unittest.TestCase):
    def client(self) -> SAPClient:
        with patch.dict("os.environ", {
            "SAP_HOST": "sap.example.test", "SAP_HTTPS_PORT": "443", "SAP_USER": "test",
            "SAP_PASSWORD": "secret", "SAP_CLIENT": "100",
        }), patch("thirdparty.sap.utils.load_dotenv"):
            return SAPClient()

    def test_custom_ca_bundle_keeps_tls_verification_enabled(self):
        context = Mock()
        with patch.dict("os.environ", {
            "SAP_HOST": "sap.example.test", "SAP_HTTPS_PORT": "443", "SAP_USER": "test",
            "SAP_PASSWORD": "secret", "SAP_CLIENT": "100",
        }), patch("thirdparty.sap.utils.load_dotenv"), \
             patch("thirdparty.sap.utils.ssl.create_default_context", return_value=context) as create, \
             patch("thirdparty.sap.utils.httpx.Client") as http:
            SAPClient(ca_bundle="C:/certs/sap-ca.pem")
        create.assert_called_once_with(cafile="C:/certs/sap-ca.pem")
        self.assertIs(http.call_args.kwargs["verify"], context)

    def test_object_type_and_odp_flag(self) -> None:
        with self.client() as client:
            with patch.object(client, "execute_sql_query", side_effect=[
                SAPSQLQueryResponse(success=True, rows=[{"TABCLASS": "VIEW"}],
                                    row_count=1, max_rows=1),
                SAPSQLQueryResponse(success=True, rows=[{"EXPOSE_EXTERNAL": "X"}],
                                    row_count=1, max_rows=1),
            ]) as execute:
                self.assertEqual(client.get_object_type("V_ORDER").object_type, "DB_VIEW")
                self.assertTrue(client.get_odp_capability("2LIS_11_VAHDR").odp_capable)
            self.assertIn("ROOSATTR", execute.call_args.args[0])

    def test_route_requires_resolved_table_or_view(self) -> None:
        details = SAPDatasourceDetails(
            DATASOURCE_ID="DS", OBJVERS="A", TYPE="TRAN", APPLICATION="SD",
            EXTRACT_STRUCTURE="S", EXTRACTOR="V_ORDER", EXTRACTION_METHOD="V",
            DELTA="", DATASOURCE_NAME="Orders",
        )
        self.assertEqual(resolve_datasource_extraction(details), SAPExtractionRoute.UNSUPPORTED)
        self.assertEqual(resolve_datasource_extraction(
            details, SAPObjectTypeInfo(object_name="V_ORDER", object_type="DB_VIEW")),
            SAPExtractionRoute.DB_VIEW)
        details.EXTRACTION_METHOD = "CA"
        self.assertEqual(resolve_datasource_extraction(details), SAPExtractionRoute.UNSUPPORTED)

    def test_unspecified_delta_is_not_claimed_supported(self):
        details = SAPDatasourceDetails(
            DATASOURCE_ID="DS", OBJVERS="A", TYPE="TRAN", APPLICATION="SD",
            EXTRACT_STRUCTURE="S", EXTRACTOR="FM", EXTRACTION_METHOD="F1",
            DELTA="X", DATASOURCE_NAME="Test")
        with self.client() as client:
            with patch.object(client, "get_datasource_details", return_value=details):
                delta = client.get_datasource_delta_details("DS")
        self.assertIsNone(delta.delta_supported)
        self.assertFalse(delta.details_resolved)

    def test_invalid_identifier_never_reaches_sql(self) -> None:
        with self.client() as client:
            with patch.object(client, "execute_sql_query") as execute:
                with self.assertRaises(ValueError):
                    client.get_odp_capability("DS' OR 1=1")
            execute.assert_not_called()

    def test_datasource_field_selection_does_not_imply_visibility(self):
        with self.client() as client:
            details = SAPDatasourceDetails(
                DATASOURCE_ID="DS", OBJVERS="A", TYPE="TRAN", APPLICATION="SD",
                EXTRACT_STRUCTURE="ZSTRUCT", EXTRACTOR="FM", EXTRACTION_METHOD="F1",
                DELTA="", DATASOURCE_NAME="Test")
            from thirdparty.sap.utils import SAPFieldMetadata
            structure = [SAPFieldMetadata(
                fieldname="VISIBLE", position=7, keyflag="", rollname="", datatype="CHAR",
                leng=10, decimals=0, checktable="", reftable="", reffield="", ddtext="")]
            with patch.object(client, "get_datasource_details", return_value=details), \
                 patch.object(client, "read_metadata", return_value=structure), \
                 patch.object(client, "execute_sql_query", return_value=SAPSQLQueryResponse(
                success=True, rows=[{"FIELD": "VISIBLE", "SELECTION": "P"},
                                    {"FIELD": "HIDDEN", "SELECTION": "A"},
                                    {"FIELD": "FILTER", "SELECTION": "1"}],
                row_count=3, max_rows=10000)) as execute:
                fields = client.get_datasource_fields("DS")
            self.assertEqual([field.hidden for field in fields], [None, None, None])
            self.assertEqual([field.extractable for field in fields], [None, None, None])
            self.assertEqual(fields[0].raw_attributes["SELECTION"], "P")
            self.assertEqual([field.position for field in fields], [7, None, None])
            self.assertIn("ROOSFIELD", execute.call_args.args[0])
            self.assertIn("SELECT *", execute.call_args.args[0])

    def test_scrape_limit_and_missing_dependency_surface(self):
        response = ABAPScraperResponse.model_validate({
            "root": {"type": "FM", "name": "ROOT"},
            "settings": {"recursive": True, "max_depth": 1, "max_objects": 2},
            "dependency_tree": {"type": "FM", "name": "ROOT", "relation": "ROOT",
                "depth": 0, "status": "OK", "dependencies": [{
                    "type": "TABLE", "name": "MISSING", "relation": "READ",
                    "depth": 1, "status": "FAILED", "dependencies": []}]},
            "objects": [], "warnings": [],
            "stats": {"objects_returned": 1, "objects_visited": 2},
        })
        assessment = assess_abap_scrape(response)
        self.assertTrue(assessment.truncated)
        self.assertEqual([item.name for item in assessment.unresolved_dependencies], ["MISSING"])

    def test_scrape_completion_flag_cannot_override_truncation(self):
        response = ABAPScraperResponse.model_validate({
            "root": {"type": "FM", "name": "ROOT"},
            "settings": {"recursive": True, "max_depth": 1, "max_objects": 2},
            "dependency_tree": {"type": "FM", "name": "ROOT", "relation": "ROOT",
                "depth": 0, "status": "OK", "dependencies": []},
            "objects": [], "warnings": [],
            "stats": {"objects_returned": 1, "objects_visited": 2},
            "hit_max_depth": False, "hit_max_objects": True, "complete": True,
        })
        assessment = assess_abap_scrape(response)
        self.assertTrue(assessment.truncated)
        self.assertFalse(assessment.complete)
