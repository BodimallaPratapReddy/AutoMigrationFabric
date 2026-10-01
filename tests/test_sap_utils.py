import base64
import json
import unittest
from unittest.mock import Mock, patch

import httpx
from pydantic import ValidationError

from thirdparty.sap import utils


class SAPClientTests(unittest.TestCase):
    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_read_metadata(self, getenv: Mock, _: Mock) -> None:
        config = {
            "SAP_HOST": "sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
        }
        getenv.side_effect = config.get

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(
                str(request.url),
                "https://sap.example.test:8177/z_mcp_abap_adt/"
                "z_tablemeta/DTFIGL_4?sap-client=800",
            )
            credentials = base64.b64decode(
                request.headers["Authorization"].removeprefix("Basic ")
            ).decode()
            self.assertEqual(credentials, "test-user:test-password")
            return httpx.Response(
                200,
                json=[
                    {
                        "fieldname": "MANDT",
                        "position": 1,
                        "keyflag": "X",
                        "rollname": "MANDT",
                        "datatype": "CLNT",
                        "leng": 3,
                        "decimals": 0,
                        "checktable": "T000",
                        "reftable": "",
                        "reffield": "",
                        "ddtext": "Client",
                    }
                ],
            )

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.SAPClient() as sap:
                fields = sap.read_metadata("DTFIGL_4")
                self.assertEqual(len(fields), 1)
                self.assertIsInstance(fields[0], utils.SAPFieldMetadata)
                self.assertEqual(fields[0].fieldname, "MANDT")
                self.assertEqual(fields[0].position, 1)
                client = sap._http
            self.assertTrue(client.is_closed)

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_execute_sql_query(self, getenv: Mock, _: Mock) -> None:
        getenv.side_effect = {
            "SAP_HOST": "https://sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
        }.get

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(
                str(request.url),
                "https://sap.example.test:8177/z_mcp_abap_adt/"
                "z_execute_sql?sap-client=800",
            )
            self.assertEqual(request.headers["content-type"], "application/json")
            self.assertEqual(
                json.loads(request.content),
                {"sql": "SELECT MANDT, BUKRS, BUTXT FROM T001", "max_rows": 100},
            )
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "rows": [{"MANDT": "800", "BUKRS": "CN01", "BUTXT": "Example"}],
                    "row_count": 1,
                    "max_rows": 1.0,
                },
            )

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.SAPClient() as sap:
                result = sap.execute_sql_query(
                    "SELECT MANDT, BUKRS, BUTXT FROM T001", max_rows=100
                )

        self.assertIsInstance(result, utils.SAPSQLQueryResponse)
        self.assertTrue(result.success)
        self.assertEqual(result.rows[0]["BUKRS"], "CN01")
        self.assertEqual(result.row_count, 1)
        self.assertIsInstance(result.max_rows, float)

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_get_datasource_details(self, getenv: Mock, _: Mock) -> None:
        getenv.side_effect = {
            "SAP_HOST": "sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
        }.get

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "POST")
            self.assertEqual(request.url.path, "/z_mcp_abap_adt/z_execute_sql")
            self.assertEqual(request.url.params["sap-client"], "800")
            self.assertEqual(
                json.loads(request.content),
                {
                    "sql": (
                        "SELECT S.OLTPSOURCE AS DATASOURCE_ID, S.OBJVERS, S.TYPE, "
                        "S.APPLNM AS APPLICATION, "
                        "S.EXSTRUCT AS EXTRACT_STRUCTURE, S.EXTRACTOR, "
                        "S.EXMETHOD AS EXTRACTION_METHOD, S.DELTA, "
                        "T.TXTLG AS DATASOURCE_NAME "
                        "FROM ROOSOURCE S LEFT JOIN ROOSOURCET T "
                        "ON S.OLTPSOURCE = T.OLTPSOURCE AND S.OBJVERS = T.OBJVERS "
                        "AND T.LANGU = 'E' "
                        "WHERE S.OLTPSOURCE = '0FI_GL_4' "
                        "AND S.OBJVERS = 'A'"
                    ),
                    "max_rows": 100,
                },
            )
            return httpx.Response(
                200,
                json={
                    "success": True,
                    "rows": [
                        {
                            "DATASOURCE_ID": "0FI_GL_4",
                            "OBJVERS": "A",
                            "TYPE": "TRAN",
                            "APPLICATION": "FI-GL",
                            "EXTRACT_STRUCTURE": "DTFIGL_4",
                            "EXTRACTOR": "BWFID_GET_FIGL_ITEM",
                            "EXTRACTION_METHOD": "F1",
                            "DELTA": "AIE",
                            "DATASOURCE_NAME": "General Ledger: Line Items with Delta Extraction",
                        }
                    ],
                    "row_count": 1,
                    "max_rows": 1.0,
                },
            )

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.SAPClient() as sap:
                result = sap.get_datasource_details("0FI_GL_4")
                with self.assertRaises(ValueError):
                    sap.get_datasource_details("0FI_GL_4' OR '1'='1")

        self.assertIsInstance(result, utils.SAPDatasourceDetails)
        self.assertEqual(result.APPLICATION, "FI-GL")
        self.assertEqual(result.EXTRACT_STRUCTURE, "DTFIGL_4")
        self.assertIs(result.TYPE, utils.SAPDatasourceType.TRANSACTIONAL_DATA)
        self.assertEqual(result.TYPE.description, "Transactional Data")
        self.assertIs(
            result.EXTRACTION_METHOD,
            utils.SAPExtractionMethod.FUNCTION_MODULE_COMPLETE,
        )
        self.assertEqual(
            result.EXTRACTION_METHOD.description,
            "Function Module (Complete Interface)",
        )
        self.assertEqual(json.loads(result.model_dump_json())["TYPE"], "TRAN")

    def test_datasource_domain_values(self) -> None:
        row = {
            "DATASOURCE_ID": "EXAMPLE",
            "OBJVERS": "A",
            "TYPE": "ATTR",
            "APPLICATION": "FI-GL",
            "EXTRACT_STRUCTURE": "EXAMPLE_STRUCT",
            "EXTRACTOR": "EXAMPLE_EXTRACTOR",
            "EXTRACTION_METHOD": "V",
            "DELTA": "",
            "DATASOURCE_NAME": "Example",
        }
        for source_type in utils.SAPDatasourceType:
            row["TYPE"] = source_type.value
            self.assertEqual(
                utils.SAPDatasourceDetails.model_validate(row).TYPE, source_type
            )
        for method in utils.SAPExtractionMethod:
            row["EXTRACTION_METHOD"] = method.value
            self.assertEqual(
                utils.SAPDatasourceDetails.model_validate(row).EXTRACTION_METHOD,
                method,
            )
        row["TYPE"] = "UNKNOWN"
        with self.assertRaises(ValidationError):
            utils.SAPDatasourceDetails.model_validate(row)

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_get_infoset_query_details(self, getenv: Mock, _: Mock) -> None:
        getenv.side_effect = {
            "SAP_HOST": "sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
        }.get

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.content, b"")
            self.assertEqual(request.url.path, "/z_mcp_abap_adt/z_infoset")
            self.assertEqual(request.url.params["sap-client"], "800")
            if request.url.params["infoset"] == "TESTJOIN":
                return httpx.Response(200, json={
                    "infoset": "TESTJOIN",
                    "tables": [
                        {"tableName": "SAIRPORT", "sourceType": "J", "usage": "ALIAS_BASE_TABLE"},
                        {"tableName": "SCARR", "sourceType": "J", "usage": "JOIN_TABLE"},
                        {"tableName": "SPFLI", "sourceType": "J", "usage": "LEADING_TABLE"},
                        {"tableName": "TAIRPORT", "sourceType": "J", "usage": "ALIAS_TABLE"},
                    ],
                })
            self.assertEqual(request.url.params["infoset"], "ZLEAP_ITEM_MARC")
            return httpx.Response(
                200,
                json={
                    "infoset": "ZLEAP_ITEM_MARC",
                    "sqlTemplate": "SELECT * FROM SPFLI INNER JOIN SAIRPORT ON SPFLI.AIRPFROM = SAIRPORT.ID",
                    "sqlTemplateComplete": True,
                    "tables": [
                        {
                            "tableName": "SPFLI",
                            "sourceType": "J",
                            "usage": "LEADING_TABLE",
                        },
                        {
                            "tableName": "SAIRPORT",
                            "sourceType": "J",
                            "usage": "JOIN_TABLE",
                        },
                    ],
                },
            )

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.SAPClient() as sap:
                result = sap.get_infoset_query_details("ZLEAP_ITEM_MARC")
                live_shape = sap.get_infoset_query_details("TESTJOIN")
                with self.assertRaises(ValueError):
                    sap.get_infoset_query_details("")

        self.assertIsInstance(result, utils.SAPInfoSetQueryDetails)
        self.assertEqual(result.infoset, "ZLEAP_ITEM_MARC")
        self.assertTrue(result.sqlTemplateComplete)
        self.assertEqual(result.tables[0].usage, "LEADING_TABLE")
        self.assertIsInstance(result.tables[1], utils.SAPInfoSetTable)
        self.assertEqual(len(live_shape.tables), 4)
        self.assertIsNone(live_shape.sqlTemplate)
        self.assertIsNone(live_shape.sqlTemplateComplete)

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_get_dbview_query_details(self, getenv: Mock, _: Mock) -> None:
        getenv.side_effect = {
            "SAP_HOST": "sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
        }.get

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.content, b"")
            self.assertEqual(request.url.path, "/z_mcp_abap_adt/z_dbview")
            self.assertEqual(
                dict(request.url.params),
                {"sap-client": "800", "view": "ZBRONZE_VBAK"},
            )
            return httpx.Response(
                200,
                json={
                    "view": "ZBRONZE_VBAK",
                    "sqlQuery": (
                        "SELECT VBAK.VBELN FROM VBAK WHERE VBAK.VKORG = '1003'"
                    ),
                    "sqlQueryComplete": True,
                    "tables": [{"tableName": "VBAK", "position": 1}],
                    "filters": [
                        {
                            "position": 1,
                            "conjunction": "",
                            "tableName": "VBAK",
                            "fieldName": "VKORG",
                            "negation": "",
                            "operator": "=",
                            "value": "'1003'",
                            "expression": "VBAK.VKORG = '1003'",
                        }
                    ],
                },
            )

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.SAPClient() as sap:
                result = sap.get_dbview_query_details("ZBRONZE_VBAK")
                with self.assertRaises(ValueError):
                    sap.get_dbview_query_details("")

        self.assertIsInstance(result, utils.SAPDBViewQueryDetails)
        self.assertEqual(result.view, "ZBRONZE_VBAK")
        self.assertTrue(result.sqlQueryComplete)
        self.assertIsInstance(result.tables[0], utils.SAPDBViewTable)
        self.assertEqual(result.tables[0].tableName, "VBAK")
        self.assertIsInstance(result.filters[0], utils.SAPDBViewFilter)
        self.assertEqual(result.filters[0].expression, "VBAK.VKORG = '1003'")

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_abap_code_scraper(self, getenv: Mock, _: Mock) -> None:
        getenv.side_effect = {
            "SAP_HOST": "sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
        }.get

        def respond(request: httpx.Request) -> httpx.Response:
            self.assertEqual(request.method, "GET")
            self.assertEqual(request.content, b"")
            self.assertEqual(request.url.path, "/z_mcp_abap_adt/z_abap_scraper")
            self.assertEqual(
                dict(request.url.params),
                {
                    "sap-client": "800",
                    "name": "BWFID_GET_FIGL_ITEM",
                    "type": "FM",
                    "recursive": "X",
                    "max_depth": "10",
                    "max_objects": "500",
                },
            )
            return httpx.Response(
                200,
                json={
                    "root": {"type": "FM", "name": "BWFID_GET_FIGL_ITEM"},
                    "settings": {
                        "recursive": True,
                        "max_depth": 10,
                        "max_objects": 500,
                    },
                    "dependency_tree": {
                        "type": "FM",
                        "name": "BWFID_GET_FIGL_ITEM",
                        "relation": "ROOT",
                        "depth": 0,
                        "source_program": "",
                        "parent_program": "",
                        "status": "OK",
                        "dependencies": [
                            {
                                "type": "PROG",
                                "name": "CHILD",
                                "relation": "CALL",
                                "depth": 1,
                                "source_program": "CHILD",
                                "parent_program": "PARENT",
                                "status": "OK",
                                "dependencies": [],
                            }
                        ],
                    },
                    "objects": [
                        {
                            "type": "FM",
                            "name": "BWFID_GET_FIGL_ITEM",
                            "source_program": "MAIN",
                            "parent_program": "",
                            "depth": 0,
                            "source_code": "FUNCTION bwfid_get_figl_item.",
                        }
                    ],
                    "warnings": [],
                    "stats": {"objects_returned": 1, "objects_visited": 2},
                },
            )

        real_client = httpx.Client
        transport = httpx.MockTransport(respond)
        with patch.object(
            utils.httpx,
            "Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            with utils.SAPClient() as sap:
                result = sap.abap_code_scraper("BWFID_GET_FIGL_ITEM")

        self.assertIsInstance(result, utils.ABAPScraperResponse)
        self.assertEqual(result.root.name, "BWFID_GET_FIGL_ITEM")
        self.assertEqual(result.dependency_tree.dependencies[0].name, "CHILD")
        self.assertEqual(result.objects[0].source_code, "FUNCTION bwfid_get_figl_item.")

    def test_invalid_metadata_response(self) -> None:
        with self.assertRaises(ValidationError):
            utils._METADATA_ADAPTER.validate_python([{"fieldname": "MANDT"}])

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv")
    def test_tls_verification_can_be_disabled_by_configuration(
        self, getenv: Mock, _: Mock
    ) -> None:
        getenv.side_effect = {
            "SAP_HOST": "sap.example.test",
            "SAP_HTTPS_PORT": "8177",
            "SAP_USER": "test-user",
            "SAP_PASSWORD": "test-password",
            "SAP_CLIENT": "800",
            "SAP_VERIFY_TLS": "false",
        }.get
        real_client = httpx.Client
        captured: dict[str, object] = {}

        def make_client(**kwargs: object) -> httpx.Client:
            captured.update(kwargs)
            return real_client(transport=httpx.MockTransport(lambda _: httpx.Response(
                200, json={"success": True, "rows": [{"TABNAME": "DD02L"}],
                           "row_count": 1, "max_rows": 1}
            )), **kwargs)

        with patch.object(utils.httpx, "Client", side_effect=make_client):
            with utils.SAPClient() as sap:
                self.assertTrue(sap.test_connection().success)

        self.assertIs(captured["verify"], False)

    @patch.object(utils, "load_dotenv")
    @patch.object(utils.os, "getenv", return_value=None)
    def test_missing_configuration(self, _: Mock, __: Mock) -> None:
        with self.assertRaisesRegex(ValueError, "SAP_HOST"):
            utils.SAPClient()


if __name__ == "__main__":
    unittest.main()
