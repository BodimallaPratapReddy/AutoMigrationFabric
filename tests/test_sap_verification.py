import unittest
from unittest.mock import Mock

import httpx

from thirdparty.sap.utils import SAPObjectTypeInfo
from thirdparty.sap.verification import SAPObjectCandidate, verify_llm_identified_sap_objects


class SAPVerificationTests(unittest.TestCase):
    def test_rejects_missing_and_type_mismatched_objects(self):
        client = Mock()
        client.get_object_type.side_effect = [
            SAPObjectTypeInfo(object_name="V_ORDERS", object_type="DB_VIEW"),
            SAPObjectTypeInfo(object_name="MISSING", object_type="UNKNOWN"),
        ]
        candidates = [
            SAPObjectCandidate(name="V_ORDERS", object_type="TRANSPARENT_TABLE", evidence=["ABAP READ"]),
            SAPObjectCandidate(name="MISSING", object_type="TRANSPARENT_TABLE", evidence=["ABAP READ"]),
        ]
        result = verify_llm_identified_sap_objects(client, candidates)
        self.assertEqual(len(result.verified), 0)
        self.assertEqual(len(result.unresolved), 2)
        client.read_metadata.assert_not_called()

    def test_404_is_unresolved_but_server_errors_propagate(self):
        client = Mock()
        client.get_object_type.return_value = SAPObjectTypeInfo(
            object_name="V_ORDERS", object_type="DB_VIEW")
        request = httpx.Request("GET", "https://sap.example.test/object")
        candidate = SAPObjectCandidate(name="V_ORDERS", object_type="DB_VIEW",
                                       evidence=["ABAP READ"])
        client.read_metadata.side_effect = httpx.HTTPStatusError(
            "not found", request=request, response=httpx.Response(404, request=request))
        result = verify_llm_identified_sap_objects(client, [candidate])
        self.assertEqual(len(result.unresolved), 1)
        client.read_metadata.side_effect = httpx.HTTPStatusError(
            "server error", request=request, response=httpx.Response(500, request=request))
        with self.assertRaises(httpx.HTTPStatusError):
            verify_llm_identified_sap_objects(client, [candidate])
