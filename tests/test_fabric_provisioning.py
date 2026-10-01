import json
import unittest
from uuid import UUID

from services.fabric_provisioning import (
    ProvisioningCancelledError,
    ProvisioningJobFailedError,
    ProvisioningLogicalFailure,
    ProvisioningResultError,
    parse_provisioning_notebook_result,
)
from thirdparty.fabric.models import FabricJobInstance, FabricJobStatus


PLAN = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
OTHER = "5b218778-e7a5-4d73-8187-f10824047715"


def job(status: FabricJobStatus, exit_value: str | None = None) -> FabricJobInstance:
    return FabricJobInstance(id=OTHER, status=status.value, normalized_status=status,
                             exit_value=exit_value)


class ProvisioningResultTests(unittest.TestCase):
    def test_success(self):
        payload = json.dumps({"status": "SUCCESS", "plan_guid": PLAN,
                              "tables_processed": 3, "views_processed": 1})
        result = parse_provisioning_notebook_result(
            job(FabricJobStatus.SUCCEEDED, payload), expected_plan_guid=PLAN)
        self.assertEqual(result.plan_guid, UUID(PLAN))
        self.assertEqual(result.tables_processed, 3)

    def test_logical_failure(self):
        payload = json.dumps({"status": "FAILED", "plan_guid": PLAN,
                              "message": "View dependency unavailable"})
        with self.assertRaises(ProvisioningLogicalFailure) as raised:
            parse_provisioning_notebook_result(job(FabricJobStatus.SUCCEEDED, payload),
                                               expected_plan_guid=PLAN)
        self.assertEqual(raised.exception.result.message, "View dependency unavailable")

    def test_invalid_results(self):
        cases = (None, "not json", json.dumps({"status": "SUCCESS", "plan_guid": OTHER}),
                 json.dumps({"status": "UNKNOWN", "plan_guid": PLAN}),
                 json.dumps({"status": "SUCCESS", "plan_guid": PLAN, "tables_processed": -1}))
        for exit_value in cases:
            with self.subTest(exit_value=exit_value), self.assertRaises(ProvisioningResultError):
                parse_provisioning_notebook_result(job(FabricJobStatus.SUCCEEDED, exit_value),
                                                   expected_plan_guid=PLAN)

    def test_fabric_failure_and_cancel(self):
        with self.assertRaises(ProvisioningJobFailedError):
            parse_provisioning_notebook_result(job(FabricJobStatus.FAILED), expected_plan_guid=PLAN)
        with self.assertRaises(ProvisioningCancelledError):
            parse_provisioning_notebook_result(job(FabricJobStatus.CANCELLED), expected_plan_guid=PLAN)
        with self.assertRaises(ProvisioningResultError):
            parse_provisioning_notebook_result(job(FabricJobStatus.UNKNOWN), expected_plan_guid=PLAN)
