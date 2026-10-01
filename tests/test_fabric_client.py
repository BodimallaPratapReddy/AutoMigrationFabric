import json
import unittest
from uuid import UUID

import httpx

from thirdparty.fabric.client import FabricClient
from thirdparty.fabric.exceptions import (
    FabricAuthenticationError,
    FabricJobSubmissionError,
    FabricNotFoundError,
    FabricPermissionError,
    FabricRateLimitError,
    FabricResponseError,
)
from thirdparty.fabric.models import FabricJobParameter, FabricJobStatus, is_terminal_job_status
from services.fabric_validation import validate_fabric_target
from thirdparty.fabric.utils import FABRIC_API_URL


WS = "cfafbeb1-8037-4d0c-896e-a46fb27ff229"
ITEM = "5b218778-e7a5-4d73-8187-f10824047715"
JOB = "2d6aa964-5f3a-4c95-a878-cc761ae71391"
LOCATION = f"{FABRIC_API_URL}workspaces/{WS}/items/{ITEM}/jobs/instances/{JOB}"


class FabricClientTests(unittest.TestCase):
    def call(self, handler, action):
        with httpx.Client(base_url=FABRIC_API_URL, transport=httpx.MockTransport(handler)) as http:
            with FabricClient(http) as fabric:
                return action(fabric)

    def test_workspace_and_lakehouse_pagination(self):
        seen = []

        def handler(request):
            seen.append(request)
            if request.url.path.endswith("/workspaces"):
                if not request.url.params:
                    return httpx.Response(200, json={"value": [{"id": WS, "displayName": "Dev"}], "continuationToken": "next"})
                return httpx.Response(200, json={"value": [{"id": ITEM, "displayName": "Prod"}]})
            return httpx.Response(200, json={"value": [{"id": ITEM, "displayName": "Bronze"}]})

        workspaces, lakehouses = self.call(handler, lambda c: (c.list_workspaces(), c.list_lakehouses(WS)))
        self.assertEqual([w.display_name for w in workspaces], ["Dev", "Prod"])
        self.assertEqual(lakehouses[0].display_name, "Bronze")
        self.assertEqual(seen[1].url.params["continuationToken"], "next")

    def test_notebook_submission_and_status(self):
        def handler(request):
            if request.method == "POST":
                self.assertEqual(request.url.path, f"/v1/workspaces/{WS}/notebooks/{ITEM}/jobs/execute/instances")
                self.assertEqual(request.url.params["beta"], "false")
                self.assertEqual(json.loads(request.content), {"parameters": [{"name": "plan_guid", "value": JOB, "type": "Text"}]})
                return httpx.Response(202, headers={"Location": LOCATION, "Retry-After": "60"})
            self.assertEqual(request.url.path, f"/v1/workspaces/{WS}/notebooks/{ITEM}/jobs/execute/instances/{JOB}")
            self.assertEqual(request.url.params["beta"], "true")
            return httpx.Response(200, json={"id": JOB, "itemId": ITEM, "status": "Failed", "failureReason": {"errorCode": "NotebookError", "message": "failed"}, "properties": {"exitValue": "bad"}})

        def action(client):
            submission = client.trigger_provisioning(workspace_id=WS, notebook_id=ITEM, plan_guid=JOB)
            run = client.get_notebook_run(WS, ITEM, submission.job_instance_id)
            return submission, run

        submission, run = self.call(handler, action)
        self.assertEqual(submission.job_instance_id, JOB)
        self.assertEqual(submission.retry_after_seconds, 60)
        self.assertEqual(run.normalized_status, FabricJobStatus.FAILED)
        self.assertEqual(run.failure_reason.error_code, "NotebookError")
        self.assertEqual(run.exit_value, "bad")

    def test_pipeline_submission_and_status(self):
        seen = []

        def handler(request):
            seen.append(request)
            if request.method == "POST":
                return httpx.Response(202, headers={"Location": LOCATION})
            return httpx.Response(200, json={"id": JOB, "status": "Completed"})

        submission, run = self.call(handler, lambda c: (c.run_pipeline(WS, ITEM), c.get_pipeline_run(WS, ITEM, JOB)))
        self.assertEqual(seen[0].url.path, f"/v1/workspaces/{WS}/dataPipelines/{ITEM}/jobs/execute/instances")
        self.assertEqual(seen[1].url.path, f"/v1/workspaces/{WS}/dataPipelines/{ITEM}/jobs/execute/instances/{JOB}")
        self.assertEqual(submission.job_instance_id, JOB)
        self.assertEqual(run.normalized_status, FabricJobStatus.SUCCEEDED)

    def test_errors_and_validation(self):
        with self.assertRaisesRegex(ValueError, "workspace_id"):
            self.call(lambda _: httpx.Response(200), lambda c: c.get_workspace("invalid"))
        with self.assertRaises(FabricPermissionError):
            self.call(lambda _: httpx.Response(403, json={"errorCode": "InsufficientPrivileges"}), lambda c: c.get_workspace(WS))
        with self.assertRaises(FabricResponseError):
            self.call(lambda _: httpx.Response(200, text="not json"), lambda c: c.get_workspace(WS))
        with self.assertRaises(FabricJobSubmissionError):
            self.call(lambda _: httpx.Response(202, headers={"Location": "https://evil.example/"}), lambda c: c.run_pipeline(WS, ITEM))

    def test_duplicate_lakehouse_name(self):
        def handler(_):
            return httpx.Response(200, json={"value": [{"id": ITEM, "displayName": "Bronze"}, {"id": JOB, "displayName": "Bronze"}]})

        with self.assertRaisesRegex(FabricResponseError, "Multiple lakehouses"):
            self.call(handler, lambda c: c.find_lakehouse_by_name(WS, "Bronze"))

    def test_notebook_and_pipeline_item_validation(self):
        # A type mismatch is surfaced before a workflow submits a job.
        self.assertEqual(self.call(lambda _: httpx.Response(200, json={
            "id": ITEM, "displayName": "Provision", "type": "Notebook"}),
            lambda c: c.get_notebook(WS, ITEM)).type, "Notebook")
        with self.assertRaisesRegex(FabricResponseError, "expected DataPipeline"):
            self.call(lambda _: httpx.Response(200, json={
                "id": ITEM, "displayName": "Provision", "type": "Notebook"}),
                lambda c: c.get_pipeline(WS, ITEM))

    def test_status_surfaces_retry_after(self):
        run = self.call(lambda _: httpx.Response(200, headers={"Retry-After": "45"},
                            json={"id": JOB, "status": "InProgress", "rootActivityId": "trace"}),
                        lambda c: c.get_pipeline_run(WS, ITEM, JOB))
        self.assertEqual(run.retry_after_seconds, 45)
        self.assertEqual(run.root_activity_id, "trace")

    def test_get_workspace_lakehouse_and_cancel(self):
        seen = []

        def handler(request):
            seen.append(request)
            if request.method == "POST":
                return httpx.Response(202)
            return httpx.Response(200, json={"id": ITEM, "displayName": "Bronze"})

        workspace, lakehouse = self.call(handler, lambda c: (
            c.get_workspace(WS), c.get_lakehouse(WS, ITEM)
        ))
        self.call(handler, lambda c: c.cancel_notebook_run(WS, ITEM, JOB))
        self.assertEqual(workspace.display_name, "Bronze")
        self.assertEqual(lakehouse.id, ITEM)
        self.assertEqual(seen[2].url.path, f"/v1/workspaces/{WS}/items/{ITEM}/jobs/instances/{JOB}/cancel")

    def test_http_error_mapping_and_retry_metadata(self):
        cases = [
            (401, FabricAuthenticationError),
            (404, FabricNotFoundError),
            (429, FabricRateLimitError),
            (503, FabricResponseError),
        ]
        for code, expected in cases:
            with self.subTest(code=code):
                def handler(_):
                    return httpx.Response(code, headers={"Retry-After": "12", "x-ms-request-id": "request-1"}, json={"errorCode": "FabricCode", "message": "sensitive value"})

                with self.assertRaises(expected) as raised:
                    self.call(handler, lambda c: c.get_workspace(WS))
                self.assertIn("request_id=request-1", str(raised.exception))
                self.assertIn("retry_after=12", str(raised.exception))
                self.assertEqual(raised.exception.retry_after_seconds, 12)
                self.assertEqual(raised.exception.status_code, code)
                self.assertNotIn("sensitive value", str(raised.exception))

    def test_submission_timeout_is_not_retried(self):
        calls = 0

        def handler(request):
            nonlocal calls
            calls += 1
            raise httpx.ReadTimeout("timeout", request=request)

        with self.assertRaises(FabricJobSubmissionError):
            self.call(handler, lambda c: c.run_pipeline(WS, ITEM))
        self.assertEqual(calls, 1)

    def test_submission_location_shapes_and_identity(self):
        pipeline_location = f"{FABRIC_API_URL}workspaces/{WS}/dataPipelines/{ITEM}/jobs/execute/instances/{JOB}"
        for location in (LOCATION, pipeline_location):
            with self.subTest(location=location):
                result = self.call(lambda _: httpx.Response(202, headers={"Location": location}),
                                   lambda c: c.run_pipeline(WS, ITEM))
                self.assertEqual(result.job_instance_id, JOB)
        bad_locations = (
            LOCATION.replace(WS, JOB), LOCATION.replace(ITEM, JOB),
            LOCATION.replace("https:", "http:"), LOCATION + "/extra",
            LOCATION.replace("api.fabric.microsoft.com", "api.fabric.microsoft.com.evil.example"),
            LOCATION + "?next=1",
        )
        for location in bad_locations:
            with self.subTest(location=location), self.assertRaises(FabricJobSubmissionError):
                self.call(lambda _: httpx.Response(202, headers={"Location": location}),
                          lambda c: c.run_pipeline(WS, ITEM))

    def test_job_identity_unknown_status_and_exit_value(self):
        result = self.call(lambda _: httpx.Response(200, json={"id": JOB, "itemId": ITEM,
            "status": "Deduped", "properties": {"exitValue": "SUCCESS"}}),
            lambda c: c.get_notebook_run(WS, ITEM, JOB))
        self.assertEqual(result.status, "Deduped")
        self.assertEqual(result.normalized_status, FabricJobStatus.UNKNOWN)
        self.assertEqual(result.exit_value, "SUCCESS")
        self.assertFalse(is_terminal_job_status(result.normalized_status))
        self.assertTrue(is_terminal_job_status(FabricJobStatus.CANCELLED))
        for data in ({"id": JOB, "itemId": WS, "status": "Completed"},
                     {"id": WS, "itemId": ITEM, "status": "Completed"}):
            with self.assertRaises(FabricResponseError):
                self.call(lambda _: httpx.Response(200, json=data),
                          lambda c: c.get_pipeline_run(WS, ITEM, JOB))

    def test_health_validation_and_pipeline_cancel(self):
        seen = []
        def handler(request):
            seen.append(request)
            if request.method == "POST":
                return httpx.Response(202)
            if request.url.path.endswith("/workspaces"):
                return httpx.Response(200, json={"value": [{"id": WS, "displayName": "Dev"}]})
            item_type = "DataPipeline" if request.url.path.endswith(f"/items/{JOB}") else "Notebook"
            return httpx.Response(200, json={"id": ITEM if item_type == "Notebook" else JOB,
                                             "displayName": "Target", "type": item_type})
        def action(client):
            health = client.test_connection()
            target = validate_fabric_target(client, workspace_id=WS, lakehouse_id=ITEM,
                                            notebook_id=ITEM, pipeline_id=JOB)
            client.cancel_pipeline_run(WS, JOB, ITEM)
            return health, target
        health, target = self.call(handler, action)
        self.assertEqual(health.accessible_workspace_count, 1)
        self.assertEqual(target.pipeline.type, "DataPipeline")
        self.assertEqual(seen[-1].url.path, f"/v1/workspaces/{WS}/items/{JOB}/jobs/instances/{ITEM}/cancel")

    def test_item_listing_and_duplicate_name(self):
        seen = []
        def handler(request):
            seen.append(request)
            return httpx.Response(200, json={"value": [
                {"id": ITEM, "displayName": "Same", "type": "Notebook"},
                {"id": JOB, "displayName": "Same", "type": "Notebook"},
            ]})
        with self.assertRaisesRegex(FabricResponseError, "Multiple Notebook"):
            self.call(handler, lambda c: c.find_notebook_by_name(WS, "Same"))
        self.assertEqual(seen[0].url.params["type"], "Notebook")

    def test_notebook_parameter_types_and_duplicates(self):
        def handler(request):
            self.assertEqual(json.loads(request.content)["parameters"], [
                {"name": "text", "value": "x", "type": "Text"},
                {"name": "flag", "value": True, "type": "Boolean"},
                {"name": "count", "value": 2, "type": "Number"},
            ])
            return httpx.Response(202, headers={"Location": LOCATION})
        self.call(handler, lambda c: c.run_notebook(WS, ITEM, {
            "text": FabricJobParameter(value="x", type="Text"), "flag": True, "count": 2,
        }))
        with self.assertRaises(ValueError):
            self.call(handler, lambda c: c.run_notebook(WS, ITEM, {"Plan": "x", "plan": "y"}))
        with self.assertRaises(ValueError):
            self.call(handler, lambda c: c.run_notebook(WS, ITEM, {"bad": float("nan")}))

    def test_cancellation_metadata(self):
        def handler(_):
            return httpx.Response(202, headers={"Location": LOCATION, "Retry-After": "17"})
        for cancel in (lambda c: c.cancel_notebook_run(WS, ITEM, JOB),
                       lambda c: c.cancel_pipeline_run(WS, ITEM, JOB)):
            result = self.call(handler, cancel)
            self.assertEqual(result.location, LOCATION)
            self.assertEqual(result.retry_after_seconds, 17)
        with self.assertRaises(FabricResponseError):
            self.call(lambda _: httpx.Response(202, headers={"Location": LOCATION.replace(ITEM, WS)}),
                      lambda c: c.cancel_pipeline_run(WS, ITEM, JOB))

    def test_target_validation_errors_and_optional_pipeline(self):
        def handler(request):
            path = request.url.path
            if path.endswith(f"/workspaces/{WS}"):
                return httpx.Response(200, json={"id": WS, "displayName": "Dev"})
            if path.endswith(f"/lakehouses/{ITEM}"):
                return httpx.Response(200, json={"id": ITEM, "displayName": "Lake"})
            return httpx.Response(200, json={"id": JOB, "displayName": "Run", "type": "Notebook"})
        target = self.call(handler, lambda c: validate_fabric_target(
            c, workspace_id=WS, lakehouse_id=ITEM, notebook_id=JOB))
        self.assertIsNone(target.pipeline)
        for missing_path in (f"/workspaces/{WS}", f"/lakehouses/{ITEM}"):
            with self.subTest(missing_path=missing_path), self.assertRaises(FabricNotFoundError):
                self.call(lambda req: httpx.Response(404) if req.url.path.endswith(missing_path)
                          else handler(req), lambda c: validate_fabric_target(
                              c, workspace_id=WS, lakehouse_id=ITEM, notebook_id=JOB))
        def wrong_type(request):
            if "/items/" in request.url.path:
                return httpx.Response(200, json={"id": JOB, "displayName": "Run", "type": "DataPipeline"})
            return handler(request)
        with self.assertRaisesRegex(FabricResponseError, "expected Notebook"):
            self.call(wrong_type, lambda c: validate_fabric_target(
                c, workspace_id=WS, lakehouse_id=ITEM, notebook_id=JOB))
        with self.assertRaisesRegex(FabricResponseError, "expected DataPipeline"):
            self.call(handler, lambda c: validate_fabric_target(
                c, workspace_id=WS, lakehouse_id=ITEM, notebook_id=JOB, pipeline_id=ITEM))


if __name__ == "__main__":
    unittest.main()
