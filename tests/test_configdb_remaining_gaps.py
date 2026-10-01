"""Repository contract tests for workflow-facing Config DB operations."""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from thirdparty.configdb.repository import (
    BatchObjectRunCreate, BatchRunRecord, ConfigDBRepository, SAPAnalysisRecord,
)


class RemainingConfigDBTests(unittest.TestCase):
    def setUp(self) -> None:
        with patch("thirdparty.configdb.utils.load_dotenv"):
            self.repo = ConfigDBRepository("fake")
        self.connection = MagicMock()
        self.connection.__enter__.return_value = self.connection
        self.cursor = self.connection.cursor.return_value.__enter__.return_value
        self.connect_patch = patch.object(self.repo, "connect", return_value=self.connection)
        self.connect_patch.start()
        self.addCleanup(self.connect_patch.stop)
        self.now = datetime.now(timezone.utc)

    def rows(self, value: dict[str, object] | None) -> None:
        self.cursor.description = [(key,) for key in value] if value else []
        self.cursor.fetchall.return_value = [tuple(value.values())] if value else []

    def analysis_row(self, plan_guid: object, version: int) -> dict[str, object]:
        return {
            "AnalysisGUID": uuid4(), "PlanGUID": plan_guid,
            "AnalysisVersion": version, "DataSourceName": "DS1",
            "RootObjectType": None, "RootObjectName": None,
            "AnalysisStatus": "DRAFT", "Summary": None, "LLMModel": None,
            "PhoenixTraceId": None, "ApprovedBy": None,
            "ApprovedTimestamp": None,
        }

    def batch_row(self, plan_guid: object) -> dict[str, object]:
        return {
            "BatchRunId": uuid4(), "PlanGUID": plan_guid,
            "BatchType": "REPLICATION", "TriggerType": None,
            "TemporalWorkflowId": None, "TemporalRunId": None,
            "FabricJobInstanceId": None, "StartedTimestamp": self.now,
            "CompletedTimestamp": None, "Status": "RUNNING",
            "TotalObjects": None, "SucceededObjects": None,
            "FailedObjects": None, "ErrorMessage": None,
        }

    def object_row(self, batch_run_id: object) -> dict[str, object]:
        return {
            "BatchObjectRunId": uuid4(), "BatchRunId": batch_run_id,
            "ObjectGUID": None, "ObjectType": "TABLE", "ObjectName": "T1",
            "FabricPipelineRunId": None, "FabricNotebookRunId": None,
            "StartedTimestamp": self.now, "CompletedTimestamp": None,
            "RowsRead": None, "RowsWritten": None,
            "OldWatermarkValue": "5", "NewWatermarkValue": "8",
            "Status": "SUCCEEDED", "ErrorMessage": None,
        }

    def test_analysis_transition_checks_status_and_version(self) -> None:
        analysis_id = uuid4()
        self.cursor.rowcount = 1
        self.repo.update_sap_analysis_status(
            analysis_id, expected_status="DRAFT",
            new_status="WAITING_APPROVAL", expected_version=2)
        sql, params = self.cursor.execute.call_args.args
        self.assertIn("AnalysisStatus = ? AND AnalysisVersion = ?", sql)
        self.assertEqual(params, ("WAITING_APPROVAL", str(analysis_id), "DRAFT", 2))
        self.cursor.rowcount = 0
        for status, new_status, version in (
            ("WAITING_APPROVAL", "DRAFT", 2),
            ("DRAFT", "WAITING_APPROVAL", 3),
        ):
            with self.subTest(status=status, version=version):
                with self.assertRaisesRegex(ValueError, "status or version changed"):
                    self.repo.update_sap_analysis_status(
                        analysis_id, expected_status=status,
                        new_status=new_status, expected_version=version)
        with self.assertRaisesRegex(ValueError, "unsupported analysis status"):
            self.repo.update_sap_analysis_status(
                analysis_id, expected_status="APPROVED",
                new_status="DRAFT", expected_version=2)

    def test_analysis_reads_by_plan_version_and_history(self) -> None:
        plan_id = uuid4()
        row = self.analysis_row(plan_id, 3)
        self.rows(row)
        latest = self.repo.get_sap_analysis_for_plan(plan_id)
        self.assertIsInstance(latest, SAPAnalysisRecord)
        self.assertEqual(latest.analysis_version, 3)
        self.assertIn("ORDER BY AnalysisVersion DESC", self.cursor.execute.call_args.args[0])
        exact = self.repo.get_sap_analysis_for_plan(plan_id, analysis_version=3)
        self.assertEqual(exact.analysis_guid, row["AnalysisGUID"])
        self.assertEqual(self.cursor.execute.call_args.args[1], (str(plan_id), 3))
        self.assertEqual(len(self.repo.list_sap_analyses_for_plan(plan_id)), 1)
        self.rows(None)
        self.assertIsNone(self.repo.get_sap_analysis_for_plan(plan_id))

    def test_watermark_update_does_not_change_replication_status(self) -> None:
        table_id = uuid4()
        self.cursor.rowcount = 1
        for value in ("123", None):
            self.repo.update_watermark(table_id, value)
            sql, params = self.cursor.execute.call_args.args
            self.assertEqual(params, (value, str(table_id)))
            self.assertNotIn("Status =", sql)
            self.assertNotIn("LastSuccessfulTimestamp", sql)
            self.assertNotIn("RowsRead", sql)
            self.assertNotIn("RowsWritten", sql)
        self.cursor.rowcount = 0
        with self.assertRaisesRegex(ValueError, "state does not exist"):
            self.repo.update_watermark(table_id, "124")

    def test_batch_reads_and_limit(self) -> None:
        plan_id = uuid4()
        batch = self.batch_row(plan_id)
        self.rows(batch)
        self.assertIsInstance(self.repo.get_batch_run(batch["BatchRunId"]), BatchRunRecord)
        self.assertEqual(len(self.repo.list_batch_runs_for_plan(plan_id, limit=5)), 1)
        sql, params = self.cursor.execute.call_args.args
        self.assertIn("TOP (5)", sql)
        self.assertIn("ORDER BY StartedTimestamp DESC", sql)
        self.assertEqual(params, (str(plan_id),))
        for invalid in (0, -1, True):
            with self.assertRaisesRegex(ValueError, "limit must be"):
                self.repo.list_batch_runs_for_plan(plan_id, limit=invalid)
        self.rows(None)
        self.assertIsNone(self.repo.get_batch_run(uuid4()))

    def test_batch_object_read_and_old_watermark_audit(self) -> None:
        batch_id = uuid4()
        run = BatchObjectRunCreate(
            batch_run_id=batch_id, object_type="TABLE",
            old_watermark_value="5")
        object_id = self.repo.start_batch_object_run(run)
        sql, params = self.cursor.execute.call_args.args
        self.assertIn("OldWatermarkValue", sql)
        self.assertIn("5", params)
        self.cursor.rowcount = 1
        self.repo.complete_batch_object_run(object_id, new_watermark_value="8")
        self.assertIn("NewWatermarkValue = ?", self.cursor.execute.call_args.args[0])
        self.rows(self.object_row(batch_id))
        records = self.repo.list_batch_object_runs(batch_id)
        self.assertEqual((records[0].old_watermark_value,
                          records[0].new_watermark_value), ("5", "8"))
        self.assertIn("ORDER BY StartedTimestamp, BatchObjectRunId",
                      self.cursor.execute.call_args.args[0])

    def test_run_id_setters_are_guarded_and_retry_safe(self) -> None:
        for setter, column in (
            (self.repo.set_batch_fabric_job_id, "FabricJobInstanceId"),
            (self.repo.set_batch_object_notebook_run_id, "FabricNotebookRunId"),
            (self.repo.set_batch_object_pipeline_run_id, "FabricPipelineRunId"),
            (self.repo.set_replication_pipeline_run_id, "LastPipelineRunId"),
        ):
            with self.subTest(column=column):
                run_id = uuid4()
                self.cursor.rowcount = 1
                setter(run_id, "job-1")
                sql, _ = self.cursor.execute.call_args.args
                self.assertIn("Status = 'RUNNING'", sql)
                self.assertIn(f"{column} IS NULL", sql)
                with self.assertRaisesRegex(ValueError, "nonempty"):
                    setter(run_id, "  ")
                self.cursor.rowcount = 0
                self.cursor.fetchone.return_value = ("job-1",)
                setter(run_id, "job-1")
                self.cursor.fetchone.return_value = ("other-job",)
                with self.assertRaises(ValueError):
                    setter(run_id, "job-1")
                self.cursor.fetchone.return_value = None
                with self.assertRaises(ValueError):
                    setter(run_id, "job-1")

    def test_cancellation_guards_terminal_states_and_retries(self) -> None:
        for cancel, table in (
            (self.repo.cancel_batch_run, "BatchRuns"),
            (self.repo.cancel_batch_object_run, "BatchObjectRuns"),
        ):
            with self.subTest(table=table):
                run_id = uuid4()
                self.cursor.rowcount = 1
                cancel(run_id, reason="user cancelled")
                sql, params = self.cursor.execute.call_args.args
                self.assertIn(f"UPDATE bronze_replication.{table}", sql)
                self.assertIn("CompletedTimestamp = SYSUTCDATETIME()", sql)
                self.assertIn("Status = 'RUNNING'", sql)
                self.assertEqual(params[0], "user cancelled")
                self.cursor.rowcount = 0
                self.cursor.fetchone.return_value = ("CANCELLED",)
                cancel(run_id)
                for terminal in ("SUCCEEDED", "FAILED"):
                    self.cursor.fetchone.return_value = (terminal,)
                    with self.assertRaisesRegex(ValueError, "cannot be cancelled"):
                        cancel(run_id)


if __name__ == "__main__":
    unittest.main()
