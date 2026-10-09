import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch
from uuid import uuid4, uuid5

from pydantic import ValidationError

from thirdparty.configdb.repository import (
    ApprovedRuntimePlan, ConfigDBRepository, DBConnectionCreate, FabricViewCreate,
    FabricViewDependencyCreate, FabricViewPlan, ReplicationConfigCreate,
    SourceTableColumnCreate, SourceTableCreate, SourceTablePlan,
    _runtime_fingerprint,
)


class ConfigDBRepositoryTests(unittest.TestCase):
    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_sap_persistence_rejects_a_competing_active_target(self, _):
        repository = ConfigDBRepository("fake")
        plan = self.plan()
        plan.tables[0].table.source_system_type = "SAP_ECC"
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("APPROVED", 2, "reviewer", datetime.utcnow(), None), (uuid4(),)]
        with patch.object(repository, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "active saved configuration"):
                repository.persist_approved_runtime_plan(plan)
        self.assertFalse(any("INSERT INTO" in call.args[0] for call in cursor.execute.call_args_list))
        connection.rollback.assert_called_once()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_wide_sap_plan_uses_bounded_column_batches(self, _):
        repository = ConfigDBRepository("fake")
        plan = self.plan()
        plan.tables[0].table.source_system_type = "SAP_ECC"
        plan.tables[0].columns = [SourceTableColumnCreate(
            sno=i + 1, column_name=f"FIELD_{i}", source_data_type="CHAR(10,0)",
            fabric_data_type="VARCHAR(10)") for i in range(225)]
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("APPROVED", 2, "reviewer", datetime.utcnow(), None), None, None]
        cursor.rowcount = 1
        with patch.object(repository, "connect", return_value=connection):
            repository.persist_approved_runtime_plan(plan)
        batches = [(sql, params) for sql, params in [c.args for c in cursor.execute.call_args_list]
                   if "INSERT INTO bronze_replication.SourceTableColumns" in sql]
        self.assertLess(len(batches), 5)
        self.assertEqual(sum(sql.count(") VALUES ") and sql.split(") VALUES ")[1].count("(")
                             for sql, _ in batches), 225)
        self.assertTrue(all(len(params) <= 2000 for _, params in batches))
        connection.commit.assert_called_once()

    def plan(self) -> ApprovedRuntimePlan:
        return ApprovedRuntimePlan(
            plan_guid=uuid4(), plan_version=2,
            tables=[SourceTablePlan(
                table=SourceTableCreate(
                    connection_name="ORACLE_PROD", source_system_type="ORACLE",
                    source_object_type="TABLE", source_table_name="ORDERS",
                    fabric_workspace_id="workspace", fabric_lakehouse_name="Bronze",
                    fabric_lakehouse_schema="dbo", fabric_table_name="orders"),
                columns=[SourceTableColumnCreate(
                    sno=1, column_name="ORDER_ID", source_data_type="NUMBER(18,0)",
                    fabric_data_type="DECIMAL(18,0)", is_primary_key=True)],
                replication=ReplicationConfigCreate(
                    write_strategy="UPSERT", merge_key_columns=["ORDER_ID", "LINE_ID"]),
            )],
        )

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_atomic_persistence_and_composite_keys(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("APPROVED", 2, "reviewer", datetime.utcnow(), None), None, None]
        cursor.rowcount = 1
        plan = self.plan()
        with patch.object(repository, "connect", return_value=connection):
            result = repository.persist_approved_runtime_plan(plan)
        self.assertEqual(result.plan_guid, plan.plan_guid)
        self.assertEqual(len(result.source_table_guids), 1)
        statements = [call.args for call in cursor.execute.call_args_list]
        self.assertTrue(any("INSERT INTO bronze_replication.SourceTables" in sql for sql, *_ in statements))
        config = next(params for sql, params in statements
                      if "INSERT INTO bronze_replication.ReplicationConfig" in sql)
        self.assertIn('["ORDER_ID","LINE_ID"]', config)
        connection.commit.assert_called_once()
        connection.rollback.assert_not_called()
        connection.close.assert_called_once()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_child_insert_failure_rolls_back(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("APPROVED", 2, "reviewer", datetime.utcnow(), None), None, None]
        def execute(sql, params):
            if "INSERT INTO bronze_replication.SourceTableColumns" in sql:
                raise RuntimeError("column insert failed")
        cursor.execute.side_effect = execute
        with patch.object(repository, "connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "column insert failed"):
                repository.persist_approved_runtime_plan(self.plan())
        connection.rollback.assert_called_once()
        connection.commit.assert_not_called()
        connection.close.assert_called_once()

    def test_replication_requires_key_and_watermark_metadata(self):
        with self.assertRaises(ValidationError):
            ReplicationConfigCreate(write_strategy="UPSERT")
        with self.assertRaises(ValidationError):
            ReplicationConfigCreate(incremental_method="WATERMARK")
        with self.assertRaises(ValidationError):
            ReplicationConfigCreate(watermark_index_name="IX_UPDATED")
        config = ReplicationConfigCreate(incremental_method="WATERMARK",
                                         watermark_column="UPDATED_AT",
                                         watermark_column_data_type="DATE",
                                         watermark_index_name="IX_UPDATED")
        self.assertEqual(config.model_dump(by_alias=True)["WatermarkIndexName"], "IX_UPDATED")

    def test_connection_details_allow_source_password_but_reject_other_secrets(self):
        record = DBConnectionCreate(source_type="ORACLE", connection_name="prod",
                                    connection_details={"username": "scott", "password": "tiger"})
        self.assertEqual(record.connection_details["password"], "tiger")
        with self.assertRaises(ValidationError):
            DBConnectionCreate(source_type="ORACLE", connection_name="prod",
                               connection_details={"host": "db", "auth": {"access_token": "secret"}})

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_planned_view_dependency_resolves_created_table_guid(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("APPROVED", 2, "reviewer", datetime.utcnow(), None), None, None]
        cursor.rowcount = 1
        plan = self.plan()
        plan.views.append(FabricViewPlan(
            view=FabricViewCreate(fabric_workspace_id="workspace", fabric_lakehouse_name="Bronze",
                                  fabric_schema_name="dbo", view_name="orders_v",
                                  view_sql="SELECT ORDER_ID FROM dbo.orders"),
            dependencies=[FabricViewDependencyCreate(
                dependency_type="SOURCE_TABLE", source_table_index=0)]))
        with patch.object(repository, "connect", return_value=connection):
            result = repository.persist_approved_runtime_plan(plan)
        statements = [call.args for call in cursor.execute.call_args_list]
        dependency_sql, params = next((sql, params) for sql, params in statements
                                      if "INSERT INTO bronze_replication.FabricViewDependencies" in sql)
        self.assertIn("SourceTableGUID", dependency_sql)
        self.assertIn(str(result.source_table_guids[0]), tuple(map(str, params)))
        self.assertNotIn("source_table_index", dependency_sql)

    def test_view_dependency_order_is_checked_before_database_access(self):
        plan = self.plan()
        plan.views.append(FabricViewPlan(
            view=FabricViewCreate(fabric_workspace_id="workspace", fabric_lakehouse_name="Bronze",
                                  fabric_schema_name="dbo", view_name="self_ref",
                                  view_sql="SELECT 1"),
            dependencies=[FabricViewDependencyCreate(
                dependency_type="FABRIC_VIEW", depends_on_view_index=0)]))
        with patch("thirdparty.configdb.utils.load_dotenv"):
            repository = ConfigDBRepository("fake")
        with patch.object(repository, "connect") as connect:
            with self.assertRaisesRegex(ValueError, "view dependency index is invalid"):
                repository.persist_approved_runtime_plan(plan)
        connect.assert_not_called()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_parent_table_reference_uses_created_guid(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect = [("APPROVED", 2, "reviewer", datetime.utcnow(), None), None, None]
        cursor.rowcount = 1
        plan = self.plan()
        child = plan.tables[0].model_copy(deep=True)
        child.table.source_table_name = "ORDER_LINES"
        child.table.fabric_table_name = "order_lines"
        child.table.parent_source_index = 0
        plan.tables.append(child)
        with patch.object(repository, "connect", return_value=connection):
            result = repository.persist_approved_runtime_plan(plan)
        inserts = [call.args for call in cursor.execute.call_args_list
                   if "INSERT INTO bronze_replication.SourceTables" in call.args[0]]
        self.assertEqual(len(inserts), 2)
        self.assertIn("ParentSourceObjectGUID", inserts[1][0])
        self.assertIn(str(result.source_table_guids[0]), tuple(map(str, inserts[1][1])))

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_retry_returns_same_ids_without_reinserting(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        plan = self.plan()
        cursor.fetchone.return_value = (
            "READY_TO_PROVISION", 2, "reviewer", datetime.utcnow(),
            _runtime_fingerprint(plan))
        with patch.object(repository, "connect", return_value=connection):
            first = repository.persist_approved_runtime_plan(plan)
        self.assertEqual(first.source_table_guids[0],
                         uuid5(plan.plan_guid, "table:2:0"))
        self.assertFalse(any("INSERT" in call.args[0]
                             for call in cursor.execute.call_args_list))

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_retry_with_different_payload_is_rejected(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        plan = self.plan()
        prior_hash = _runtime_fingerprint(plan)
        plan.tables[0].table.fabric_table_name = "changed"
        cursor.fetchone.return_value = (
            "READY_TO_PROVISION", 2, "reviewer", datetime.utcnow(), prior_hash)
        with patch.object(repository, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "retry payload differs"):
                repository.persist_approved_runtime_plan(plan)
        connection.rollback.assert_called_once()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_transaction_rejects_autocommit_connection(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        connection.autocommit = True
        with patch.object(repository, "connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "autocommit disabled"):
                with repository.transaction():
                    self.fail("transaction must not begin")
        connection.close.assert_called_once()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_plan_approval_checks_version(self, _):
        repository = ConfigDBRepository("fake")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.rowcount = 0
        with patch.object(repository, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "cannot be approved"):
                repository.record_plan_approval(uuid4(), approved_by="reviewer",
                                                expected_plan_version=2)
        self.assertIn("PlanVersion = ?", cursor.execute.call_args.args[0])
        connection.rollback.assert_called_once()
