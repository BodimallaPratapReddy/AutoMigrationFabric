import unittest
from datetime import datetime
from unittest.mock import MagicMock, Mock, patch
from uuid import UUID

from pydantic import ValidationError

from thirdparty.configdb.utils import (
    ConfigDB,
    FabricWorkspace,
    SourceTableColumnCreate,
    SourceTableCreate,
    WatermarkControlCreate,
)


class ConfigDBTests(unittest.TestCase):
    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_list_fabric_workspaces(self, _: Mock) -> None:
        db = ConfigDB("Server=example;Database=config")
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        created = datetime(2026, 9, 21, 2, 23, 53)
        cursor.fetchall.return_value = [
            (1, "Test_Gopi", "5a590541-0088-465b-b8a3-d7609f270a5f", created, created)
        ]

        with patch.object(db, "connect", return_value=connection):
            workspaces = db.list_fabric_workspaces()

        cursor.execute.assert_called_once_with(
            "SELECT Id, WorkspaceName, WorkspaceId, "
            "CreatedTimestamp, UpdatedTimestamp "
            "FROM bronze_replication.FabricWorkspaces ORDER BY Id"
        )
        self.assertEqual(
            workspaces,
            [
                FabricWorkspace(
                    id=1,
                    workspace_name="Test_Gopi",
                    workspace_id="5a590541-0088-465b-b8a3-d7609f270a5f",
                    created_timestamp=created,
                    updated_timestamp=created,
                )
            ],
        )
        connection.__exit__.assert_called_once()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_list_active_fabric_workspaces(self, _: Mock) -> None:
        db = ConfigDB("Server=example;Database=config")
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []

        with patch.object(db, "connect", return_value=connection):
            assert db.list_fabric_workspaces(active_only=True) == []

        cursor.execute.assert_called_once_with(
            "SELECT Id, WorkspaceName, WorkspaceId, "
            "CreatedTimestamp, UpdatedTimestamp "
            "FROM bronze_replication.FabricWorkspaces "
            "WHERE IsActive = 1 ORDER BY WorkspaceName, Id"
        )

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_insert_table_and_columns_in_shared_transaction(self, _: Mock) -> None:
        db = ConfigDB("Server=example;Database=config")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (1,)
        columns = [
            SourceTableColumnCreate(
                sno=1,
                column_name="PO_HEADER_ID",
                source_data_type="NUMBER",
                fabric_data_type="DOUBLE",
            ),
            SourceTableColumnCreate(
                sno=2,
                column_name="TYPE_LOOKUP_CODE",
                description="Type code",
                source_data_type="VARCHAR2(25)",
                fabric_data_type="STRING",
            ),
        ]

        with patch.object(db, "connect", return_value=connection) as connect:
            with connection:
                guid = db.insert_source_table(
                    SourceTableCreate(
                        connection_name="ORACLE-RPTP", source_table_name="PO.PO_HEADERS_ALL"
                    ),
                    connection=connection,
                )
                db.insert_source_table_columns(guid, columns, connection=connection)

        UUID(guid)
        connect.assert_not_called()
        insert_sql, insert_params = cursor.execute.call_args_list[0].args
        self.assertIn("INSERT INTO bronze_replication.SourceTables", insert_sql)
        self.assertEqual(insert_params[:3], (guid, "ORACLE-RPTP", "PO.PO_HEADERS_ALL"))
        self.assertEqual(insert_params[-2:], (True, None))
        self.assertEqual(cursor.execute.call_args_list[1].args[1], (guid,))
        batch_sql, batch_rows = cursor.executemany.call_args.args
        self.assertIn("INSERT INTO bronze_replication.SourceTableColumns", batch_sql)
        self.assertEqual(
            batch_rows,
            [
                (guid, 1, "PO_HEADER_ID", None, "NUMBER", "DOUBLE"),
                (guid, 2, "TYPE_LOOKUP_CODE", "Type code", "VARCHAR2(25)", "STRING"),
            ],
        )
        connection.__exit__.assert_called_once()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_columns_reject_missing_parent_without_insert(self, _: Mock) -> None:
        db = ConfigDB("Server=example;Database=config")
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = None

        with self.assertRaisesRegex(ValueError, "does not exist"):
            db.insert_source_table_columns(
                "279cc8af-7785-4916-9f37-96ba1b9e5a7d",
                [
                    SourceTableColumnCreate(
                        sno=1,
                        column_name="ID",
                        source_data_type="NUMBER",
                        fabric_data_type="INT",
                    )
                ],
                connection=connection,
            )

        cursor.executemany.assert_not_called()

    @patch("thirdparty.configdb.utils.load_dotenv")
    def test_insert_watermark_control(self, _: Mock) -> None:
        db = ConfigDB("Server=example;Database=config")
        connection = MagicMock()
        cursor = connection.__enter__.return_value.cursor.return_value.__enter__.return_value
        watermark = WatermarkControlCreate(
            source_table_full_name="PO.PO_HEADERS_ALL",
            destination_table_full_name="bronze.po_headers_all",
            watermark_column="LAST_UPDATE_DATE",
            watermark_column_data_type="DATE",
            last_watermark_value="2026-01-01 00:00:00",
            max_row_fetch=5000,
            ingestion_flag=True,
            load_strategy="INCREMENTAL",
            merge_key_column="PO_HEADER_ID",
        )

        with patch.object(db, "connect", return_value=connection):
            db.insert_watermark_control(watermark)

        sql, params = cursor.execute.call_args.args
        self.assertIn("INSERT INTO bronze_replication.watermark_control", sql)
        self.assertIn("GETUTCDATE()", sql)
        self.assertEqual(
            params,
            (
                "PO.PO_HEADERS_ALL",
                "bronze.po_headers_all",
                "LAST_UPDATE_DATE",
                "DATE",
                "2026-01-01 00:00:00",
                5000,
                True,
                "INCREMENTAL",
                "PO_HEADER_ID",
            ),
        )
        connection.__exit__.assert_called_once()

    def test_watermark_model_rejects_invalid_batch_size(self) -> None:
        with self.assertRaises(ValidationError):
            WatermarkControlCreate(
                source_table_full_name="PO.PO_HEADERS_ALL",
                destination_table_full_name="bronze.po_headers_all",
                watermark_column="LAST_UPDATE_DATE",
                watermark_column_data_type="DATE",
                max_row_fetch=-1,
            )


if __name__ == "__main__":
    unittest.main()
