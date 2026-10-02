import json
import sys
import unittest
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock, patch

from pydantic import ValidationError

from thirdparty.oracle.utils import (
    OracleClient,
    OracleConnectionSettings,
    OracleIndex,
    OracleTableColumn,
)


class OracleClientTests(unittest.TestCase):
    def make_client(self) -> OracleClient:
        return OracleClient(OracleConnectionSettings(
            username="user", password="secret", dsn="example/DB"))

    def test_connection_and_table_existence(self) -> None:
        client = self.make_client()
        with patch.object(client, "execute_sql", side_effect=[
            json.dumps({"columns": ["HEALTH_CHECK"], "rows": [[1]], "row_count": 1}),
            json.dumps({"columns": ["FOUND"], "rows": [[1]], "row_count": 1}),
        ]) as execute:
            self.assertTrue(client.test_connection().success)
            self.assertTrue(client.table_exists(" ont ", " orders "))
        self.assertEqual(execute.call_args.args[1],
                         {"schema_name": "ONT", "table_name": "ORDERS"})

    def test_composite_keys_and_indexes(self) -> None:
        client = self.make_client()
        key_rows = json.dumps({
            "columns": ["CONSTRAINT_NAME", "COLUMN_NAME", "POSITION"],
            "rows": [["PK_ORDER", "ORDER_ID", 1], ["PK_ORDER", "LINE_ID", 2]],
            "row_count": 2,
        })
        index_rows = json.dumps({
            "columns": ["INDEX_NAME", "UNIQUENESS", "INDEX_TYPE", "COLUMN_NAME", "COLUMN_POSITION"],
            "rows": [["IDX_ORDER", "UNIQUE", "NORMAL", "ORDER_ID", 1],
                     ["IDX_ORDER", "UNIQUE", "NORMAL", "LINE_ID", 2]],
            "row_count": 2,
        })
        with patch.object(client, "execute_sql", side_effect=[key_rows, key_rows, index_rows]):
            primary = client.get_primary_key("ONT", "ORDERS")
            unique = client.get_unique_keys("ONT", "ORDERS")
            indexes = client.get_indexes("ONT", "ORDERS")
        self.assertEqual([c.name for c in primary.columns], ["ORDER_ID", "LINE_ID"])
        self.assertEqual(len(unique), 1)
        self.assertEqual([c.position for c in indexes[0].columns], [1, 2])

    def test_watermark_candidates_only_temporal_columns(self) -> None:
        client = self.make_client()
        columns = [
            OracleTableColumn(ID=1, COLUMN_NAME="ID", DATATYPE="NUMBER", DESC=None),
            OracleTableColumn(ID=2, COLUMN_NAME="LAST_UPDATE_DATE", DATATYPE="DATE", DESC=None,
                              DATA_TYPE="DATE", NULLABLE=0),
            OracleTableColumn(ID=3, COLUMN_NAME="CHANGED_AT", DATATYPE="TIMESTAMP", DESC=None,
                              DATA_TYPE="TIMESTAMP(6)", NULLABLE=0),
        ]
        with patch.object(client, "get_table_schema", return_value=columns), \
             patch.object(client, "get_indexes", return_value=[]):
            candidates = client.get_watermark_candidates("ONT", "ORDERS")
        self.assertEqual([item.column_name for item in candidates], ["LAST_UPDATE_DATE", "CHANGED_AT"])
        self.assertFalse(candidates[0].nullable)
        self.assertEqual(candidates[1].data_type, "TIMESTAMP")

    def test_execute_sql_returns_json_and_closes_connection(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.description = [
            SimpleNamespace(name="ID"),
            SimpleNamespace(name="CREATED"),
            SimpleNamespace(name="DATA"),
        ]
        cursor.fetchmany.return_value = [
            (Decimal("12345678901234567890.25"), datetime(2026, 9, 30, 12, 30), b"abc"),
        ]

        with patch.object(client, "connect", return_value=connection):
            result = json.loads(client.execute_sql("SELECT ID, CREATED FROM T WHERE ID = :id", {"id": 1}))

        self.assertEqual(result["columns"], ["ID", "CREATED", "DATA"])
        self.assertEqual(
            result["rows"],
            [["12345678901234567890.25", "2026-09-30T12:30:00", {"base64": "YWJj"}]],
        )
        self.assertEqual(result["row_count"], 1)
        self.assertEqual(
            cursor.execute.call_args_list[0].args, ("SET TRANSACTION READ ONLY",)
        )
        self.assertEqual(
            cursor.execute.call_args_list[1].args,
            ("SELECT ID, CREATED FROM T WHERE ID = :id", {"id": 1}),
        )
        connection.rollback.assert_called_once()
        connection.close.assert_called_once()

    def test_identifier_validation_and_schema_lookup(self) -> None:
        client = self.make_client()
        with patch.object(client, "execute_sql", return_value=json.dumps({
            "columns": ["FOUND"], "rows": [[1]], "row_count": 1,
        })) as execute:
            self.assertTrue(client.schema_exists(" ont$# "))
        self.assertEqual(execute.call_args.args[1], {"schema_name": "ONT$#"})
        for bad in ("A.B", "A B", "ONT; DROP TABLE X"):
            with self.assertRaises(ValueError):
                client.get_table_schema("ONT", bad)

    def test_bounded_query_rejects_oversized_result(self) -> None:
        client = self.make_client()
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.description = [SimpleNamespace(name="ID")]
        cursor.fetchmany.return_value = [(1,), (2,), (3,)]
        with patch.object(client, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "exceeds max_rows"):
                client.execute_sql("SELECT ID FROM T", max_rows=2)
        cursor.fetchmany.assert_called_once_with(3)
        cursor.fetchall.assert_not_called()
        connection.close.assert_called_once()

    def test_execute_query_returns_typed_result(self) -> None:
        client = self.make_client()
        with patch.object(client, "execute_sql", return_value=json.dumps({
            "columns": ["ID"], "rows": [[1]], "row_count": 1,
        })) as execute:
            result = client.execute_query("SELECT ID FROM T", max_rows=5)
        self.assertEqual(result.rows, [[1]])
        execute.assert_called_once_with("SELECT ID FROM T", None, max_rows=5)

    def test_table_info_statistics_and_inspection(self) -> None:
        client = self.make_client()
        info_json = json.dumps({
            "columns": ["TEMPORARY", "PARTITIONED", "NUM_ROWS", "LAST_ANALYZED"],
            "rows": [["N", "NO", 250, "2026-09-30T12:00:00"]], "row_count": 1,
        })
        stats_json = json.dumps({
            "columns": ["NUM_ROWS", "BLOCKS", "AVG_ROW_LEN", "LAST_ANALYZED"],
            "rows": [[250, 10, 24, "2026-09-30T12:00:00"]], "row_count": 1,
        })
        with patch.object(client, "execute_sql", side_effect=[info_json, stats_json]), \
             patch.object(client, "get_primary_key", return_value=None), \
             patch.object(client, "get_unique_keys", return_value=[]), \
             patch.object(client, "get_table_schema", return_value=[]), \
             patch.object(client, "get_indexes", return_value=[]):
            inspection = client.inspect_table(" ont ", " orders ")
        self.assertEqual(inspection.table_info.estimated_rows, 250)
        self.assertEqual(inspection.statistics.blocks, 10)
        self.assertFalse(inspection.partition_info.partitioned)
        self.assertIn("No primary key found", inspection.warnings)

    def test_watermark_ranking_and_creation_semantics(self) -> None:
        client = self.make_client()
        columns = [
            OracleTableColumn(ID=1, COLUMN_NAME="CREATION_DATE", DATATYPE="DATE", DESC=None),
            OracleTableColumn(ID=2, COLUMN_NAME="LAST_UPDATE_DATE", DATATYPE="DATE", DESC=None,
                              NULLABLE=False),
            OracleTableColumn(ID=3, COLUMN_NAME="EVENT_DATE", DATATYPE="DATE", DESC=None),
        ]
        from thirdparty.oracle.utils import OracleIndexDefinition, OracleKeyColumn
        indexes = [OracleIndexDefinition(index_name="IX", uniqueness="NONUNIQUE",
                    index_type="NORMAL", columns=[OracleKeyColumn(name="LAST_UPDATE_DATE", position=1)])]
        with patch.object(client, "get_table_schema", return_value=columns), \
             patch.object(client, "get_indexes", return_value=indexes):
            candidates = client.get_watermark_candidates("ONT", "ORDERS")
        self.assertEqual(candidates[0].column_name, "LAST_UPDATE_DATE")
        self.assertTrue(candidates[0].leading_index_column)
        self.assertEqual(candidates[-1].reason, "insert-only candidate")
        self.assertFalse(any(candidate.recommended for candidate in candidates))
        self.assertEqual(candidates[0].index_details[0].column_position, 1)

    def test_missing_statistics_do_not_trigger_count(self) -> None:
        client = self.make_client()
        query_result = json.dumps({
            "columns": ["NUM_ROWS", "BLOCKS", "AVG_ROW_LEN", "LAST_ANALYZED"],
            "rows": [[None, None, None, None]], "row_count": 1,
        })
        with patch.object(client, "execute_sql", return_value=query_result) as execute:
            statistics = client.get_table_statistics("ONT", "ORDERS")
        self.assertFalse(statistics.statistics_available)
        self.assertIsNone(statistics.estimated_rows)
        self.assertNotIn("COUNT(*)", execute.call_args.args[0])

    def test_stale_statistics_hide_estimate(self) -> None:
        client = self.make_client()
        query_result = json.dumps({
            "columns": ["NUM_ROWS", "BLOCKS", "AVG_ROW_LEN", "LAST_ANALYZED", "STALE_STATS"],
            "rows": [[500, 20, 40, "2026-09-30T12:00:00", "YES"]], "row_count": 1,
        })
        with patch.object(client, "execute_sql", return_value=query_result):
            statistics = client.get_table_statistics("ONT", "ORDERS")
        self.assertFalse(statistics.statistics_available)
        self.assertIsNone(statistics.estimated_rows)
        self.assertIsNotNone(statistics.last_analyzed)

    def test_partition_metadata(self) -> None:
        client = self.make_client()
        partitioning = json.dumps({
            "columns": ["PARTITIONING_TYPE", "SUBPARTITIONING_TYPE"],
            "rows": [["RANGE", "NONE"]], "row_count": 1,
        })
        partitions = json.dumps({
            "columns": ["PARTITION_NAME", "PARTITION_POSITION", "NUM_ROWS"],
            "rows": [["P2025", 1, 100], ["P2026", 2, None]], "row_count": 2,
        })
        with patch.object(client, "execute_sql", side_effect=[partitioning, partitions]):
            info = client.get_partition_info("ONT", "ORDERS")
        self.assertTrue(info.partitioned)
        self.assertEqual([part.position for part in info.partitions], [1, 2])
        self.assertIsNone(info.partitions[1].estimated_rows)

    def test_execute_sql_rejects_write_before_connecting(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        with patch.object(client, "connect") as connect:
            with self.assertRaisesRegex(ValueError, "SELECT or WITH"):
                client.execute_sql("DELETE FROM T")
        connect.assert_not_called()

    def test_execute_sql_rolls_back_and_closes_on_query_error(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = [None, RuntimeError("query failed")]
        with patch.object(client, "connect", return_value=connection):
            with self.assertRaisesRegex(RuntimeError, "query failed"):
                client.execute_sql("SELECT * FROM T")
        connection.rollback.assert_called_once()
        connection.close.assert_called_once()

    def test_get_index_delegates_to_full_index_discovery(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        from thirdparty.oracle.utils import OracleIndexDefinition, OracleKeyColumn
        index = OracleIndexDefinition(index_name="IDX_SOH_LAST_UPDATED_DATE",
            uniqueness="NONUNIQUE", index_type="NORMAL", columns=[
                OracleKeyColumn(name="LAST_UPDATED_DATE", position=1),
                OracleKeyColumn(name="ORDER_ID", position=2),
            ])
        with patch.object(client, "get_indexes", return_value=[index]) as get_indexes:
            result = client.get_index(" ont ", " sales_order_header ")

        self.assertEqual(
            result,
            [OracleIndex(
                INDEX_NAME="IDX_SOH_LAST_UPDATED_DATE",
                COLUMN_NAME="LAST_UPDATED_DATE",
                COLUMN_POSITION=1,
            )],
        )
        get_indexes.assert_called_once_with("ONT", "SALES_ORDER_HEADER")
        with patch.object(client, "get_indexes", return_value=[index]):
            self.assertEqual(client.get_index("ont", "sales_order_header", " order_id ")[0].column_position, 2)

    def test_get_index_requires_schema_and_table(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        with patch.object(client, "execute_sql") as execute:
            with self.assertRaisesRegex(ValueError, "schema_name"):
                client.get_index(" ", "SALES_ORDER_HEADER")
            with self.assertRaisesRegex(ValueError, "table_name"):
                client.get_index("ONT", " ")
            with self.assertRaisesRegex(ValueError, "column_name"):
                client.get_index("ONT", "SALES_ORDER_HEADER", " ")
        execute.assert_not_called()

    def test_get_table_schema_binds_names_and_queries_column_metadata(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        query_result = json.dumps({
            "columns": ["ID", "COLUMN_NAME", "DATATYPE", "DESC"],
            "rows": [[1, "ORDER_ID", "NUMBER", None]],
            "row_count": 1,
        })
        with patch.object(client, "execute_sql", return_value=query_result) as execute:
            result = client.get_table_schema(" ont ", " sales_order_header ")

        self.assertEqual(
            result,
            [OracleTableColumn(
                ID=1, COLUMN_NAME="ORDER_ID", DATATYPE="NUMBER", DESC=None
            )],
        )
        sql, parameters = execute.call_args.args
        self.assertIn("c.column_id AS id", sql)
        self.assertIn("c.char_length", sql)
        self.assertIn("c.data_precision", sql)
        self.assertIn("c.data_scale", sql)
        self.assertIn('cc.comments AS "DESC"', sql)
        self.assertIn("LEFT JOIN all_col_comments cc", sql)
        self.assertIn("ORDER BY c.column_id", sql)
        self.assertEqual(
            parameters,
            {"schema_name": "ONT", "table_name": "SALES_ORDER_HEADER"},
        )

    def test_get_table_schema_requires_schema_and_table(self) -> None:
        settings = OracleConnectionSettings(
            username="oracle_user", password="secret", dsn="oracle.example.test/ORCL"
        )
        client = OracleClient(settings)
        with patch.object(client, "execute_sql") as execute:
            with self.assertRaisesRegex(ValueError, "schema_name"):
                client.get_table_schema(" ", "SALES_ORDER_HEADER")
            with self.assertRaisesRegex(ValueError, "table_name"):
                client.get_table_schema("ONT", " ")
        execute.assert_not_called()

    def test_windows_uses_native_client_and_connects(self) -> None:
        with TemporaryDirectory() as directory:
            (Path(directory) / "oci.dll").touch()
            settings = OracleConnectionSettings(
                username="oracle_user",
                password="secret",
                host="oracle.example.test",
                service_name="ORCL",
                client_path=Path(directory),
            )
            driver = Mock()
            driver.makedsn.return_value = "test-dsn"

            with patch("thirdparty.oracle.utils.platform.system", return_value="Windows"):
                with patch.dict(sys.modules, {"oracledb": driver}):
                    connection = OracleClient(settings).connect()

            self.assertIs(connection, driver.connect.return_value)
            driver.init_oracle_client.assert_called_once_with(
                lib_dir=str(Path(directory).resolve())
            )
            driver.makedsn.assert_called_once_with(
                "oracle.example.test", 1521, service_name="ORCL"
            )
            driver.connect.assert_called_once_with(
                user="oracle_user", password="secret", dsn="test-dsn"
            )

    def test_linux_uses_loader_path_and_direct_dsn(self) -> None:
        with TemporaryDirectory() as directory:
            (Path(directory) / "libclntsh.so.23.1").touch()
            settings = OracleConnectionSettings(
                username="oracle_user",
                password="secret",
                dsn="oracle.example.test:1521/ORCL",
                client_path=Path(directory),
            )
            driver = Mock()

            with patch("thirdparty.oracle.utils.platform.system", return_value="Linux"):
                with patch.dict(sys.modules, {"oracledb": driver}):
                    OracleClient(settings).connect()

            driver.init_oracle_client.assert_called_once_with()
            driver.makedsn.assert_not_called()
            driver.connect.assert_called_once_with(
                user="oracle_user",
                password="secret",
                dsn="oracle.example.test:1521/ORCL",
            )

    def test_linux_reports_missing_native_libraries(self) -> None:
        with TemporaryDirectory() as directory:
            settings = OracleConnectionSettings(
                username="oracle_user",
                password="secret",
                dsn="oracle.example.test:1521/ORCL",
                client_path=Path(directory),
            )
            driver = Mock()

            with patch("thirdparty.oracle.utils.platform.system", return_value="Linux"):
                with patch.dict(sys.modules, {"oracledb": driver}):
                    with self.assertRaisesRegex(FileNotFoundError, "libclntsh.so"):
                        OracleClient(settings).connect()

            driver.connect.assert_not_called()

    def test_native_client_initialization_is_idempotent(self) -> None:
        with TemporaryDirectory() as first, TemporaryDirectory() as second:
            (Path(first) / "oci.dll").touch()
            (Path(second) / "oci.dll").touch()
            driver = Mock()
            settings = OracleConnectionSettings(
                username="user", password="secret", dsn="example/DB", client_path=Path(first))
            with patch("thirdparty.oracle.utils.platform.system", return_value="Windows"):
                with patch.dict(sys.modules, {"oracledb": driver}):
                    client = OracleClient(settings)
                    client.connect()
                    client.connect()
                    driver.init_oracle_client.assert_called_once()
                    other = OracleClient(settings.model_copy(update={"client_path": Path(second)}))
                    with self.assertRaisesRegex(RuntimeError, "different client path"):
                        other.connect()

    def test_connection_settings_validate_address(self) -> None:
        with self.assertRaises(ValidationError):
            OracleConnectionSettings(username="oracle_user", password="secret")

    def test_connection_settings_accept_camel_case_details(self) -> None:
        settings = OracleConnectionSettings.model_validate(
            {
                "username": "oracle_user",
                "password": "secret",
                "host": "oracle.example.test",
                "serviceName": "ORCL",
                "clientPath": "C:/oracle/client",
            }
        )
        self.assertEqual(settings.service_name, "ORCL")
        self.assertEqual(settings.client_path, Path("C:/oracle/client"))


if __name__ == "__main__":
    unittest.main()
