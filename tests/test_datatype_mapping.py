import copy
import unittest

from thirdparty.datatype_mapping import (
    _policy, _validate_oracle_policy, map_oracle_column, map_oracle_column_to_fabric,
    map_oracle_table_columns, map_sap_field,
)
from thirdparty.oracle.utils import OracleTableColumn
from thirdparty.oracle.utils import OracleTableInfo, OracleTableInspection
from thirdparty.sap.utils import SAPFieldMetadata
from app.migration.contracts import MigrationInput
from app.migration.plans import oracle_plan


class TypeMappingTests(unittest.TestCase):
    def oracle(self, datatype: str, **extra: object) -> OracleTableColumn:
        return OracleTableColumn(ID=1, COLUMN_NAME="VALUE", DATATYPE=datatype,
                                 DESC=None, **extra)

    def test_oracle_number_and_timestamp_policy(self) -> None:
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(18,2)")).fabric_type,
                         "DECIMAL(18,2)")
        unbounded = map_oracle_column(self.oracle("NUMBER"))
        self.assertEqual(unbounded.fabric_type, "STRING")
        self.assertTrue(unbounded.supported)
        self.assertEqual(unbounded.warning,
                         _policy("oracle")["number_rules"]["unbounded_number_warning"])
        self.assertEqual(unbounded.rule_name, "UNBOUNDED_NUMBER")
        self.assertEqual(map_oracle_column(self.oracle("DATE")).fabric_type, "TIMESTAMP")
        zoned = map_oracle_column(self.oracle("TIMESTAMP WITH TIME ZONE"))
        self.assertEqual(zoned.fabric_type, "TIMESTAMP")
        self.assertTrue(zoned.lossy)

    def test_sap_numeric_and_unknown(self) -> None:
        fields = dict(fieldname="AMOUNT", position=1, keyflag="", rollname="",
                      leng=13, decimals=2, checktable="", reftable="", reffield="", ddtext="")
        self.assertEqual(map_sap_field(SAPFieldMetadata(datatype="CURR", **fields)).fabric_type,
                         "DECIMAL(13,2)")
        self.assertEqual(map_sap_field(SAPFieldMetadata(datatype="CUSTOM", **fields)).fabric_type,
                         "VARCHAR(MAX)")

    def test_oracle_number_precision_and_explicit_types(self) -> None:
        self.assertEqual(map_oracle_column(self.oracle("VARCHAR2(50)", DATA_TYPE="VARCHAR2")).fabric_type,
                         "STRING")
        self.assertEqual(map_oracle_column_to_fabric(self.oracle("NUMBER(9)")).fabric_type,
                         "INT")
        self.assertEqual(map_oracle_column(self.oracle("NUMBER", DATA_PRECISION=12)).fabric_type,
                         "BIGINT")
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(38,10)")).fabric_type,
                         "DECIMAL(38,10)")
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(39,0)")).fabric_type, "STRING")
        self.assertFalse(map_oracle_column(self.oracle("NUMBER(39,0)")).supported)
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(10,-1)")).fabric_type, "DECIMAL(11,0)")
        high_scale = map_oracle_column(self.oracle("NUMBER(38,39)"))
        self.assertEqual(high_scale.fabric_type, "STRING")
        self.assertTrue(high_scale.supported)
        self.assertFalse(high_scale.lossy)
        self.assertEqual(high_scale.rule_name, "NUMBER_SCALE_OVERFLOW")
        self.assertIn("Scale 39", high_scale.warning)
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(10,20)")).fabric_type, "STRING")
        self.assertEqual(map_oracle_column(self.oracle("LONG")).fabric_type, "STRING")

    def test_oracle_json_fixed_and_default_types(self) -> None:
        self.assertEqual(map_oracle_column(self.oracle("LONG RAW")).fabric_type, "BINARY")
        self.assertEqual(map_oracle_column(self.oracle("FLOAT")).fabric_type, "DOUBLE")
        self.assertEqual(map_oracle_column(self.oracle("INTEGER")).fabric_type, "BIGINT")
        self.assertFalse(map_oracle_column(self.oracle("XMLTYPE")).supported)

    def test_oracle_policy_special_types_and_source_precision(self) -> None:
        timestamp = map_oracle_column(self.oracle("TIMESTAMP", DATA_TYPE="TIMESTAMP(6)"))
        self.assertEqual(timestamp.source_type, "TIMESTAMP(6)")
        zoned = map_oracle_column(self.oracle(
            "TIMESTAMP", DATA_TYPE="TIMESTAMP(6) WITH LOCAL TIME ZONE"))
        self.assertTrue(zoned.lossy)
        self.assertIn("local-time-zone", zoned.warning)
        interval = map_oracle_column(self.oracle("INTERVAL DAY(2) TO SECOND(6)"))
        self.assertEqual(interval.fabric_type, "STRING")
        self.assertTrue(interval.lossy)
        self.assertFalse(map_oracle_column(self.oracle("UNKNOWN_TYPE")).supported)

    def test_oracle_json_categories_and_validation(self) -> None:
        examples = {
            "NVARCHAR2(100)": "STRING", "CHAR(10)": "STRING", "CLOB": "STRING",
            "RAW": "BINARY", "BLOB": "BINARY", "NUMBER(5,0)": "INT",
            "NUMBER(10,0)": "BIGINT", "NUMBER(18,0)": "BIGINT",
            "NUMBER(20,0)": "DECIMAL(20,0)", "BINARY_FLOAT": "FLOAT",
            "BINARY_DOUBLE": "DOUBLE", "TIMESTAMP(3)": "TIMESTAMP",
        }
        columns = [self.oracle(kind) for kind in examples]
        self.assertEqual([mapped.fabric_type for mapped in map_oracle_table_columns(columns)],
                         list(examples.values()))
        policy = copy.deepcopy(_policy("oracle"))
        policy["number_rules"]["integer_precision_rules"][1]["max_precision"] = 8
        with self.assertRaisesRegex(ValueError, "thresholds"):
            _validate_oracle_policy(policy)
        policy = copy.deepcopy(_policy("oracle"))
        del policy["number_rules"]["negative_scale_rule"]
        with self.assertRaisesRegex(ValueError, "negative scale rule"):
            _validate_oracle_policy(policy)

    def test_oracle_number_boundaries_follow_updated_policy(self) -> None:
        cases = {
            "NUMBER": ("STRING", "UNBOUNDED_NUMBER"),
            "NUMBER(5,0)": ("INT", "NUMBER_INTEGER"),
            "NUMBER(9,0)": ("INT", "NUMBER_INTEGER"),
            "NUMBER(12,0)": ("BIGINT", "NUMBER_INTEGER"),
            "NUMBER(18,0)": ("BIGINT", "NUMBER_INTEGER"),
            "NUMBER(25,0)": ("DECIMAL(25,0)", "NUMBER_INTEGER"),
            "NUMBER(38,0)": ("DECIMAL(38,0)", "NUMBER_INTEGER"),
            "NUMBER(10,2)": ("DECIMAL(10,2)", "NUMBER_DECIMAL"),
            "NUMBER(20,6)": ("DECIMAL(20,6)", "NUMBER_DECIMAL"),
            "NUMBER(38,38)": ("DECIMAL(38,38)", "NUMBER_DECIMAL"),
            "NUMBER(38,39)": ("STRING", "NUMBER_SCALE_OVERFLOW"),
            "NUMBER(38,127)": ("STRING", "NUMBER_SCALE_OVERFLOW"),
            "NUMBER(5,-1)": ("DECIMAL(6,0)", "NUMBER_NEGATIVE_SCALE"),
            "NUMBER(10,-2)": ("DECIMAL(12,0)", "NUMBER_NEGATIVE_SCALE"),
            "NUMBER(20,-5)": ("DECIMAL(25,0)", "NUMBER_NEGATIVE_SCALE"),
            "NUMBER(38,-1)": ("STRING", "NUMBER_NEGATIVE_SCALE_OVERFLOW"),
        }
        for source, (target, rule_name) in cases.items():
            with self.subTest(source=source):
                result = map_oracle_column(self.oracle(source))
                self.assertEqual((result.fabric_type, result.rule_name), (target, rule_name))
                self.assertTrue(result.supported)
                self.assertFalse(result.lossy)
        self.assertIn("Adjusted precision = 10 + 2 = 12",
                      map_oracle_column(self.oracle("NUMBER(10,-2)")).warning)

    def test_oracle_negative_scale_uses_adjusted_precision(self) -> None:
        cases = {
            "NUMBER(5,-1)": "DECIMAL(6,0)",
            "NUMBER(10,-2)": "DECIMAL(12,0)",
            "NUMBER(20,-5)": "DECIMAL(25,0)",
            "NUMBER(38,-1)": "STRING",
        }
        for source, target in cases.items():
            with self.subTest(source=source):
                mapping = map_oracle_column(self.oracle(source))
                self.assertEqual(mapping.fabric_type, target)
                self.assertIn("Adjusted precision =", mapping.warning)

    def test_oracle_plan_retains_source_types_and_revised_fabric_types(self) -> None:
        migration = MigrationInput(
            migration_approach="ORACLE_TABLE", source_connection_name="Oracle Local",
            source_schema_name="ONT", source_object_name="NUMBERS",
            fabric_workspace_id="5a590541-0088-465b-b8a3-d7609f270a5f",
            fabric_lakehouse_id="64d625a2-5ca0-432e-a6b7-522211c89352",
            fabric_schema_name="local_oracle",
        )
        inspection = OracleTableInspection(
            schema_name="ONT", table_name="NUMBERS",
            table_info=OracleTableInfo(schema_name="ONT", table_name="NUMBERS", exists=True),
            columns=[self.oracle("NUMBER(38,127)"),
                     OracleTableColumn(ID=2, COLUMN_NAME="ROUNDED",
                                       DATATYPE="NUMBER(10,-2)", DESC=None)],
        )
        plan, review = oracle_plan(migration, "Bronze", inspection)
        self.assertEqual([(column.source_data_type, column.fabric_data_type)
                          for column in plan.columns],
                         [("NUMBER(38,127)", "STRING"), ("NUMBER(10,-2)", "DECIMAL(12,0)")])
        self.assertEqual([item["rule_name"] for item in review["columns"]],
                         ["NUMBER_SCALE_OVERFLOW", "NUMBER_NEGATIVE_SCALE"])

    def test_sap_json_length_fixed_and_binary_types(self) -> None:
        fields = dict(fieldname="VALUE", position=1, keyflag="", rollname="",
                      leng=13, decimals=2, checktable="", reftable="", reffield="", ddtext="")
        def mapped(kind):
            return map_sap_field(SAPFieldMetadata(datatype=kind, **fields)).fabric_type
        self.assertEqual(mapped("CHAR"), "VARCHAR(13)")
        self.assertEqual(mapped("RAW"), "VARBINARY(13)")
        self.assertEqual(mapped("STRING"), "VARCHAR(MAX)")
        self.assertEqual(mapped("DATS"), "VARCHAR(8)")
        self.assertEqual(mapped("TIMS"), "VARCHAR(6)")
        self.assertEqual(mapped("FLTP"), "FLOAT")

    def test_sap_client_and_time_have_explicit_text_mappings(self) -> None:
        for name, datatype, length, target in [
            ("MANDT", "CLNT", 3, "VARCHAR(3)"),
            ("ABHOV", "TIMS", 6, "VARCHAR(6)"),
        ]:
            with self.subTest(datatype=datatype):
                field = SAPFieldMetadata(
                    fieldname=name, datatype=datatype, position=1,
                    keyflag="", rollname="", leng=length, decimals=0,
                    checktable="", reftable="", reffield="", ddtext="",
                )
                mapping = map_sap_field(field)
                self.assertEqual(mapping.source_type, f"{datatype}({length},0)")
                self.assertEqual(mapping.fabric_type, target)
                self.assertTrue(mapping.supported)
                self.assertFalse(mapping.lossy)
                self.assertIsNone(mapping.warning)
