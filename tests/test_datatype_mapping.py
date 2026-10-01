import unittest

from thirdparty.datatype_mapping import map_oracle_column, map_oracle_column_to_fabric, map_sap_field
from thirdparty.oracle.utils import OracleTableColumn
from thirdparty.sap.utils import SAPFieldMetadata


class TypeMappingTests(unittest.TestCase):
    def oracle(self, datatype: str, **extra: object) -> OracleTableColumn:
        return OracleTableColumn(ID=1, COLUMN_NAME="VALUE", DATATYPE=datatype,
                                 DESC=None, **extra)

    def test_oracle_number_and_timestamp_policy(self) -> None:
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(18,2)")).fabric_type,
                         "DECIMAL(18,2)")
        self.assertFalse(map_oracle_column(self.oracle("NUMBER")).supported)
        self.assertEqual(map_oracle_column(self.oracle("DATE")).fabric_type, "TIMESTAMP")
        self.assertFalse(map_oracle_column(
            self.oracle("TIMESTAMP WITH TIME ZONE")).supported)

    def test_sap_numeric_and_unknown(self) -> None:
        fields = dict(fieldname="AMOUNT", position=1, keyflag="", rollname="",
                      leng=13, decimals=2, checktable="", reftable="", reffield="", ddtext="")
        self.assertEqual(map_sap_field(SAPFieldMetadata(datatype="CURR", **fields)).fabric_type,
                         "DECIMAL(13,2)")
        self.assertFalse(map_sap_field(SAPFieldMetadata(datatype="CUSTOM", **fields)).supported)

    def test_oracle_number_precision_and_explicit_types(self) -> None:
        self.assertEqual(map_oracle_column(self.oracle("VARCHAR2(50)", DATA_TYPE="VARCHAR2")).fabric_type,
                         "STRING")
        self.assertEqual(map_oracle_column_to_fabric(self.oracle("NUMBER(9)")).fabric_type,
                         "DECIMAL(9,0)")
        self.assertEqual(map_oracle_column(self.oracle("NUMBER", DATA_PRECISION=12)).fabric_type,
                         "DECIMAL(12,0)")
        self.assertEqual(map_oracle_column(self.oracle("NUMBER(38,10)")).fabric_type,
                         "DECIMAL(38,10)")
        self.assertFalse(map_oracle_column(self.oracle("NUMBER(39,0)")).supported)
        self.assertFalse(map_oracle_column(self.oracle("NUMBER(10,-1)")).supported)
        self.assertEqual(map_oracle_column(self.oracle("LONG")).fabric_type, "STRING")
