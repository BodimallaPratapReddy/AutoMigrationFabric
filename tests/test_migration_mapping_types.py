import pytest

from app.migration.mapping_types import normalize_fabric_type


def test_mapping_override_types_are_normalized_and_bounded():
    assert normalize_fabric_type(" decimal ( 18 , 2 ) ") == "DECIMAL(18,2)"
    assert normalize_fabric_type("bigint") == "BIGINT"
    for value in ("DECIMAL(39,0)", "DECIMAL(10,11)", "VARCHAR(100); DROP TABLE X", ""):
        with pytest.raises(ValueError):
            normalize_fabric_type(value)
