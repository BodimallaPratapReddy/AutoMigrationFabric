"""Unique-index proposals must preserve composite identity and expose uncertainty."""
from app.migration.plans import oracle_primary_key_candidates
from thirdparty.oracle.utils import (
    OracleIndexDefinition, OracleKey, OracleKeyColumn, OracleTableColumn,
    OracleTableInfo, OracleTableInspection,
)


def inspection(indexes, primary_key=None):
    return OracleTableInspection(
        schema_name="ONT", table_name="ORDERS",
        table_info=OracleTableInfo(schema_name="ONT", table_name="ORDERS", exists=True),
        primary_key=primary_key, indexes=indexes,
        columns=[OracleTableColumn(ID=position, COLUMN_NAME=name, DATATYPE="NUMBER",
                                  DESC=None, NULLABLE=nullable)
                 for position, (name, nullable) in enumerate(
                     [("ID", False), ("TENANT", False), ("OPTIONAL", True)], 1)],
    )


def index(name, columns, *, unique="UNIQUE", kind="NORMAL", status="VALID"):
    return OracleIndexDefinition(index_name=name, uniqueness=unique, index_type=kind,
                                 status=status, columns=[
        OracleKeyColumn(name=column, position=position)
        for position, column in enumerate(columns, 1)])


def test_proposals_preserve_composite_keys_rank_nullable_last_and_exclude_unsafe_indexes():
    source = inspection([
        index("NULLABLE", ["OPTIONAL"]),
        index("COMPOSITE", ["TENANT", "ID"]),
        index("NONUNIQUE", ["ID"], unique="NONUNIQUE"),
        index("EXPRESSION", ["ID"], kind="FUNCTION-BASED NORMAL"),
        index("HIDDEN", ["SYS_NC00001$"]),
        index("BROKEN", ["ID"], status="UNUSABLE"),
    ])
    candidates = oracle_primary_key_candidates(source)
    assert [item["index_name"] for item in candidates] == ["COMPOSITE", "NULLABLE"]
    assert candidates[0]["columns"] == ["TENANT", "ID"]
    assert candidates[0]["warnings"] == []
    assert "Nullable" in candidates[1]["warnings"][0]


def test_partition_usability_and_unknown_nullability_are_not_assumed():
    source = inspection([index("PARTITIONED", ["ID"], status="N/A")])
    source.columns[0].nullable = None
    candidate = oracle_primary_key_candidates(source)[0]
    assert len(candidate["warnings"]) == 2


def test_database_primary_key_takes_precedence():
    source = inspection([index("UNIQUE_ID", ["ID"])], primary_key=OracleKey(
        constraint_name="PK", columns=[OracleKeyColumn(name="ID", position=1)]))
    assert oracle_primary_key_candidates(source) == []
