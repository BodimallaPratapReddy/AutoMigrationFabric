"""Build typed, deterministic runtime plans from adapter metadata."""

from uuid import UUID

from thirdparty.configdb.repository import (
    ApprovedRuntimePlan, ReplicationConfigCreate, SourceTableColumnCreate,
    SourceTableCreate, SourceTablePlan,
)
from thirdparty.datatype_mapping import map_oracle_table_columns, map_sap_field
from thirdparty.oracle.utils import OracleTableInspection
from thirdparty.sap.utils import SAPDeltaDetails, SAPODPCapability, SAPTableSchema, is_sap_watermark_field

from .contracts import MigrationInput


def oracle_primary_key_candidates(inspection: OracleTableInspection) -> list[dict]:
    """Propose ordinary unique indexes; selection still requires user approval."""
    if inspection.primary_key:
        return []
    columns = {column.column_name: column for column in inspection.columns}
    candidates = []
    for index in inspection.indexes:
        names = [column.name for column in sorted(index.columns, key=lambda column: column.position)]
        if (index.uniqueness.upper() != "UNIQUE"
                or index.index_type.upper() not in {"NORMAL", "NORMAL/REV", "IOT - TOP"}
                or index.status == "UNUSABLE" or not names
                or any(name not in columns for name in names)):
            continue
        warnings = []
        nullable = [name for name in names if columns[name].nullable is True]
        unknown = [name for name in names if columns[name].nullable is None]
        if nullable:
            warnings.append("Nullable key fields require a source null check: " + ", ".join(nullable))
        if unknown:
            warnings.append("Nullability is unknown: " + ", ".join(unknown))
        if index.status != "VALID":
            warnings.append("Index/partition usability needs verification")
        candidates.append({"index_name": index.index_name, "columns": names,
                           "source": "UNIQUE_INDEX", "warnings": warnings,
                           "reason": "Unique source index; confirm non-null values and stable row identity"})
    return sorted(candidates, key=lambda candidate: (
        bool(candidate["warnings"]), len(candidate["columns"]), candidate["index_name"]))


def _table(input: MigrationInput, lakehouse_name: str, *, object_type: str,
           source_name: str, source_system: str) -> SourceTableCreate:
    return SourceTableCreate(
        connection_name=input.source_connection_name,
        source_system_type=source_system,
        source_object_type=object_type,
        source_table_name=source_name,
        source_schema_name=input.source_schema_name,
        fabric_workspace_id=input.fabric_workspace_id,
        fabric_lakehouse_id=input.fabric_lakehouse_id,
        fabric_lakehouse_name=lakehouse_name,
        fabric_lakehouse_schema=input.fabric_schema_name,
        fabric_table_name=input.fabric_target_name or input.source_object_name,
        data_source_type="ODP" if input.migration_approach == "SAP_ODP" else None,
    )


def oracle_plan(input: MigrationInput, lakehouse_name: str,
                inspection: OracleTableInspection) -> tuple[SourceTablePlan, dict]:
    keys = {item.name.upper() for item in inspection.primary_key.columns} if inspection.primary_key else set()
    candidates = {item.column_name.upper() for item in inspection.watermark_candidates}
    columns = []
    review_columns = []
    for column, mapping in zip(inspection.columns, map_oracle_table_columns(inspection.columns), strict=True):
        review_columns.append({"name": column.column_name, "source_type": column.datatype,
                               "fabric_type": mapping.fabric_type, "warning": mapping.warning,
                               "supported": mapping.supported, "lossy": mapping.lossy,
                               "rule_name": mapping.rule_name})
        if mapping.fabric_type is None:
            raise ValueError(f"Column {column.column_name} needs a supported Fabric datatype mapping")
        columns.append(SourceTableColumnCreate(
            sno=column.id, column_name=column.column_name,
            source_data_type=mapping.source_type, fabric_data_type=mapping.fabric_type,
            description=column.description, is_primary_key=column.column_name.upper() in keys,
            is_nullable=column.nullable,
            is_watermark_candidate=column.column_name.upper() in candidates))
    plan = SourceTablePlan(
        table=_table(input, lakehouse_name, object_type="TABLE",
                     source_name=inspection.table_name, source_system="ORACLE"),
        columns=columns,
        replication=ReplicationConfigCreate(
            incremental_method="FULL", write_strategy="REPLACE",
            primary_key_columns=sorted(keys) or None,
            ingestion_flag=False, pipeline_workspace_id=input.fabric_workspace_id,
            pipeline_item_id=input.replication_pipeline_id))
    return plan, {"source": f"{inspection.schema_name}.{inspection.table_name}",
                  "columns": review_columns, "primary_key": sorted(keys),
                  "unique_keys": [key.model_dump(mode="json") for key in inspection.unique_keys],
                  "indexes": [index.model_dump(mode="json") for index in inspection.indexes],
                  "primary_key_candidates": oracle_primary_key_candidates(inspection),
                  "watermark_candidates": [x.model_dump(mode="json") for x in inspection.watermark_candidates],
                  "load_method": "FULL", "write_strategy": "REPLACE",
                  "warnings": inspection.warnings}


def sap_table_plan(input: MigrationInput, lakehouse_name: str,
                   schema: SAPTableSchema, *, object_type: str = "TABLE",
                   delta: SAPDeltaDetails | None = None) -> tuple[SourceTablePlan, dict]:
    keys = set(schema.primary_key_columns)
    columns = []
    review_columns = []
    for field in schema.columns:
        mapping = map_sap_field(field)
        review_columns.append({"name": field.fieldname, "source_type": mapping.source_type,
                               "fabric_type": mapping.fabric_type, "warning": mapping.warning})
        if not mapping.supported or mapping.fabric_type is None:
            raise ValueError(f"Field {field.fieldname} needs a supported Fabric datatype mapping")
        columns.append(SourceTableColumnCreate(
            sno=field.position, column_name=field.fieldname,
            source_data_type=mapping.source_type, fabric_data_type=mapping.fabric_type,
            description=field.ddtext, is_primary_key=field.fieldname in keys,
            is_watermark_candidate=is_sap_watermark_field(field)))
    # An unresolved SAP delta indicator is never treated as a working delta route.
    use_delta = bool(delta and delta.details_resolved and delta.delta_supported is True)
    method = "SAP_ODP_DELTA" if use_delta else "FULL"
    plan = SourceTablePlan(
        table=_table(input, lakehouse_name, object_type=object_type,
                     source_name=schema.table_name, source_system="SAP_ECC"),
        columns=columns,
        replication=ReplicationConfigCreate(
            incremental_method=method, write_strategy="REPLACE" if method == "FULL" else "APPEND",
            primary_key_columns=sorted(keys) or None, ingestion_flag=False,
            pipeline_workspace_id=input.fabric_workspace_id,
            pipeline_item_id=input.replication_pipeline_id))
    return plan, {"source": schema.table_name, "columns": review_columns,
                  "primary_key": sorted(keys), "load_method": method,
                  "write_strategy": plan.replication.write_strategy,
                  "warnings": delta.warnings if delta else []}


def approved_plan(plan_guid: str, version: int, planned: SourceTablePlan) -> ApprovedRuntimePlan:
    return ApprovedRuntimePlan(plan_guid=UUID(plan_guid), plan_version=version, tables=[planned])
