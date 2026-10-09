"""Source discovery is isolated from deterministic workflow execution."""

from temporalio import activity

from thirdparty.sap.utils import SAPTableSchema

from .contracts import MigrationInput
from .plans import oracle_plan, sap_table_plan
from .source_connections import oracle_client, sap_client


@activity.defn(name="discover_oracle")
def discover_oracle(payload: dict) -> dict:
    input = MigrationInput.model_validate(payload["input"])
    client = oracle_client(input.source_connection_name)
    client.test_connection()
    inspection = client.inspect_table(input.source_schema_name, input.source_object_name)
    planned, review = oracle_plan(input, payload["lakehouse_name"], inspection)
    return {"table_plan": planned.model_dump(mode="json"), "review": review}


@activity.defn(name="discover_sap_table")
def discover_sap_table(payload: dict) -> dict:
    input = MigrationInput.model_validate(payload["input"])
    with sap_client(input.source_connection_name) as client:
        client.test_connection()
        if not client.table_exists(input.source_object_name):
            raise ValueError(f"SAP table {input.source_object_name} was not found")
        schema = client.get_table_schema(input.source_object_name)
        watermark = client.get_watermark_candidates(input.source_object_name)
    planned, review = sap_table_plan(input, payload["lakehouse_name"], schema)
    review["watermark_candidates"] = [
        {"column_name": item.field_name, "data_type": item.datatype,
         "indexed": item.indexed, "nullable": item.nullable,
         "index_details": [], "reason": item.reason} for item in watermark]
    return {"table_plan": planned.model_dump(mode="json"), "review": review}


@activity.defn(name="discover_sap_odp")
def discover_sap_odp(payload: dict) -> dict:
    input = MigrationInput.model_validate(payload["input"])
    with sap_client(input.source_connection_name) as client:
        client.test_connection()
        details = client.get_datasource_details(input.source_object_name)
        if details is None:
            raise ValueError(f"SAP DataSource {input.source_object_name} was not found")
        capability = client.get_odp_capability(input.source_object_name)
        if capability.odp_capable is not True:
            reason = "unsupported" if capability.odp_capable is False else "unknown"
            return {"failure_reason": f"ODP_CAPABILITY_{reason.upper()}: "
                    f"ODP capability is {reason} for {input.source_object_name}"}
        fields = client.get_datasource_fields(input.source_object_name)
        if not details.EXTRACT_STRUCTURE:
            raise ValueError("DataSource has no extract structure")
        schema_fields = client.read_metadata(details.EXTRACT_STRUCTURE)
        exposed = {field.field_name for field in fields if field.extractable is not False}
        selected = [field for field in schema_fields if field.fieldname in exposed]
        if not selected:
            raise ValueError("DataSource has no verified exposed fields")
        delta = client.get_datasource_delta_details(input.source_object_name)
    schema = SAPTableSchema(table_name=input.source_object_name, columns=selected,
                            primary_key_columns=[field.fieldname for field in selected if field.keyflag.upper() == "X"])
    planned, review = sap_table_plan(input, payload["lakehouse_name"], schema,
                                     object_type="SAP_DATASOURCE", delta=delta)
    review["odp_capability"] = capability.model_dump(mode="json")
    review["delta"] = delta.model_dump(mode="json")
    review["datasource"] = details.model_dump(mode="json")
    return {"table_plan": planned.model_dump(mode="json"), "review": review}


@activity.defn(name="validate_oracle_key")
def validate_oracle_key(payload: dict) -> dict:
    input = MigrationInput.model_validate(payload["input"])
    try:
        return oracle_client(input.source_connection_name).validate_replication_key(
            input.source_schema_name, input.source_object_name, payload["columns"])
    except Exception as exc:
        # Credentials, SQL errors, and sampled source values must not enter workflow history.
        return {"valid": False, "status": "UNVERIFIED", "columns": payload["columns"],
                "error": f"Source key check could not complete ({type(exc).__name__}); retry or use full load"}


ACTIVITIES = [discover_oracle, discover_sap_table, discover_sap_odp, validate_oracle_key]
