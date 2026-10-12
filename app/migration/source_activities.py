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


@activity.defn(name='sap_agent_metadata')
def sap_agent_metadata(payload: dict) -> dict:
    input=MigrationInput.model_validate(payload['input'])
    with sap_client(input.source_connection_name) as client:
        client.test_connection()
        if not client.table_exists(input.source_object_name):
            raise ValueError('SAP source must be a visible transparent table')
        schema=client.get_table_schema(input.source_object_name)
        watermarks=client.get_watermark_candidates(input.source_object_name)
        text=client.execute_sql_query(f"SELECT DDTEXT FROM DD02T WHERE TABNAME = '{input.source_object_name}' AND DDLANGUAGE = 'E' AND AS4LOCAL = 'A'",max_rows=1)
        if not text.success:
            raise ValueError('SAP table description lookup failed')
        from .sap_semantics import read_semantics
        semantics = read_semantics(client, schema.columns)
        return dict(schema=schema.model_dump(mode='json'),watermark_candidates=[c.model_dump(mode='json') for c in watermarks],
            description=text.rows[0].get('DDTEXT') if text.rows else None,semantic_metadata=semantics,
            source_binding=dict(connection_name=input.source_connection_name,sap_client=client.sap_client,
                host=client._http.base_url.host,https_port=client._http.base_url.port))


@activity.defn(name='validate_sap_key')
def validate_sap_key(payload: dict) -> dict:
    from thirdparty.sap.extraction import identifier
    from .primary_keys import validate_primary_key
    input=MigrationInput.model_validate(payload['input'])
    columns=payload['columns']
    try:
        table=identifier(input.source_object_name)
        with sap_client(input.source_connection_name) as client:
            schema=client.get_table_schema(table)
            columns=validate_primary_key(columns,[c.fieldname for c in schema.columns])
            if not columns: raise ValueError('Select at least one SAP key field')
            from thirdparty.sap.extraction import quoted
            names=[quoted(c) for c in columns]
            date_keys={c.fieldname for c in schema.columns if c.datatype=='DATS'}
            null_conditions=[f'{n} IS NULL' for n in names]
            null_conditions += [f"{quoted(c)} = '00000000'" for c in columns if c in date_keys]
            nulls=client.execute_sql_query(f"SELECT COUNT(*) AS BAD_KEYS FROM {quoted(table)} WHERE "+' OR '.join(null_conditions),max_rows=1)
            duplicates=client.execute_sql_query(f"SELECT {', '.join(names)}, COUNT(*) AS KEY_COUNT FROM {quoted(table)} GROUP BY {', '.join(names)} HAVING COUNT(*) > 1",max_rows=1)
            if not nulls.success or len(nulls.rows)!=1 or nulls.truncated or not duplicates.success or (duplicates.truncated and not duplicates.rows):
                raise ValueError('SAP full-scope key validation was incomplete')
            count=int(nulls.rows[0]['BAD_KEYS'])
            return dict(valid=count==0 and not duplicates.rows,has_nulls=count>0,has_duplicates=bool(duplicates.rows),
                columns=columns,status='CHECKED',scope='FULL_TABLE',sap_client=client.sap_client)
    except Exception as exc:
        return dict(valid=False,columns=columns,status='UNVERIFIED',error=f'SAP key validation did not complete ({type(exc).__name__}); retry or use full refresh')


ACTIVITIES = [discover_oracle, discover_sap_table, discover_sap_odp, validate_oracle_key,sap_agent_metadata,validate_sap_key]
