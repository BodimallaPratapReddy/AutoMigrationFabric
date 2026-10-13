"""Prevent implicit replacement of registered targets in a view bundle."""
from uuid import UUID
from temporalio import activity
from thirdparty.configdb.repository import ConfigDBRepository


@activity.defn
def check_oracle_view_targets(payload):
    db = ConfigDBRepository()
    bundle = payload["bundle"]
    for table in bundle["tables"]:
        target = table["table"]
        matches = db.list_target_table_records(workspace_id=target["fabric_workspace_id"],
            lakehouse_id=target["fabric_lakehouse_id"], schema_name=target["fabric_lakehouse_schema"],
            table_name=target["fabric_table_name"], exclude_plan_guid=UUID(payload["plan_guid"]))
        if matches:
            raise ValueError(f"Table target {target['fabric_lakehouse_schema']}.{target['fabric_table_name']} "
                             "already exists; choose an isolated target name before approval")
    for planned in bundle["views"]:
        target = planned["view"]
        # Includes saved as well as provisioned views: neither may be silently replaced.
        with db.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1 FROM bronze_replication.FabricViews WHERE FabricWorkspaceId=? "
                    "AND FabricLakehouseId=? AND FabricSchemaName=? AND ViewName=? AND IsActive=1 AND PlanGUID<>?",
                    (target["fabric_workspace_id"], target["fabric_lakehouse_id"], target["fabric_schema_name"],
                     target["view_name"], payload["plan_guid"]))
                if cursor.fetchone():
                    raise ValueError(f"View target {target['fabric_schema_name']}.{target['view_name']} "
                                     "already exists; choose a new target name before approval")
    return {"valid": True}
