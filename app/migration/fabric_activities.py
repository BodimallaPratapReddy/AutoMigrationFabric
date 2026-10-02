"""Each Fabric activity makes one bounded adapter call."""

from temporalio import activity

from services.fabric_provisioning import parse_provisioning_notebook_result
from services.fabric_validation import validate_fabric_target
from thirdparty.fabric.client import FabricClient


@activity.defn(name="validate_fabric")
def validate_fabric(payload: dict) -> dict:
    with FabricClient() as client:
        result = validate_fabric_target(
            client, workspace_id=payload["fabric_workspace_id"],
            lakehouse_id=payload["fabric_lakehouse_id"],
            notebook_id=payload["provisioning_notebook_id"],
            pipeline_id=payload["replication_pipeline_id"])
    return {"lakehouse_name": result.lakehouse.display_name,
            "workspace_name": result.workspace.display_name}


@activity.defn(name="submit_provisioning")
def submit_provisioning(payload: dict) -> dict:
    with FabricClient() as client:
        result = client.trigger_provisioning(
            workspace_id=payload["fabric_workspace_id"],
            notebook_id=payload["provisioning_notebook_id"],
            plan_guid=payload["plan_guid"])
    return result.model_dump(mode="json")


@activity.defn(name="get_provisioning_status")
def get_provisioning_status(payload: dict) -> dict:
    with FabricClient() as client:
        result = client.get_notebook_run(payload["fabric_workspace_id"],
                                         payload["provisioning_notebook_id"], payload["job_id"])
    return result.model_dump(mode="json")


@activity.defn(name="parse_provisioning")
def parse_provisioning(payload: dict) -> dict:
    from thirdparty.fabric.models import FabricJobInstance
    result = parse_provisioning_notebook_result(
        FabricJobInstance.model_validate(payload["job"]), expected_plan_guid=payload["plan_guid"])
    return result.model_dump(mode="json")


@activity.defn(name="cancel_provisioning")
def cancel_provisioning(payload: dict) -> dict:
    with FabricClient() as client:
        return client.cancel_notebook_run(payload["fabric_workspace_id"],
                                          payload["provisioning_notebook_id"],
                                          payload["job_id"]).model_dump(mode="json")


@activity.defn(name="submit_replication")
def submit_replication(payload: dict) -> dict:
    with FabricClient() as client:
        result = client.run_pipeline(payload["fabric_workspace_id"],
                                     payload["replication_pipeline_id"])
    return result.model_dump(mode="json")


@activity.defn(name="get_replication_status")
def get_replication_status(payload: dict) -> dict:
    with FabricClient() as client:
        result = client.get_pipeline_run(payload["fabric_workspace_id"],
                                         payload["replication_pipeline_id"], payload["job_id"])
    return result.model_dump(mode="json")


@activity.defn(name="cancel_replication")
def cancel_replication(payload: dict) -> dict:
    with FabricClient() as client:
        return client.cancel_pipeline_run(payload["fabric_workspace_id"],
                                          payload["replication_pipeline_id"],
                                          payload["job_id"]).model_dump(mode="json")


ACTIVITIES = [validate_fabric, submit_provisioning, get_provisioning_status,
              parse_provisioning, cancel_provisioning, submit_replication,
              get_replication_status, cancel_replication]
