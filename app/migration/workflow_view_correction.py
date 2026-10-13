"""New workflow type; existing view executions keep their original history."""
from temporalio import workflow
from .workflow_oracle_view import OpenCodeOracleViewMigrationWorkflow
from .workflow_base import READ_RETRY, WRITE_RETRY
from .view_correction import repairable_views, validate_correction


@workflow.defn
class OpenCodeOracleViewCorrectionWorkflow(OpenCodeOracleViewMigrationWorkflow):
    @workflow.run
    async def run(self, input: dict) -> dict:
        return await super().run(input)

    async def _discover(self, input: dict, lakehouse_name: str, discovery_activity: str) -> dict:
        self._discovery_input, self._lakehouse_name = input, lakehouse_name
        result = await super()._discover(input, lakehouse_name, discovery_activity)
        if not input.get("view_correction"):
            self._bundle = result
        return result

    async def _execute_fabric(self, input: dict, *, handoff_to_scheduler=False, replicate=False) -> None:
        guid = self.state["plan_guid"]
        await self._activity("validate_fabric", input, retry=READ_RETRY)
        for attempt in range(3):
            await self._check_cancel()
            common = {"plan_guid": guid, "workflow_id": self.state["workflow_id"],
                      "run_id": workflow.info().run_id, "version": self.state["plan_version"]}
            await self._activity("transition_plan", {**common, "expected": "READY_TO_PROVISION", "target": "PROVISIONING"})
            self._phase("PROVISIONING_VIEW_ATTEMPT")
            batch = await self._activity("start_batch", {**common, "batch_type": "PROVISIONING", "pending_only": True,
                                                         "attempt_version": common["version"]}, retry=WRITE_RETRY)
            self.state["batch_run_id"] = batch
            submission = await self._activity("submit_provisioning", {**input, "plan_guid": guid}, timeout=60)
            job_id = submission["job_instance_id"]
            self.state["fabric_job_instance_id"] = job_id
            await self._activity("record_batch_job", {"batch_run_id": batch, "job_id": job_id}, retry=WRITE_RETRY)
            self._phase("WAITING_FOR_VIEW_PROVISIONING")
            job = await self._watch_job(input, kind="provisioning", job_id=job_id)
            failure = None
            try:
                report = await self._activity("parse_provisioning", {"job": job, "plan_guid": guid})
            except Exception as exc:
                cause = getattr(exc, "cause", None)
                if getattr(cause, "type", None) != "FabricProvisioningLogicalFailure" or not cause.details:
                    raise
                failure, report = exc, cause.details[0]
            self.state["provisioning_report"] = report
            completion = await self._activity("record_view_attempt", {**common, "batch_run_id": batch,
                "job_id": job_id, "result": report}, retry=WRITE_RETRY)
            self.state["batch_run_id"] = None
            await self._check_cancel()
            if failure is None:
                if not completion["all_provisioned"]:
                    raise ValueError("Not all approved objects were provisioned")
                await self._activity("transition_plan", {**common, "expected": "PROVISIONING", "target": "PROVISIONED"})
                if handoff_to_scheduler:
                    await self._activity("activate_replication_config", {"plan_guid": guid}, retry=WRITE_RETRY)
                self.state.update(provisioning_status="SUCCEEDED", replication_status="SKIPPED")
                return
            self.state["provisioning_status"] = "FAILED"
            eligible = repairable_views(report)
            if not eligible or attempt == 2:
                raise failure
            self.state["view_correction_attempt"] = attempt + 1
            context = {"previous": self._bundle, "eligible": eligible, "failure": report,
                       "attempt": attempt + 1}
            self._phase("REQUESTING_VIEW_CORRECTION", message="Returning the failed SQL to the planning agent")
            proposed = await self._discover({**self._discovery_input, "view_correction": context,
                "agent_revision_start": self.state.get("agent_prompt_revision", 0) + 1}, self._lakehouse_name, "opencode")
            if proposed.get("failure_reason"):
                raise ValueError(proposed["failure_reason"])
            changes = validate_correction(self._bundle, proposed, eligible)
            proposed["review"]["correction"] = {"attempt": attempt + 1, "maximum_attempts": 2,
                "failure": report, "changes": changes, "proposed_version": common["version"] + 1}
            self.state["review"] = proposed["review"]
            choice = await self._gate("WAITING_FOR_PLAN_APPROVAL", ("approve_plan", "reject_plan"))
            if choice["action"] == "reject_plan":
                self._phase("VIEW_CORRECTION_REJECTED")
                raise ValueError("Reviewer rejected the SQL correction")
            await self._check_cancel()
            self.state["plan_version"] = await self._activity("persist_view_correction", {
                **common, **context, "proposed": proposed, "actor": choice["actor"]}, timeout=180, retry=WRITE_RETRY)
            self._bundle = proposed
