"""SAP DataSource rebuild workflow with two review gates."""

from temporalio import workflow

from .workflow_base import MigrationCancelled, MigrationWorkflowBase, READ_RETRY, WRITE_RETRY


@workflow.defn
class SAPDataSourceRebuildWorkflow(MigrationWorkflowBase):
    @workflow.run
    async def run(self, input: dict) -> dict:
        info = workflow.info()
        self.state = {"workflow_id": info.workflow_id, "migration_approach": "SAP_REBUILD",
                      "source_object_name": input["source_object_name"], "phase": "CREATED",
                      "status": "RUNNING", "plan_guid": None, "plan_version": 1,
                      "analysis_version": 1, "waiting_for_user": False, "review": None,
                      "last_error": None, "fabric_job_instance_id": None,
                      "pipeline_job_instance_id": None, "batch_run_id": None,
                      "provisioning_status": None, "replication_status": None}
        try:
            guid = await self._activity("create_migration_plan", {**input,
                "workflow_id": info.workflow_id}, retry=WRITE_RETRY)
            self.state["plan_guid"] = guid
            await self._activity("set_temporal_ids", {"plan_guid": guid,
                "workflow_id": info.workflow_id, "run_id": info.run_id})
            self._phase("VALIDATING_FABRIC")
            fabric = await self._activity("validate_fabric", input, retry=READ_RETRY)
            lakehouse_name = fabric["lakehouse_name"]
            self._phase("ANALYZING_EXTRACTOR")
            analysis = await self._activity("analyze_rebuild", {"input": input,
                "plan_guid": guid, "version": 1}, timeout=600)
            if analysis.get("failure_reason"):
                self.state["last_error"] = analysis["failure_reason"]
                await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                    "expected": "DRAFT", "target": "FAILED"})
                self._phase("FAILED", message=analysis["failure_reason"])
                self.state["status"] = "FAILED"
                return self.state
            await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                "expected": "DRAFT", "target": "WAITING_ANALYSIS_APPROVAL"})
            while True:
                self.state["review"] = {"kind": "analysis", "version": self.state["analysis_version"],
                    "route": analysis["route"], "analysis": analysis["analysis"],
                    "verified_objects": [{"name": x["name"], "type": x["object_type"]}
                                         for x in analysis["verified"]["verified"]],
                    "warnings": analysis["warnings"]}
                choice = await self._gate("WAITING_FOR_ANALYSIS_APPROVAL",
                                          ("approve_analysis", "reject_analysis", "analysis_feedback"))
                if choice["action"] == "reject_analysis":
                    await self._activity("transition_plan", {"plan_guid": guid,
                        "version": self.state["plan_version"],
                        "expected": "WAITING_ANALYSIS_APPROVAL", "target": "REJECTED"})
                    self._phase("REJECTED")
                    self.state["status"] = "REJECTED"
                    return self.state
                if choice["action"] == "approve_analysis":
                    await self._activity("approve_analysis", {"analysis_guid": analysis["analysis_guid"],
                        "approved_by": choice["actor"], "version": self.state["analysis_version"]})
                    await self._activity("transition_plan", {"plan_guid": guid,
                        "version": self.state["plan_version"],
                        "expected": "WAITING_ANALYSIS_APPROVAL", "target": "ANALYSIS_APPROVED"})
                    break
                self._phase("ANALYZING_EXTRACTOR")
                version = await self._activity("revise_analysis_version", {
                    "analysis_guid": analysis["analysis_guid"], "plan_guid": guid,
                    "version": self.state["analysis_version"]})
                self.state["analysis_version"] = version
                analysis = await self._activity("analyze_rebuild", {"input": input,
                    "plan_guid": guid, "version": version,
                    "previous_analysis": analysis["analysis"],
                    "feedback": choice["message"]}, timeout=600)
                if analysis.get("failure_reason"):
                    self.state["last_error"] = analysis["failure_reason"]
                    await self._activity("transition_plan", {"plan_guid": guid,
                        "version": self.state["plan_version"],
                        "expected": "DRAFT", "target": "FAILED"})
                    self._phase("FAILED", message=analysis["failure_reason"])
                    self.state["status"] = "FAILED"
                    return self.state
                await self._activity("transition_plan", {"plan_guid": guid,
                    "version": self.state["plan_version"],
                    "expected": "DRAFT", "target": "WAITING_ANALYSIS_APPROVAL"})
            self._phase("GENERATING_FABRIC_PLAN")
            proposal = await self._activity("generate_rebuild_plan", {"input": input,
                "plan_guid": guid, "version": self.state["plan_version"],
                "analysis": analysis["analysis"], "verified": analysis["verified"],
                "lakehouse_name": lakehouse_name}, timeout=300)
            await self._activity("transition_plan", {"plan_guid": guid,
                "version": self.state["plan_version"],
                "expected": "ANALYSIS_APPROVED", "target": "WAITING_PLAN_APPROVAL"})
            while True:
                self.state["review"] = {"kind": "fabric_plan",
                    "version": self.state["plan_version"], "proposal": proposal}
                choice = await self._gate("WAITING_FOR_FABRIC_PLAN_APPROVAL",
                                          ("approve_plan", "reject_plan", "plan_feedback"))
                if choice["action"] == "reject_plan":
                    await self._activity("transition_plan", {"plan_guid": guid,
                        "version": self.state["plan_version"],
                        "expected": "WAITING_PLAN_APPROVAL", "target": "REJECTED"})
                    self._phase("REJECTED")
                    self.state["status"] = "REJECTED"
                    return self.state
                if choice["action"] == "approve_plan":
                    await self._activity("approve_runtime_plan", {"plan_guid": guid,
                        "version": self.state["plan_version"], "approved_by": choice["actor"]})
                    break
                self._phase("GENERATING_FABRIC_PLAN")
                version = await self._activity("revise_plan_version", {"plan_guid": guid,
                    "version": self.state["plan_version"]})
                self.state["plan_version"] = version
                proposal = await self._activity("generate_rebuild_plan", {"input": input,
                    "plan_guid": guid, "version": version, "analysis": analysis["analysis"],
                    "verified": analysis["verified"], "lakehouse_name": lakehouse_name,
                    "previous_plan": proposal, "feedback": choice["message"]}, timeout=300)
                await self._activity("transition_plan", {"plan_guid": guid, "version": version,
                    "expected": "DRAFT", "target": "WAITING_PLAN_APPROVAL"})
            self._phase("PERSISTING_RUNTIME_CONFIG")
            persisted = await self._activity("persist_verified_rebuild_plan", {"input": input,
                "plan_guid": guid, "analysis_guid": analysis["analysis_guid"],
                "proposal": proposal, "version": self.state["plan_version"],
                "lakehouse_name": lakehouse_name}, timeout=60)
            if input.get("provisioning_notebook_id"):
                # A patch marker preserves replay of earlier pipeline-submitting histories.
                if workflow.patched("structure-only-rebuild-v1"):
                    await self._execute_fabric(input, handoff_to_scheduler=True)
                else:
                    await self._execute_fabric(input, replicate=bool(
                        persisted["source_table_guids"] and input.get("replication_pipeline_id")))
                self._phase("COMPLETED")
                self.state["status"] = "COMPLETED"
            else:
                self.state["provisioning_status"] = "SKIPPED"
                self.state["replication_status"] = "SKIPPED"
                self._phase("READY_TO_PROVISION", message="Approved plan saved; no provisioning notebook selected")
                self.state["status"] = "PLANNED"
        except MigrationCancelled:
            phase = self.state["phase"]
            self._phase("CANCELLED")
            self.state["status"] = "CANCELLED"
            if self.state.get("plan_guid"):
                await self._activity("fail_migration", {"plan_guid": self.state["plan_guid"],
                    "phase": phase, "batch_run_id": self.state.get("batch_run_id"),
                    "message": "Cancelled by user", "cancelled": True})
        except Exception as exc:
            phase = self.state["phase"]
            self.state["last_error"] = f"{phase}: {type(exc).__name__}"
            self.state["status"] = "FAILED"
            if self.state.get("plan_guid"):
                try:
                    await self._activity("fail_migration", {"plan_guid": self.state["plan_guid"],
                        "phase": phase, "batch_run_id": self.state.get("batch_run_id"),
                        "message": self.state["last_error"]})
                except Exception:
                    self.state["last_error"] += "; Config DB failure update failed"
            message = ("Fabric submission may have been accepted without a returned job ID; "
                       "reconcile the Fabric job before retrying."
                       if phase in {"PROVISIONING", "REPLICATING"} and
                       not (self.state["fabric_job_instance_id"] if phase == "PROVISIONING"
                            else self.state["pipeline_job_instance_id"]) else None)
            self._phase("FAILED", message=message)
        return self.state
