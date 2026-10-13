"""A new workflow type for agent-authored Oracle view bundles."""
from temporalio import workflow
from .workflow_opencode import OpenCodeOracleMigrationWorkflow
from .workflow_base import MigrationCancelled, READ_RETRY, WRITE_RETRY, INITIAL_PLAN_RETRY


@workflow.defn
class OpenCodeOracleViewMigrationWorkflow(OpenCodeOracleMigrationWorkflow):
    @workflow.run
    async def run(self, input: dict) -> dict:
        self._input = input
        info = workflow.info()
        self.state = {"workflow_id": info.workflow_id, "migration_approach": input['migration_approach'],
            "source_object_name": input["source_object_name"], "phase": "CREATED", "status": "RUNNING",
            "plan_guid": None, "plan_version": 1, "analysis_version": 1, "waiting_for_user": False}
        try:
            guid = await self._activity("create_migration_plan", {**input, "workflow_id": info.workflow_id},
                timeout=45, retry=INITIAL_PLAN_RETRY)
            self.state["plan_guid"] = guid
            await self._activity("set_temporal_ids", {"plan_guid": guid, "workflow_id": info.workflow_id,
                "run_id": info.run_id})
            fabric = await self._activity("validate_fabric", input, retry=READ_RETRY)
            self._phase("DISCOVERING_VIEW_DEPENDENCIES")
            graph = None if getattr(self,'_skip_source_graph',False) else await self._activity(getattr(self,'_view_graph_activity','oracle_view_graph'), {"input": input}, timeout=300, retry=READ_RETRY)
            discovery_input = {**input, "source_view_graph": graph, "plan_guid": guid}
            bundle = await self._discover(discovery_input, fabric["lakehouse_name"], "opencode")
            if bundle.get('analysis_rejected'):
                self.state['status']='REJECTED'
                self._phase('REJECTED')
                return self.state
            if bundle.get("failure_reason"):
                if input.get('sap_mcp_discovery') and workflow.patched('sap-agent-failure-diagnostic-v1'):
                    from temporalio.exceptions import ApplicationError
                    raise ApplicationError(bundle['failure_reason'][:4000], type='SAPAgentStopped', non_retryable=True)
                raise ValueError(bundle["failure_reason"])
            self.state["review"] = bundle["review"]
            await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                "expected": getattr(self, '_initial_plan_status', 'DRAFT'), "target": "WAITING_PLAN_APPROVAL"})
            choice = await self._review_bundle(bundle)
            if choice["action"] == "reject_plan":
                await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                    "expected": "WAITING_PLAN_APPROVAL", "target": "REJECTED"})
                self.state["status"] = "REJECTED"
                self._phase("REJECTED")
                return self.state
            await self._check_cancel()
            self._phase("CHECKING_EXISTING_TARGETS")
            # Recheck immediately before saving: never overwrite a registered target implicitly.
            await self._activity("check_oracle_view_targets", {"bundle": bundle, "plan_guid": guid},
                retry=READ_RETRY)
            await self._activity("approve_runtime_plan", {"plan_guid": guid, "version": 1,
                "approved_by": choice["actor"]})
            self._phase("PERSISTING_RUNTIME_CONFIG")
            await self._activity("persist_runtime_plan", {"plan_guid": guid, "plan_version": 1,
                "tables": bundle["tables"], "views": bundle["views"]}, timeout=180, retry=WRITE_RETRY)
            if input.get("provisioning_notebook_id"):
                await self._execute_fabric(input, handoff_to_scheduler=True)
                self.state["status"] = "COMPLETED"
                self._phase("COMPLETED")
            else:
                self.state.update(status="PLANNED", provisioning_status="SKIPPED", replication_status="SKIPPED")
                self._phase("READY_TO_PROVISION", message="Approved view and base-table plan saved; no notebook selected")
        except Exception as exc:
            cancelled = isinstance(exc, MigrationCancelled)
            phase = self.state["phase"]
            cause = getattr(exc, "cause", None)
            # Only expose adapter-authored diagnostics; arbitrary downstream errors may
            # contain connection strings or source data.
            detail = (cause.message if getattr(cause, "type", None) == "OpenCodeTransportError"
                      else type(exc).__name__)
            if getattr(exc, 'type', None) == 'SAPAgentStopped':
                detail = exc.message
            if getattr(cause, "type", None) == "FabricProvisioningLogicalFailure":
                report = cause.details[0] if cause.details else {}
                self.state["provisioning_report"] = report
                self.state["provisioning_status"] = "FAILED"
                detail = (report.get("message") or cause.message)[:2000]
            self.state.update(status="CANCELLED" if cancelled else "FAILED",
                last_error=None if cancelled else f"{phase}: {detail}")
            if self.state.get("plan_guid"):
                await self._activity("fail_migration", {"plan_guid": self.state["plan_guid"], "phase": phase,
                    "message": "Cancelled by user" if cancelled else self.state["last_error"],
                    "cancelled": cancelled, "batch_run_id": self.state.get("batch_run_id")})
            self._phase(self.state["status"])
        return self.state

    async def _review_bundle(self, bundle: dict) -> dict:
        return await self._gate("WAITING_FOR_PLAN_APPROVAL", ("approve_plan", "reject_plan"))
