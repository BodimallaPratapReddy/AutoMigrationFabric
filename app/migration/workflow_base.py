"""Shared durable migration orchestration.

All external work is dispatched by activity name. The only values in workflow
history are IDs, discovered metadata, reviewed plans, and job status snapshots.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from .mapping_types import normalize_fabric_type


READ_RETRY = RetryPolicy(initial_interval=timedelta(seconds=2),
                         maximum_interval=timedelta(seconds=30), maximum_attempts=5)
WRITE_RETRY = RetryPolicy(initial_interval=timedelta(seconds=2),
                          maximum_interval=timedelta(seconds=10), maximum_attempts=3)
INITIAL_PLAN_RETRY = RetryPolicy(initial_interval=timedelta(seconds=3),
                                 maximum_interval=timedelta(seconds=20), maximum_attempts=6)
NO_RETRY = RetryPolicy(maximum_attempts=1)


class MigrationWorkflowBase:
    def __init__(self) -> None:
        self.state: dict = {}
        self._commands: list[dict] = []
        self._cancelled = False

    @workflow.query
    def get_state(self) -> dict:
        return self.state

    @workflow.signal
    def command(self, payload: dict) -> None:
        if payload.get("action") == "cancel":
            self._cancelled = True
        else:
            self._commands.append(payload)

    async def _activity(self, name: str, payload: dict, *, timeout: int = 30,
                        retry: RetryPolicy = NO_RETRY) -> dict | str | None:
        return await workflow.execute_activity(name, payload,
            start_to_close_timeout=timedelta(seconds=timeout), retry_policy=retry)

    def _phase(self, phase: str, *, message: str | None = None, waiting: bool = False) -> None:
        self.state["phase"] = phase
        self.state["waiting_for_user"] = waiting
        self.state["current_message"] = message

    async def _gate(self, phase: str, actions: tuple[str, ...]) -> dict:
        self._phase(phase, waiting=True)
        while True:
            await workflow.wait_condition(lambda: self._cancelled or bool(self._commands))
            if self._cancelled:
                raise MigrationCancelled()
            command = self._commands.pop(0)
            if command.get("action") in actions:
                self.state["waiting_for_user"] = False
                return command

    async def _check_cancel(self) -> None:
        if self._cancelled:
            raise MigrationCancelled()

    def _apply_oracle_column_mappings(self, discovered: dict, choices: list[dict], actor: str) -> None:
        planned = discovered["table_plan"]["columns"]
        expected = {item["column_name"] for item in planned}
        names = [item["name"] for item in choices]
        if len(names) != len(expected) or len(set(names)) != len(names) or set(names) != expected:
            raise ValueError("Every discovered Oracle column must be mapped exactly once")
        selected = {item["name"]: normalize_fabric_type(item["fabric_type"]) for item in choices}
        for column in planned:
            column["fabric_data_type"] = selected[column["column_name"]]
        for column in self.state["review"]["columns"]:
            column.setdefault("suggested_fabric_type", column["fabric_type"])
            column.setdefault("suggested_warning", column.get("warning"))
            column["fabric_type"] = selected[column["name"]]
            column["mapping_changed"] = column["fabric_type"] != column["suggested_fabric_type"]
            column["warning"] = (f"User selected {column['fabric_type']}; verify source values fit"
                                 if column["mapping_changed"] else column["suggested_warning"])
        self.state["review"]["mapping_approved_by"] = actor

    async def _watch_job(self, input: dict, *, kind: str, job_id: str) -> dict:
        status_activity = "get_provisioning_status" if kind == "provisioning" else "get_replication_status"
        cancel_activity = "cancel_provisioning" if kind == "provisioning" else "cancel_replication"
        cancellation_sent = False
        unknown_count = 0
        while True:
            if self._cancelled and not cancellation_sent:
                # POST ambiguity is handled by status checks; never retry this POST.
                try:
                    await self._activity(cancel_activity, {**input, "job_id": job_id}, timeout=60)
                except Exception:
                    pass
                cancellation_sent = True
            job = await self._activity(status_activity, {**input, "job_id": job_id}, retry=READ_RETRY)
            status = job["normalized_status"]
            self.state[f"{kind}_status"] = status
            unknown_count = unknown_count + 1 if status == "UNKNOWN" else 0
            if unknown_count >= 12:
                raise RuntimeError(f"Fabric {kind} job status stayed unknown")
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                if self._cancelled:
                    raise MigrationCancelled()
                if status != "SUCCEEDED":
                    raise RuntimeError(f"Fabric {kind} job ended with {status}")
                return job
            delay = job.get("retry_after_seconds") or 10
            await workflow.sleep(max(2, min(delay, 60)))

    async def _execute_fabric(self, input: dict, *, replicate: bool = False,
                              handoff_to_scheduler: bool = False) -> None:
        guid = self.state["plan_guid"]
        common = {"plan_guid": guid, "workflow_id": self.state["workflow_id"],
                  "run_id": workflow.info().run_id, "version": self.state["plan_version"]}
        self._phase("VALIDATING_FABRIC")
        await self._activity("validate_fabric", input, retry=READ_RETRY)
        await self._check_cancel()
        await self._activity("transition_plan", {**common, "expected": "READY_TO_PROVISION",
                                                  "target": "PROVISIONING"})
        self._phase("PROVISIONING")
        batch = await self._activity("start_batch", {**common, "batch_type": "PROVISIONING"},
                                     retry=WRITE_RETRY)
        self.state["batch_run_id"] = batch
        submission = await self._activity("submit_provisioning", {**input, "plan_guid": guid}, timeout=60)
        job_id = submission["job_instance_id"]
        self.state["fabric_job_instance_id"] = job_id
        await self._activity("record_batch_job", {"batch_run_id": batch, "job_id": job_id},
                             retry=WRITE_RETRY)
        self._phase("WAITING_FOR_PROVISIONING")
        job = await self._watch_job(input, kind="provisioning", job_id=job_id)
        result = await self._activity("parse_provisioning", {"job": job, "plan_guid": guid})
        self.state["provisioning_report"] = result
        await self._activity("finish_provisioning", {**common, "batch_run_id": batch,
                                                      "job_id": job_id, "result": result},
                             retry=WRITE_RETRY)
        self.state["batch_run_id"] = None
        await self._check_cancel()
        self._phase("PREPARING_SCHEDULED_LOAD" if handoff_to_scheduler else "READY_FOR_REPLICATION")
        if not replicate:
            if handoff_to_scheduler:
                await self._activity("activate_replication_config", {"plan_guid": guid},
                                     retry=WRITE_RETRY)
            self.state["replication_status"] = "SKIPPED"
            self._phase("VALIDATING_RESULT")
            return
        # Replication config was persisted disabled. Enable it only after provisioning.
        await self._activity("enable_replication", {"plan_guid": guid})
        self._phase("REPLICATING")
        batch = await self._activity("start_batch", {**common, "batch_type": "REPLICATION"},
                                     retry=WRITE_RETRY)
        self.state["batch_run_id"] = batch
        submission = await self._activity("submit_replication", input, timeout=60)
        job_id = submission["job_instance_id"]
        self.state["pipeline_job_instance_id"] = job_id
        await self._activity("record_pipeline_job", {"batch_run_id": batch,
                                                     "plan_guid": guid, "job_id": job_id},
                             retry=WRITE_RETRY)
        self._phase("WAITING_FOR_REPLICATION")
        await self._watch_job(input, kind="replication", job_id=job_id)
        await self._activity("finish_replication", {"batch_run_id": batch, "plan_guid": guid},
                             retry=WRITE_RETRY)
        self.state["batch_run_id"] = None
        self._phase("VALIDATING_RESULT")

    async def _execute_target_change(self, input: dict) -> None:
        guid = self.state["plan_guid"]
        self._phase("VALIDATING_TARGET_CHANGE")
        approved = await self._activity("validate_target_change_run", {
            "plan_guid": guid, "fabric_workspace_id": input["fabric_workspace_id"],
            "fabric_lakehouse_id": input["fabric_lakehouse_id"]}, retry=READ_RETRY)
        self.state["review"]["target_review"]["decision"] = approved["decision"]
        await self._activity("validate_fabric", input, retry=READ_RETRY)
        await self._check_cancel()
        self._phase("SUBMITTING_TARGET_CHANGE")
        submission = await self._activity("submit_provisioning", {**input, "plan_guid": guid}, timeout=60)
        job_id = submission["job_instance_id"]
        self.state["fabric_job_instance_id"] = job_id
        self._phase("WAITING_FOR_TARGET_CHANGE_JOB")
        job = await self._watch_job(input, kind="provisioning", job_id=job_id)
        result = await self._activity("parse_provisioning", {"job": job, "plan_guid": guid})
        self.state["provisioning_report"] = result
        await self._activity("finish_target_change", {"plan_guid": guid,
            "version": approved["plan_version"], "result": result}, retry=WRITE_RETRY)
        self.state["provisioning_status"] = "SUCCEEDED"
        self.state["replication_status"] = "SKIPPED"

    async def _run(self, input: dict, discovery_activity: str) -> dict:
        info = workflow.info()
        self.state = {"workflow_id": info.workflow_id, "migration_approach": input["migration_approach"],
                      "source_object_name": input["source_object_name"], "phase": "CREATED",
                      "status": "RUNNING", "plan_guid": None, "plan_version": 1,
                      "analysis_version": 1, "waiting_for_user": False, "review": None,
                      "last_error": None, "fabric_job_instance_id": None,
                      "pipeline_job_instance_id": None, "batch_run_id": None,
                      "provisioning_status": None, "replication_status": None,
                      "provisioning_report": None}
        try:
            guid = await self._activity("create_migration_plan", {**input,
                "workflow_id": info.workflow_id}, timeout=45, retry=INITIAL_PLAN_RETRY)
            self.state["plan_guid"] = guid
            await self._activity("set_temporal_ids", {"plan_guid": guid,
                                                       "workflow_id": info.workflow_id,
                                                       "run_id": info.run_id})
            await self._check_cancel()
            self._phase("VALIDATING_SOURCE")
            fabric = await self._activity("validate_fabric", input, retry=READ_RETRY)
            self._phase("DISCOVERING_SOURCE")
            discovered = await self._activity(discovery_activity,
                {"input": input, "lakehouse_name": fabric["lakehouse_name"]},
                timeout=300 if discovery_activity == "discover_rebuild" else 120,
                retry=READ_RETRY)
            if discovered.get("failure_reason"):
                self.state["last_error"] = discovered["failure_reason"]
                await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                    "expected": "DRAFT", "target": "FAILED"})
                self._phase("FAILED", message=discovered["failure_reason"])
                self.state["status"] = "FAILED"
                return self.state
            self.state["review"] = discovered["review"]
            self._phase("BUILDING_PLAN")
            await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                "expected": "DRAFT", "target": "WAITING_PLAN_APPROVAL"})
            if input["migration_approach"] == "ORACLE_TABLE":
                while True:
                    mapping_choice = await self._gate("WAITING_FOR_COLUMN_MAPPING_APPROVAL",
                                                      ("approve_columns", "reject_columns"))
                    if mapping_choice["action"] == "reject_columns":
                        choice = {"action": "reject_plan"}
                        break
                    self._apply_oracle_column_mappings(discovered, mapping_choice["columns"],
                                                       mapping_choice["actor"])
                    choice = await self._gate("WAITING_FOR_WATERMARK_APPROVAL",
                                              ("approve_watermark", "reject_watermark", "edit_columns"))
                    if choice["action"] == "edit_columns":
                        continue
                    if choice["action"] == "reject_watermark":
                        choice = {"action": "reject_plan"}
                    break
            else:
                choice = await self._gate("WAITING_FOR_PLAN_APPROVAL",
                                          ("approve_plan", "reject_plan"))
            if choice["action"] == "reject_plan":
                await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                    "expected": "WAITING_PLAN_APPROVAL", "target": "REJECTED"})
                self._phase("REJECTED")
                self.state["status"] = "REJECTED"
                return self.state
            if input["migration_approach"] == "ORACLE_TABLE" and choice.get("watermark_column"):
                selected = next((item for item in discovered["review"]["watermark_candidates"]
                                 if item["column_name"] == choice["watermark_column"]), None)
                if selected is None:
                    raise ValueError("Selected watermark was not discovered")
                replication = discovered["table_plan"]["replication"]
                if not replication.get("primary_key_columns"):
                    raise ValueError("Watermark upserts require a primary key")
                indexes = sorted(selected["index_details"],
                                 key=lambda item: (item["column_position"] != 1,
                                                   item["index_name"]))
                index_name = indexes[0]["index_name"] if indexes else None
                replication.update({"incremental_method": "WATERMARK",
                                    "watermark_column": selected["column_name"],
                                    "watermark_column_data_type": selected["data_type"],
                                    "watermark_index_name": index_name,
                                    "write_strategy": "UPSERT"})
                self.state["review"]["selected_watermark"] = selected["column_name"]
                self.state["review"]["selected_watermark_index"] = index_name
                self.state["review"]["load_method"] = "WATERMARK"
                self.state["review"]["write_strategy"] = "UPSERT"
            if input["migration_approach"] == "ORACLE_TABLE":
                self._phase("CHECKING_EXISTING_TARGET")
                target_review = await self._activity("review_existing_target",
                    {"plan_guid": guid, "table_plan": discovered["table_plan"]},
                    retry=READ_RETRY)
                definitions = target_review["definitions"]
                if definitions:
                    self.state["review"]["target_review"] = target_review
                    exact = next((item for item in definitions
                                  if item["comparison"]["same"]), None)
                    if exact:
                        message = ("This target already has the same saved structure in "
                                   f"plan {exact['plan_guid']}. No second configuration was saved.")
                        await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                            "expected": "WAITING_PLAN_APPROVAL", "target": "REJECTED"})
                        self._phase("DUPLICATE_TARGET", message=message)
                        self.state["status"] = "REJECTED"
                        return self.state
                    existing = next((item for item in definitions
                                     if item["provisioning_status"] == "PROVISIONED"), definitions[0])
                    target_review["selected_existing_guid"] = existing["source_table_guid"]
                    target_review["is_provisioned_record"] = (
                        existing["provisioning_status"] == "PROVISIONED")
                    self.state["review"]["target_review"] = target_review
                    requested = await self._gate("WAITING_FOR_TARGET_CHANGE_APPROVAL",
                                                 ("approve_target_change", "reject_target_change"))
                    if requested["action"] == "reject_target_change":
                        await self._activity("transition_plan", {"plan_guid": guid, "version": 1,
                            "expected": "WAITING_PLAN_APPROVAL", "target": "REJECTED"})
                        self._phase("REJECTED")
                        self.state["status"] = "REJECTED"
                        return self.state
                    decision = requested["decision"]
                    allowed = ({"ALTER_FUTURE", "ALTER_BACKFILL", "REPLACE_FULL"}
                               if target_review["is_provisioned_record"] else {"REVISE_PLANNED"})
                    if decision not in allowed:
                        raise ValueError("Target change decision is incompatible with existing target")
                    await self._activity("record_target_change_request", {
                        "plan_guid": guid, "version": 1, "actor": requested["actor"],
                        "decision": decision, "target_guid": existing["source_table_guid"],
                        "review": target_review, "proposed_plan": discovered["table_plan"]})
                    self.state["review"]["target_review"]["decision"] = decision
                    if input.get("provisioning_notebook_id"):
                        await self._execute_target_change(input)
                        self._phase("COMPLETED", message=f"{decision} applied by the Fabric notebook")
                        self.state["status"] = "COMPLETED"
                    else:
                        self._phase("TARGET_CHANGE_RECORDED", message=(
                            "Change choice saved. Select a provisioning notebook and run the approved change."))
                        self.state["status"] = "CHANGE_REQUESTED"
                    return self.state
            await self._activity("approve_runtime_plan", {"plan_guid": guid, "version": 1,
                                                            "approved_by": choice["actor"]})
            self._phase("PERSISTING_RUNTIME_CONFIG")
            await self._activity("persist_runtime_plan", {"plan_guid": guid,
                "plan_version": 1, "tables": [discovered["table_plan"]], "views": []})
            if input.get("provisioning_notebook_id"):
                # Keep the old command sequence for histories that already submitted a pipeline.
                if workflow.patched("structure-only-migration-v1"):
                    await self._execute_fabric(input, handoff_to_scheduler=True)
                else:
                    await self._execute_fabric(input, replicate=bool(input.get("replication_pipeline_id")))
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
            # Keep diagnostics free of adapter payloads, connection details and tokens.
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
                       if phase in {"PROVISIONING", "REPLICATING", "SUBMITTING_TARGET_CHANGE"} and
                       not (self.state["fabric_job_instance_id"] if phase == "PROVISIONING"
                            else self.state["pipeline_job_instance_id"] if phase == "REPLICATING"
                            else self.state["fabric_job_instance_id"]) else None)
            self._phase("FAILED", message=message)
        return self.state


class MigrationCancelled(Exception):
    pass


