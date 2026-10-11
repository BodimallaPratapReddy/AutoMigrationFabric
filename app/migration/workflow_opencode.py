"""New workflow type: old Oracle histories retain their original command sequence."""

from datetime import timedelta

from temporalio import workflow

from .agent_forms import validate_reply
from .workflow_base import MigrationWorkflowBase, READ_RETRY


@workflow.defn
class OpenCodeOracleMigrationWorkflow(MigrationWorkflowBase):
    @workflow.run
    async def run(self, input: dict) -> dict:
        return await self._run(input, "opencode")

    async def _discover(self, input: dict, lakehouse_name: str, discovery_activity: str) -> dict:
        session = await self._activity("opencode_create", {
            "workflow_id": self.state["workflow_id"], "input": input}, timeout=60, retry=READ_RETRY)
        self.state["agent_session_id"] = session["session_id"]
        common = {**session, "input": input, "lakehouse_name": lakehouse_name}
        revision = input.get("agent_revision_start", 0)
        self.state["agent_prompt_revision"] = revision
        try:
            self._phase("SUBMITTING_AGENT_PROMPT", message="Submitting the discovery request to OpenCode")
            await self._activity("opencode_prompt", {**common, **({"revision": revision} if revision else {})}, timeout=90)
            for _ in range(720):
                await self._check_cancel()
                self._phase("WAITING_FOR_AGENT", message="OpenCode is discovering the source and planning")
                snapshot = await self._activity("opencode_poll", session, timeout=90, retry=READ_RETRY)
                status = snapshot["status"]
                self.state["agent_progress"] = snapshot.get("progress", [])
                if status in {"FORM", "PERMISSION"}:
                    request = {**snapshot["request"], "kind": status}
                    self.state["agent_request"] = request
                    while True:
                        self._phase("WAITING_FOR_AGENT_INPUT", waiting=True)
                        try:
                            await workflow.wait_condition(lambda: self._cancelled or bool(self._commands),
                                                          timeout=timedelta(seconds=30))
                        except TimeoutError:
                            # Also notice answers/cancellations submitted directly in OpenCode.
                            latest = await self._activity("opencode_poll", session, timeout=90, retry=READ_RETRY)
                            if latest.get("request", {}).get("id") != request["id"]:
                                reply = None
                                break
                            continue
                        await self._check_cancel()
                        reply = self._commands.pop(0)
                        if reply.get("action") != "agent_reply":
                            continue
                        try:
                            validate_reply(request, reply)
                        except (ValueError, TypeError) as exc:
                            self.state["current_message"] = str(exc)
                            continue
                        break
                    if reply is not None:
                        await self._activity("opencode_reply", {**session, **reply, "kind": status}, timeout=30)
                    self.state["agent_request"] = None
                elif status == "COMPLETED":
                    self.state["agent_report"] = snapshot["text"]
                    result = await self._activity("opencode_validate", {**common, "text": snapshot["text"]},
                        timeout=180 if input["migration_approach"] == "ORACLE_VIEW" else 30)
                    if "validation_error" not in result:
                        self.state["agent_validation_error"] = None
                        return result
                    self.state["agent_validation_error"] = result["validation_error"]
                    choice = await self._gate("WAITING_FOR_AGENT_REVISION", ("agent_feedback",))
                    if not choice.get("actor") or not choice.get("message"):
                        raise ValueError("Agent revision requires feedback and reviewer")
                    revision += 1
                    self.state["agent_prompt_revision"] = revision
                    self.state["agent_report"] = None
                    self.state["agent_validation_error"] = None
                    self._phase("SUBMITTING_AGENT_PROMPT", message="Submitting the plan revision to OpenCode")
                    await self._activity("opencode_prompt", {**common, "revision": revision,
                        "feedback": choice["message"]}, timeout=90)
                elif status == "FAILED":
                    return {"failure_reason": snapshot["error"]}
                try:
                    await workflow.wait_condition(lambda: self._cancelled, timeout=timedelta(seconds=5))
                except TimeoutError:
                    pass
            raise RuntimeError("OpenCode discovery exceeded polling budget")
        except BaseException:
            # Best effort; failure to interrupt must not mask the original failure/cancellation.
            try:
                await self._activity("opencode_interrupt", session, timeout=30)
            except Exception:
                pass
            raise
