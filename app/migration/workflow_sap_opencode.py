from temporalio import workflow
from .workflow_opencode import OpenCodeOracleMigrationWorkflow
from .workflow_base import READ_RETRY
from thirdparty.sap.extraction import build_extract_sql


@workflow.defn
class OpenCodeSAPTableMigrationWorkflow(OpenCodeOracleMigrationWorkflow):
    @workflow.run
    async def run(self, input: dict) -> dict:
        self._sap_agent=True
        self._source_key_activity='validate_sap_key'
        return await self._run(input,'opencode')

    async def _discover(self, input: dict, lakehouse_name: str, discovery_activity: str) -> dict:
        metadata=await self._activity('sap_agent_metadata',{'input':input},timeout=180,retry=READ_RETRY)
        return await super()._discover({**input,'source_sap_metadata':metadata},lakehouse_name,discovery_activity)

    async def _watermark_gate(self, discovered: dict, *, review_key: bool) -> dict:
        choice = await super()._watermark_gate(discovered, review_key=review_key)
        # Upgrade already-discovered, still-unapproved drafts without rewriting
        # previously persisted histories or changing approved source expressions.
        # This marker is reached after a new approval, so an existing execution
        # waiting at the watermark gate can safely adopt the corrected generator.
        if choice['action'] == 'approve_watermark' and workflow.patched('sap-native-extraction-after-watermark-v1'):
            plan = discovered['table_plan']
            query = build_extract_sql(plan['table'], plan['columns'])
            plan['table']['extract_sql'] = query
            self.state['review']['extract_sql'] = query
            self.state['review']['extract_sql_dialect'] = 'SAP_NATIVE_SQL'
        return choice
