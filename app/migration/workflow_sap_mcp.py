"""MCP-only SAP source discovery. Legacy types remain registered for replay."""
import asyncio
from temporalio import workflow
from .workflow_base import MigrationCancelled
from .workflow_sap_opencode import OpenCodeSAPTableMigrationWorkflow
from .workflow_opencode import OpenCodeOracleMigrationWorkflow
from .workflow_view_replication import OpenCodeOracleViewReplicationWorkflow


class SAPMCPKeyReview:
    async def _check_oracle_key(self,discovered,columns):
        review=self.state['review'];replication=discovered['table_plan']['replication']
        replication.pop('key_validation',None)
        revision=self.state.get('agent_prompt_revision',0)+1
        self.state['agent_prompt_revision']=revision
        self._phase('VALIDATING_PRIMARY_KEY',message='OpenCode is checking source key evidence through SAP MCP')
        metadata=review.get('source_metadata')
        if metadata is None:
            from .sap_bundle_storage import graph_metadata
            metadata=graph_metadata(self._bundle,review['source'])
        task=asyncio.create_task(self._activity('opencode_sap_key_check',dict(input=self._input,columns=columns,
            metadata=metadata,plan_columns=discovered['table_plan']['columns'],
            session_id=self.state['agent_session_id'],workflow_id=self.state['workflow_id'],revision=revision),timeout=300))
        cancelled=asyncio.create_task(workflow.wait_condition(lambda:self._cancelled))
        try:
            done,_=await workflow.wait([task,cancelled],return_when=asyncio.FIRST_COMPLETED)
            if cancelled in done:
                task.cancel()
                await self._activity('opencode_interrupt',{'session_id':self.state['agent_session_id']},timeout=30)
                raise MigrationCancelled()
            try:result=await task
            except Exception:result=dict(valid=False,status='UNVERIFIED',scope='FULL_TABLE',columns=columns,
                error='Source MCP key validation did not complete; revise or choose no key/full refresh')
        finally:cancelled.cancel()
        review['key_validation']=result
        if (result.get('valid') is not True or result.get('columns')!=columns or
                result.get('status')!='CHECKED' or result.get('scope')!='FULL_TABLE'):
            review['primary_key_error']=result.get('error') or 'MCP source key evidence is insufficient'
            return False
        replication['key_validation']=result;review.pop('primary_key_error',None)
        return True


@workflow.defn
class OpenCodeMCPSAPTableMigrationWorkflow(SAPMCPKeyReview,OpenCodeSAPTableMigrationWorkflow):
    @workflow.run
    async def run(self,input:dict)->dict:
        self._sap_agent=True
        return await self._run({**input,'sap_mcp_discovery':True},'opencode')

    async def _discover(self,input,lakehouse_name,discovery_activity):
        return await OpenCodeOracleMigrationWorkflow._discover(self,input,lakehouse_name,discovery_activity)


@workflow.defn
class OpenCodeMCPSAPViewMigrationWorkflow(SAPMCPKeyReview,OpenCodeOracleViewReplicationWorkflow):
    @workflow.run
    async def run(self,input:dict)->dict:
        self._skip_source_graph=True
        self._source_table_approach='SAP_TABLE'
        return await super().run({**input,'sap_mcp_discovery':True})
