"""New DataSource workflow types preserve existing built-in workflow histories."""
from temporalio import workflow
from .workflow_oracle_view import OpenCodeOracleViewMigrationWorkflow
from .workflow_view_replication import OpenCodeOracleViewReplicationWorkflow
from .workflow_opencode import OpenCodeOracleMigrationWorkflow
from .workflow_base import READ_RETRY
from .workflow_sap_mcp import SAPMCPKeyReview


class SAPDataSourceSafeReads:
    async def _activity(self,name,payload,**kwargs):
        if name in {'transition_plan','validate_fabric','get_provisioning_status','fail_migration'}:
            # Reads and guarded status updates are safe to retry after delayed
            # network calls. Never apply this retry to notebook submission.
            kwargs.update(timeout=90,retry=READ_RETRY)
        return await super()._activity(name,payload,**kwargs)


@workflow.defn
class OpenCodeSAPODPMigrationWorkflow(SAPDataSourceSafeReads,OpenCodeOracleViewMigrationWorkflow):
    @workflow.run
    async def run(self,input:dict)->dict:
        self._skip_source_graph=True
        result=await super().run({**input,'sap_mcp_discovery':True})
        if result['status']=='COMPLETED':
            result['current_message']='ODP schema provisioned. Ingestion remains disabled until the external consumer supports ODP_MANAGED and the post-extraction conversion contract.'
        return result

    async def _execute_fabric(self,input,**kwargs):
        # A new consumer contract must not silently enter a legacy APPEND pipeline.
        await super()._execute_fabric(input,handoff_to_scheduler=False)

    async def _discover(self,input,lakehouse_name,discovery_activity):
        return await super()._discover(input,lakehouse_name,discovery_activity)

    async def _review_bundle(self,bundle):
        discovered={'table_plan':bundle['tables'][0],'review':bundle['review']}
        self.state['review']=bundle['review']
        while True:
            choice=await self._gate('WAITING_FOR_COLUMN_MAPPING_APPROVAL',('approve_columns','reject_columns'))
            if choice['action']=='reject_columns':return {'action':'reject_plan'}
            try:
                if not choice.get('actor'):raise ValueError('Column approval requires a reviewer')
                self._apply_oracle_column_mappings(discovered,choice.get('columns') or [],choice['actor'])
                bundle['review'].pop('mapping_error',None)
                break
            except (ValueError,TypeError,KeyError) as exc:bundle['review']['mapping_error']=str(exc)
        # ODP owns delta and delete detection: no table key/watermark/delete gates.
        return await super()._review_bundle(bundle)


@workflow.defn
class OpenCodeSAPDataSourceRebuildWorkflow(SAPDataSourceSafeReads,SAPMCPKeyReview,OpenCodeOracleViewReplicationWorkflow):
    @workflow.run
    async def run(self,input:dict)->dict:
        self._skip_source_graph=True
        self._source_table_approach='SAP_TABLE'
        self._initial_plan_status='ANALYSIS_APPROVED'
        return await super().run({**input,'sap_mcp_discovery':True})

    async def _discover(self,input,lakehouse_name,discovery_activity):
        if input.get('view_correction'):
            return await super()._discover(input,lakehouse_name,discovery_activity)
        context=None
        previous=None; feedback=None
        while True:
            version=self.state['analysis_version']
            request={**input,'source_datasource_context':context,'datasource_stage':'ANALYSIS',
                'analysis_version':version,'previous_analysis':previous,'analysis_feedback':feedback,
                'agent_revision_start':self.state.get('agent_prompt_revision',-1)+1}
            analysis=await OpenCodeOracleMigrationWorkflow._discover(self,request,lakehouse_name,discovery_activity)
            if analysis.get('failure_reason'):return analysis
            context=analysis['context']
            await self._activity('transition_plan',{'plan_guid':input['plan_guid'],'version':1,
                'expected':'DRAFT','target':'WAITING_ANALYSIS_APPROVAL'})
            self.state['review']=dict(kind='analysis',discovery_provider='OPENCODE',version=version,
                route=analysis['route'],analysis=analysis['analysis'],warnings=analysis['warnings'],
                verified_objects=[{'name':o['name'],'type':o['object_type']} for o in analysis['verified']['verified']])
            choice=await self._gate('WAITING_FOR_ANALYSIS_APPROVAL',('approve_analysis','reject_analysis','analysis_feedback'))
            if choice['action']=='reject_analysis':
                await self._activity('transition_plan',{'plan_guid':input['plan_guid'],'version':1,
                    'expected':'WAITING_ANALYSIS_APPROVAL','target':'REJECTED'})
                return {'failure_reason':'Extractor analysis rejected by reviewer','analysis_rejected':True}
            if choice['action']=='approve_analysis':
                if not choice.get('actor'):raise ValueError('Analysis approval requires a reviewer')
                await self._activity('approve_analysis',{'analysis_guid':analysis['analysis_guid'],
                    'approved_by':choice['actor'],'version':version})
                await self._activity('transition_plan',{'plan_guid':input['plan_guid'],'version':1,
                    'expected':'WAITING_ANALYSIS_APPROVAL','target':'ANALYSIS_APPROVED'})
                break
            if not choice.get('actor') or not choice.get('message'):raise ValueError('Analysis feedback requires a reviewer and message')
            self.state['analysis_version']=await self._activity('revise_analysis_version',{
                'analysis_guid':analysis['analysis_guid'],'plan_guid':input['plan_guid'],'version':version})
            previous=analysis['analysis'];feedback=choice['message']
        source={**input,'source_datasource_context':context,'analysis_guid':analysis['analysis_guid']}
        return await super()._discover({**source,'datasource_stage':'PLAN',
            'agent_revision_start':self.state.get('agent_prompt_revision',0)+1},lakehouse_name,discovery_activity)
