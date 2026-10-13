from temporalio import workflow
from .workflow_view_replication import OpenCodeOracleViewReplicationWorkflow


@workflow.defn
class OpenCodeSAPViewMigrationWorkflow(OpenCodeOracleViewReplicationWorkflow):
    @workflow.run
    async def run(self,input:dict)->dict:
        self._view_graph_activity='sap_view_graph'
        self._source_table_approach='SAP_TABLE'
        self._source_key_activity='validate_sap_key'
        return await super().run(input)
