"""Top-level migration workflow registration."""

from temporalio import workflow

from .workflow_base import MigrationWorkflowBase
from .workflow_rebuild import SAPDataSourceRebuildWorkflow
from .workflow_opencode import OpenCodeOracleMigrationWorkflow
from .workflow_oracle_view import OpenCodeOracleViewMigrationWorkflow
from .workflow_view_correction import OpenCodeOracleViewCorrectionWorkflow
from .workflow_view_replication import OpenCodeOracleViewReplicationWorkflow


@workflow.defn
class OracleTableMigrationWorkflow(MigrationWorkflowBase):
    @workflow.run
    async def run(self, input: dict) -> dict:
        return await self._run(input, "discover_oracle")


@workflow.defn
class SAPTableMigrationWorkflow(MigrationWorkflowBase):
    @workflow.run
    async def run(self, input: dict) -> dict:
        return await self._run(input, "discover_sap_table")


@workflow.defn
class SAPODPDataSourceMigrationWorkflow(MigrationWorkflowBase):
    @workflow.run
    async def run(self, input: dict) -> dict:
        return await self._run(input, "discover_sap_odp")


WORKFLOWS = [OracleTableMigrationWorkflow, SAPTableMigrationWorkflow,
             SAPODPDataSourceMigrationWorkflow, SAPDataSourceRebuildWorkflow,
             OpenCodeOracleMigrationWorkflow, OpenCodeOracleViewMigrationWorkflow,
             OpenCodeOracleViewCorrectionWorkflow, OpenCodeOracleViewReplicationWorkflow]
