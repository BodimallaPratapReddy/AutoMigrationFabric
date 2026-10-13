/* ConfigDB test reset: delete one plan and its owned configuration/history.
   Run against the ConfigDB SQL Server database, not the Lakehouse Spark engine.

   1. Stop/cancel this plan's Temporal workflow and Fabric jobs, and pause its
      scheduled loader before executing: deleting configuration does not stop them.
   2. Set @PlanGUID below. Run with @Execute = 0 to preview counts and targets.
   3. Set @Execute = 1 and rerun to commit the deletion.

   This does NOT drop physical Lakehouse tables/views, delete Oracle/SAP data,
   remove shared connections/workspaces, or delete Temporal/OpenCode history.
   To recreate physical targets, remove those separately in the Lakehouse.
   Unexpected foreign keys fail and roll back; other plans are never cascaded.
*/
SET NOCOUNT ON;
SET XACT_ABORT ON;

DECLARE @PlanGUID uniqueidentifier = NULL; -- Replace NULL with 'your-plan-guid'.
DECLARE @Execute bit = 0;                 -- 0 = preview; 1 = delete and commit.

IF @PlanGUID IS NULL
    THROW 51000, 'Set @PlanGUID to the specific ConfigDB PlanGUID.', 1;

DECLARE @Tables TABLE (GUID uniqueidentifier PRIMARY KEY);
DECLARE @Views TABLE (GUID uniqueidentifier PRIMARY KEY);
DECLARE @Analyses TABLE (GUID uniqueidentifier PRIMARY KEY);
DECLARE @Batches TABLE (GUID uniqueidentifier PRIMARY KEY);
DECLARE @Counts TABLE (TableName sysname, RowsToDelete bigint);

BEGIN TRY
    BEGIN TRANSACTION;

    IF NOT EXISTS (SELECT 1 FROM bronze_replication.MigrationPlans WITH (UPDLOCK, HOLDLOCK)
                   WHERE PlanGUID = @PlanGUID)
        THROW 51001, 'PlanGUID was not found; nothing was deleted.', 1;

    INSERT @Tables SELECT GUID FROM bronze_replication.SourceTables WITH (UPDLOCK, HOLDLOCK) WHERE PlanGUID = @PlanGUID;
    INSERT @Views SELECT ViewGUID FROM bronze_replication.FabricViews WITH (UPDLOCK, HOLDLOCK) WHERE PlanGUID = @PlanGUID;
    INSERT @Analyses SELECT AnalysisGUID FROM bronze_replication.SAPAnalysis WITH (UPDLOCK, HOLDLOCK) WHERE PlanGUID = @PlanGUID;
    INSERT @Batches SELECT BatchRunId FROM bronze_replication.BatchRuns WITH (UPDLOCK, HOLDLOCK) WHERE PlanGUID = @PlanGUID;

    SELECT PlanGUID, SourceConnectionName, SourceObjectName, Status, PlanVersion,
           TemporalWorkflowId, TemporalRunId
    FROM bronze_replication.MigrationPlans WHERE PlanGUID = @PlanGUID;
    SELECT 'TABLE' AS ObjectType, FabricWorkspaceId, FabricLakehouseId,
           FabricLakehouseSchema AS FabricSchema, FabricTableName AS ObjectName
    FROM bronze_replication.SourceTables WHERE PlanGUID = @PlanGUID
    UNION ALL
    SELECT 'VIEW', FabricWorkspaceId, FabricLakehouseId, FabricSchemaName, ViewName
    FROM bronze_replication.FabricViews WHERE PlanGUID = @PlanGUID;

    INSERT @Counts
    SELECT 'BatchObjectRuns', COUNT_BIG(*) FROM bronze_replication.BatchObjectRuns WHERE BatchRunId IN (SELECT GUID FROM @Batches)
    UNION ALL SELECT 'BatchRuns', COUNT_BIG(*) FROM bronze_replication.BatchRuns WHERE PlanGUID = @PlanGUID
    UNION ALL SELECT 'TargetChangeRuns', COUNT_BIG(*) FROM bronze_replication.TargetChangeRuns WHERE PlanGUID = @PlanGUID
    UNION ALL SELECT 'FabricViewDependencies', COUNT_BIG(*) FROM bronze_replication.FabricViewDependencies WHERE ViewGUID IN (SELECT GUID FROM @Views)
    UNION ALL SELECT 'FabricViews', COUNT_BIG(*) FROM bronze_replication.FabricViews WHERE PlanGUID = @PlanGUID
    UNION ALL SELECT 'ReplicationState', COUNT_BIG(*) FROM bronze_replication.ReplicationState WHERE SourceTableGUID IN (SELECT GUID FROM @Tables)
    UNION ALL SELECT 'ReplicationConfig', COUNT_BIG(*) FROM bronze_replication.ReplicationConfig WHERE SourceTableGUID IN (SELECT GUID FROM @Tables)
    UNION ALL SELECT 'SourceTableColumns', COUNT_BIG(*) FROM bronze_replication.SourceTableColumns WHERE GUID IN (SELECT GUID FROM @Tables)
    UNION ALL SELECT 'SourceTables', COUNT_BIG(*) FROM bronze_replication.SourceTables WHERE PlanGUID = @PlanGUID
    UNION ALL SELECT 'SAPAnalysisObjects', COUNT_BIG(*) FROM bronze_replication.SAPAnalysisObjects WHERE AnalysisGUID IN (SELECT GUID FROM @Analyses)
    UNION ALL SELECT 'SAPAnalysis', COUNT_BIG(*) FROM bronze_replication.SAPAnalysis WHERE PlanGUID = @PlanGUID
    UNION ALL SELECT 'MigrationPlans', COUNT_BIG(*) FROM bronze_replication.MigrationPlans WHERE PlanGUID = @PlanGUID;
    SELECT TableName, RowsToDelete FROM @Counts ORDER BY TableName;

    IF @Execute = 0
    BEGIN
        ROLLBACK TRANSACTION;
        PRINT 'Preview only. Set @Execute = 1 to delete the displayed plan-owned rows.';
        RETURN;
    END;

    IF EXISTS (SELECT 1 FROM bronze_replication.BatchRuns WHERE PlanGUID = @PlanGUID AND Status = 'RUNNING')
       OR EXISTS (SELECT 1 FROM bronze_replication.ReplicationState WHERE SourceTableGUID IN (SELECT GUID FROM @Tables) AND Status = 'RUNNING')
       OR EXISTS (SELECT 1 FROM bronze_replication.TargetChangeRuns WHERE PlanGUID = @PlanGUID AND Phase <> 'COMPLETED')
        THROW 51002, 'A run is still marked active. Stop and reconcile it before deleting the plan.', 1;

    -- Preserve references owned by other plans; do not delete their dependency rows.
    IF EXISTS (SELECT 1 FROM bronze_replication.FabricViewDependencies
               WHERE ViewGUID NOT IN (SELECT GUID FROM @Views)
                 AND (SourceTableGUID IN (SELECT GUID FROM @Tables) OR DependsOnViewGUID IN (SELECT GUID FROM @Views)))
       OR EXISTS (SELECT 1 FROM bronze_replication.SourceTables
                  WHERE PlanGUID <> @PlanGUID AND ParentSourceObjectGUID IN (SELECT GUID FROM @Tables))
       OR EXISTS (SELECT 1 FROM bronze_replication.TargetChangeRuns
                  WHERE PlanGUID <> @PlanGUID AND SourceTableGUID IN (SELECT GUID FROM @Tables))
       OR EXISTS (SELECT 1 FROM bronze_replication.BatchObjectRuns
                  WHERE BatchRunId NOT IN (SELECT GUID FROM @Batches)
                    AND ((ObjectType = 'TABLE' AND ObjectGUID IN (SELECT GUID FROM @Tables))
                      OR (ObjectType = 'VIEW' AND ObjectGUID IN (SELECT GUID FROM @Views))))
        THROW 51003, 'Another plan or batch references these objects. Resolve those references first.', 1;

    DELETE FROM bronze_replication.BatchObjectRuns WHERE BatchRunId IN (SELECT GUID FROM @Batches);
    DELETE FROM bronze_replication.BatchRuns WHERE PlanGUID = @PlanGUID;
    DELETE FROM bronze_replication.TargetChangeRuns WHERE PlanGUID = @PlanGUID;
    DELETE FROM bronze_replication.FabricViewDependencies WHERE ViewGUID IN (SELECT GUID FROM @Views);
    DELETE FROM bronze_replication.FabricViews WHERE PlanGUID = @PlanGUID;
    DELETE FROM bronze_replication.ReplicationState WHERE SourceTableGUID IN (SELECT GUID FROM @Tables);
    DELETE FROM bronze_replication.ReplicationConfig WHERE SourceTableGUID IN (SELECT GUID FROM @Tables);
    DELETE FROM bronze_replication.SourceTableColumns WHERE GUID IN (SELECT GUID FROM @Tables);
    DELETE FROM bronze_replication.SourceTables WHERE PlanGUID = @PlanGUID;
    DELETE FROM bronze_replication.SAPAnalysisObjects WHERE AnalysisGUID IN (SELECT GUID FROM @Analyses);
    DELETE FROM bronze_replication.SAPAnalysis WHERE PlanGUID = @PlanGUID;
    DELETE FROM bronze_replication.MigrationPlans WHERE PlanGUID = @PlanGUID;

    COMMIT TRANSACTION;
    PRINT 'Plan and its owned ConfigDB records deleted successfully.';
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
