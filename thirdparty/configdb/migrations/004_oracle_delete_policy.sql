/* Additive and rerunnable. Apply before starting the updated API/worker.
   No source data is loaded or deleted by this migration. */
IF COL_LENGTH('bronze_replication.ReplicationConfig', 'DeletePolicy') IS NULL
    ALTER TABLE bronze_replication.ReplicationConfig ADD DeletePolicy nvarchar(max) NULL;
GO
IF COL_LENGTH('bronze_replication.ReplicationConfig', 'KeyValidation') IS NULL
    ALTER TABLE bronze_replication.ReplicationConfig ADD KeyValidation nvarchar(max) NULL;
GO
IF COL_LENGTH('bronze_replication.ReplicationState', 'LastReconciledTimestamp') IS NULL
    ALTER TABLE bronze_replication.ReplicationState ADD LastReconciledTimestamp datetime2 NULL;
GO
IF COL_LENGTH('bronze_replication.ReplicationState', 'LastReconciledSCN') IS NULL
    ALTER TABLE bronze_replication.ReplicationState ADD LastReconciledSCN bigint NULL;
GO
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE name = 'CK_ReplicationConfig_DeletePolicy_JSON'
                 AND parent_object_id = OBJECT_ID('bronze_replication.ReplicationConfig'))
    ALTER TABLE bronze_replication.ReplicationConfig
    ADD CONSTRAINT CK_ReplicationConfig_DeletePolicy_JSON
        CHECK (DeletePolicy IS NULL OR ISJSON(DeletePolicy) = 1);
GO
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints
               WHERE name = 'CK_ReplicationConfig_KeyValidation_JSON'
                 AND parent_object_id = OBJECT_ID('bronze_replication.ReplicationConfig'))
    ALTER TABLE bronze_replication.ReplicationConfig
    ADD CONSTRAINT CK_ReplicationConfig_KeyValidation_JSON
        CHECK (KeyValidation IS NULL OR ISJSON(KeyValidation) = 1);
GO

CREATE OR ALTER VIEW bronze_replication.vw_ReplicationQueue
AS
SELECT
    st.PlanGUID,
    st.GUID AS SourceTableGUID,

    st.ConnectionName,
    st.SourceSystemType,
    st.SourceObjectType,
    st.SourceSchemaName,
    st.SourceTableName,

    st.FabricWorkspaceId,
    st.FabricLakehouseName,
    st.FabricLakehouseId,
    st.FabricLakehouseSchema,
    st.FabricTableName,

    rc.IncrementalMethod,
    rc.WatermarkColumn,
    rc.WatermarkColumnDataType,
    rc.WatermarkIndexName,

    rc.WriteStrategy,

    rc.PrimaryKeyColumns,
    rc.MergeKeyColumns,

    rc.EffectiveFromColumn,
    rc.EffectiveToColumn,
    rc.CurrentFlagColumn,

    rc.MaxRowFetch,
    rc.IngestionFlag,

    rc.PipelineWorkspaceId,
    rc.PipelineItemId,

    rc.DeletePolicy,
    rc.KeyValidation,
    COALESCE(JSON_VALUE(rc.DeletePolicy, '$.mode'), 'NONE') AS DeleteDetectionMethod,
    JSON_VALUE(rc.DeletePolicy, '$.behavior') AS DeleteAction,
    JSON_VALUE(rc.DeletePolicy, '$.soft_delete_column') AS SoftDeleteColumn,
    JSON_VALUE(rc.DeletePolicy, '$.soft_delete_predicate') AS SoftDeletePredicate,
    JSON_QUERY(rc.DeletePolicy, '$.soft_delete_values') AS SoftDeleteValues,
    CASE JSON_VALUE(rc.DeletePolicy, '$.watermark_tracks_soft_delete')
        WHEN 'true' THEN CAST(1 AS bit) WHEN 'false' THEN CAST(0 AS bit) END AS SoftDeleteWatermarkConfirmed,
    TRY_CONVERT(int, JSON_VALUE(rc.DeletePolicy, '$.reconcile_interval_minutes')) AS ReconcileIntervalMinutes,
    CASE JSON_VALUE(rc.DeletePolicy, '$.reconcile_require_complete_snapshot')
        WHEN 'true' THEN CAST(1 AS bit) WHEN 'false' THEN CAST(0 AS bit) END AS ReconcileRequireCompleteSnapshot,
    CASE JSON_VALUE(rc.DeletePolicy, '$.reconcile_require_consistent_snapshot')
        WHEN 'true' THEN CAST(1 AS bit) WHEN 'false' THEN CAST(0 AS bit) END AS ReconcileRequireConsistentSnapshot,
    rs.LastReconciledTimestamp,
    rs.LastReconciledSCN,

    rs.LastWatermarkValue,
    rs.LastSuccessfulBatchRunId,
    rs.LastPipelineRunId,
    rs.LastSuccessfulTimestamp,
    rs.Status AS ReplicationStatus

FROM bronze_replication.SourceTables st

INNER JOIN bronze_replication.ReplicationConfig rc
    ON rc.SourceTableGUID = st.GUID

LEFT JOIN bronze_replication.ReplicationState rs
    ON rs.SourceTableGUID = st.GUID

WHERE st.IsActive = 1
AND rc.IngestionFlag = 1
AND st.ProvisioningStatus = 'PROVISIONED';
GO
