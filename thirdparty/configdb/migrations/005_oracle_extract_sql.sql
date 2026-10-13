/* Additive and rerunnable. Existing configurations remain NULL until reviewed. */
IF COL_LENGTH('bronze_replication.SourceTables', 'ExtractSql') IS NULL
    ALTER TABLE bronze_replication.SourceTables ADD ExtractSql nvarchar(max) NULL;
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
    st.ExtractSql,

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
