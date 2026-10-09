/*
===============================================================================
 Automated Migration Platform - Config DB Bootstrap
===============================================================================

Purpose
-------
Creates a NEW SQL database for the Oracle/SAP -> Microsoft Fabric automated
migration platform.

Default database name:
    AutoMigrationConfigDB

Change the database name below if required.

Target:
    Microsoft SQL Server / Azure SQL / Fabric-compatible T-SQL where supported.

Main schema:
    bronze_replication

Created objects
---------------
1. DBConnections
2. FabricWorkspaces
3. MigrationPlans
4. SAPAnalysis
5. SAPAnalysisObjects
6. SourceTables
7. SourceTableColumns
8. ReplicationConfig
9. ReplicationState
10. FabricViews
11. FabricViewDependencies
12. BatchRuns
13. BatchObjectRuns

Runtime views
-------------
1. vw_ApprovedPlansReadyToProvision
2. vw_TableProvisioningQueue
3. vw_ViewProvisioningQueue
4. vw_ReplicationQueue
5. vw_FabricViewDependencies

Architecture
------------
Discovery / AI analysis
        ->
Human approval
        ->
MigrationPlans
        ->
Runtime configuration
        ->
Fabric parameterized notebook / replication pipeline

===============================================================================
*/

USE master;
GO

/* ===========================================================================
   0. CREATE DATABASE
   =========================================================================== */

IF DB_ID(N'AutoMigrationConfigDB') IS NULL
BEGIN
    CREATE DATABASE AutoMigrationConfigDB;
END;
GO

USE AutoMigrationConfigDB;
GO

SET XACT_ABORT ON;
GO


/* ===========================================================================
   1. CREATE APPLICATION SCHEMA
   =========================================================================== */

IF SCHEMA_ID(N'bronze_replication') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA bronze_replication');
END;
GO


/* ===========================================================================
   2. DATABASE CONNECTIONS
   =========================================================================== */

CREATE TABLE bronze_replication.DBConnections
(
    Id                  bigint IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_DBConnections PRIMARY KEY,

    SourceType          varchar(50) NOT NULL,
    -- Examples:
    -- ORACLE
    -- SAP_ECC
    -- SAP_S4HANA
    -- SQLSERVER
    -- POSTGRESQL
    -- FABRIC_SQL

    ConnectionName      varchar(200) NOT NULL,

    ConnectionDetails   nvarchar(max) NOT NULL,
    -- JSON containing non-secret connection metadata.
    -- Prefer external secret management for passwords/secrets.

    ConnectionTags      nvarchar(max) NULL,
    -- JSON array.

    IsActive            bit NOT NULL
        CONSTRAINT DF_DBConnections_IsActive DEFAULT (1),

    CreatedTimestamp    datetime2 NOT NULL
        CONSTRAINT DF_DBConnections_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp    datetime2 NOT NULL
        CONSTRAINT DF_DBConnections_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT UQ_DBConnections_ConnectionName
        UNIQUE (ConnectionName),

    CONSTRAINT CK_DBConnections_ConnectionDetails_JSON
        CHECK (ISJSON(ConnectionDetails) = 1),

    CONSTRAINT CK_DBConnections_ConnectionTags_JSON
        CHECK (ConnectionTags IS NULL OR ISJSON(ConnectionTags) = 1)
);
GO


/* ===========================================================================
   3. FABRIC WORKSPACES
   =========================================================================== */

CREATE TABLE bronze_replication.FabricWorkspaces
(
    Id                  bigint IDENTITY(1,1) NOT NULL
        CONSTRAINT PK_FabricWorkspaces PRIMARY KEY,

    WorkspaceName       nvarchar(256) NOT NULL,
    WorkspaceId         nvarchar(100) NOT NULL,

    IsActive            bit NOT NULL
        CONSTRAINT DF_FabricWorkspaces_IsActive DEFAULT (1),

    CreatedTimestamp    datetime2 NOT NULL
        CONSTRAINT DF_FabricWorkspaces_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp    datetime2 NOT NULL
        CONSTRAINT DF_FabricWorkspaces_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT UQ_FabricWorkspaces_WorkspaceId
        UNIQUE (WorkspaceId)
);
GO


/* ===========================================================================
   4. MIGRATION PLAN / APPROVAL
   =========================================================================== */

CREATE TABLE bronze_replication.MigrationPlans
(
    PlanGUID                uniqueidentifier NOT NULL
        CONSTRAINT PK_MigrationPlans PRIMARY KEY
        CONSTRAINT DF_MigrationPlans_PlanGUID DEFAULT NEWID(),

    SourceConnectionName    varchar(200) NOT NULL,

    SourceSystemType        varchar(50) NOT NULL,
    -- ORACLE, SAP_ECC

    SourceObjectType        varchar(50) NOT NULL,
    -- TABLE, SAP_DATASOURCE

    SourceObjectName        varchar(500) NOT NULL,

    MigrationApproach       varchar(50) NOT NULL,
    -- DIRECT_REPLICATION
    -- SAP_ODP
    -- SAP_REBUILD

    AnalysisVersion         int NOT NULL
        CONSTRAINT DF_MigrationPlans_AnalysisVersion DEFAULT (1),

    PlanVersion             int NOT NULL
        CONSTRAINT DF_MigrationPlans_PlanVersion DEFAULT (1),

    RuntimePlanHash         char(64) NULL,
    -- SHA-256 of the approved runtime payload for idempotent activity retries.

    Status                  varchar(50) NOT NULL
        CONSTRAINT DF_MigrationPlans_Status DEFAULT ('DRAFT'),
    -- DRAFT
    -- WAITING_ANALYSIS_APPROVAL
    -- ANALYSIS_APPROVED
    -- WAITING_PLAN_APPROVAL
    -- APPROVED
    -- READY_TO_PROVISION
    -- PROVISIONING
    -- PROVISIONED
    -- FAILED
    -- REJECTED
    -- SUPERSEDED
    -- CANCELLED
    -- CHANGE_REQUESTED (approved structural change awaiting Fabric notebook)
    -- CHANGE_APPLIED (approved structural change applied by Fabric notebook)

    ApprovedBy              nvarchar(256) NULL,
    ApprovedTimestamp       datetime2 NULL,

    TemporalWorkflowId      varchar(200) NULL,
    TemporalRunId           varchar(200) NULL,

    Notes                   nvarchar(max) NULL,

    CreatedTimestamp        datetime2 NOT NULL
        CONSTRAINT DF_MigrationPlans_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp        datetime2 NOT NULL
        CONSTRAINT DF_MigrationPlans_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_MigrationPlans_DBConnections
        FOREIGN KEY (SourceConnectionName)
        REFERENCES bronze_replication.DBConnections(ConnectionName),

    CONSTRAINT CK_MigrationPlans_AnalysisVersion
        CHECK (AnalysisVersion > 0),

    CONSTRAINT CK_MigrationPlans_PlanVersion
        CHECK (PlanVersion > 0)
);
GO

CREATE INDEX IX_MigrationPlans_Source
ON bronze_replication.MigrationPlans
(
    SourceConnectionName,
    SourceSystemType,
    SourceObjectName
);
GO

CREATE INDEX IX_MigrationPlans_Status
ON bronze_replication.MigrationPlans
(
    Status,
    UpdatedTimestamp
);
GO


/* ===========================================================================
   5. SAP ANALYSIS / LINEAGE
   =========================================================================== */

CREATE TABLE bronze_replication.SAPAnalysis
(
    AnalysisGUID             uniqueidentifier NOT NULL
        CONSTRAINT PK_SAPAnalysis PRIMARY KEY
        CONSTRAINT DF_SAPAnalysis_GUID DEFAULT NEWID(),

    PlanGUID                 uniqueidentifier NOT NULL,

    AnalysisVersion          int NOT NULL,

    DataSourceName           varchar(256) NOT NULL,

    RootObjectType           varchar(50) NULL,
    RootObjectName           varchar(256) NULL,

    AnalysisStatus           varchar(50) NOT NULL
        CONSTRAINT DF_SAPAnalysis_Status DEFAULT ('DRAFT'),
    -- DRAFT
    -- WAITING_APPROVAL
    -- APPROVED
    -- REJECTED
    -- SUPERSEDED

    Summary                  nvarchar(max) NULL,

    LLMModel                 varchar(200) NULL,
    PhoenixTraceId           varchar(200) NULL,

    ApprovedBy               nvarchar(256) NULL,
    ApprovedTimestamp        datetime2 NULL,

    CreatedTimestamp         datetime2 NOT NULL
        CONSTRAINT DF_SAPAnalysis_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp         datetime2 NOT NULL
        CONSTRAINT DF_SAPAnalysis_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_SAPAnalysis_MigrationPlans
        FOREIGN KEY (PlanGUID)
        REFERENCES bronze_replication.MigrationPlans(PlanGUID),

    CONSTRAINT UQ_SAPAnalysis_PlanVersion
        UNIQUE (PlanGUID, AnalysisVersion),

    CONSTRAINT CK_SAPAnalysis_AnalysisVersion
        CHECK (AnalysisVersion > 0)
);
GO


CREATE TABLE bronze_replication.SAPAnalysisObjects
(
    AnalysisObjectGUID       uniqueidentifier NOT NULL
        CONSTRAINT PK_SAPAnalysisObjects PRIMARY KEY
        CONSTRAINT DF_SAPAnalysisObjects_GUID DEFAULT NEWID(),

    AnalysisGUID             uniqueidentifier NOT NULL,

    ObjectType               varchar(50) NOT NULL,
    -- FM, TABLE, VIEW, INCLUDE, CLASS, METHOD, MACRO

    ObjectName               varchar(256) NOT NULL,

    ParentObjectName         varchar(256) NULL,

    DependencyType           varchar(100) NULL,
    DependencyDepth          int NULL,

    ObservedFromScraper      bit NOT NULL
        CONSTRAINT DF_SAPAnalysisObjects_Observed DEFAULT (1),

    InterpretedRole          varchar(100) NULL,
    -- SOURCE
    -- LOOKUP
    -- TRANSFORMATION
    -- DELTA_LOGIC
    -- CONFIGURATION
    -- LOGGING
    -- HELPER

    Interpretation           nvarchar(max) NULL,

    ProposedAction           varchar(50) NULL,
    -- INCLUDE
    -- EXCLUDE
    -- REUSE_EXISTING
    -- ANALYZE_DEEPER

    UserDecision             varchar(50) NULL,
    -- APPROVED
    -- REJECTED
    -- OVERRIDDEN

    UserDecisionNotes        nvarchar(max) NULL,

    IsReplicationRequired    bit NULL,

    IsActive                 bit NOT NULL
        CONSTRAINT DF_SAPAnalysisObjects_IsActive DEFAULT (1),

    CreatedTimestamp         datetime2 NOT NULL
        CONSTRAINT DF_SAPAnalysisObjects_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp         datetime2 NOT NULL
        CONSTRAINT DF_SAPAnalysisObjects_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_SAPAnalysisObjects_SAPAnalysis
        FOREIGN KEY (AnalysisGUID)
        REFERENCES bronze_replication.SAPAnalysis(AnalysisGUID),

    CONSTRAINT CK_SAPAnalysisObjects_Depth
        CHECK (DependencyDepth IS NULL OR DependencyDepth >= 0)
);
GO

CREATE INDEX IX_SAPAnalysisObjects_AnalysisGUID
ON bronze_replication.SAPAnalysisObjects
(
    AnalysisGUID,
    ObjectType,
    IsActive
);
GO


/* ===========================================================================
   6. SOURCE / REPLICATED TABLE METADATA
   =========================================================================== */

CREATE TABLE bronze_replication.SourceTables
(
    GUID                    uniqueidentifier NOT NULL
        CONSTRAINT PK_SourceTables PRIMARY KEY
        CONSTRAINT DF_SourceTables_GUID DEFAULT NEWID(),

    PlanGUID                uniqueidentifier NOT NULL,

    ConnectionName          varchar(200) NOT NULL,

    SourceSystemType        varchar(50) NOT NULL,
    -- ORACLE
    -- SAP_ECC

    SourceObjectType        varchar(50) NOT NULL,
    -- TABLE
    -- SAP_DATASOURCE
    -- SAP_UNDERLYING_TABLE

    SourceSchemaName        varchar(256) NULL,

    SourceTableName         varchar(500) NOT NULL,

    ParentSourceObjectGUID  uniqueidentifier NULL,

    DataSourceType          varchar(100) NULL,

    ApplicationComponent    varchar(100) NULL,

    Delta                   nvarchar(100) NULL,

    ReplicationMethod       varchar(50) NULL,
    -- DIRECT_TABLE
    -- SAP_ODP
    -- TABLE_REPLICATION

    ObjectRole              varchar(50) NULL,
    -- SOURCE
    -- LOOKUP
    -- REFERENCE
    -- CONFIGURATION

    FabricWorkspaceName     nvarchar(256) NULL,
    FabricWorkspaceId       nvarchar(100) NOT NULL,

    FabricLakehouseName     nvarchar(256) NOT NULL,

    FabricLakehouseId       nvarchar(100) NULL,

    FabricLakehouseSchema   nvarchar(256) NOT NULL,

    FabricTableName         nvarchar(256) NOT NULL,

    ProvisioningStatus      varchar(50) NOT NULL
        CONSTRAINT DF_SourceTables_ProvisioningStatus DEFAULT ('PENDING'),
    -- PENDING
    -- IN_PROGRESS
    -- PROVISIONED
    -- FAILED
    -- SKIPPED

    ProvisioningError       nvarchar(max) NULL,

    LastProvisioningRunId   varchar(200) NULL,

    ProvisionedTimestamp    datetime2 NULL,

    IsActive                bit NOT NULL
        CONSTRAINT DF_SourceTables_IsActive DEFAULT (1),

    CreatedTimestamp        datetime2 NOT NULL
        CONSTRAINT DF_SourceTables_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp        datetime2 NOT NULL
        CONSTRAINT DF_SourceTables_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_SourceTables_MigrationPlans
        FOREIGN KEY (PlanGUID)
        REFERENCES bronze_replication.MigrationPlans(PlanGUID),

    CONSTRAINT FK_SourceTables_DBConnections
        FOREIGN KEY (ConnectionName)
        REFERENCES bronze_replication.DBConnections(ConnectionName),

    CONSTRAINT FK_SourceTables_ParentSourceObject
        FOREIGN KEY (ParentSourceObjectGUID)
        REFERENCES bronze_replication.SourceTables(GUID)
);
GO

CREATE INDEX IX_SourceTables_PlanGUID
ON bronze_replication.SourceTables
(
    PlanGUID,
    ProvisioningStatus,
    IsActive
);
GO

CREATE INDEX IX_SourceTables_Source
ON bronze_replication.SourceTables
(
    ConnectionName,
    SourceSchemaName,
    SourceTableName
);
GO

CREATE INDEX IX_SourceTables_FabricTarget
ON bronze_replication.SourceTables
(
    FabricWorkspaceId,
    FabricLakehouseName,
    FabricLakehouseSchema,
    FabricTableName
);
GO


/* ===========================================================================
   7. SOURCE COLUMN / DATA TYPE MAPPING
   =========================================================================== */

CREATE TABLE bronze_replication.SourceTableColumns
(
    SourceTableColumnGUID   uniqueidentifier NOT NULL
        CONSTRAINT PK_SourceTableColumns PRIMARY KEY
        CONSTRAINT DF_SourceTableColumns_GUID DEFAULT NEWID(),

    GUID                    uniqueidentifier NOT NULL,
    -- SourceTables.GUID

    Sno                     int NOT NULL,

    ColumnName              varchar(256) NOT NULL,

    TargetColumnName        nvarchar(256) NULL,

    Description             nvarchar(1000) NULL,

    SourceDataType          varchar(256) NOT NULL,

    FabricDataType          varchar(256) NOT NULL,

    IsPrimaryKey            bit NOT NULL
        CONSTRAINT DF_SourceTableColumns_IsPrimaryKey DEFAULT (0),

    IsNullable              bit NULL,

    IsWatermarkCandidate    bit NOT NULL
        CONSTRAINT DF_SourceTableColumns_IsWatermarkCandidate DEFAULT (0),

    IsSelected              bit NOT NULL
        CONSTRAINT DF_SourceTableColumns_IsSelected DEFAULT (1),

    SourceExpression        nvarchar(max) NULL,

    TransformationNotes     nvarchar(max) NULL,

    CreatedTimestamp        datetime2 NOT NULL
        CONSTRAINT DF_SourceTableColumns_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp        datetime2 NOT NULL
        CONSTRAINT DF_SourceTableColumns_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_SourceTableColumns_SourceTables
        FOREIGN KEY (GUID)
        REFERENCES bronze_replication.SourceTables(GUID),

    CONSTRAINT UQ_SourceTableColumns_Ordinal
        UNIQUE (GUID, Sno),

    CONSTRAINT UQ_SourceTableColumns_Name
        UNIQUE (GUID, ColumnName),

    CONSTRAINT CK_SourceTableColumns_Sno
        CHECK (Sno > 0)
);
GO

CREATE INDEX IX_SourceTableColumns_GUID
ON bronze_replication.SourceTableColumns
(
    GUID,
    Sno
);
GO


/* ===========================================================================
   8. REPLICATION CONFIGURATION
   =========================================================================== */

CREATE TABLE bronze_replication.ReplicationConfig
(
    ReplicationConfigGUID     uniqueidentifier NOT NULL
        CONSTRAINT PK_ReplicationConfig PRIMARY KEY
        CONSTRAINT DF_ReplicationConfig_GUID DEFAULT NEWID(),

    SourceTableGUID           uniqueidentifier NOT NULL,

    IncrementalMethod         varchar(50) NOT NULL
        CONSTRAINT DF_ReplicationConfig_IncrementalMethod DEFAULT ('FULL'),
    -- FULL
    -- WATERMARK
    -- SAP_ODP_DELTA
    -- CDC

    WatermarkColumn           varchar(256) NULL,

    WatermarkColumnDataType   varchar(100) NULL,

    WatermarkIndexName        varchar(256) NULL,

    WriteStrategy             varchar(50) NOT NULL
        CONSTRAINT DF_ReplicationConfig_WriteStrategy DEFAULT ('APPEND'),
    -- APPEND
    -- UPSERT
    -- SCD1
    -- SCD2
    -- REPLACE

    PrimaryKeyColumns         nvarchar(max) NULL,
    -- JSON array, e.g. ["ID"]

    MergeKeyColumns           nvarchar(max) NULL,
    -- JSON array, e.g. ["ORDER_ID","LINE_ID"]

    EffectiveFromColumn       varchar(256) NULL,

    EffectiveToColumn         varchar(256) NULL,

    CurrentFlagColumn         varchar(256) NULL,

    MaxRowFetch               int NOT NULL
        CONSTRAINT DF_ReplicationConfig_MaxRowFetch DEFAULT (0),

    IngestionFlag             bit NOT NULL
        CONSTRAINT DF_ReplicationConfig_IngestionFlag DEFAULT (1),

    PipelineWorkspaceId       nvarchar(100) NULL,

    PipelineItemId            nvarchar(100) NULL,

    DeletePolicy              nvarchar(max) NULL,
    KeyValidation             nvarchar(max) NULL,

    CreatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_ReplicationConfig_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_ReplicationConfig_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_ReplicationConfig_SourceTables
        FOREIGN KEY (SourceTableGUID)
        REFERENCES bronze_replication.SourceTables(GUID),

    CONSTRAINT UQ_ReplicationConfig_SourceTable
        UNIQUE (SourceTableGUID),

    CONSTRAINT CK_ReplicationConfig_MaxRowFetch
        CHECK (MaxRowFetch >= 0),

    CONSTRAINT CK_ReplicationConfig_DeletePolicy_JSON
        CHECK (DeletePolicy IS NULL OR ISJSON(DeletePolicy) = 1),
    CONSTRAINT CK_ReplicationConfig_KeyValidation_JSON
        CHECK (KeyValidation IS NULL OR ISJSON(KeyValidation) = 1),

    CONSTRAINT CK_ReplicationConfig_PrimaryKeys_JSON
        CHECK (PrimaryKeyColumns IS NULL OR ISJSON(PrimaryKeyColumns) = 1),

    CONSTRAINT CK_ReplicationConfig_MergeKeys_JSON
        CHECK (MergeKeyColumns IS NULL OR ISJSON(MergeKeyColumns) = 1),

    CONSTRAINT CK_ReplicationConfig_Watermark
        CHECK
        (
            IncrementalMethod <> 'WATERMARK'
            OR
            (
                WatermarkColumn IS NOT NULL
                AND WatermarkColumnDataType IS NOT NULL
            )
        ),

    CONSTRAINT CK_ReplicationConfig_SCD2Keys
        CHECK
        (
            WriteStrategy <> 'SCD2'
            OR MergeKeyColumns IS NOT NULL
            OR PrimaryKeyColumns IS NOT NULL
        )
);
GO


/* ===========================================================================
   9. REPLICATION RUNTIME STATE / WATERMARK
   =========================================================================== */

CREATE TABLE bronze_replication.ReplicationState
(
    SourceTableGUID           uniqueidentifier NOT NULL
        CONSTRAINT PK_ReplicationState PRIMARY KEY,

    LastWatermarkValue        nvarchar(1000) NULL,

    LastSuccessfulBatchRunId  uniqueidentifier NULL,

    LastPipelineRunId         varchar(200) NULL,

    LastRunStartedTimestamp   datetime2 NULL,

    LastRunCompletedTimestamp datetime2 NULL,

    LastSuccessfulTimestamp   datetime2 NULL,

    LastReconciledTimestamp   datetime2 NULL,
    LastReconciledSCN         bigint NULL,

    RowsRead                  bigint NULL,

    RowsWritten               bigint NULL,

    Status                    varchar(50) NOT NULL
        CONSTRAINT DF_ReplicationState_Status DEFAULT ('NOT_STARTED'),
    -- NOT_STARTED
    -- RUNNING
    -- SUCCEEDED
    -- FAILED

    ErrorMessage              nvarchar(max) NULL,

    UpdatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_ReplicationState_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_ReplicationState_SourceTables
        FOREIGN KEY (SourceTableGUID)
        REFERENCES bronze_replication.SourceTables(GUID),

    CONSTRAINT CK_ReplicationState_RowsRead
        CHECK (RowsRead IS NULL OR RowsRead >= 0),

    CONSTRAINT CK_ReplicationState_RowsWritten
        CHECK (RowsWritten IS NULL OR RowsWritten >= 0)
);
GO

/* Durable data-change guard. For an existing database apply migration 003. */
CREATE TABLE bronze_replication.TargetChangeRuns
(
    PlanGUID uniqueidentifier NOT NULL PRIMARY KEY,
    SourceTableGUID uniqueidentifier NOT NULL,
    Action varchar(30) NOT NULL,
    Phase varchar(30) NOT NULL,
    StageTableName nvarchar(256) NOT NULL,
    SnapshotSCN bigint NULL,
    SourceRows bigint NULL,
    CreatedTimestamp datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    UpdatedTimestamp datetime2 NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT FK_TargetChangeRuns_Plan FOREIGN KEY (PlanGUID)
        REFERENCES bronze_replication.MigrationPlans(PlanGUID),
    CONSTRAINT FK_TargetChangeRuns_Table FOREIGN KEY (SourceTableGUID)
        REFERENCES bronze_replication.SourceTables(GUID),
    CONSTRAINT CK_TargetChangeRuns_Action CHECK (Action IN ('ALTER_BACKFILL', 'REPLACE_FULL')),
    CONSTRAINT CK_TargetChangeRuns_Phase CHECK
        (Phase IN ('STARTED', 'STAGED', 'MUTATION_STARTED', 'COMPLETED'))
);
GO

CREATE INDEX IX_TargetChangeRuns_SourceTable
ON bronze_replication.TargetChangeRuns (SourceTableGUID, CreatedTimestamp DESC);
GO


/* ===========================================================================
   10. FABRIC DERIVED OBJECTS - VIEWS
   =========================================================================== */

CREATE TABLE bronze_replication.FabricViews
(
    ViewGUID                  uniqueidentifier NOT NULL
        CONSTRAINT PK_FabricViews PRIMARY KEY
        CONSTRAINT DF_FabricViews_GUID DEFAULT NEWID(),

    PlanGUID                  uniqueidentifier NOT NULL,

    FabricWorkspaceName       nvarchar(256) NULL,

    FabricWorkspaceId         nvarchar(100) NOT NULL,

    FabricLakehouseName       nvarchar(256) NOT NULL,

    FabricLakehouseId         nvarchar(100) NULL,

    FabricSchemaName          nvarchar(256) NOT NULL,

    ViewName                  nvarchar(256) NOT NULL,

    ViewType                  varchar(50) NOT NULL
        CONSTRAINT DF_FabricViews_ViewType DEFAULT ('SQL_VIEW'),
    -- SQL_VIEW
    -- MATERIALIZED_TABLE
    -- SPARK_DERIVED_TABLE

    ViewSQL                   nvarchar(max) NOT NULL,

    LogicDescription          nvarchar(max) NULL,

    CreationOrder             int NOT NULL
        CONSTRAINT DF_FabricViews_CreationOrder DEFAULT (1),

    IsActive                  bit NOT NULL
        CONSTRAINT DF_FabricViews_IsActive DEFAULT (1),

    ProvisioningStatus        varchar(50) NOT NULL
        CONSTRAINT DF_FabricViews_Status DEFAULT ('PENDING'),

    ProvisioningError         nvarchar(max) NULL,

    LastProvisioningRunId     varchar(200) NULL,

    ProvisionedTimestamp      datetime2 NULL,

    CreatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_FabricViews_Created DEFAULT SYSUTCDATETIME(),

    UpdatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_FabricViews_Updated DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_FabricViews_MigrationPlans
        FOREIGN KEY (PlanGUID)
        REFERENCES bronze_replication.MigrationPlans(PlanGUID),

    CONSTRAINT UQ_FabricViews_Target
        UNIQUE
        (
            PlanGUID,
            FabricWorkspaceId,
            FabricLakehouseName,
            FabricSchemaName,
            ViewName
        ),

    CONSTRAINT CK_FabricViews_CreationOrder
        CHECK (CreationOrder > 0)
);
GO

CREATE INDEX IX_FabricViews_ProvisioningQueue
ON bronze_replication.FabricViews
(
    PlanGUID,
    ProvisioningStatus,
    IsActive,
    CreationOrder
);
GO


CREATE TABLE bronze_replication.FabricViewDependencies
(
    ViewDependencyGUID        uniqueidentifier NOT NULL
        CONSTRAINT PK_FabricViewDependencies PRIMARY KEY
        CONSTRAINT DF_FabricViewDependencies_GUID DEFAULT NEWID(),

    ViewGUID                  uniqueidentifier NOT NULL,

    DependencyType            varchar(50) NOT NULL,
    -- SOURCE_TABLE
    -- FABRIC_VIEW
    -- EXISTING_OBJECT

    SourceTableGUID           uniqueidentifier NULL,

    DependsOnViewGUID         uniqueidentifier NULL,

    ExistingObjectName        nvarchar(500) NULL,

    DependencySequence        int NOT NULL
        CONSTRAINT DF_FabricViewDependencies_Sequence DEFAULT (1),

    CreatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_FabricViewDependencies_Created DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_FabricViewDependencies_View
        FOREIGN KEY (ViewGUID)
        REFERENCES bronze_replication.FabricViews(ViewGUID),

    CONSTRAINT FK_FabricViewDependencies_SourceTable
        FOREIGN KEY (SourceTableGUID)
        REFERENCES bronze_replication.SourceTables(GUID),

    CONSTRAINT FK_FabricViewDependencies_DependsOnView
        FOREIGN KEY (DependsOnViewGUID)
        REFERENCES bronze_replication.FabricViews(ViewGUID),

    CONSTRAINT CK_FabricViewDependencies_Sequence
        CHECK (DependencySequence > 0),

    CONSTRAINT CK_FabricViewDependencies_Reference
        CHECK
        (
            (DependencyType = 'SOURCE_TABLE' AND SourceTableGUID IS NOT NULL)
            OR
            (DependencyType = 'FABRIC_VIEW' AND DependsOnViewGUID IS NOT NULL)
            OR
            (DependencyType = 'EXISTING_OBJECT' AND ExistingObjectName IS NOT NULL)
        )
);
GO

CREATE INDEX IX_FabricViewDependencies_ViewGUID
ON bronze_replication.FabricViewDependencies
(
    ViewGUID,
    DependencySequence
);
GO


/* ===========================================================================
   11. BATCH / EXECUTION AUDIT
   =========================================================================== */

CREATE TABLE bronze_replication.BatchRuns
(
    BatchRunId                uniqueidentifier NOT NULL
        CONSTRAINT PK_BatchRuns PRIMARY KEY
        CONSTRAINT DF_BatchRuns_Id DEFAULT NEWID(),

    PlanGUID                  uniqueidentifier NULL,

    BatchType                 varchar(50) NOT NULL,
    -- PROVISIONING
    -- REPLICATION
    -- VALIDATION

    TriggerType               varchar(50) NULL,
    -- USER
    -- TEMPORAL
    -- SCHEDULE
    -- PIPELINE

    TemporalWorkflowId        varchar(200) NULL,

    TemporalRunId             varchar(200) NULL,

    FabricJobInstanceId       varchar(200) NULL,

    StartedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_BatchRuns_Started DEFAULT SYSUTCDATETIME(),

    CompletedTimestamp        datetime2 NULL,

    Status                    varchar(50) NOT NULL
        CONSTRAINT DF_BatchRuns_Status DEFAULT ('RUNNING'),
    -- RUNNING
    -- SUCCEEDED
    -- FAILED
    -- CANCELLED

    TotalObjects              int NULL,

    SucceededObjects          int NULL,

    FailedObjects             int NULL,

    ErrorMessage              nvarchar(max) NULL,

    CreatedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_BatchRuns_Created DEFAULT SYSUTCDATETIME(),

    CONSTRAINT FK_BatchRuns_MigrationPlans
        FOREIGN KEY (PlanGUID)
        REFERENCES bronze_replication.MigrationPlans(PlanGUID),

    CONSTRAINT CK_BatchRuns_Counts
        CHECK
        (
            (TotalObjects IS NULL OR TotalObjects >= 0)
            AND (SucceededObjects IS NULL OR SucceededObjects >= 0)
            AND (FailedObjects IS NULL OR FailedObjects >= 0)
        )
);
GO

CREATE INDEX IX_BatchRuns_Status
ON bronze_replication.BatchRuns
(
    Status,
    StartedTimestamp
);
GO


CREATE TABLE bronze_replication.BatchObjectRuns
(
    BatchObjectRunId          uniqueidentifier NOT NULL
        CONSTRAINT PK_BatchObjectRuns PRIMARY KEY
        CONSTRAINT DF_BatchObjectRuns_Id DEFAULT NEWID(),

    BatchRunId                uniqueidentifier NOT NULL,

    ObjectGUID                uniqueidentifier NULL,

    ObjectType                varchar(50) NOT NULL,
    -- TABLE
    -- VIEW
    -- NOTEBOOK
    -- PIPELINE

    ObjectName                nvarchar(500) NULL,

    FabricPipelineRunId       varchar(200) NULL,

    FabricNotebookRunId       varchar(200) NULL,

    StartedTimestamp          datetime2 NOT NULL
        CONSTRAINT DF_BatchObjectRuns_Started DEFAULT SYSUTCDATETIME(),

    CompletedTimestamp        datetime2 NULL,

    RowsRead                  bigint NULL,

    RowsWritten               bigint NULL,

    OldWatermarkValue         nvarchar(1000) NULL,

    NewWatermarkValue         nvarchar(1000) NULL,

    Status                    varchar(50) NOT NULL
        CONSTRAINT DF_BatchObjectRuns_Status DEFAULT ('RUNNING'),

    ErrorMessage              nvarchar(max) NULL,

    CONSTRAINT FK_BatchObjectRuns_BatchRuns
        FOREIGN KEY (BatchRunId)
        REFERENCES bronze_replication.BatchRuns(BatchRunId),

    CONSTRAINT CK_BatchObjectRuns_Rows
        CHECK
        (
            (RowsRead IS NULL OR RowsRead >= 0)
            AND
            (RowsWritten IS NULL OR RowsWritten >= 0)
        )
);
GO

CREATE INDEX IX_BatchObjectRuns_BatchRunId
ON bronze_replication.BatchObjectRuns
(
    BatchRunId,
    Status
);
GO


/* ===========================================================================
   12. RUNTIME CONTRACT VIEW - APPROVED PLANS
   =========================================================================== */

CREATE VIEW bronze_replication.vw_ApprovedPlansReadyToProvision
AS
SELECT
    mp.PlanGUID,
    mp.SourceConnectionName,
    mp.SourceSystemType,
    mp.SourceObjectType,
    mp.SourceObjectName,
    mp.MigrationApproach,
    mp.AnalysisVersion,
    mp.PlanVersion,
    mp.Status,
    mp.TemporalWorkflowId,
    mp.TemporalRunId,
    mp.ApprovedBy,
    mp.ApprovedTimestamp
FROM bronze_replication.MigrationPlans mp
WHERE mp.Status IN ('APPROVED', 'READY_TO_PROVISION');
GO


/* ===========================================================================
   13. RUNTIME CONTRACT VIEW - TABLE PROVISIONING
   =========================================================================== */

CREATE VIEW bronze_replication.vw_TableProvisioningQueue
AS
SELECT
    st.PlanGUID,
    st.GUID AS SourceTableGUID,
    st.ConnectionName,
    st.SourceSystemType,
    st.SourceObjectType,
    st.SourceSchemaName,
    st.SourceTableName,
    st.ReplicationMethod,
    st.ObjectRole,

    st.FabricWorkspaceName,
    st.FabricWorkspaceId,
    st.FabricLakehouseName,
    st.FabricLakehouseId,
    st.FabricLakehouseSchema,
    st.FabricTableName,

    st.ProvisioningStatus,
    st.IsActive
FROM bronze_replication.SourceTables st
INNER JOIN bronze_replication.MigrationPlans mp
    ON mp.PlanGUID = st.PlanGUID
WHERE mp.Status IN
(
    'APPROVED',
    'READY_TO_PROVISION',
    'PROVISIONING'
)
AND st.IsActive = 1
AND st.ProvisioningStatus IN
(
    'PENDING',
    'FAILED'
);
GO


/* ===========================================================================
   14. RUNTIME CONTRACT VIEW - FABRIC VIEW PROVISIONING
   =========================================================================== */

CREATE VIEW bronze_replication.vw_ViewProvisioningQueue
AS
SELECT
    fv.PlanGUID,
    fv.ViewGUID,

    fv.FabricWorkspaceName,
    fv.FabricWorkspaceId,
    fv.FabricLakehouseName,
    fv.FabricLakehouseId,
    fv.FabricSchemaName,

    fv.ViewName,
    fv.ViewType,
    fv.ViewSQL,
    fv.LogicDescription,
    fv.CreationOrder,
    fv.ProvisioningStatus

FROM bronze_replication.FabricViews fv
INNER JOIN bronze_replication.MigrationPlans mp
    ON mp.PlanGUID = fv.PlanGUID

WHERE mp.Status IN
(
    'APPROVED',
    'READY_TO_PROVISION',
    'PROVISIONING'
)
AND fv.IsActive = 1
AND fv.ProvisioningStatus IN
(
    'PENDING',
    'FAILED'
);
GO


/* ===========================================================================
   15. RUNTIME CONTRACT VIEW - REPLICATION PIPELINE
   =========================================================================== */

CREATE VIEW bronze_replication.vw_ReplicationQueue
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


/* ===========================================================================
   16. RUNTIME CONTRACT VIEW - VIEW DEPENDENCIES
   =========================================================================== */

CREATE VIEW bronze_replication.vw_FabricViewDependencies
AS
SELECT
    fv.PlanGUID,

    fv.ViewGUID,
    fv.ViewName,

    fvd.DependencySequence,
    fvd.DependencyType,

    fvd.SourceTableGUID,
    st.SourceTableName,
    st.FabricTableName AS SourceFabricTableName,

    fvd.DependsOnViewGUID,
    dependent_view.ViewName AS DependsOnViewName,

    fvd.ExistingObjectName

FROM bronze_replication.FabricViews fv

INNER JOIN bronze_replication.FabricViewDependencies fvd
    ON fvd.ViewGUID = fv.ViewGUID

LEFT JOIN bronze_replication.SourceTables st
    ON st.GUID = fvd.SourceTableGUID

LEFT JOIN bronze_replication.FabricViews dependent_view
    ON dependent_view.ViewGUID = fvd.DependsOnViewGUID;
GO


/* ===========================================================================
   17. OPTIONAL INITIAL REPLICATION STATE CREATION HELPER VIEW
   =========================================================================== */

CREATE VIEW bronze_replication.vw_ReplicationConfigWithoutState
AS
SELECT
    rc.SourceTableGUID
FROM bronze_replication.ReplicationConfig rc
LEFT JOIN bronze_replication.ReplicationState rs
    ON rs.SourceTableGUID = rc.SourceTableGUID
WHERE rs.SourceTableGUID IS NULL;
GO


/* ===========================================================================
   18. POST-CREATION VALIDATION
   =========================================================================== */

PRINT '==============================================================';
PRINT 'AutoMigrationConfigDB created successfully.';
PRINT 'Schema: bronze_replication';
PRINT '==============================================================';
GO

SELECT
    s.name AS SchemaName,
    t.name AS TableName
FROM sys.tables t
INNER JOIN sys.schemas s
    ON s.schema_id = t.schema_id
WHERE s.name = 'bronze_replication'
ORDER BY t.name;
GO

SELECT
    s.name AS SchemaName,
    v.name AS ViewName
FROM sys.views v
INNER JOIN sys.schemas s
    ON s.schema_id = v.schema_id
WHERE s.name = 'bronze_replication'
ORDER BY v.name;
GO
