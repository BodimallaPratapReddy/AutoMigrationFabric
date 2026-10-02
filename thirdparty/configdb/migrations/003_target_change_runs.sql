/* Apply once to the Fabric configuration SQL database before running
   ALTER_BACKFILL or REPLACE_FULL from nb_migration_provisioning. */

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
