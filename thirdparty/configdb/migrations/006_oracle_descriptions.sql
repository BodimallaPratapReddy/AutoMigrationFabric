/* Additive and rerunnable. No existing descriptions are rewritten. */
IF COL_LENGTH('bronze_replication.SourceTableColumns', 'Description') <> -1
    ALTER TABLE bronze_replication.SourceTableColumns ALTER COLUMN Description nvarchar(max) NULL;
GO
IF COL_LENGTH('bronze_replication.SourceTables', 'Description') IS NULL
    ALTER TABLE bronze_replication.SourceTables ADD Description nvarchar(max) NULL;
GO
IF COL_LENGTH('bronze_replication.SourceTables', 'EtrmDescription') IS NULL
    ALTER TABLE bronze_replication.SourceTables ADD EtrmDescription nvarchar(max) NULL;
GO
IF COL_LENGTH('bronze_replication.SourceTableColumns', 'EtrmDescription') IS NULL
    ALTER TABLE bronze_replication.SourceTableColumns ADD EtrmDescription nvarchar(max) NULL;
GO
