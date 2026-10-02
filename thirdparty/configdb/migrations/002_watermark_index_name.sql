-- Apply to databases already created from config_db_new_database.sql.
IF COL_LENGTH('bronze_replication.ReplicationConfig', 'WatermarkIndexName') IS NULL
BEGIN
    ALTER TABLE bronze_replication.ReplicationConfig
    ADD WatermarkIndexName varchar(256) NULL;
END;
GO
