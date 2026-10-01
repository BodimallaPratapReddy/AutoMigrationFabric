-- Apply to databases already created from config_db_new_database.sql.
IF COL_LENGTH('bronze_replication.MigrationPlans', 'RuntimePlanHash') IS NULL
BEGIN
    ALTER TABLE bronze_replication.MigrationPlans
    ADD RuntimePlanHash char(64) NULL;
END;
GO
