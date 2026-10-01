# Table: bronze_replication.DBConnections

| ColumnName       | DataType  | ColumnDescription                                                                                   |
|------------------|-----------|---------------------------------------------------------------------------------------------------|
| Id               | bigint    | Unique identifier for each database connection configuration.                                      |
| SourceType       | varchar   | Type of source system. Examples: Oracle, SAP, SQLServer, PostgreSQL, FabricSQL.                    |
| ConnectionName   | varchar   | User-friendly unique name identifying the database connection configuration.                       |
| ConnectionDetails| json      | Source-specific connection configuration stored as JSON, such as host, port, service name, etc.   |
| ConnectionTags   | json      | JSON array of tags used to categorize and search database connections.                             |
| CreatedTimestamp | datetime2 | UTC timestamp when the database connection configuration was created.                              |
| UpdatedTimestamp | datetime2 | UTC timestamp when the database connection configuration was last updated.                         |

Sample Data:
| Id | SourceType | ConnectionName | ConnectionDetails | ConnectionTags | CreatedTimestamp | UpdatedTimestamp |
|----|------------|----------------|-------------------|----------------|------------------|------------------|
| 3  | Oracle     | ORACLE-RPTQ    | {...}             | ["Qty","Ebs"]  | 9/18/2026 10:17  | 9/18/2026 10:17  |
| 4  | Oracle     | ORACLE-RPTP    | {...}             | ["production"] | 9/18/2026 10:20  | 9/18/2026 10:20  |
| 5  | SAP S/4HANA| SAP-P05        | {...}             | ["production"] | 9/18/2026 12:59  | 9/18/2026 12:59  |

---

# Table: bronze_replication.FabricWorkspaces

| ColumnName       | DataType  | ColumnDescription |
|------------------|-----------|-------------------|
| Id               | int       |                   |
| WorkspaceName    | nvarchar  |                   |
| WorkspaceId      | nvarchar  |                   |
| CreatedTimestamp | datetime2 |                   |
| UpdatedTimestamp | datetime2 |                   |

Sample Data:
| Id | WorkspaceName | WorkspaceId                             | CreatedTimestamp   | UpdatedTimestamp   |
|----|---------------|---------------------------------------|--------------------|--------------------|
| 1  | Test_Gopi     | 5a590541-0088-465b-b8a3-d7609f270a5f  | 9/21/2026 2:23:53  | 9/21/2026 2:23:53  |

---

# Table: bronze_replication.SourceTableColumns

| ColumnName     | DataType       | ColumnDescription                                                        |
|----------------|----------------|--------------------------------------------------------------------------|
| GUID           | uniqueidentifier| Unique identifier of the source table.                                  |
| Sno            | int            | Ordinal position of the column in the source table.                     |
| ColumnName     | varchar        | Name of the column in the source table.                                 |
| Description    | varchar        | Business or technical description of the source column.                 |
| SourceDataType | varchar        | Original data type of the column in the source system.                  |
| FabricDataType | varchar        | Target Microsoft Fabric data type mapped from the source data type.     |

Sample Data:
| GUID                                   | Sno | ColumnName       | Description | SourceDataType | FabricDataType |
|----------------------------------------|-----|------------------|-------------|----------------|----------------|
| 279cc8af-7785-4916-9f37-96ba1b9e5a7d | 1   | PO_HEADER_ID     |             | NUMBER         | DOUBLE         |
| 279cc8af-7785-4916-9f37-96ba1b9e5a7d | 2   | AGENT_ID         |             | NUMBER(9)      | INT            |
| 279cc8af-7785-4916-9f37-96ba1b9e5a7d | 3   | TYPE_LOOKUP_CODE |             | VARCHAR2(25)   | STRING         |

---

# Table: bronze_replication.SourceTables

| ColumnName          | DataType       | ColumnDescription                                                        |
|---------------------|----------------|--------------------------------------------------------------------------|
| GUID                | uniqueidentifier| Globally unique identifier for the source table metadata record.        |
| ConnectionName      | varchar        | Name of the source database connection associated with the source table.|
| SourceTableName     | varchar        | Name of the table in the source system.                                 |
| CreatedTimestamp    | datetime2      | UTC timestamp when the source table metadata record was created.        |
| UpdatedTimestamp    | datetime2      | UTC timestamp when the source table metadata record was last updated.   |
| DataSourceType      | varchar        |                                                                          |
| ApplicationComponent| varchar        |                                                                          |
| FabricWorkspaceName | nvarchar       |                                                                          |
| FabricWorkspaceId   | nvarchar       |                                                                          |
| FabricLakehouseName | nvarchar       |                                                                          |
| FabricLakehouseSchema| nvarchar      |                                                                          |
| FabricTableName     | nvarchar       |                                                                          |
| ProvisionedTimestamp| datetime2      |                                                                          |
| IsActive            | bit            |                                                                          |
| Delta               | nvarchar       |                                                                          |

Sample Data:
| GUID                                   | ConnectionName | SourceTableName     | CreatedTimestamp   | UpdatedTimestamp   | IsActive |
|----------------------------------------|----------------|---------------------|--------------------|--------------------|----------|
| 279cc8af-7785-4916-9f37-96ba1b9e5a7d | ORACLE-RPTP    | PO.PO_HEADERS_ALL    | 9/26/2026 3:21:41  | 9/26/2026 3:22:59  | True     |
| 12ce2829-cd27-4257-8da0-999d87880a2d | SAP-D05        | ZBRONZE_VBAK         | 9/26/2026 3:17:47  | 9/26/2026 3:18:07  | True     |

---

# Table: bronze_replication.watermark_control

| ColumnName              | DataType  | ColumnDescription |
|-------------------------|-----------|-------------------|
| source_table_full_name   | varchar   |                   |
| destination_table_full_name| varchar |                   |
| watermark_column        | varchar   |                   |
| watermark_column_data_type| varchar  |                   |
| last_watermark_value    | varchar   |                   |
| max_row_fetch           | int       |                   |
| ingestion_flag          | bit       |                   |
| load_strategy           | varchar   |                   |
| merge_key_column        | varchar   |                   |
| last_modified_timestamp | datetime