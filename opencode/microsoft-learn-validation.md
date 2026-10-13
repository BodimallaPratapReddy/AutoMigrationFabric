# Microsoft Learn MCP validation

Documentation-only OpenCode run using amazon-bedrock/global.anthropic.claude-haiku-5-5 and oracle-view-to-fabric. Microsoft Learn MCP tools were called; the SQL examples below were not executed.

Both topics are covered by official Microsoft Learn articles. The searches did not return a dedicated Spark SQL managed-table reference or a standalone `CREATE VIEW` sample for the SQL analytics endpoint, so those points are based on the closest articles.

## 1. Creating a Delta Lake table with Spark SQL

**Where it runs:** A Fabric Lakehouse, through Spark notebooks. Spark is the write path. The SQL analytics endpoint cannot create tables.

**Example from the official tutorial** (`%%sql` cell):

```sql
CREATE OR REPLACE TABLE delta.`Tables/dbo/fact_sale`
USING DELTA
PARTITIONED BY (Year, Quarter)
AS
SELECT *,
       year(InvoiceDateKey) AS Year,
       quarter(InvoiceDateKey) AS Quarter,
       month(InvoiceDateKey) AS Month
FROM parquet.`Files/wwi-raw-data/full/fact_sale_1y_full`;
```

Key points from the docs:
- Tables in the lakehouse `Tables` folder are Delta tables and show up in the Lakehouse explorer. Fabric's default table format is `delta` (`spark.sql.sources.default`).
- Optional session settings include V-Order and Optimize Write. The article notes that defaults vary by workspace creation date and runtime version.
- The tutorial example uses a path-based `delta.\`Tables/...\`` identifier. I did not confirm from these results the exact syntax for a named managed table, so check that before using it.

Sources:
- https://learn.microsoft.com/fabric/data-engineering/tutorial-lakehouse-data-preparation#create-delta-tables
- https://learn.microsoft.com/fabric/data-engineering/lakehouse-and-delta-tables#apache-spark-engine-and-data-formats
- https://learn.microsoft.com/fabric/data-engineering/lakehouse-and-delta-tables#how-fabric-finds-your-tables-automatically

## 2. Creating a view on the SQL analytics endpoint

**Where it runs:** The lakehouse's auto-provisioned SQL analytics endpoint, using T-SQL in the query editor.

**What the docs say you can do:**
- Run T-SQL `SELECT` queries against Delta tables, including tables reached through shortcuts.
- Create views, functions and stored procedures in T-SQL. These objects persist in the endpoint.
- Apply object- and row-level security.

**What it cannot do:**
- Insert, update or delete data. The endpoint is read-only over Delta data.
- Create, alter or drop tables. Those operations are only supported in Fabric Warehouse, not in the lakehouse endpoint.
- See Delta tables created by Spark code outside the `Tables` area unless they are exposed through shortcuts. Non-Delta formats (Parquet, CSV) are not queryable through the endpoint.

Sources:
- https://learn.microsoft.com/fabric/data-engineering/lakehouse-sql-analytics-endpoint#what-you-can-do
- https://learn.microsoft.com/fabric/data-warehouse/tsql-surface-area#t-sql-surface-area (items 3 and 3.1)
- https://learn.microsoft.com/fabric/data-engineering/lakehouse-overview#lakehouse-sql-analytics-endpoint

## Differences

| Aspect | Delta table via Spark SQL | View via SQL analytics endpoint |
|---|---|---|
| Purpose | Stores data (write path) | Defines a query over existing Delta data (read path) |
| Engine | Apache Spark (Lakehouse notebooks) | T-SQL on the SQL analytics endpoint |
| Creates data | Yes (`CREATE TABLE ... USING DELTA`, `CREATE OR REPLACE TABLE ... AS SELECT`) | No; a view stores no data |
| Read/write | Read and write | Read-only over Delta tables |
| Dialect | Spark SQL | T-SQL |
| Storage location | Lakehouse `Tables` folder in OneLake | Metadata object in the endpoint |
| Visibility | Automatically discovered in the Lakehouse explorer | Available through the endpoint and Power BI connections |

In short, use Spark SQL to build and populate Delta tables, then use T-SQL on the SQL analytics endpoint to build read-only views on top of them. A T-SQL example does not confirm Spark support, and the reverse is also true. The Spark table example and the endpoint view capabilities come from different articles, so verify each syntax against the engine you plan to use.