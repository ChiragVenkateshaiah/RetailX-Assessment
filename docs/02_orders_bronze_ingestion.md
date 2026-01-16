# Overview
This module ingests raw order data into the Bronze layer of the lakehouse using Apache Spark + Delta Lake.

The Bronze layer acts as the system of record, preserving raw data with minimal transformation while enriching it with ingestion metadata for lineage and auditability

## Target Table
- Catalog: `migration_project_db_ws`
- Schema: `bronze`
- Table: `orders`
- Format: Delta Lake

```sql
migration_project_db_ws.bronze.orders
```

---

## Source Data (Assessment Context)
### Why DBFS FileStore?
For this assessment, the source CSV file is uploaded to Databricks FileStore, a workspace-managed filesystem backed by DBFS

#### Path used:
```bash
/FileStore/tables/sample_orders.csv
```

### How the file is uploaded
1. Navigate to Databricks Workspace
2. Click Data -> Add Data
3. Upload `sample_orders.csv`
4. Databricks automatically places the file under:
```bash
/FileStore/tables/
```
#### Screenshots Captured
- File upload confirmation
- File path showing `/FileStore/tables/sample_orders.csv`

### Why DBFS is appropriate for the assessment
- No external cloud permissions required
- Fully deterministic and reproducible
- Eliminates infrastructure noise
- Allows focus on Bronze layer design and governance
> Note: DBFS is used here as a stand-in raw landing zone for assessment purposes.

## Production Perspective
In a production environment, this same ingestion logic would read from:
- Azure Data Lake Storage (ADLS Gen 2)
- Amazon S3
- Or anothe cloud object store via a Unity Catalog External Location
> Only the source path changes -- the Bronze ingestion logic remains identical

Example:
```bash
abfss://bronze@datalake.dfs.core.windows.net/orders/
```

---

## Ingestion Logic
### Notebook
`02_bronze_orders_ingestion.py`

### Code Walkthrough
```python
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define explicit schema to avoid inference-related issues
schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# Source path (DBFS for assessment)
orders_path = "/FileStore/tables/sample_orders.csv"

# Read raw CSV data
df_orders = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(orders_path)
)

# Add ingestion metadata
df_orders_bronze = (
    df_orders
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("oracle"))
)

# Write to Bronze Delta table
df_orders_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("retailx.bronze.orders")
```

---

## Ingestion Metadata
The following columns are added at ingestion time:
| Column                | Purpose                                   |
| --------------------- | ----------------------------------------- |
| `ingestion_timestamp` | Tracks when the data entered the lake     |
| `source_system`       | Identifies upstream system (e.g., Oracle) |

These fields support:
- Lineage tracking
- Debugging
- Replayability
- Audit requirements

---

## Validation
After ingestion, data is validated using:
```sql
SELECT * FROM migration_project_db_ws.bronze.orders LIMIT 10;
```
