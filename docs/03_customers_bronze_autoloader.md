# Bronze Customers Ingestion -- Auto Loader

## Overview
This module implements incremental Bronze ingestion for customer master data using Databricks Auto Loader.
The goal is to land customer files into a Unity Catalog-governed Bronze Delta table with minimal transformation while ensuring scalability, fault tolerance, and exactly-once ingestion semantics.

---

## Target Table
- Catalog: `migration_project_db_ws`
- Schema: `bronze`
- Table: `customers`
- Format: Delta Lake
- Ingestion Type: Streaming (Auto Loader)

```sql
migration_project_db_ws.bronze.customers
```
---

## Source Configuration
### Source Directory
```text
/FileStore/tables/customers/
```
> This directory is monitored continuously by Auto Loader

### Why DBFS for the Assessment?
- Simplifies ingestion (no external credentials required)
- Eliminates infrastructure noise
- Allows focus on lakehouse design and ingestion logic

> In production, this directory would map to a cloud object store (e.g., ADLS Gen2) via a Unity Catalog External Location.

---

## Schema Definition Explicit
An explicit schema is defined to:
- Prevent inference issues
- Avoid silent schema drift
- Align with production ingestion practices

```python
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_date", StringType(), True)
])
```
---

## Auto Loader Ingestion Logic
### Notebook
`03_customers_bronze_autoloader.py`
#### Step 1 -- Read Stream Using Auto Loader
```python
df_customers = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/tmp/customer_schema")
    .schema(schema)
    .load("/FileStore/tables/customers/")
)
```
#### Key points:
- `cloudFiles` enables Auto Loader
- Folder-based ingestion supports incremental file arrival
- `schemaLocation` stores inferred/validated schema metadata

---

#### Step 2 -- Add Bronze Ingestion Metadata
```python
from pyspark.sql.functions import current_timestamp, lit

df_customers_bronze = (
    df_customers
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("crm"))
)
```
#### Purpose of metadata:
- Lineage tracking
- Auditability
- Replay and debugging support

---

#### Step 3 -- Write Stream to Bronze Delta Table
```python
query = (
    df_customers_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/customer_checkpoint")
    .outputMode("append")
    .toTable("retailx.bronze.customers")
)
```
#### Why this matters:
- Checkpoints ensure exactly-once processing
- Append mode preserves immutability of Bronze data
- Writing to a UC table enables goverance and discoverability

---

## Step-by-Step Simulation of Incremental Ingestion
### Step 1 -- Start the Auto Loader Stream
- Run the notebook before uploading any files
- Streaming query enters `RUNNING` state

### Step 2 -- Upload First File
Upload:
```text
sample_customers_2024_01.csv
```
Destination:
```text
/FileStore/tables/customers/
```
#### Auto Loader:
- Detects the new file
- Ingest it once
- Writes records to `migration_project_db_ws.bronze.customers`

### Step 3 -- Upload Second File (Incremental Test)
Upload:
```text
sample_customers_2024_02.csv
```
#### Auto Loader:
- Detects only the new file
- Does NOT reprocess the first file
- Appends new records to Bronze

### Step 4 -- Validation
```sql
SELECT COUNT(*) FROM migration_project_db_ws.bronze.customers;
```
Row count increases incrementally with each new file.

---

## Production Alignment Note
In production:
- The monitored directory would be an external location (ADLS Gen2/S3)
- Files would arrive via upstream systems or pipelines
- The Auto Loader logic would remain unchanged
> Only the source path changes.

---

## Summary
- Auto Loader enables scalable, incremental ingestion
- Bronze layer remains raw and immutable
- Schema and ingestion metadata are enforced
- Fully aligned with lakehouse best practices
