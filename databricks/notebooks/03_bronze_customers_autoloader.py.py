# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("created_date", StringType(), True)
])

# COMMAND ----------

customer_path = "/FileStore/tables/customers"

df_customers = (
    spark.readStream
    .format("CloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", "/tmp/customer_schema")
    .schema(schema)
    .load(customer_path)
)

# COMMAND ----------

df_customers_bronze = (
    df_customers
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("crm"))
)

# COMMAND ----------

query = (
    df_customers_bronze.writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/customer_checkpoint")
    .outputMode("append")
    .toTable("migration_project_db_ws.bronze.customers")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM migration_project_db_ws.bronze.customers;