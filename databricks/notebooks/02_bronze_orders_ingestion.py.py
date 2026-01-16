# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("order_date", StringType(), True),
    StructField("amount", DoubleType(), True)
])

# COMMAND ----------

order_path = "/FileStore/tables/sample_orders.csv"

# COMMAND ----------

df_orders = (
    spark.read
    .option("header", "true")
    .schema(schema)
    .csv(order_path)
)

# COMMAND ----------

df_orders_bronze = (
    df_orders
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_system", lit("oracle"))
)

# COMMAND ----------

df_orders_bronze.write.format("delta").mode("append").saveAsTable("migration_project_db_ws.bronze.orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM migration_project_db_ws.bronze.orders LIMIT 10;