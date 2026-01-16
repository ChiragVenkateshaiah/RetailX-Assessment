# Databricks notebook source
# Read Bronze Orders (Validation)

bronze_orders_df = spark.read.table("bronze.orders")
bronze_orders_df.display()

# COMMAND ----------

# Inspect Bronze Schema

bronze_orders_df.printSchema()

# COMMAND ----------

# Silver transformation

from pyspark.sql.functions import col, to_date, row_number
from pyspark.sql.window import Window


# Read Bronze
bronze_orders_df = spark.read.table("bronze.orders")

# Deduplication window
window_spec = Window.partitionBy("order_id").orderBy(col("ingestion_timestamp").desc())

silver_orders_df = (
    bronze_orders_df
    # Data quality filters
    .filter(col("order_id").isNotNull())
    .filter(col("customer_id").isNotNull())
    .filter(col("amount") > 0)

    # Convert order_date from int to date (yyyyMMdd)
    .withColumn(
        "order_date",
        to_date(col("order_date").cast("string"))
    )

    # Deduplication
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num") == 1)
    .drop("row_num")
)

# COMMAND ----------

silver_orders_df.display()

# COMMAND ----------

# write silver orders table

silver_orders_df.write.mode("overwrite").saveAsTable("silver.orders")

# COMMAND ----------

spark.read.table("silver.orders").printSchema()