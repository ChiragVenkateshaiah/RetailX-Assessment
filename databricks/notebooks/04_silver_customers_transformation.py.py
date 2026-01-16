# Databricks notebook source
# Validation of bronze layer
spark.read.table("migration_project_db_ws.bronze.customers").display()

# COMMAND ----------

# Expection from the Bronze Layer
# Columns are messy (string types, nulls, duplicates)

# COMMAND ----------

# Inspect Bronze Schema
spark.read.table("migration_project_db_ws.bronze.customers").printSchema()

# COMMAND ----------

from pyspark.sql.functions import col, to_date, row_number, lower
from pyspark.sql.window import Window


bronze_df = spark.read.table("bronze.customers")


# COMMAND ----------

# Standardize Column Names
customers_df = (
    bronze_df
    .withColumnRenamed("CUSTOMER_ID", "customer_id")
    .withColumnRenamed("FIRST_NAME", "first_name")
    .withColumnRenamed("LAST_NAME", "last_name")
    .withColumnRenamed("EMAIL", "email")
    .withColumnRenamed("COUNTRY", "country")
    .withColumnRenamed("CREATED_DATE", "created_date")
)

# COMMAND ----------

# Clean and Transform Data

window_spec = Window.partitionBy("customer_id").orderBy(col("created_date").desc())

silver_customers_df = (
    customers_df
    .withColumn("customer_id", col("customer_id").cast("int"))
    .withColumn("created_date", to_date(col("created_date")))
    .withColumn("email", lower(col("email")))
    .filter(col("customer_id").isNotNull())
    .filter(col("email").isNotNull())
    .withColumn("row_num", row_number().over(window_spec))
    .filter(col("row_num")==1)
    .drop("row_num")
)

# COMMAND ----------

silver_customers_df.display()

# COMMAND ----------

silver_customers_df.write.mode("overwrite").saveAsTable("migration_project_db_ws.silver.customers")

# COMMAND ----------

spark.read.table("silver.customers").printSchema()