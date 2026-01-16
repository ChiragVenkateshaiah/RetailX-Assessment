# Databricks notebook source
# Read silver orders
silver_orders_df = spark.read.table("silver.orders")
silver_orders_df.display()

# COMMAND ----------

# Business Question
# How much revenue do we generate per day?

from pyspark.sql.functions import sum

gold_daily_sales_df = (
    silver_orders_df
    .groupBy("order_date")
    .agg(
        sum("amount").alias("daily_sales")
    )
    .orderBy("order_date")
)

# COMMAND ----------

gold_daily_sales_df.display()

# COMMAND ----------

gold_daily_sales_df.write.mode("overwrite").saveAsTable("gold.daily_sales")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.daily_sales;

# COMMAND ----------

# Revenue per customer

from pyspark.sql.functions import sum

gold_revenue_per_customer_df = (
    silver_orders_df
    .groupBy("customer_id")
    .agg(
        sum("amount").alias("total_revenue")
    )
    .orderBy("total_revenue", ascending=False)
)

# COMMAND ----------

gold_revenue_per_customer_df.display()

# COMMAND ----------

gold_revenue_per_customer_df.write.mode("overwrite").saveAsTable("gold.revenue_per_customer")

# COMMAND ----------

# Enriched Gold Aggregation

silver_orders_df = spark.read.table("silver.orders")
silver_customers_df = spark.read.table("silver.customers")

# COMMAND ----------

gold_daily_sales_country_df = (
    silver_orders_df.alias("o")
    .join(
        silver_customers_df.alias("c"),
        on="customer_id",
        how="inner"
    )
    .groupBy("order_date", "country")
    .agg(
        sum("amount").alias("daily_sales")
    )
    .orderBy("order_date", "country")
)

# COMMAND ----------

gold_daily_sales_country_df.display()