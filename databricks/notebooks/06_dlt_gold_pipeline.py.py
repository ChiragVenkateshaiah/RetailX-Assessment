# Databricks notebook source
import dlt
from pyspark.sql.functions import sum

# COMMAND ----------

# DLT Gold: Daily Sales

@dlt.table(
    name="daily_sales",
    comment="Gold table: Daily total sales"
)
@dlt.expect("valid_order_date", "order_date IS NOT NULL")
@dlt.expect("positive_sales", "daily_sales > 0")
def daily_sales():
    return(
        spark.read.table("silver.orders")
        .groupBy("order_date")
        .agg(sum("amount").alias("daily_sales"))
    )

# COMMAND ----------

# DLT Gold: Revenue per Customer

@dlt.table(
    name="revenue_per_customer",
    comment="Gold Table: Total revenue per customer"
)
@dlt.expect("valid_customer", "customer_id IS NOT NULL")
@dlt.expect("positive_revenue", "total_revenue > 0")
def revenue_per_customer():
    return (
        spark.read.table("silver.orders")
        .groupBy("customer_id")
        .agg(sum("amount").alias("total_revenue"))
    )

# COMMAND ----------

@dlt.table(
    name="daily_sales_by_country",
    comment="Gold Table: Daily sales enriched with customer country"
)
@dlt.expect("valid_country", "country IS NOT NULL")
@dlt.expect("positive_sales", "daily_sales > 0")
def daily_sales_by_country():
    orders_df = spark.read.table("silver.orders")
    customers_df = spark.read.table("silver.customers")

    return(
        orders_df.alias("o")
        .join(
            customers_df.alias("c"),
            on="customer_id",
            how="inner"
        )
        .groupBy("order_date", "country")
        .agg(sum("amount").alias("daily_sales"))
    )