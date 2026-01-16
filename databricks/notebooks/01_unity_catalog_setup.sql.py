# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC -- Use catalog
# MAGIC USE CATALOG migration_project_db_ws;
# MAGIC
# MAGIC -- Create schemas
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS IN migration_project_db_ws;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CREATE EXTERNAL LOCATION retailx_ext
# MAGIC -- URL 'abfss://datalake@storageaccount.dfs.core.windows.net/'
# MAGIC -- WITH (STORAGE CREDENTIAL my_cred);
# MAGIC