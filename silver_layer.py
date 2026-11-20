# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Read Bronze table
bronze_df = spark.table("workspace.default.bronze_sales")

# COMMAND ----------

# Remove records where UnitPrice <= 0
silver_df = bronze_df.filter(
    F.col("UnitPrice") > 0
)

# Handle NULL CustomerID values
silver_df = silver_df.withColumn(
    "customer_id",
    F.when(
        F.col("CustomerID").isNull(),
        F.lit("Guest")
    ).otherwise(F.col("CustomerID").cast("string"))
)

# Convert InvoiceDate from string to timestamp
silver_df = silver_df.withColumn(
    "invoice_timestamp",
    F.to_timestamp(F.col("InvoiceDate"), "M/d/yyyy H:mm")
)

# Inspect transformed data
display(silver_df.limit(10))

# COMMAND ----------

silver_table = "workspace.default.silver_sales"

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver table created: {silver_table}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM workspace.default.silver_sales;