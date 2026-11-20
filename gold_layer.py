# Databricks notebook source
from pyspark.sql import functions as F

# silv table
silver_df = spark.table("workspace.default.silver_sales")

print("Silver records:", silver_df.count())
silver_df.printSchema()

# COMMAND ----------


# GD Table: Total Revenue by Country

total_revenue_by_country = (
    silver_df
    .withColumn(
        "revenue",
        F.col("Quantity") * F.col("UnitPrice")
    )
    .groupBy("Country")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
        F.countDistinct("InvoiceNo").alias("total_transactions"),
        F.sum("Quantity").alias("total_units_sold")
    )
    .orderBy(F.desc("total_revenue"))
)

# Save as Delta table
(
    total_revenue_by_country.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.total_revenue_by_country")
)

display(total_revenue_by_country)


# COMMAND ----------


# GD Table 2: High-Value Customers

high_value_customers = (
    silver_df
    .filter(F.col("customer_id") != "Guest")
    .withColumn(
        "revenue",
        F.col("Quantity") * F.col("UnitPrice")
    )
    .groupBy("customer_id")
    .agg(
        F.round(F.sum("revenue"), 2).alias("total_spend"),
        F.countDistinct("InvoiceNo").alias("total_transactions"),
        F.sum("Quantity").alias("total_units_purchased")
    )
    .filter(F.col("total_spend") > 5000)
    .orderBy(F.desc("total_spend"))
)

# Save as Delta table
(
    high_value_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.default.high_value_customers")
)

display(high_value_customers)


# COMMAND ----------

spark.sql("""
OPTIMIZE workspace.default.high_value_customers
ZORDER BY (customer_id)
""")

spark.sql("""
OPTIMIZE workspace.default.total_revenue_by_country
ZORDER BY (Country)
""")