# Databricks notebook source
# Define the landing zone
base_path = "/Volumes/workspace/default/global_retail_landing"

# Define source, schema, and checkpoint paths
source_path = f"{base_path}/data.csv"
schema_path = f"{base_path}/schema/"
checkpoint_path = f"{base_path}/checkpoints/"

# Create directories if they don't exist
dbutils.fs.mkdirs(schema_path)
dbutils.fs.mkdirs(checkpoint_path)

print("GlobalRetail-360 landing zone prepared!")
print("Source:", source_path)
print("Schema:", schema_path)
print("Checkpoint:", checkpoint_path)

# COMMAND ----------


# Initialize Auto Loader

bronze_stream = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("pathGlobFilter", "*.csv")
    .load(base_path)
)

# Inspect the inferred schema
bronze_stream.printSchema()

# COMMAND ----------


from pyspark.sql.functions import (
    col,
    current_timestamp
)

# Add ingestion metadata
bronze_with_metadata = (
    bronze_stream
    .withColumn(
        "_input_file_name",
        col("_metadata.file_path")
    )
    .withColumn(
        "_ingest_timestamp",
        current_timestamp()
    )
)

# Inspect the resulting schema
bronze_with_metadata.printSchema()

# COMMAND ----------


bronze_query = (
    bronze_with_metadata.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("workspace.default.bronze_sales")
)

bronze_query.awaitTermination()

print("Bronze ingestion completed successfully!")
