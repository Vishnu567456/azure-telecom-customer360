# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

PROBE_PATH = (
    "abfss://telecom@vishnutelecom360dev.dfs.core.windows.net/"
    "file-events-probe"
)

PROBE_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("message", StringType(), True),
    StructField("created_at", StringType(), True),
])

@dp.table(
    name="file_events_probe",
    comment="Managed file events Auto Loader verification stream"
)
def file_events_probe():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "true")
        .schema(PROBE_SCHEMA)
        .load(PROBE_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at")
        )
    )
