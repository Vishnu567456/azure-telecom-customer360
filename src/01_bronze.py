# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F

ROOT = "/Volumes/vishnu_telecom/ops/files"

CUSTOMER_SCHEMA = """
event_id STRING,
customer_id STRING,
sequence BIGINT,
effective_at STRING,
customer_name STRING,
email STRING,
region STRING,
segment STRING,
op STRING
"""

SUBSCRIPTION_SCHEMA = """
event_id STRING,
subscription_id STRING,
customer_id STRING,
sequence BIGINT,
effective_at STRING,
plan_id STRING,
status STRING,
op STRING
"""

INVOICE_SCHEMA = """
invoice_id STRING,
customer_id STRING,
invoice_date STRING,
amount DOUBLE,
status STRING
"""

PAYMENT_SCHEMA = """
payment_id STRING,
invoice_id STRING,
customer_id STRING,
payment_time STRING,
amount DOUBLE,
status STRING
"""

USAGE_SCHEMA = """
event_id STRING,
event_time STRING,
customer_id STRING,
tower_id STRING,
event_type STRING,
bytes_up BIGINT,
bytes_down BIGINT,
duration_seconds BIGINT
"""

REGION_SCHEMA = """
region_id STRING,
region_name STRING
"""

PLAN_SCHEMA = """
plan_id STRING,
plan_name STRING,
monthly_fee DOUBLE,
data_gb INT
"""

TOWER_SCHEMA = """
tower_id STRING,
region_id STRING,
city STRING,
technology STRING
"""


def autoload_json(path, schema):
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(schema)
        .load(path)
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="vishnu_telecom.bronze.customers_cdc_raw",
    comment="Raw customer CDC envelopes incrementally ingested with Auto Loader.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def customers_cdc_raw():
    return autoload_json(
        f"{ROOT}/landing/customers_cdc",
        CUSTOMER_SCHEMA
    )


@dp.table(
    name="vishnu_telecom.bronze.subscriptions_cdc_raw",
    comment="Raw subscription CDC envelopes incrementally ingested with Auto Loader.",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true"
    }
)
def subscriptions_cdc_raw():
    return autoload_json(
        f"{ROOT}/landing/subscriptions_cdc",
        SUBSCRIPTION_SCHEMA
    )


@dp.table(
    name="vishnu_telecom.bronze.invoices_raw",
    comment="Raw telecom invoice stream.",
    table_properties={"quality": "bronze"}
)
def invoices_raw():
    return autoload_json(
        f"{ROOT}/landing/invoices",
        INVOICE_SCHEMA
    )


@dp.table(
    name="vishnu_telecom.bronze.payments_raw",
    comment="Raw telecom payment stream.",
    table_properties={"quality": "bronze"}
)
def payments_raw():
    return autoload_json(
        f"{ROOT}/landing/payments",
        PAYMENT_SCHEMA
    )


@dp.table(
    name="vishnu_telecom.bronze.usage_events_raw",
    comment="Raw telecom usage/CDR-style event stream.",
    table_properties={"quality": "bronze"}
)
def usage_events_raw():
    return autoload_json(
        f"{ROOT}/landing/usage_events",
        USAGE_SCHEMA
    )


@dp.table(
    name="vishnu_telecom.bronze.digital_events_raw",
    comment="Semi-structured digital events retained as Databricks VARIANT.",
    table_properties={"quality": "bronze"}
)
def digital_events_raw():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(f"{ROOT}/landing/digital_events")
        .select(
            F.try_parse_json(F.col("value")).alias("raw_event"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.current_timestamp().alias("_ingested_at")
        )
    )


@dp.materialized_view(
    name="vishnu_telecom.bronze.ref_regions",
    comment="Region reference data."
)
def ref_regions():
    return (
        spark.read
        .schema(REGION_SCHEMA)
        .json(f"{ROOT}/reference/regions")
    )


@dp.materialized_view(
    name="vishnu_telecom.bronze.ref_plans",
    comment="Telecom plan reference data."
)
def ref_plans():
    return (
        spark.read
        .schema(PLAN_SCHEMA)
        .json(f"{ROOT}/reference/plans")
    )


@dp.materialized_view(
    name="vishnu_telecom.bronze.ref_cell_towers",
    comment="Cell-tower reference data."
)
def ref_cell_towers():
    return (
        spark.read
        .schema(TOWER_SCHEMA)
        .json(f"{ROOT}/reference/cell_towers")
    )
