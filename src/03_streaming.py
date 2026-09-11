# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


# ============================================================
# USAGE / CDR EVENTS
# ============================================================

def typed_usage():
    return (
        spark.readStream
        .table("vishnu_telecom.bronze.usage_events_raw")
        .withColumn(
            "event_time",
            F.expr("try_cast(event_time AS TIMESTAMP)")
        )
        .withColumn(
            "event_type",
            F.upper(F.trim(F.col("event_type")))
        )
    )


USAGE_VALID = """
_rescued_data IS NULL
AND event_id IS NOT NULL
AND event_time IS NOT NULL
AND customer_id IS NOT NULL
AND tower_id IS NOT NULL
AND event_type IN ('DATA','VOICE','SMS')
AND COALESCE(bytes_up, 0) >= 0
AND COALESCE(bytes_down, 0) >= 0
AND COALESCE(duration_seconds, 0) >= 0
"""


@dp.table(
    name="vishnu_telecom.ops.usage_quarantine",
    comment="Rejected telecom usage events."
)
def usage_quarantine():
    return (
        typed_usage()
        .filter(~F.coalesce(F.expr(USAGE_VALID), F.lit(False)))
        .withColumn(
            "reject_reason",
            F.lit("Invalid telecom usage event")
        )
    )


@dp.table(
    name="vishnu_telecom.silver.usage_events",
    comment="Validated and event-time deduplicated telecom usage events.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)
@dp.expect_or_drop(
    "valid_usage_event",
    USAGE_VALID
)
def usage_events():
    return (
        typed_usage()
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )


# Separate streaming stage for aggregation.
@dp.table(
    name="vishnu_telecom.gold.usage_5m",
    comment="Five-minute telecom usage metrics by event type.",
    table_properties={"quality": "gold"},
    cluster_by_auto=True
)
def usage_5m():
    return (
        spark.readStream
        .table("vishnu_telecom.silver.usage_events")
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "5 minutes"),
            F.col("event_type")
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("bytes_up").alias("bytes_up"),
            F.sum("bytes_down").alias("bytes_down"),
            F.sum("duration_seconds").alias("duration_seconds")
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "bytes_up",
            "bytes_down",
            "duration_seconds"
        )
    )


# ============================================================
# DIGITAL EVENTS — VARIANT
# ============================================================

def parsed_digital():
    df = spark.readStream.table(
        "vishnu_telecom.bronze.digital_events_raw"
    )

    return df.select(
        F.try_variant_get(
            "raw_event", "$.event_id", "STRING"
        ).alias("event_id"),

        F.try_variant_get(
            "raw_event", "$.event_time", "STRING"
        ).alias("_event_time_raw"),

        F.try_variant_get(
            "raw_event", "$.customer_id", "STRING"
        ).alias("customer_id"),

        F.try_variant_get(
            "raw_event", "$.event_name", "STRING"
        ).alias("event_name"),

        F.try_variant_get(
            "raw_event", "$.payload", "VARIANT"
        ).alias("payload"),

        "raw_event",
        "_source_file",
        "_ingested_at"
    ).withColumn(
        "event_time",
        F.expr("try_cast(_event_time_raw AS TIMESTAMP)")
    ).drop("_event_time_raw")


DIGITAL_VALID = """
raw_event IS NOT NULL
AND event_id IS NOT NULL
AND event_time IS NOT NULL
AND customer_id IS NOT NULL
AND event_name IS NOT NULL
"""


@dp.table(
    name="vishnu_telecom.ops.digital_quarantine",
    comment="Malformed or invalid semi-structured digital events."
)
def digital_quarantine():
    return (
        parsed_digital()
        .filter(~F.coalesce(F.expr(DIGITAL_VALID), F.lit(False)))
        .withColumn(
            "reject_reason",
            F.lit("Invalid digital event")
        )
    )


@dp.table(
    name="vishnu_telecom.silver.digital_events",
    comment="Validated semi-structured digital events retaining VARIANT payload.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)
@dp.expect_or_drop(
    "valid_digital_event",
    DIGITAL_VALID
)
def digital_events():
    return (
        parsed_digital()
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )
