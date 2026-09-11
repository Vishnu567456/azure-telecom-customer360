# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import types as T


EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType(), False),
    T.StructField("event_time", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("tower_id", T.StringType(), False),
    T.StructField("event_type", T.StringType(), False),
    T.StructField("bytes_up", T.LongType(), True),
    T.StructField("bytes_down", T.LongType(), True),
    T.StructField("duration_seconds", T.LongType(), True),
])


def _eventhubs_options():
    namespace = spark.conf.get("telecom.eventhubs.namespace")
    eventhub_name = spark.conf.get("telecom.eventhubs.name")
    service_credential = spark.conf.get("telecom.eventhubs.service_credential")

    return {
        "kafka.bootstrap.servers": f"{namespace}.servicebus.windows.net:9093",
        "subscribe": eventhub_name,
        "databricks.serviceCredential": service_credential,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false",
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "30000",
    }


@dp.table(
    name="vishnu_telecom.bronze.usage_events_eventhub_raw",
    comment="Raw realtime telecom usage events ingested from Azure Event Hubs through its Kafka endpoint.",
    table_properties={"quality": "bronze"},
)
def usage_events_eventhub_raw():
    return (
        spark.readStream
        .format("kafka")
        .options(**_eventhubs_options())
        .load()
        .select(
            F.col("value").cast("string").alias("json_payload"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )


def _typed_realtime_usage():
    return (
        spark.readStream
        .table("vishnu_telecom.bronze.usage_events_eventhub_raw")
        .withColumn("payload", F.from_json("json_payload", EVENT_SCHEMA))
        .select(
            F.col("payload.event_id").alias("event_id"),
            F.expr("try_cast(payload.event_time AS TIMESTAMP)").alias("event_time"),
            F.col("payload.customer_id").alias("customer_id"),
            F.col("payload.tower_id").alias("tower_id"),
            F.upper(F.trim(F.col("payload.event_type"))).alias("event_type"),
            F.coalesce(F.col("payload.bytes_up"), F.lit(0)).alias("bytes_up"),
            F.coalesce(F.col("payload.bytes_down"), F.lit(0)).alias("bytes_down"),
            F.coalesce(F.col("payload.duration_seconds"), F.lit(0)).alias("duration_seconds"),
            "kafka_timestamp",
            "kafka_partition",
            "kafka_offset",
            "_ingested_at",
        )
    )


REALTIME_VALID = """
event_id IS NOT NULL
AND event_time IS NOT NULL
AND customer_id IS NOT NULL
AND tower_id IS NOT NULL
AND event_type IN ('DATA','VOICE','SMS')
AND bytes_up >= 0
AND bytes_down >= 0
AND duration_seconds >= 0
"""


@dp.table(
    name="vishnu_telecom.ops.usage_eventhub_quarantine",
    comment="Rejected realtime Event Hubs usage events.",
)
def usage_eventhub_quarantine():
    return (
        _typed_realtime_usage()
        .filter(~F.coalesce(F.expr(REALTIME_VALID), F.lit(False)))
        .withColumn("reject_reason", F.lit("Invalid realtime telecom usage event"))
    )


@dp.table(
    name="vishnu_telecom.silver.usage_events_realtime",
    comment="Validated realtime Event Hubs usage events with event-time watermarking and deduplication.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True,
)
@dp.expect_or_drop("valid_realtime_usage_event", REALTIME_VALID)
def usage_events_realtime():
    return (
        _typed_realtime_usage()
        .withWatermark("event_time", "10 minutes")
        .dropDuplicatesWithinWatermark(["event_id"])
    )


@dp.table(
    name="vishnu_telecom.gold.usage_realtime_5m",
    comment="Five-minute realtime usage metrics from Azure Event Hubs.",
    table_properties={"quality": "gold"},
    cluster_by_auto=True,
)
def usage_realtime_5m():
    return (
        spark.readStream
        .table("vishnu_telecom.silver.usage_events_realtime")
        .withWatermark("event_time", "10 minutes")
        .groupBy(
            F.window("event_time", "5 minutes"),
            F.col("event_type"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("bytes_up").alias("bytes_up"),
            F.sum("bytes_down").alias("bytes_down"),
            F.sum("duration_seconds").alias("duration_seconds"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_type",
            "event_count",
            "bytes_up",
            "bytes_down",
            "duration_seconds",
        )
    )
