# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


def typed_customers():
    return (
        spark.readStream
        .table("vishnu_telecom.bronze.customers_cdc_raw")
        .withColumn(
            "effective_at",
            F.expr("try_cast(effective_at AS TIMESTAMP)")
        )
        .withColumn("op", F.upper(F.trim(F.col("op"))))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
    )


CUSTOMER_VALID = """
_rescued_data IS NULL
AND event_id IS NOT NULL
AND customer_id IS NOT NULL
AND sequence > 0
AND effective_at IS NOT NULL
AND op IN ('U','D')
AND (
    op = 'D'
    OR (
        customer_name IS NOT NULL
        AND email RLIKE '^[^@\\\\s]+@[^@\\\\s]+\\\\.[^@\\\\s]+$'
        AND region IS NOT NULL
        AND segment IS NOT NULL
    )
)
"""


@dp.table(
    name="vishnu_telecom.silver.customer_changes_valid",
    comment="Validated customer CDC source used by AUTO CDC.",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop(
    "valid_customer_cdc_envelope",
    CUSTOMER_VALID
)
def customer_changes_valid():
    return typed_customers()


@dp.table(
    name="vishnu_telecom.ops.customer_quarantine",
    comment="Rejected customer CDC records retained for investigation."
)
def customer_quarantine():
    return (
        typed_customers()
        .filter(~F.coalesce(F.expr(CUSTOMER_VALID), F.lit(False)))
        .withColumn(
            "reject_reason",
            F.lit("Invalid customer CDC envelope or payload")
        )
    )


# ------------------------------------------------------------
# CUSTOMER SCD TYPE 1
# ------------------------------------------------------------

dp.create_streaming_table(
    name="vishnu_telecom.silver.customer_current",
    comment="Current customer state maintained using Lakeflow AUTO CDC SCD1.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)

dp.create_auto_cdc_flow(
    name="customer_scd1_flow",
    target="vishnu_telecom.silver.customer_current",
    source="vishnu_telecom.silver.customer_changes_valid",
    keys=["customer_id"],
    sequence_by=F.col("sequence"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=[
        "op",
        "_rescued_data",
        "_source_file",
        "_ingested_at"
    ],
    stored_as_scd_type=1
)


# ------------------------------------------------------------
# CUSTOMER SCD TYPE 2
#
# Only region and segment are historical Type-2 attributes.
# Email/name changes update the current historical version
# without creating a new region/segment history row.
# ------------------------------------------------------------

dp.create_streaming_table(
    name="vishnu_telecom.silver.customer_history",
    comment="Customer region/segment history using Lakeflow AUTO CDC SCD2.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)

dp.create_auto_cdc_flow(
    name="customer_scd2_flow",
    target="vishnu_telecom.silver.customer_history",
    source="vishnu_telecom.silver.customer_changes_valid",
    keys=["customer_id"],
    sequence_by=F.col("sequence"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=[
        "op",
        "_rescued_data",
        "_source_file",
        "_ingested_at"
    ],
    stored_as_scd_type=2,
    track_history_column_list=[
        "region",
        "segment"
    ]
)


def typed_subscriptions():
    return (
        spark.readStream
        .table("vishnu_telecom.bronze.subscriptions_cdc_raw")
        .withColumn(
            "effective_at",
            F.expr("try_cast(effective_at AS TIMESTAMP)")
        )
        .withColumn("op", F.upper(F.trim(F.col("op"))))
        .withColumn("status", F.upper(F.trim(F.col("status"))))
    )


SUBSCRIPTION_VALID = """
_rescued_data IS NULL
AND event_id IS NOT NULL
AND subscription_id IS NOT NULL
AND customer_id IS NOT NULL
AND sequence > 0
AND effective_at IS NOT NULL
AND op IN ('U','D')
AND (
    op = 'D'
    OR (
        plan_id IS NOT NULL
        AND status IN ('ACTIVE','SUSPENDED','CANCELLED')
    )
)
"""


@dp.table(
    name="vishnu_telecom.silver.subscription_changes_valid",
    comment="Validated subscription CDC source.",
    table_properties={"quality": "silver"}
)
@dp.expect_or_drop(
    "valid_subscription_cdc_envelope",
    SUBSCRIPTION_VALID
)
def subscription_changes_valid():
    return typed_subscriptions()


@dp.table(
    name="vishnu_telecom.ops.subscription_quarantine",
    comment="Rejected subscription CDC records."
)
def subscription_quarantine():
    return (
        typed_subscriptions()
        .filter(~F.coalesce(F.expr(SUBSCRIPTION_VALID), F.lit(False)))
        .withColumn(
            "reject_reason",
            F.lit("Invalid subscription CDC envelope or payload")
        )
    )


dp.create_streaming_table(
    name="vishnu_telecom.silver.subscription_current",
    comment="Current subscription state using Lakeflow AUTO CDC SCD1.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)

dp.create_auto_cdc_flow(
    name="subscription_scd1_flow",
    target="vishnu_telecom.silver.subscription_current",
    source="vishnu_telecom.silver.subscription_changes_valid",
    keys=["subscription_id"],
    sequence_by=F.col("sequence"),
    apply_as_deletes=F.expr("op = 'D'"),
    except_column_list=[
        "op",
        "_rescued_data",
        "_source_file",
        "_ingested_at"
    ],
    stored_as_scd_type=1
)
