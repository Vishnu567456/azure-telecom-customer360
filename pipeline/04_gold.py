# Databricks notebook source
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.table(
    name="vishnu_telecom.silver.invoices",
    comment="Validated telecom invoices.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)
@dp.expect_or_drop(
    "valid_invoice",
    """
    invoice_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND invoice_date IS NOT NULL
    AND amount >= 0
    AND status IS NOT NULL
    """
)
def invoices():
    return (
        spark.readStream
        .table("vishnu_telecom.bronze.invoices_raw")
        .withColumn(
            "invoice_date",
            F.expr("try_cast(invoice_date AS DATE)")
        )
        .withColumn(
            "amount",
            F.col("amount").cast("DECIMAL(18,2)")
        )
        .withColumn(
            "status",
            F.upper(F.trim(F.col("status")))
        )
    )


@dp.table(
    name="vishnu_telecom.silver.payments",
    comment="Validated telecom payment transactions.",
    table_properties={"quality": "silver"},
    cluster_by_auto=True
)
@dp.expect_or_drop(
    "valid_payment",
    """
    payment_id IS NOT NULL
    AND invoice_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND payment_time IS NOT NULL
    AND amount >= 0
    AND status IN ('SUCCESS','FAILED','PENDING')
    """
)
def payments():
    return (
        spark.readStream
        .table("vishnu_telecom.bronze.payments_raw")
        .withColumn(
            "payment_time",
            F.expr("try_cast(payment_time AS TIMESTAMP)")
        )
        .withColumn(
            "amount",
            F.col("amount").cast("DECIMAL(18,2)")
        )
        .withColumn(
            "status",
            F.upper(F.trim(F.col("status")))
        )
    )


@dp.materialized_view(
    name="vishnu_telecom.gold.customer_360",
    comment="Current customer, subscription and plan view."
)
def customer_360():

    customers = spark.read.table(
        "vishnu_telecom.silver.customer_current"
    ).alias("c")

    subscriptions = spark.read.table(
        "vishnu_telecom.silver.subscription_current"
    ).alias("s")

    plans = spark.read.table(
        "vishnu_telecom.bronze.ref_plans"
    ).alias("p")

    return (
        customers
        .join(
            subscriptions,
            F.col("c.customer_id") == F.col("s.customer_id"),
            "left"
        )
        .join(
            plans,
            F.col("s.plan_id") == F.col("p.plan_id"),
            "left"
        )
        .select(
            F.col("c.customer_id"),
            F.col("c.customer_name"),
            F.col("c.email"),
            F.col("c.region"),
            F.col("c.segment"),
            F.col("s.subscription_id"),
            F.col("s.status").alias("subscription_status"),
            F.col("p.plan_id"),
            F.col("p.plan_name"),
            F.col("p.monthly_fee"),
            F.col("p.data_gb")
        )
    )


@dp.materialized_view(
    name="vishnu_telecom.gold.billing_summary",
    comment="Invoice and successful-payment metrics by billing date."
)
def billing_summary():

    invoices = spark.read.table(
        "vishnu_telecom.silver.invoices"
    )

    payments = (
        spark.read.table("vishnu_telecom.silver.payments")
        .filter(F.col("status") == "SUCCESS")
        .groupBy("invoice_id")
        .agg(
            F.sum("amount").alias("successful_payment_amount")
        )
    )

    return (
        invoices
        .join(payments, "invoice_id", "left")
        .withColumn(
            "successful_payment_amount",
            F.coalesce(
                F.col("successful_payment_amount"),
                F.lit(0).cast("DECIMAL(18,2)")
            )
        )
        .groupBy("invoice_date")
        .agg(
            F.countDistinct("invoice_id").alias("invoice_count"),
            F.sum("amount").alias("invoiced_amount"),
            F.sum("successful_payment_amount").alias("collected_amount")
        )
        .withColumn(
            "outstanding_amount",
            F.col("invoiced_amount") - F.col("collected_amount")
        )
    )


@dp.materialized_view(
    name="vishnu_telecom.gold.usage_by_region",
    comment="Current-region usage analytics."
)
def usage_by_region():

    usage = spark.read.table(
        "vishnu_telecom.silver.usage_events"
    ).alias("u")

    customers = spark.read.table(
        "vishnu_telecom.silver.customer_current"
    ).alias("c")

    return (
        usage
        .join(
            customers,
            F.col("u.customer_id") == F.col("c.customer_id"),
            "left"
        )
        .groupBy(
            F.to_date(F.col("u.event_time")).alias("usage_date"),
            F.col("c.region"),
            F.col("u.event_type")
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("u.bytes_up").alias("bytes_up"),
            F.sum("u.bytes_down").alias("bytes_down"),
            F.sum("u.duration_seconds").alias("duration_seconds")
        )
    )
