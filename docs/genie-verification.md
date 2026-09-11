# Databricks Genie Verification

## Scope

This document records the controlled end-to-end validation of the Databricks Genie agent used by the synthetic Telecom Customer 360 portfolio project.

The Genie agent is named `Telecom Customer 360 Genie` and is configured to use:

- `vishnu_telecom.gold.customer_360_metrics` for governed KPI and aggregate questions.
- `vishnu_telecom.sharing.customer_360_safe` for sanitized customer-level detail.

The configuration is generated reproducibly by `tools/build_genie_space_spec.py`.

## Cost-aware creation

The Genie agent was created while the attached serverless SQL warehouse was in `STOPPED` state. Agent creation itself did not require starting the warehouse.

## Controlled natural-language validation

A single Genie conversation was started with the question:

> How many active subscribers are there and what is monthly recurring revenue?

The message completed successfully.

Genie generated SQL against the governed Unity Catalog Metric View:

```sql
SELECT
  MEASURE(`active_subscribers`) AS active_subscribers,
  MEASURE(`monthly_recurring_revenue`) AS monthly_recurring_revenue
FROM `vishnu_telecom`.`gold`.`customer_360_metrics`
GROUP BY ALL
```

The natural-language answer returned:

- Active subscribers: `2`
- Monthly recurring revenue: `1698.0`

These values match the independently verified semantic-layer results for the synthetic dataset.

## Verification result

**PASS:** Databricks Genie was implemented, connected to the governed semantic layer, and successfully answered the controlled KPI question with the expected values.

Only synthetic data is used by this project.