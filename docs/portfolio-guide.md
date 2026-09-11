# Portfolio & Interview Guide

## Project Title

**Enterprise Telecom Customer 360 & Revenue Intelligence Platform**

## One-line Summary

Built an end-to-end Azure Databricks lakehouse for a synthetic telecom domain using ADLS Gen2, Auto Loader, Delta Lake, Lakeflow Declarative Pipelines, Structured Streaming, Unity Catalog governance, Metric Views, AI/BI dashboards, Declarative Automation Bundles, and GitHub Actions CI.

## Architecture Flow

```text
Synthetic telecom sources
        |
        v
ADLS Gen2
        |
        v
Auto Loader / Bronze Delta
        |
        v
Lakeflow Declarative Pipelines
        |
        +--> Silver CDC / AUTO CDC
        |       +--> SCD Type 1 current state
        |       +--> SCD Type 2 history
        |       +--> Watermark + deduplication
        |       +--> Data-quality quarantine
        |
        v
Gold data products
        |
        +--> customer_360
        +--> billing_summary
        +--> usage_5m
        +--> usage_by_region
        |
        v
Unity Catalog Metric View
        |
        v
AI/BI Dashboard
```

Unity Catalog provides centralized cataloging, tags, lineage, storage governance, and ABAC masking across the platform.

## 90-second Interview Explanation

I built an Azure Databricks telecom Customer 360 lakehouse using synthetic data. The raw customer, subscription, usage, billing, and digital-event files land in ADLS Gen2 and are ingested incrementally into Bronze Delta tables using Auto Loader.

In the Silver layer, I implemented CDC and AUTO CDC, including SCD Type 1 for current-state records and SCD Type 2 for historical tracking. I also added event-time watermarking and deduplication for usage streaming, data-quality expectations with quarantine handling, and VARIANT processing for evolving nested JSON payloads.

The Gold layer produces curated datasets such as customer_360, billing_summary, usage_5m, and usage_by_region. I created a Unity Catalog Metric View for reusable business metrics and published an AI/BI dashboard for customer and revenue analytics.

For governance, I used Unity Catalog tagging, lineage, and ABAC column masking so PII such as email is masked for downstream consumers. I also moved the existing Databricks pipeline, runner job, and dashboard under Declarative Automation Bundle management and added automated GitHub Actions CI checks without triggering Databricks compute.

## Resume-ready Project Bullets

- Built an Azure Databricks medallion lakehouse for synthetic telecom customer, subscription, usage, billing, and digital-event data using ADLS Gen2, Auto Loader, PySpark, Delta Lake, and Lakeflow Declarative Pipelines.
- Implemented CDC, AUTO CDC, SCD Type 1/2, event-time watermarking, deduplication, VARIANT processing, data-quality expectations, and quarantine handling for reliable incremental processing.
- Developed Gold Customer 360 and revenue data products, governed business metrics through a Unity Catalog Metric View, and published an AI/BI dashboard for customer and revenue analytics.
- Applied Unity Catalog governance with tagging, lineage, and ABAC column masking for PII protection, and managed deployment configuration with Databricks Declarative Automation Bundles.
- Added no-compute automated CI with GitHub Actions to validate Python syntax, bundle contracts, dashboard JSON, and cost/run guardrails.

## Key Technical Talking Points

### Auto Loader

Used for incremental file ingestion into Bronze streaming tables with explicit schemas, metadata capture, rescued-data handling, and checkpoint-driven processing.

### CDC / SCD

Used sequence-aware CDC to process inserts, updates, deletes, late-arriving changes, and out-of-order records. Current-state tables use SCD Type 1 semantics, while historical customer changes are maintained using SCD Type 2.

### Structured Streaming

Usage events use a 10-minute event-time watermark and deduplication within the watermark. Test data includes a deliberately late event to validate late-arrival behavior.

### Data Quality

Expectations validate important business fields before downstream processing. Invalid records are retained in quarantine datasets rather than being silently discarded.

### VARIANT

Digital-event payloads use semi-structured nested JSON. VARIANT is used to handle evolving payload structures while extracting required campaign and location attributes.

### Governance

Unity Catalog is used for catalog/schema organization, storage credential and external-location governance, governed tags, data classification, lineage, and ABAC masking. The dashboard demonstrates masked email values rather than exposing the underlying PII.

### Metric View and AI/BI

A governed Metric View centralizes customer count, active subscribers, monthly recurring revenue, and average monthly fee for downstream analytics. The AI/BI dashboard consumes the semantic layer and curated Gold data.

### Declarative Automation Bundles

The existing live pipeline, runner job, and dashboard were bound to bundle resources before deployment. The controlled deployment completed without creating or deleting resources, and the post-deployment plan showed all three resources unchanged.

### CI

GitHub Actions runs Python compilation and repository contract tests. CI does not authenticate to Databricks and does not execute pipelines, which keeps validation safe and compute-free.

## Verified Synthetic Results

| Metric | Result |
|---|---:|
| Customers | 3 |
| Active subscribers | 2 |
| Monthly recurring revenue | 1698 |
| Average active monthly fee | 849 |

Additional verified behaviors include SCD history, delete handling, out-of-order CDC, late-event exclusion, quarantine records, CDF change types, managed file-event discovery, lineage, and PII masking.

## Implementation Boundaries

Implemented and verified in the portfolio environment:

- ADLS Gen2 and Unity Catalog storage integration
- Auto Loader and Delta Lake
- Bronze / Silver / Gold processing
- Lakeflow Declarative Pipelines
- CDC / AUTO CDC / SCD1 / SCD2
- Structured Streaming, watermarking, and deduplication
- Data-quality expectations and quarantine
- VARIANT semi-structured processing
- Delta Change Data Feed demonstration
- Managed file-event probe
- Unity Catalog tagging, lineage, and ABAC masking
- Metric View
- AI/BI dashboard
- Lakeflow runner job
- Declarative Automation Bundle deployment
- Automated tests and GitHub Actions CI

Intentionally not claimed as live production implementations:

- Azure Event Hubs streaming source
- Genie
- Open / Delta Sharing extension
- Fully automated GitHub-to-Databricks continuous deployment
- A deployed production environment

## Cost-aware Design

The portfolio uses serverless, manually triggered workloads and no scheduled runner job. CI is designed to run without Databricks compute. Production-style configuration is kept as an inactive template rather than deploying duplicate resources.

## Data Safety

All records in this repository are synthetic and created specifically for the portfolio project. No employer, customer, or telecom production data is included.
