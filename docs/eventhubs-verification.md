# Azure Event Hubs Realtime Verification

The project includes an independently deployed Azure Event Hubs realtime ingestion path using the Kafka-compatible endpoint and a Unity Catalog service credential backed by the existing Azure Databricks Access Connector managed identity.

## Architecture

```text
Synthetic telecom events
        |
        v
Azure Event Hubs (Standard, 2 partitions)
        |
        v
Lakeflow Declarative Pipeline / Kafka source
        |
        +--> Bronze: usage_events_eventhub_raw
        |
        +--> Silver: usage_events_realtime
        |      - validation
        |      - 10 minute watermark
        |      - event_id deduplication
        |
        +--> Ops: usage_eventhub_quarantine
        |
        +--> Gold: usage_realtime_5m
               - 5 minute event-time aggregation
```

## Authentication

No Event Hubs connection string or SAS key is stored in the repository.

The Databricks pipeline uses the Unity Catalog service credential:

```text
telecom_eventhubs_credential
```

The service credential is backed by the Azure Databricks Access Connector managed identity. The managed identity was granted `Azure Event Hubs Data Receiver`; the local synthetic-event producer used Azure CLI identity with `Azure Event Hubs Data Sender`.

## Controlled Test

The first test batch published six synthetic records:

- RT001 — valid DATA event
- RT002 — valid VOICE event
- RT003 — valid SMS event
- RT003 — deliberate duplicate
- RT004 — valid DATA event
- RT_BAD — deliberately invalid negative-byte event

A final synthetic event, `RT_WM001`, was published with a later event time to advance the streaming watermark and close the original 5-minute Gold aggregation window.

## Azure-Verified Results

The final Databricks SQL verification succeeded with all expected values:

| Check | Actual | Expected | Result |
|---|---:|---:|---|
| Bronze rows | 7 | 7 | PASS |
| Silver rows | 5 | 5 | PASS |
| RT003 after deduplication | 1 | 1 | PASS |
| RT_BAD quarantine rows | 1 | 1 | PASS |
| Closed Gold windows | 3 | 3 | PASS |
| Original Gold event count | 4 | 4 | PASS |

This verifies the complete realtime path from Azure Event Hubs through Bronze, Silver, quarantine, watermark/deduplication, and Gold aggregation.

## Cost Cleanup

After verification:

- the existing serverless SQL warehouse was stopped;
- the temporary Event Hubs namespace used for the test was deleted;
- the source code and reproducible deployment/test helpers remain in GitHub.

The Event Hubs integration is therefore **implemented, tested, and Azure-verified**, while the temporary billable Event Hubs infrastructure is intentionally not left running.
