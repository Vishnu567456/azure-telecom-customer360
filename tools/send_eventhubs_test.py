#!/usr/bin/env python3
"""Send deterministic synthetic telecom events to Azure Event Hubs using Azure CLI identity.

No secrets or connection strings are used. The signed-in Azure CLI identity must have
Azure Event Hubs Data Sender on the target namespace or event hub.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import AzureCliCredential


def build_events():
    now = datetime.now(timezone.utc).replace(microsecond=0)

    def ts(seconds):
        return (now + timedelta(seconds=seconds)).isoformat().replace("+00:00", "Z")

    return [
        {
            "event_id": "RT001",
            "event_time": ts(0),
            "customer_id": "C001",
            "tower_id": "T001",
            "event_type": "DATA",
            "bytes_up": 1000,
            "bytes_down": 5000,
            "duration_seconds": 0,
        },
        {
            "event_id": "RT002",
            "event_time": ts(5),
            "customer_id": "C003",
            "tower_id": "T002",
            "event_type": "VOICE",
            "bytes_up": 0,
            "bytes_down": 0,
            "duration_seconds": 60,
        },
        {
            "event_id": "RT003",
            "event_time": ts(10),
            "customer_id": "C004",
            "tower_id": "T001",
            "event_type": "SMS",
            "bytes_up": 0,
            "bytes_down": 0,
            "duration_seconds": 0,
        },
        {
            "event_id": "RT003",
            "event_time": ts(11),
            "customer_id": "C004",
            "tower_id": "T001",
            "event_type": "SMS",
            "bytes_up": 0,
            "bytes_down": 0,
            "duration_seconds": 0,
        },
        {
            "event_id": "RT004",
            "event_time": ts(15),
            "customer_id": "C001",
            "tower_id": "T003",
            "event_type": "DATA",
            "bytes_up": 2000,
            "bytes_down": 7000,
            "duration_seconds": 0,
        },
        {
            "event_id": "RT_BAD",
            "event_time": ts(20),
            "customer_id": "C003",
            "tower_id": "T002",
            "event_type": "DATA",
            "bytes_up": -1,
            "bytes_down": 100,
            "duration_seconds": 0,
        },
    ]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--eventhub", required=True)
    args = parser.parse_args()

    credential = AzureCliCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=f"{args.namespace}.servicebus.windows.net",
        eventhub_name=args.eventhub,
        credential=credential,
    )

    events = build_events()

    with producer:
        batch = producer.create_batch()
        for event in events:
            batch.add(EventData(json.dumps(event, separators=(",", ":"))))
        producer.send_batch(batch)

    print(f"Sent {len(events)} synthetic telecom events")
    print("Expected Bronze rows       : 6")
    print("Expected Silver valid IDs  : RT001, RT002, RT003, RT004")
    print("Expected quarantine ID     : RT_BAD")
    print("Duplicate test             : RT003 sent twice")


if __name__ == "__main__":
    main()
