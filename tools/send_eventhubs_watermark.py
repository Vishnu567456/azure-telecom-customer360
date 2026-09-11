#!/usr/bin/env python3
"""Send one synthetic future-dated telecom event to advance the event-time watermark.

This helper is used only for the controlled Event Hubs validation run. It uses the
signed-in Azure CLI identity and does not require a connection string or SAS key.
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import AzureCliCredential


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--eventhub", required=True)
    args = parser.parse_args()

    event_time = (
        datetime.now(timezone.utc).replace(microsecond=0) + timedelta(minutes=20)
    ).isoformat().replace("+00:00", "Z")

    event = {
        "event_id": "RT_WM001",
        "event_time": event_time,
        "customer_id": "C001",
        "tower_id": "T003",
        "event_type": "DATA",
        "bytes_up": 1,
        "bytes_down": 1,
        "duration_seconds": 0,
    }

    credential = AzureCliCredential()
    producer = EventHubProducerClient(
        fully_qualified_namespace=f"{args.namespace}.servicebus.windows.net",
        eventhub_name=args.eventhub,
        credential=credential,
    )

    with producer:
        batch = producer.create_batch()
        batch.add(EventData(json.dumps(event, separators=(",", ":"))))
        producer.send_batch(batch)

    print("Sent watermark advancement event: RT_WM001")
    print("Event time:", event_time)
    print("Purpose   : close the earlier 5-minute event-time window for Gold validation")


if __name__ == "__main__":
    main()
