#!/usr/bin/env python3
"""Build a Lakeflow pipeline JSON spec for the temporary Event Hubs validation pipeline."""

import argparse
import json


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--workspace-notebook",
        default="/Shared/azure-telecom-customer360/realtime/01_eventhubs_ingest",
    )
    args = parser.parse_args()

    spec = {
        "name": "vishnu-telecom-eventhubs-realtime",
        "catalog": "vishnu_telecom",
        "schema": "bronze",
        "serverless": True,
        "continuous": False,
        "photon": True,
        "libraries": [
            {
                "notebook": {
                    "path": args.workspace_notebook,
                }
            }
        ],
        "configuration": {
            "telecom.eventhubs.namespace": args.namespace,
            "telecom.eventhubs.name": "usage-realtime",
            "telecom.eventhubs.service_credential": "telecom_eventhubs_credential",
        },
        "tags": {
            "project": "azure-telecom-customer360",
            "environment": "dev",
            "source": "azure-event-hubs",
            "cost-control": "manual-runs-only",
        },
    }

    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(spec, handle, indent=2)
        handle.write("\n")

    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
