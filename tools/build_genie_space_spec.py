#!/usr/bin/env python3
"""Build a reproducible Genie Space create request for the telecom portfolio.

This helper creates configuration only. It does not call Databricks APIs or start compute.
"""

import argparse
import json
from pathlib import Path


def build_serialized_space():
    return {
        "version": 2,
        "config": {
            "sample_questions": [
                {
                    "id": "01f10000000000000000000000000001",
                    "question": [
                        "How many active subscribers are there and what is monthly recurring revenue?"
                    ],
                },
                {
                    "id": "01f10000000000000000000000000002",
                    "question": ["Show customer count and revenue by region."],
                },
                {
                    "id": "01f10000000000000000000000000003",
                    "question": ["Which active customers are on each plan?"],
                },
            ]
        },
        "data_sources": {
            "metric_views": [
                {
                    "identifier": "vishnu_telecom.gold.customer_360_metrics",
                    "description": [
                        "Governed Customer 360 semantic metrics for customer count, active subscribers, monthly recurring revenue, and average monthly fee."
                    ],
                }
            ],
            "tables": [
                {
                    "identifier": "vishnu_telecom.sharing.customer_360_safe",
                    "description": [
                        "Sanitized synthetic Customer 360 detail table with PII email excluded."
                    ],
                }
            ],
        },
        "instructions": {
            "text_instructions": [
                {
                    "id": "01f10000000000000000000000000011",
                    "content": [
                        "Use vishnu_telecom.gold.customer_360_metrics for KPI and aggregate questions. Use vishnu_telecom.sharing.customer_360_safe for customer-level detail. The dataset is synthetic. Do not infer or invent PII fields that are not present."
                    ],
                }
            ],
            "example_question_sqls": [
                {
                    "id": "01f10000000000000000000000000021",
                    "question": [
                        "How many active subscribers are there and what is monthly recurring revenue?"
                    ],
                    "sql": [
                        "SELECT MEASURE(active_subscribers) AS active_subscribers, MEASURE(monthly_recurring_revenue) AS monthly_recurring_revenue FROM vishnu_telecom.gold.customer_360_metrics"
                    ],
                },
                {
                    "id": "01f10000000000000000000000000022",
                    "question": ["Show customer count and revenue by region."],
                    "sql": [
                        "SELECT region, MEASURE(customer_count) AS customer_count, MEASURE(monthly_recurring_revenue) AS monthly_recurring_revenue FROM vishnu_telecom.gold.customer_360_metrics GROUP BY ALL ORDER BY monthly_recurring_revenue DESC"
                    ],
                },
            ],
        },
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument(
        "--parent-path",
        default="/Workspace/Shared/azure-telecom-customer360",
    )
    args = parser.parse_args()

    serialized = json.dumps(build_serialized_space(), separators=(",", ":"))

    request = {
        "warehouse_id": args.warehouse_id,
        "parent_path": args.parent_path,
        "serialized_space": serialized,
        "title": "Telecom Customer 360 Genie",
        "description": "Natural-language analytics over governed synthetic Telecom Customer 360 metrics and sanitized detail data.",
    }

    output = Path(args.output)
    output.write_text(json.dumps(request, indent=2) + "\n")
    print(f"Wrote {output}")


if __name__ == "__main__":
    main()
