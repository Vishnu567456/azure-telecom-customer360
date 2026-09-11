# OpenSharing / Delta Sharing Verification

This project includes an end-to-end Databricks OpenSharing validation using only synthetic telecom data.

## Governance-safe sharing design

The governed Gold `vishnu_telecom.gold.customer_360` materialized view contains an email column protected by a Unity Catalog ABAC policy. An attempt to share that materialized view directly was correctly blocked because the share owner was still governed by the ABAC policy.

Instead of bypassing the policy, the project creates a dedicated sharing-safe Delta table:

`vishnu_telecom.sharing.customer_360_safe`

The table contains only non-PII columns:

- `customer_id`
- `customer_name`
- `region`
- `segment`
- `subscription_status`
- `plan_name`
- `monthly_fee`
- `data_gb`

The `email` column is intentionally excluded.

## Provider configuration

Permanent share:

`telecom_customer360_open_share`

Shared object mapping:

`vishnu_telecom.sharing.customer_360_safe` -> `analytics.customer_360`

External OpenSharing was enabled temporarily on the metastore with a one-hour recipient-token lifetime. A temporary TOKEN recipient was created and granted `SELECT` on the share.

## External consumer validation

The recipient credential was downloaded once and consumed from a local isolated Python environment using the official open-source `delta-sharing` client.

The external client discovered exactly one shared table:

`telecom_customer360_open_share.analytics.customer_360`

The external read returned exactly 3 synthetic rows and the expected eight non-PII columns. No `email` column was exposed.

Verified synthetic rows:

| customer_id | region | segment | subscription_status | plan_name | monthly_fee |
|---|---|---|---|---|---:|
| C001 | SOUTH | Consumer | ACTIVE | Premium 5G | 699.0 |
| C003 | WEST | Enterprise | ACTIVE | Ultra 5G | 999.0 |
| C004 | SOUTH | Enterprise | null | null | null |

Result:

`PASS: external OpenSharing consumer successfully read 3 sanitized Customer 360 rows.`

## Security cleanup

Immediately after the successful external read:

- the temporary TOKEN recipient was deleted;
- the downloaded `.share` credential file was removed from the local machine;
- the metastore OpenSharing scope was restored from `INTERNAL_AND_EXTERNAL` to `INTERNAL`;
- the recipient was verified to no longer exist.

The permanent share and sanitized table remain as reproducible portfolio artifacts, while no temporary external credential remains active.

## Cost posture

The sharing configuration and recipient lifecycle are control-plane operations. The external consumer test used the Delta Sharing protocol directly and did not require starting the project SQL warehouse or Lakeflow pipeline.
