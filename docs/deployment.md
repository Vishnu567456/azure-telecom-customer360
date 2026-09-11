# Deployment Strategy

This repository separates the live development deployment from production-style configuration so the portfolio can demonstrate enterprise deployment practices without accidentally creating duplicate cloud resources.

## Development

The root `databricks.yml` contains the active `dev` target. Existing Databricks resources were imported into bundle management by binding the existing pipeline, job, and dashboard before the first controlled deployment.

The first bundle deployment was reviewed with a plan before execution. It updated the existing bound resources without creating or deleting resources, and the post-deployment plan reported all managed resources unchanged.

Operational guardrails:

- no scheduled pipeline refresh
- no continuous pipeline mode
- one concurrent runner job at most
- queueing enabled
- no automatic full refresh
- no automatic pipeline execution from `bundle deploy`
- plan reviewed before deployment
- diagnostic migration output excluded from Git

## Production-style configuration

`config/databricks.prod.example.yml` is an intentionally non-active production target template. The root bundle does not include it, so it cannot be deployed accidentally.

A real production rollout should use:

- a separate production workspace and catalog
- a dedicated service principal or workload identity federation
- GitHub environment protection / approval
- a reviewed `databricks bundle plan`
- `databricks bundle deploy --fail-on-active-runs`
- post-deployment identity and resource-ID verification
- a separate explicit action to run the job or pipeline only when required

The production template remains **Implemented but Not Deployed** in this portfolio project by design.

## CI and CD boundary

GitHub Actions CI compiles the Python sources and runs the repository contract tests without connecting to Azure or starting Databricks compute.

The live Databricks bundle deployment has been tested from an authenticated local Databricks CLI profile. Automated cloud deployment from GitHub is intentionally not enabled in this public portfolio repository because production credentials / workload identity are not provisioned here.

This keeps the public project reproducible while avoiding unsafe credential handling and accidental billable runs.
