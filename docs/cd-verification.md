# GitHub-to-Databricks CD Verification

This project uses a dedicated Databricks service principal and GitHub Actions OIDC workload identity federation for secretless continuous deployment.

## Security model

The deployment identity is the Databricks service principal:

`azure-telecom-customer360-github-cd`

No Databricks PAT, password, client secret, or long-lived GitHub secret is used for Databricks authentication.

The GitHub workflow requests a short-lived OIDC token and exchanges it with Databricks using `DATABRICKS_AUTH_TYPE=github-oidc`.

The federation policy is restricted to the repository and the GitHub environment `databricks-dev`.

## Deployment state safety

The active development bundle is pinned to the already-existing deployment path:

`/Workspace/Users/iaovishnuddubey@gmail.com/.bundle/azure-telecom-customer360/dev`

This preserves the original bundle state for the existing pipeline, runner job, and dashboard instead of creating a second service-principal-specific deployment state.

Before GitHub was allowed to deploy, the local safety plan was verified as:

`Plan: 0 to add, 0 to change, 0 to delete, 3 unchanged`

The service principal was then granted `CAN_MANAGE` only on the existing bundle directory and the three existing bundle-managed resources.

## GitHub Actions guardrails

The `CD` workflow is manual (`workflow_dispatch`) and includes these controls:

- `contents: read`
- `id-token: write`
- environment: `databricks-dev`
- concurrency group preventing overlapping dev deployments
- federated identity verification before deployment
- bundle validation
- pre-deployment bundle plan
- hard failure if the plan contains any resource additions or deletions
- post-deployment bundle plan verification

The workflow does not start the Lakeflow pipeline, run the runner job, or start a SQL warehouse.

## Verified deployment

The first OIDC attempt failed safely before deployment because the live GitHub token used an immutable repository-owner/repository-id subject and workspace-token audience. The federation policy was corrected in place without recreating the service principal or policy.

The same failed GitHub Actions job was then re-run and completed successfully.

Verified runtime identity:

`azure-telecom-customer360-github-cd`

Pre-deployment plan:

`Plan: 0 to add, 0 to change, 0 to delete, 3 unchanged`

Deployment result:

`Resources: 0 created, 0 changed, 0 deleted, 3 unchanged`

Post-deployment plan:

`Plan: 0 to add, 0 to change, 0 to delete, 3 unchanged`

The bundle synchronized repository files into the existing deployment folder while preserving all three live Databricks resources.

**PASS:** GitHub Actions authenticated to Databricks through OIDC workload identity federation and performed a real no-duplicate, no-delete bundle deployment with a clean post-deployment plan.
