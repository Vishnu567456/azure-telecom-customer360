import json
import py_compile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PortfolioContractTests(unittest.TestCase):
    def test_required_files_exist(self):
        required = [
            ROOT / "databricks.yml",
            ROOT / ".github/workflows/cd.yml",
            ROOT / "resources/vishnu_telecom_customer360.pipeline.yml",
            ROOT / "resources/vishnu_telecom_customer360_runner.job.yml",
            ROOT / "resources/telecom_customer_360_revenue_intelligence.dashboard.yml",
            ROOT / "src/01_bronze.py",
            ROOT / "src/02_cdc.py",
            ROOT / "src/03_streaming.py",
            ROOT / "src/04_gold.py",
            ROOT / "src/telecom_customer_360_revenue_intelligence.lvdash.json",
            ROOT / "config/databricks.prod.example.yml",
            ROOT / "config/eventhubs.pipeline.example.yml",
            ROOT / "docs/deployment.md",
            ROOT / "docs/eventhubs-verification.md",
            ROOT / "docs/opensharing-verification.md",
            ROOT / "docs/genie-verification.md",
            ROOT / "docs/cd-verification.md",
            ROOT / "realtime/01_eventhubs_ingest.py",
            ROOT / "tools/build_eventhubs_pipeline_spec.py",
            ROOT / "tools/build_genie_space_spec.py",
            ROOT / "tools/send_eventhubs_test.py",
            ROOT / "tools/send_eventhubs_watermark.py",
        ]
        missing = [str(path.relative_to(ROOT)) for path in required if not path.exists()]
        self.assertEqual(missing, [], f"Missing required project files: {missing}")

    def test_python_sources_compile(self):
        for folder in (
            ROOT / "src",
            ROOT / "pipeline",
            ROOT / "realtime",
            ROOT / "tools",
        ):
            for path in sorted(folder.glob("*.py")):
                py_compile.compile(str(path), doraise=True)

    def test_pipeline_bundle_contract(self):
        text = (ROOT / "resources/vishnu_telecom_customer360.pipeline.yml").read_text()
        for source in ("01_bronze.py", "02_cdc.py", "03_streaming.py", "04_gold.py"):
            self.assertIn(source, text)
        self.assertNotIn("05_file_events_probe.py", text)
        self.assertIn("serverless: true", text)
        self.assertIn("catalog: vishnu_telecom", text)
        self.assertIn("schema: bronze", text)
        self.assertIn("cost-control: manual-runs-only", text)

    def test_job_cost_and_run_guardrails(self):
        text = (ROOT / "resources/vishnu_telecom_customer360_runner.job.yml").read_text()
        self.assertIn("performance_target: STANDARD", text)
        self.assertIn("max_concurrent_runs: 1", text)
        self.assertIn("enabled: true", text)
        self.assertIn("full_refresh: false", text)
        self.assertIn("timeout_seconds: 1800", text)
        self.assertIn("max_retries: 0", text)
        self.assertNotIn("schedule:", text)
        self.assertNotIn("trigger:", text)

    def test_dashboard_json_is_valid(self):
        path = ROOT / "src/telecom_customer_360_revenue_intelligence.lvdash.json"
        data = json.loads(path.read_text())
        self.assertGreaterEqual(len(data.get("datasets", [])), 3)
        self.assertGreaterEqual(len(data.get("pages", [])), 1)

    def test_bundle_has_safe_dev_target(self):
        text = (ROOT / "databricks.yml").read_text()
        self.assertIn("name: azure-telecom-customer360", text)
        self.assertIn("dev:", text)
        self.assertIn("default: true", text)
        self.assertIn("root_path:", text)
        self.assertIn("/Workspace/Users/iaovishnuddubey@gmail.com/.bundle/${bundle.name}/${bundle.target}", text)
        self.assertNotIn("mode: development", text)
        self.assertNotIn("prod:", text)

    def test_production_template_is_non_active(self):
        prod = (ROOT / "config/databricks.prod.example.yml").read_text()
        root = (ROOT / "databricks.yml").read_text()
        self.assertIn("prod:", prod)
        self.assertIn("mode: production", prod)
        self.assertIn("REPLACE-WITH-PRODUCTION-WORKSPACE", prod)
        self.assertNotIn("config/databricks.prod.example.yml", root)

    def test_eventhubs_realtime_contract(self):
        source = (ROOT / "realtime/01_eventhubs_ingest.py").read_text()
        template = (ROOT / "config/eventhubs.pipeline.example.yml").read_text()

        self.assertIn('.format("kafka")', source)
        self.assertIn('"databricks.serviceCredential"', source)
        self.assertIn('withWatermark("event_time", "10 minutes")', source)
        self.assertIn('dropDuplicatesWithinWatermark(["event_id"])', source)
        self.assertIn("usage_eventhub_quarantine", source)
        self.assertIn("usage_realtime_5m", source)

        self.assertIn("serverless: true", template)
        self.assertIn("continuous: false", template)
        self.assertIn("telecom_eventhubs_credential", template)
        self.assertIn("cost-control: manual-runs-only", template)

    def test_eventhubs_helpers_are_secretless(self):
        paths = [
            ROOT / "realtime/01_eventhubs_ingest.py",
            ROOT / "tools/send_eventhubs_test.py",
            ROOT / "tools/send_eventhubs_watermark.py",
            ROOT / "config/eventhubs.pipeline.example.yml",
        ]
        text = "\n".join(path.read_text() for path in paths).lower()

        forbidden = [
            "sharedaccesskey=",
            "sharedaccesskeyname=",
            "endpoint=sb://",
            "connectionstring",
        ]
        for token in forbidden:
            self.assertNotIn(token, text)

    def test_opensharing_verification_is_governance_safe(self):
        text = (ROOT / "docs/opensharing-verification.md").read_text().lower()
        self.assertIn("customer_360_safe", text)
        self.assertIn("email", text)
        self.assertIn("external opensharing consumer successfully read 3 sanitized", text)
        self.assertIn("restored from `internal_and_external` to `internal`", text)
        self.assertIn("temporary token recipient was deleted", text)
        self.assertNotIn("bearerToken", text)
        self.assertNotIn("activation_url", text)

    def test_genie_verification_contract(self):
        helper = (ROOT / "tools/build_genie_space_spec.py").read_text()
        doc = (ROOT / "docs/genie-verification.md").read_text().lower()

        self.assertIn("customer_360_metrics", helper)
        self.assertIn("customer_360_safe", helper)
        self.assertIn("active subscribers", helper.lower())
        self.assertIn("monthly recurring revenue", helper.lower())

        self.assertIn("databricks genie", doc)
        self.assertIn("active subscribers: `2`", doc)
        self.assertIn("monthly recurring revenue: `1698.0`", doc)
        self.assertIn("**pass:**", doc)

    def test_cd_workflow_is_secretless_and_guarded(self):
        workflow = (ROOT / ".github/workflows/cd.yml").read_text()
        doc = (ROOT / "docs/cd-verification.md").read_text().lower()

        self.assertIn("workflow_dispatch:", workflow)
        self.assertIn("id-token: write", workflow)
        self.assertIn("DATABRICKS_AUTH_TYPE: github-oidc", workflow)
        self.assertIn("environment: databricks-dev", workflow)
        self.assertIn("Safety guard blocked deployment", workflow)
        self.assertIn("databricks bundle deploy -t dev", workflow)
        self.assertNotIn("DATABRICKS_TOKEN", workflow)
        self.assertNotIn("client_secret", workflow.lower())

        self.assertIn("0 created, 0 changed, 0 deleted, 3 unchanged", doc)
        self.assertIn("post-deployment plan", doc)
        self.assertIn("**pass:**", doc)

    def test_migration_diagnostics_are_ignored(self):
        text = (ROOT / ".gitignore").read_text()
        self.assertIn("migration/", text)


if __name__ == "__main__":
    unittest.main()
