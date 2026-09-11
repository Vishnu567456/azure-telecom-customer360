import json
import py_compile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class PortfolioContractTests(unittest.TestCase):
    def test_required_files_exist(self):
        required = [
            ROOT / "databricks.yml",
            ROOT / "resources/vishnu_telecom_customer360.pipeline.yml",
            ROOT / "resources/vishnu_telecom_customer360_runner.job.yml",
            ROOT / "resources/telecom_customer_360_revenue_intelligence.dashboard.yml",
            ROOT / "src/01_bronze.py",
            ROOT / "src/02_cdc.py",
            ROOT / "src/03_streaming.py",
            ROOT / "src/04_gold.py",
            ROOT / "src/telecom_customer_360_revenue_intelligence.lvdash.json",
        ]
        missing = [str(path.relative_to(ROOT)) for path in required if not path.exists()]
        self.assertEqual(missing, [], f"Missing required project files: {missing}")

    def test_python_sources_compile(self):
        for folder in (ROOT / "src", ROOT / "pipeline"):
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
        self.assertNotIn("mode: development", text)

    def test_migration_diagnostics_are_ignored(self):
        text = (ROOT / ".gitignore").read_text()
        self.assertIn("migration/", text)


if __name__ == "__main__":
    unittest.main()
