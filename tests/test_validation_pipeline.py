"""Validation tooling and reproducibility contract checks."""

from __future__ import annotations

import copy
import csv
import tempfile
import unittest
from pathlib import Path

import yaml

import sources
from simulator import generate_runs_from_config
from tools.validation.calibrate_microbench import run_calibration
from tools.validation.crosscheck_ps import run_crosscheck


class ValidationPipelineChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open("configs/runs.yaml", "r", encoding="utf-8") as handle:
            cls.base_config = yaml.safe_load(handle)

    def _small_config(self) -> dict:
        cfg = copy.deepcopy(self.base_config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [
            sources.SCENARIO_CPU_ONLY,
            sources.SCENARIO_PIM_HOST_BOUNCE,
            sources.SCENARIO_PIM_FLOWCXL_DIRECT,
        ]
        return cfg

    def test_calibration_parser_and_output_schema(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = run_calibration(config=cfg, out_dir=Path(tmpdir))
            raw_path = Path(summary["raw_csv"])
            fit_path = Path(summary["fit_yaml"])
            overlay_path = Path(summary["overlay_yaml"])
            self.assertTrue(raw_path.exists())
            self.assertTrue(fit_path.exists())
            self.assertTrue(overlay_path.exists())

            with raw_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                cols = reader.fieldnames or []
            for col in ["system_id", "path", "payload_bytes", "concurrency", "measured_s", "simulated_s"]:
                self.assertIn(col, cols)

    def test_overlay_merge_determinism(self) -> None:
        import run as run_module

        base = self._small_config()
        overlay = {
            "link_constant_overrides": {
                sources.LINK_CXL_SWITCH: {"bandwidth_Bps": 70e9, "latency_s": 300e-9}
            },
            "cxl_direct_concurrency": {"dma_issue_fixed_s": 1.0e-7},
        }
        merged_a, links_a = run_module._apply_validation_overlay(copy.deepcopy(base), copy.deepcopy(overlay))
        merged_b, links_b = run_module._apply_validation_overlay(copy.deepcopy(base), copy.deepcopy(overlay))
        self.assertEqual(merged_a, merged_b)
        self.assertEqual(links_a, links_b)

    def test_validation_overlay_does_not_mutate_sources_links(self) -> None:
        import run as run_module

        base = self._small_config()
        overlay = {
            "link_constant_overrides": {
                sources.LINK_CXL_SWITCH: {"bandwidth_Bps": 70e9, "latency_s": 300e-9}
            },
        }
        before = copy.deepcopy(sources.LINKS)
        _, links_catalog = run_module._apply_validation_overlay(copy.deepcopy(base), copy.deepcopy(overlay))
        self.assertEqual(before, sources.LINKS)
        self.assertNotEqual(before[sources.LINK_CXL_SWITCH], links_catalog[sources.LINK_CXL_SWITCH])

    def test_overlay_isolation_between_two_runs(self) -> None:
        cfg = self._small_config()
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["size_multipliers"] = [1.0]
        direct_link = str(cfg["link_profile"]["cxl_direct_link"])
        before = copy.deepcopy(sources.LINKS)

        links_fast = {k: dict(v) for k, v in sources.LINKS.items()}
        links_slow = {k: dict(v) for k, v in sources.LINKS.items()}
        links_fast[direct_link]["bandwidth_Bps"] = float(links_fast[direct_link]["bandwidth_Bps"]) * 2.0
        links_slow[direct_link]["bandwidth_Bps"] = max(1.0, float(links_slow[direct_link]["bandwidth_Bps"]) * 0.5)

        metrics_fast, _ = generate_runs_from_config(cfg, links_catalog=links_fast)
        metrics_slow, _ = generate_runs_from_config(cfg, links_catalog=links_slow)
        self.assertEqual(before, sources.LINKS)
        self.assertLess(float(metrics_fast[0]["makespan_s"]), float(metrics_slow[0]["makespan_s"]))

    def test_simulator_uses_injected_links_catalog(self) -> None:
        cfg = self._small_config()
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE]
        cfg["size_multipliers"] = [1.0]
        host_h2d = str(cfg["link_profile"]["host_h2d_link"])
        host_d2h = str(cfg["link_profile"]["host_d2h_link"])

        links_fast = {k: dict(v) for k, v in sources.LINKS.items()}
        links_slow = {k: dict(v) for k, v in sources.LINKS.items()}
        links_fast[host_h2d]["bandwidth_Bps"] = float(links_fast[host_h2d]["bandwidth_Bps"]) * 2.0
        links_fast[host_d2h]["bandwidth_Bps"] = float(links_fast[host_d2h]["bandwidth_Bps"]) * 2.0
        links_slow[host_h2d]["bandwidth_Bps"] = max(1.0, float(links_slow[host_h2d]["bandwidth_Bps"]) * 0.5)
        links_slow[host_d2h]["bandwidth_Bps"] = max(1.0, float(links_slow[host_d2h]["bandwidth_Bps"]) * 0.5)

        metrics_fast, _ = generate_runs_from_config(cfg, links_catalog=links_fast)
        metrics_slow, _ = generate_runs_from_config(cfg, links_catalog=links_slow)
        self.assertLess(float(metrics_fast[0]["makespan_s"]), float(metrics_slow[0]["makespan_s"]))

    def test_crosscheck_output_schema(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            summary = run_crosscheck(config=cfg, out_dir=Path(tmpdir))
            cross_path = Path(summary["crosscheck_csv"])
            self.assertTrue(cross_path.exists())
            with cross_path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                cols = reader.fieldnames or []
            for col in ["pattern", "payload_bytes", "concurrency", "mape_percent", "passes_tolerance"]:
                self.assertIn(col, cols)

    def test_provenance_table_rendered_in_report(self) -> None:
        import report as report_module

        cfg = self._small_config()
        metrics, _ = generate_runs_from_config(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir)
            metrics_path = artifacts_dir / "metrics.csv"
            with metrics_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(metrics[0].keys()))
                writer.writeheader()
                writer.writerows(metrics)
            cfg_path = artifacts_dir / "cfg.yaml"
            with cfg_path.open("w", encoding="utf-8") as handle:
                yaml.safe_dump(cfg, handle, sort_keys=False)

            report_module.main(
                [
                    "--config",
                    str(cfg_path),
                    "--artifacts-dir",
                    str(artifacts_dir),
                    "--metrics-file",
                    str(metrics_path),
                ]
            )
            report_text = (artifacts_dir / "report" / "report.md").read_text(encoding="utf-8")
            self.assertIn("## Parameter Provenance", report_text)
            self.assertIn("link_profile.host_h2d_link", report_text)

    def test_paper_config_discovery_and_run_matrix_determinism(self) -> None:
        self.assertTrue(Path("paper/configs/fig_main.yaml").exists())
        self.assertTrue(Path("paper/configs/fig_validation.yaml").exists())
        self.assertTrue(Path("paper/configs/ablations.yaml").exists())

        cfg = self._small_config()
        metrics_a, _ = generate_runs_from_config(cfg)
        metrics_b, _ = generate_runs_from_config(cfg)
        run_ids_a = [row["run_id"] for row in metrics_a]
        run_ids_b = [row["run_id"] for row in metrics_b]
        self.assertEqual(run_ids_a, run_ids_b)

    def test_claims_file_exists_and_has_required_sections(self) -> None:
        claims_path = Path("paper/CLAIMS.md")
        self.assertTrue(claims_path.exists())
        text = claims_path.read_text(encoding="utf-8")
        required_markers = [
            "Claim statement",
            "Supporting artifacts",
            "Canonical config",
            "Reproduce",
            "Parameter provenance",
            "Sensitivity statement",
            "Residual caveats",
        ]
        for marker in required_markers:
            self.assertIn(marker, text)


if __name__ == "__main__":
    unittest.main()
