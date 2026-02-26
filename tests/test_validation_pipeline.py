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
        merged_a = run_module._apply_validation_overlay(copy.deepcopy(base), copy.deepcopy(overlay))
        merged_b = run_module._apply_validation_overlay(copy.deepcopy(base), copy.deepcopy(overlay))
        self.assertEqual(merged_a, merged_b)

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


if __name__ == "__main__":
    unittest.main()
