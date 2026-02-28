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

    def _write_measured_inputs(self, root_dir: Path, cfg: dict, *, include_direct: bool = True) -> dict:
        payloads = [int(v) for v in cfg["validation"]["calibration"]["payload_bytes"]]
        concs = [int(v) for v in cfg["validation"]["calibration"]["concurrency_levels"]]
        system_id = str(cfg["validation"]["system_id"])

        spec = {
            "host_h2d": (8.9e-6, 6.65e9, 0.035, "true"),
            "host_d2h": (9.4e-6, 4.72e9, 0.040, "true"),
            "bounce": (21.0e-6, 2.52e9, 0.055, "na"),
            "direct": (0.62e-6, 46.8e9, 0.025, "na"),
        }
        paths = ["host_h2d", "host_d2h", "bounce"] + (["direct"] if include_direct else [])
        out_map = {}

        for path_name in paths:
            base, bw, scale, pinned = spec[path_name]
            out_path = root_dir / f"{path_name}.csv"
            with out_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(
                    [
                        "system_id",
                        "path",
                        "payload_bytes",
                        "concurrency",
                        "repetition",
                        "time_s",
                        "tool",
                        "pinned",
                        "notes",
                    ]
                )
                for payload in payloads:
                    for conc in concs:
                        time_s = base + (float(payload) / bw) * (1.0 + scale * (conc - 1))
                        writer.writerow(
                            [
                                system_id,
                                path_name,
                                payload,
                                conc,
                                0,
                                f"{time_s:.12f}",
                                "custom",
                                pinned,
                                "test_input",
                            ]
                        )
            out_map[path_name] = str(out_path)

        return out_map

    def test_calibration_parser_and_output_schema(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            cfg["validation"]["calibration"]["measured_inputs"] = self._write_measured_inputs(tmp_path, cfg)
            summary = run_calibration(config=cfg, out_dir=tmp_path)
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

    def test_calibration_requires_measured_inputs_for_required_paths(self) -> None:
        cfg = self._small_config()
        bad_map = dict(cfg["validation"]["calibration"]["measured_inputs"])
        bad_map["host_h2d"] = "does/not/exist.csv"
        cfg["validation"]["calibration"]["measured_inputs"] = bad_map
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises((FileNotFoundError, ValueError, KeyError)):
                run_calibration(config=cfg, out_dir=Path(tmpdir))

    def test_calibration_accepts_missing_direct_with_fallback_status(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            measured_inputs = self._write_measured_inputs(tmp_path, cfg, include_direct=False)
            measured_inputs["direct"] = str(tmp_path / "direct_missing.csv")
            cfg["validation"]["calibration"]["measured_inputs"] = measured_inputs
            cfg["validation"]["calibration"]["optional_paths"] = ["direct"]
            summary = run_calibration(config=cfg, out_dir=tmp_path)
            with Path(summary["fit_yaml"]).open("r", encoding="utf-8") as handle:
                fit_payload = yaml.safe_load(handle) or {}
            status = fit_payload.get("calibration_status", {})
            self.assertEqual(status.get("direct"), "fallback_crosscheck")

    def test_measured_csv_schema_validation(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            measured_inputs = self._write_measured_inputs(tmp_path, cfg)
            malformed_path = tmp_path / "host_h2d_malformed.csv"
            malformed_path.write_text(
                "system_id,path,payload_bytes,concurrency,repetition\n"
                "system_x_2026q1,host_h2d,4194304,1,0\n",
                encoding="utf-8",
            )
            measured_inputs["host_h2d"] = str(malformed_path)
            cfg["validation"]["calibration"]["measured_inputs"] = measured_inputs
            with self.assertRaises(ValueError):
                run_calibration(config=cfg, out_dir=tmp_path)

    def test_fit_model_latency_plus_bw_outputs_expected_keys(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            cfg["validation"]["calibration"]["measured_inputs"] = self._write_measured_inputs(tmp_path, cfg)
            summary = run_calibration(config=cfg, out_dir=tmp_path)
            with Path(summary["fit_yaml"]).open("r", encoding="utf-8") as handle:
                fit_payload = yaml.safe_load(handle) or {}
            host_h2d = fit_payload["paths"]["host_h2d"]
            for key in ["bandwidth_Bps", "latency_s", "mape_percent", "n_points", "fit_concurrency"]:
                self.assertIn(key, host_h2d)

    def test_bounce_decomposition_produces_host_touch_overlay(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            cfg["validation"]["calibration"]["measured_inputs"] = self._write_measured_inputs(tmp_path, cfg)
            summary = run_calibration(config=cfg, out_dir=tmp_path)
            with Path(summary["overlay_yaml"]).open("r", encoding="utf-8") as handle:
                overlay = yaml.safe_load(handle) or {}
            stage_defaults = overlay.get("stage_defaults", {})
            self.assertIn("host_touch_Bps", stage_defaults)
            self.assertIn("host_touch_fixed_s", stage_defaults)
            self.assertGreater(float(stage_defaults["host_touch_Bps"]), 0.0)
            self.assertGreaterEqual(float(stage_defaults["host_touch_fixed_s"]), 0.0)

    def test_pcie_ceiling_check_emits_flags(self) -> None:
        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            cfg["validation"]["calibration"]["measured_inputs"] = self._write_measured_inputs(tmp_path, cfg)
            summary = run_calibration(config=cfg, out_dir=tmp_path)
            with Path(summary["fit_yaml"]).open("r", encoding="utf-8") as handle:
                fit_payload = yaml.safe_load(handle) or {}
            ceiling = fit_payload.get("ceiling_check", {})
            self.assertIn("ceiling_check_pass", ceiling)
            self.assertIn("ceiling_violation_paths", ceiling)

            strict_cfg = copy.deepcopy(cfg)
            strict_cfg["validation"]["calibration"]["ceiling_check"]["max_one_way_utilization_fraction"] = 0.01
            strict_cfg["validation"]["calibration"]["ceiling_check"]["fail_on_violation"] = True
            with self.assertRaises(ValueError):
                run_calibration(config=strict_cfg, out_dir=tmp_path)

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

    def test_run_and_report_with_validation_overlay_still_emit_grouped_absolute_artifacts(self) -> None:
        import report as report_module
        import run as run_module

        cfg = self._small_config()
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir)
            cfg_path = artifacts_dir / "cfg.yaml"
            with cfg_path.open("w", encoding="utf-8") as handle:
                yaml.safe_dump(cfg, handle, sort_keys=False)

            run_module.main(["--config", str(cfg_path), "--artifacts-dir", str(artifacts_dir)])
            report_module.main(["--config", str(cfg_path), "--artifacts-dir", str(artifacts_dir)])
            plot_dir = artifacts_dir / "report"
            expected = list(plot_dir.glob("plot_makespan_grouped_*.png"))
            self.assertTrue(expected)

    def test_report_labels_unmeasured_direct_as_crosscheck_validated(self) -> None:
        import report as report_module

        cfg = self._small_config()
        metrics, _ = generate_runs_from_config(cfg)
        with tempfile.TemporaryDirectory() as tmpdir:
            artifacts_dir = Path(tmpdir)
            validation_dir = artifacts_dir / "validation"
            validation_dir.mkdir(parents=True, exist_ok=True)

            metrics_path = artifacts_dir / "metrics.csv"
            with metrics_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(metrics[0].keys()))
                writer.writeheader()
                writer.writerows(metrics)

            (validation_dir / "microbench_fit.yaml").write_text(
                yaml.safe_dump(
                    {
                        "system_id": "system_x_2026q1",
                        "calibration_status": {
                            "host_h2d": "measured",
                            "host_d2h": "measured",
                            "bounce": "measured",
                            "direct": "fallback_crosscheck",
                        },
                        "paths": {
                            "host_h2d": {
                                "calibration_status": "measured",
                                "bandwidth_Bps": 6.6e9,
                                "latency_s": 8e-6,
                                "mape_percent": 2.0,
                                "r2": 0.99,
                                "n_points": 3,
                                "fit_concurrency": 1,
                            }
                        },
                        "host_touch_fit": {
                            "status": "measured_decomposition",
                            "host_touch_Bps": 2.5e10,
                            "host_touch_fixed_s": 2e-6,
                        },
                        "ceiling_check": {
                            "enabled": True,
                            "ceiling_check_pass": True,
                            "pcie_gen": 4,
                            "lane_width": 16,
                            "one_way_threshold_Bps": 3.0e10,
                            "ceiling_violation_paths": [],
                            "ceiling_violation_notes": "",
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            with (validation_dir / "cxl_ps_crosscheck.csv").open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "pattern",
                        "payload_bytes",
                        "concurrency",
                        "mape_percent",
                        "max_abs_error_s",
                        "passes_tolerance",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "pattern": "burst",
                        "payload_bytes": 4194304,
                        "concurrency": 4,
                        "mape_percent": 1.2,
                        "max_abs_error_s": 0.0,
                        "passes_tolerance": True,
                    }
                )

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
            self.assertIn("fallback cross-check", report_text)

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
