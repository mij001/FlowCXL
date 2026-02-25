"""Checks for unified TPCH+DeepVariant matrix and grouped absolute reporting."""

from __future__ import annotations

import copy
import csv
import importlib
import os
import unittest
from pathlib import Path

import yaml

import sources
from simulator import generate_runs_from_config


class SimulatorChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open("configs/runs.yaml", "r", encoding="utf-8") as handle:
            cls.config = yaml.safe_load(handle)
        cls.metrics, cls.traces = generate_runs_from_config(cls.config)

    def _find_row(
        self,
        dataset_profile: str,
        scenario: str,
        multiplier: float,
        workload_variant: str = "base",
        metrics: list[dict] | None = None,
    ) -> dict:
        rows = self.metrics if metrics is None else metrics
        for row in rows:
            if (
                row["dataset_profile"] == dataset_profile
                and row["scenario"] == scenario
                and abs(float(row["stage_size_multiplier"]) - float(multiplier)) < 1e-12
                and str(row.get("workload_variant", "")) == workload_variant
            ):
                return row
        raise KeyError((dataset_profile, scenario, multiplier, workload_variant))

    def _write_metrics_and_run_report(self) -> None:
        try:
            import pandas as pd  # noqa: F401
            import matplotlib.pyplot as plt  # noqa: F401
            report = importlib.import_module("report")
        except Exception as exc:  # pragma: no cover
            self.skipTest(f"report dependencies unavailable: {exc}")

        artifacts_dir = Path("artifacts")
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = artifacts_dir / "metrics.csv"
        with metrics_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(self.metrics[0].keys()))
            writer.writeheader()
            writer.writerows(self.metrics)

        report.main()

    def test_run_matrix_includes_dv_and_tpch_profiles_all_variants(self) -> None:
        expected_profiles = (
            list(self.config["workload_sweep"]["tpch_profiles"])
            + list(self.config["workload_sweep"]["deepvariant_profiles"])
        )
        expected_variants = [entry["name"] for entry in self.config["workload_variants"]]
        expected_count = (
            len(expected_profiles)
            * len(expected_variants)
            * len(self.config["size_multipliers"])
            * len(self.config["scenarios"])
        )

        self.assertEqual(len(self.metrics), expected_count)

        profiles_seen = {row["dataset_profile"] for row in self.metrics}
        variants_seen = {row["workload_variant"] for row in self.metrics}
        self.assertEqual(profiles_seen, set(expected_profiles))
        self.assertEqual(variants_seen, set(expected_variants))

    def test_metrics_schema_drops_deepvariant_mode(self) -> None:
        row = self.metrics[0]
        self.assertNotIn("deepvariant_mode", row)
        for key in ["workload_family", "workload_profile", "workload_variant", "baseline_id"]:
            self.assertIn(key, row)

    def test_traces_schema_drops_deepvariant_mode(self) -> None:
        self.assertTrue(self.traces)
        row = self.traces[0]
        self.assertNotIn("deepvariant_mode", row)
        for key in ["workload_family", "workload_profile", "workload_variant", "baseline_id"]:
            self.assertIn(key, row)

    def test_baseline_id_no_mode_component(self) -> None:
        for row in self.metrics:
            multiplier_token = str(row["stage_size_multiplier"]).replace(".", "p")
            expected = (
                f"{row['workload_family']}|{row['workload_profile']}|"
                f"{row['workload_variant']}|m{multiplier_token}"
            )
            self.assertEqual(str(row["baseline_id"]), expected)
            self.assertNotIn("|new|", str(row["baseline_id"]))
            self.assertNotIn("|legacy|", str(row["baseline_id"]))

    def test_ingressless_skips_stage1_ingress_for_pim_scenarios(self) -> None:
        profiles = [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]
        for profile in profiles:
            for scenario in [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]:
                base = self._find_row(profile, scenario, 1.0, "base")
                ingressless = self._find_row(profile, scenario, 1.0, "ingressless")
                self.assertGreater(float(base["total_bytes_host_h2d_ingress"]), 0.0)
                self.assertEqual(float(ingressless["total_bytes_host_h2d_ingress"]), 0.0)

    def test_retention_colocated_triggers_retain_bytes_positive(self) -> None:
        profiles = [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]
        for profile in profiles:
            row = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "retention_colocated")
            self.assertGreater(float(row["total_bytes_pim_retained"]), 0.0)
            run_id = str(row["run_id"])
            handoff_rows = [
                t for t in self.traces if t["run_id"] == run_id and str(t.get("handoff_mode", ""))
            ]
            self.assertTrue(handoff_rows)
            self.assertTrue(any(t["handoff_mode"] == "retain" for t in handoff_rows))

    def test_switch_striping_reports_striping_factor_gt_one(self) -> None:
        profiles = [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]
        for profile in profiles:
            row = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "switch_striping")
            self.assertGreater(float(row["cxl_effective_striping_factor"]), 1.0)
            self.assertGreater(float(row["cxl_active_direct_endpoints"]), 1.0)

    def test_direct_not_slower_than_bounce_all_tpch_points(self) -> None:
        for profile in [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        ]:
            for multiplier in self.config["size_multipliers"]:
                bounce = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, float(multiplier), "base")
                direct = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, float(multiplier), "base")
                self.assertLessEqual(float(direct["makespan_s"]), float(bounce["makespan_s"]))

    def test_tpch_high_profile_1x_bounce_direct_ratio_at_least_2x(self) -> None:
        bounce = self._find_row(
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.SCENARIO_PIM_HOST_BOUNCE,
            1.0,
            "base",
        )
        direct = self._find_row(
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.SCENARIO_PIM_FLOWCXL_DIRECT,
            1.0,
            "base",
        )
        ratio = float(bounce["makespan_s"]) / float(direct["makespan_s"])
        self.assertGreaterEqual(ratio, 2.0)

    def test_tpch_high_profile_1x_cpu_direct_ratio_at_least_1p2(self) -> None:
        cpu = self._find_row(
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.SCENARIO_CPU_ONLY,
            1.0,
            "base",
        )
        direct = self._find_row(
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.SCENARIO_PIM_FLOWCXL_DIRECT,
            1.0,
            "base",
        )
        ratio = float(cpu["makespan_s"]) / float(direct["makespan_s"])
        self.assertGreaterEqual(ratio, 1.2)

    def test_high_profile_bounce_dominant_lb_is_host_link_or_host_touch(self) -> None:
        bounce = self._find_row(
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.SCENARIO_PIM_HOST_BOUNCE,
            1.0,
            "base",
        )
        self.assertIn(str(bounce["dominant_lb_component"]), {"host_link", "host_touch"})

    def test_report_generates_grouped_absolute_files_per_profile_variant(self) -> None:
        self._write_metrics_and_run_report()
        report_dir = Path("artifacts/report")
        generated = set(os.listdir(report_dir))

        profiles = sorted({str(row["workload_profile"]) for row in self.metrics})
        variants = sorted({str(row["workload_variant"]) for row in self.metrics})
        for profile in profiles:
            profile_token = profile.replace(".", "p")
            for variant in variants:
                variant_token = variant.replace(".", "p")
                self.assertIn(f"plot_makespan_grouped_{profile_token}_{variant_token}.png", generated)
                self.assertIn(f"plot_energy_grouped_{profile_token}_{variant_token}.png", generated)
                self.assertIn(f"plot_makespan_grouped_pim_only_{profile_token}_{variant_token}.png", generated)
                self.assertIn(f"plot_energy_grouped_pim_only_{profile_token}_{variant_token}.png", generated)

    def test_report_main_shows_base_and_ingressless_only(self) -> None:
        self._write_metrics_and_run_report()
        text = Path("artifacts/report/report.md").read_text(encoding="utf-8")
        self.assertIn("## Main Results", text)
        self.assertIn("## Appendix: Additional Variants", text)
        main_block = text.split("## Main Results", 1)[1].split("## Appendix: Additional Variants", 1)[0]
        self.assertIn("| base", main_block)
        self.assertIn("| ingressless", main_block)
        self.assertNotIn("| retention_colocated", main_block)
        self.assertNotIn("| switch_striping", main_block)

    def test_report_appendix_contains_retention_and_switch_variants(self) -> None:
        self._write_metrics_and_run_report()
        text = Path("artifacts/report/report.md").read_text(encoding="utf-8")
        appendix = text.split("## Appendix: Additional Variants", 1)[1]
        self.assertIn("retention_colocated", appendix)
        self.assertIn("switch_striping", appendix)

    def test_report_has_no_legacy_or_mode_sections(self) -> None:
        self._write_metrics_and_run_report()
        text = Path("artifacts/report/report.md").read_text(encoding="utf-8")
        self.assertNotIn("DeepVariant New vs Legacy", text)
        self.assertNotIn("deepvariant_mode", text)
        self.assertNotIn("legacy switch", text.lower())

    def test_backward_compat_without_workload_sweep(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg.pop("workload_sweep", None)
        cfg.pop("workload_variants", None)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_CPU_ONLY]
        metrics, traces = generate_runs_from_config(cfg)
        self.assertEqual(len(metrics), 1)
        self.assertTrue(traces)
        self.assertNotIn("deepvariant_mode", metrics[0])


if __name__ == "__main__":
    unittest.main()
