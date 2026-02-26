"""Invariant checks for simulator correctness, schema contract, and tooling."""

from __future__ import annotations

import copy
import csv
import importlib
import math
import os
import tempfile
import unittest
from math import prod
from pathlib import Path

import matplotlib
import yaml

import sources
from simulator import CXLProcessorShareScheduler, generate_runs_from_config


class SimulatorInvariantChecks(unittest.TestCase):
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
        report = importlib.import_module("report")
        artifacts_dir = Path("artifacts")
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = artifacts_dir / "metrics.csv"
        with metrics_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(self.metrics[0].keys()))
            writer.writeheader()
            writer.writerows(self.metrics)
        report.main([])

    def test_deepvariant_doc_model_consistency(self) -> None:
        equations = Path("docs/equations.md").read_text(encoding="utf-8")
        self.assertIn("X0..X5", equations)
        self.assertIn("make_examples_frontend", equations)
        self.assertIn("call_variants_post", equations)

    def test_deepvariant_public_vs_execution_stage_shape(self) -> None:
        for profile_id in [
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]:
            profile = sources.DATASET_PROFILES[profile_id]
            self.assertEqual(profile["pipeline_template"], sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE)
            self.assertEqual(tuple(profile["public_stage_names"]), sources.DEEPVARIANT_PUBLIC_STAGE_NAMES)
            self.assertEqual(tuple(profile["stage_names"]), sources.DEEPVARIANT_KERNEL_STAGE_NAMES)
            self.assertEqual(len(profile["public_stage_names"]), 3)
            self.assertEqual(len(profile["stage_names"]), 5)
            self.assertEqual(len(profile["boundaries_bytes"]), 6)

    def test_deepvariant_internal_boundaries_derived_from_profile_params(self) -> None:
        for profile_id in [
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]:
            profile = sources.DATASET_PROFILES[profile_id]
            params = profile["parameters"]
            num_examples = int(round(
                float(params["covered_bases"])
                * float(params["candidate_density_per_base_at_ref_coverage"])
                * (float(params["coverage_x"]) / float(params["candidate_density_ref_coverage_x"]))
            ))
            num_examples = max(1, num_examples)
            x0 = int(round(
                float(params["covered_bases"])
                * float(params["coverage_x"])
                * float(params["aligned_bytes_per_covered_base"])
            ))
            x1 = int(round(num_examples * float(params["frontend_bytes_per_example"])))
            tensor_bytes = num_examples * prod(int(dim) for dim in params["example_shape"]) * int(
                params["example_element_bytes"]
            )
            x2 = int(round(tensor_bytes * float(params["tensor_overhead_factor"])))
            x3 = int(round(num_examples * float(params["infer_output_bytes_per_example"])))
            x4 = int(round(num_examples * float(params["call_post_output_bytes_per_example"])))
            x5 = int(round(num_examples * int(params["postprocess_output_bytes_per_example"])))
            self.assertEqual(profile["boundaries_bytes"], [x0, x1, x2, x3, x4, x5])

    def test_num_stages_public_and_num_kernels_execution(self) -> None:
        for profile in [sources.PROFILE_DV_ILLUMINA_WGS_30X, sources.PROFILE_DV_ILLUMINA_WES_100X]:
            row = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "base")
            self.assertEqual(int(row["num_stages"]), 3)
            self.assertEqual(int(row["num_kernels"]), 5)
        for profile in [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        ]:
            row = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "base")
            self.assertEqual(int(row["num_stages"]), 3)
            self.assertEqual(int(row["num_kernels"]), 3)

    def test_ingressless_skips_first_host_to_pim_transfer(self) -> None:
        for profile in [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        ]:
            base = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "base")
            ingressless = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "ingressless")
            self.assertGreater(float(base["total_bytes_host_h2d_ingress"]), 0.0)
            self.assertEqual(float(ingressless["total_bytes_host_h2d_ingress"]), 0.0)

        for profile in [
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]:
            base = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "base")
            ingressless = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0, "ingressless")
            self.assertEqual(float(base["total_bytes_host_h2d_ingress"]), 0.0)
            self.assertEqual(float(ingressless["total_bytes_host_h2d_ingress"]), 0.0)
            self.assertGreater(float(base["total_bytes_host_h2d_stage"]), 0.0)
            self.assertEqual(float(ingressless["total_bytes_host_h2d_stage"]), 0.0)

    def test_retention_colocated_triggers_retain(self) -> None:
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
            self.assertTrue(any(t["handoff_mode"] == "retain" for t in handoff_rows))

    def test_switch_striping_exercises_topology(self) -> None:
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

    def test_cxl_processor_sharing_symmetric_overlap(self) -> None:
        scheduler = CXLProcessorShareScheduler(bw_total_Bps=100.0, slots=4)
        ok, events = scheduler.try_admit(transfer_id=1, bytes_total=100, at_t=0.0)
        self.assertTrue(ok)
        self.assertEqual(len(events), 1)
        first_initial_t_done = events[0][0]
        self.assertAlmostEqual(first_initial_t_done, 1.0, places=9)

        ok, events = scheduler.try_admit(transfer_id=2, bytes_total=100, at_t=0.5)
        self.assertTrue(ok)
        by_id = {transfer_id: t_done for t_done, transfer_id, _ in events}
        self.assertGreater(by_id[1], first_initial_t_done)
        self.assertAlmostEqual(by_id[1], 1.5, places=9)
        self.assertAlmostEqual(by_id[2], 2.5, places=9)

    def test_cxl_processor_sharing_equal_jobs_equal_finish(self) -> None:
        scheduler = CXLProcessorShareScheduler(bw_total_Bps=100.0, slots=4)
        ok, _ = scheduler.try_admit(transfer_id=1, bytes_total=100, at_t=0.0)
        self.assertTrue(ok)
        ok, events = scheduler.try_admit(transfer_id=2, bytes_total=100, at_t=0.0)
        self.assertTrue(ok)
        by_id = {transfer_id: t_done for t_done, transfer_id, _ in events}
        self.assertAlmostEqual(by_id[1], by_id[2], places=9)
        self.assertAlmostEqual(by_id[1], 2.0, places=9)

    def test_cxl_slot_cap_respected(self) -> None:
        scheduler = CXLProcessorShareScheduler(bw_total_Bps=100.0, slots=1)
        ok, _ = scheduler.try_admit(transfer_id=1, bytes_total=100, at_t=0.0)
        self.assertTrue(ok)
        ok, _ = scheduler.try_admit(transfer_id=2, bytes_total=100, at_t=0.0)
        self.assertFalse(ok)
        self.assertTrue(math.isfinite(scheduler.next_completion_time(at_t=0.0)))

    def test_direct_trace_effective_bw_matches_completion_duration(self) -> None:
        direct_rows = [
            row
            for row in self.traces
            if row.get("op_type") == "TRANSFER" and row.get("transfer_path") == "cxl_direct"
        ]
        self.assertTrue(direct_rows)
        for row in direct_rows[:200]:
            duration = float(row["duration_s"])
            if duration <= 0:
                continue
            expected = float(row["bytes"]) / duration
            actual = float(row.get("cxl_effective_bw_Bps", 0.0))
            self.assertAlmostEqual(actual, expected, places=6)

    def test_config_requires_memory_system_by_template_only(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg.pop("memory_system_by_template", None)
        with self.assertRaises(KeyError):
            generate_runs_from_config(cfg)

    def test_legacy_keys_fail_fast_with_memory_system_guidance(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["cpu_mem_Bps_by_stage_by_template"] = {}
        with self.assertRaises(ValueError) as ctx:
            generate_runs_from_config(cfg)
        self.assertIn("memory_system_by_template", str(ctx.exception))

    def test_run_cli_config_and_resolved_snapshots(self) -> None:
        run_module = importlib.import_module("run")
        cfg = copy.deepcopy(self.config)
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "cfg.yaml"
            artifacts_dir = Path(tmpdir) / "out"
            with config_path.open("w", encoding="utf-8") as handle:
                yaml.safe_dump(cfg, handle, sort_keys=False)
            run_module.main(
                [
                    "--config",
                    str(config_path),
                    "--artifacts-dir",
                    str(artifacts_dir),
                    "--trace-yaml-max-events",
                    "16",
                ]
            )
            self.assertTrue((artifacts_dir / "config_input.yaml").exists())
            self.assertTrue((artifacts_dir / "config_resolved_base.yaml").exists())
            self.assertTrue((artifacts_dir / "run_matrix.csv").exists())
            self.assertTrue((artifacts_dir / "metrics.csv").exists())

    def test_report_agg_backend_headless(self) -> None:
        report_module = importlib.import_module("report")
        self.assertEqual(matplotlib.get_backend().lower(), "agg")

        with tempfile.TemporaryDirectory() as tmpdir:
            metrics_path = Path(tmpdir) / "metrics.csv"
            with metrics_path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=list(self.metrics[0].keys()))
                writer.writeheader()
                writer.writerows(self.metrics)
            cfg_path = Path(tmpdir) / "cfg.yaml"
            with cfg_path.open("w", encoding="utf-8") as handle:
                yaml.safe_dump(self.config, handle, sort_keys=False)
            report_module.main(
                [
                    "--config",
                    str(cfg_path),
                    "--artifacts-dir",
                    tmpdir,
                    "--metrics-file",
                    str(metrics_path),
                ]
            )
            self.assertTrue((Path(tmpdir) / "report" / "report.md").exists())

    def test_report_structure_main_appendix_variants(self) -> None:
        self._write_metrics_and_run_report()
        text = Path("artifacts/report/report.md").read_text(encoding="utf-8")
        self.assertIn("## Main Results", text)
        self.assertIn("## Appendix: Additional Variants", text)
        main_block = text.split("## Main Results", 1)[1].split("## Appendix: Additional Variants", 1)[0]
        self.assertIn("| base", main_block)
        self.assertIn("| ingressless", main_block)
        self.assertNotIn("| retention_colocated", main_block)
        self.assertNotIn("| switch_striping", main_block)
        appendix = text.split("## Appendix: Additional Variants", 1)[1]
        self.assertIn("retention_colocated", appendix)
        self.assertIn("switch_striping", appendix)
        self.assertNotIn("legacy switch", text.lower())

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

    def test_metrics_schema_clean_metadata(self) -> None:
        row = self.metrics[0]
        self.assertNotIn("deepvariant_mode", row)
        self.assertIn("num_kernels", row)
        self.assertIn("cxl_bw_model", row)
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


if __name__ == "__main__":
    unittest.main()
