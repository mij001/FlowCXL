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

    def _run_custom(self, cfg: dict) -> tuple[list[dict], list[dict]]:
        return generate_runs_from_config(cfg)

    @staticmethod
    def _tpch_transition_keys() -> tuple[str, str]:
        return ("scan_filter_project->join", "join->groupby_agg")

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

    def test_scheduler_token_is_integer_and_stale_events_rejected(self) -> None:
        scheduler = CXLProcessorShareScheduler(bw_total_Bps=100.0, slots=4)
        ok, first_events = scheduler.try_admit(transfer_id=1, bytes_total=100, at_t=0.0)
        self.assertTrue(ok)
        stale_token = first_events[0][2]
        self.assertIsInstance(scheduler._active[1]["token"], int)

        ok, second_events = scheduler.try_admit(transfer_id=2, bytes_total=100, at_t=0.0)
        self.assertTrue(ok)
        by_id = {transfer_id: token for _, transfer_id, token in second_events}
        self.assertIsInstance(by_id[1], int)
        self.assertNotEqual(stale_token, by_id[1])

        stale_valid, _ = scheduler.complete_if_valid(transfer_id=1, token=stale_token, at_t=0.5)
        self.assertFalse(stale_valid)
        valid, _ = scheduler.complete_if_valid(transfer_id=1, token=by_id[1], at_t=2.0)
        self.assertTrue(valid)

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

    def test_tiling_model_schema_validation(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, _ = self._tpch_transition_keys()
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"][join_key]["mapping_type"] = "INVALID"
        with self.assertRaises(ValueError):
            generate_runs_from_config(cfg)

    def test_boundary_mappings_use_transition_keys_only(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, reduce_key = self._tpch_transition_keys()
        self.assertIn(join_key, cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"])
        self.assertIn(reduce_key, cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"])

    def test_numeric_boundary_keys_rejected_with_actionable_error(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            "1": {"mapping_id": "bad_numeric_1", "mapping_type": "IDENTITY"},
            "2": {"mapping_id": "bad_numeric_2", "mapping_type": "IDENTITY"},
        }
        with self.assertRaises(ValueError) as ctx:
            generate_runs_from_config(cfg)
        self.assertIn("transition keys", str(ctx.exception))

    def test_mapping_id_required_and_propagated(self) -> None:
        cfg_missing = copy.deepcopy(self.config)
        cfg_missing["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, _ = self._tpch_transition_keys()
        cfg_missing["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"][join_key].pop(
            "mapping_id", None
        )
        with self.assertRaises(ValueError):
            generate_runs_from_config(cfg_missing)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        metrics, traces = self._run_custom(cfg)
        self.assertIn("tpch_join_shuffle_v1", str(metrics[0]["mapping_ids_used"]))
        self.assertTrue(any(str(t.get("mapping_id", "")) for t in traces))

    def test_trace_contains_domain_mapping_and_barrier_fields(self) -> None:
        row = self.traces[0]
        for key in [
            "domain_in_id",
            "domain_out_id",
            "domain_in_tile_id",
            "domain_out_tile_id",
            "mapping_type",
            "mapping_id",
            "kernel_class",
            "glue_type",
            "barrier_dependency_wait_s",
            "glue_queue_wait_s",
            "barrier_total_wait_s",
            "barrier_wait_s",
            "aggregation_expected",
            "aggregation_received",
            "pim_mode",
            "pim_mode_compute_multiplier",
            "pim_mode_mem_multiplier",
            "pim_mode_command_overhead_s",
        ]:
            self.assertIn(key, row)

    def test_new_metrics_columns_append_only(self) -> None:
        row = self.metrics[0]
        for key in [
            "retile_enabled",
            "num_tile_domains",
            "total_glue_copy_bytes",
            "total_glue_reduce_bytes",
            "total_glue_shuffle_bytes",
            "total_glue_time_component_s",
            "total_glue_transfer_time_component_s",
            "total_barrier_dependency_wait_time_component_s",
            "total_glue_queue_wait_time_component_s",
            "total_barrier_wait_time_component_s",
            "lb_glue_s",
            "total_pim_mode_command_overhead_s",
            "mapping_ids_used",
        ]:
            self.assertIn(key, row)

    def test_group_k_to_1_barrier_blocks_until_all_contributors_and_glue_done(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        metrics, traces = self._run_custom(cfg)
        self.assertTrue(metrics[0]["retile_enabled"])
        glue_rows = [t for t in traces if t["op_type"] == "GLUE_REDUCE"]
        stage3_rows = [t for t in traces if t["op_type"] == "COMPUTE" and int(t["stage_id"]) == 3]
        self.assertTrue(glue_rows)
        self.assertTrue(stage3_rows)
        glue_end_by_tile = {int(r["domain_out_tile_id"]): float(r["t_end"]) for r in glue_rows}
        for row in stage3_rows:
            out_tile = int(row["domain_in_tile_id"])
            self.assertIn(out_tile, glue_end_by_tile)
            self.assertGreaterEqual(float(row["t_req"]), glue_end_by_tile[out_tile])

    def test_repartition_hash_conservation_and_partition_readiness(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        metrics, traces = self._run_custom(cfg)
        row = metrics[0]
        self.assertGreater(float(row["total_glue_shuffle_bytes"]), 0.0)
        stage2_compute = [t for t in traces if t["op_type"] == "COMPUTE" and int(t["stage_id"]) == 2]
        shuffle = [t for t in traces if t["op_type"] == "GLUE_SHUFFLE"]
        self.assertTrue(stage2_compute)
        self.assertTrue(shuffle)
        shuffle_end_by_tile = {int(r["domain_out_tile_id"]): float(r["t_end"]) for r in shuffle}
        for row2 in stage2_compute:
            in_tile = int(row2["domain_in_tile_id"])
            self.assertIn(in_tile, shuffle_end_by_tile)
            self.assertGreaterEqual(float(row2["t_req"]), shuffle_end_by_tile[in_tile])
        expected = int(shuffle[0]["aggregation_expected"])
        self.assertGreater(expected, 1)
        self.assertTrue(all(int(r["aggregation_expected"]) == expected for r in shuffle))
        self.assertTrue(all(int(r["aggregation_received"]) == expected for r in shuffle))
        max_stage1_end = max(
            float(t["t_end"]) for t in traces if t["op_type"] == "COMPUTE" and int(t["stage_id"]) == 1
        )
        min_shuffle_start = min(float(t["t_start"]) for t in shuffle)
        self.assertGreaterEqual(min_shuffle_start, max_stage1_end)

    def test_no_consumer_ready_before_expected_contributions(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        _, traces = self._run_custom(cfg)
        glue_rows = [t for t in traces if str(t["op_type"]).startswith("GLUE_")]
        self.assertTrue(glue_rows)
        for row in glue_rows:
            self.assertEqual(int(row["aggregation_received"]), int(row["aggregation_expected"]))

    def test_group_k_tail_last_group_smaller_and_conserves_bytes(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, reduce_key = self._tpch_transition_keys()
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            join_key: {"mapping_id": "tail_identity_join_v1", "mapping_type": "IDENTITY"},
            reduce_key: {
                "mapping_id": "tail_group_reduce_v1",
                "mapping_type": "GROUP_K_TO_1",
                "group_k": 5,
                "glue_type": "GLUE_REDUCE",
                "glue_device": "pim",
                "glue_fixed_s": 2e-7,
                "glue_compute_Bps": 7e10,
                "glue_mem_Bps": 2e11,
                "glue_transfer_path": "none",
            },
        }
        metrics, traces = self._run_custom(cfg)
        glue_rows = [t for t in traces if t["op_type"] == "GLUE_REDUCE"]
        self.assertTrue(glue_rows)
        expected_counts = [int(r["aggregation_expected"]) for r in glue_rows]
        self.assertIn(5, expected_counts)
        self.assertTrue(any(v < 5 for v in expected_counts))
        total_glue_bytes = sum(int(r["bytes"]) for r in glue_rows)
        profile = sources.DATASET_PROFILES[sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        expected_boundary2 = int(profile["boundaries_bytes"][2])
        self.assertEqual(total_glue_bytes, expected_boundary2)
        self.assertEqual(float(metrics[0]["total_glue_reduce_bytes"]), float(expected_boundary2))

    def test_barrier_wait_decomposes_dependency_and_glue_queue_components(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["glue_resource_mode"] = "shared_consumer_compute"
        cfg["stage_defaults"]["pim_units"] = 1
        join_key, _ = self._tpch_transition_keys()
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"][join_key]["partitions"] = 4
        metrics, traces = self._run_custom(cfg)
        row = metrics[0]
        total_dep = float(row["total_barrier_dependency_wait_time_component_s"])
        total_queue = float(row["total_glue_queue_wait_time_component_s"])
        total_bar = float(row["total_barrier_wait_time_component_s"])
        self.assertAlmostEqual(total_dep + total_queue, total_bar, places=6)
        glue_rows = [t for t in traces if str(t["op_type"]).startswith("GLUE_")]
        self.assertTrue(glue_rows)
        for gr in glue_rows:
            dep = float(gr["barrier_dependency_wait_s"])
            q = float(gr["glue_queue_wait_s"])
            total = float(gr["barrier_total_wait_s"])
            self.assertAlmostEqual(dep + q, total, places=6)
            self.assertAlmostEqual(float(gr["barrier_wait_s"]), total, places=6)

    def test_glue_queue_wait_positive_under_resource_contention(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["glue_resource_mode"] = "shared_consumer_compute"
        cfg["stage_defaults"]["pim_units"] = 1
        join_key, _ = self._tpch_transition_keys()
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"][join_key]["partitions"] = 4
        metrics, traces = self._run_custom(cfg)
        self.assertGreater(float(metrics[0]["total_glue_queue_wait_time_component_s"]), 0.0)
        self.assertTrue(any(float(t.get("glue_queue_wait_s", 0.0)) > 0.0 for t in traces if str(t["op_type"]).startswith("GLUE_")))

    def test_shared_consumer_compute_glue_contends_with_compute_pool(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["glue_resource_mode"] = "shared_consumer_compute"
        _, traces = self._run_custom(cfg)
        glue_rows = [t for t in traces if str(t["op_type"]).startswith("GLUE_")]
        self.assertTrue(glue_rows)
        self.assertTrue(all(str(r["resource"]).startswith(("cpu_stage_", "pim_stage_")) for r in glue_rows))

    def test_dedicated_pool_mode_preserves_separate_glue_pool_behavior(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["glue_resource_mode"] = "dedicated_pool"
        metrics, traces = self._run_custom(cfg)
        glue_rows = [t for t in traces if str(t["op_type"]).startswith("GLUE_")]
        self.assertTrue(glue_rows)
        self.assertTrue(all(str(r["resource"]).startswith("glue_") for r in glue_rows))
        self.assertGreater(float(metrics[0]["lb_glue_s"]), 0.0)

    def test_shared_mode_rejects_glue_device_consumer_mismatch(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["glue_resource_mode"] = "shared_consumer_compute"
        join_key, _ = self._tpch_transition_keys()
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"][join_key]["glue_device"] = "cpu"
        with self.assertRaises(ValueError):
            self._run_custom(cfg)

    def test_retile_disabled_identity_matches_legacy_makespan_within_tolerance(self) -> None:
        cfg_a = copy.deepcopy(self.config)
        cfg_a["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg_a["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg_a["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg_a["size_multipliers"] = [1.0]
        cfg_a["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg_a["tiling_model_by_template"]["tpch_3op"]["enabled"] = False

        cfg_b = copy.deepcopy(cfg_a)
        cfg_b["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, reduce_key = self._tpch_transition_keys()
        cfg_b["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            join_key: {"mapping_id": "test_identity_join_v1", "mapping_type": "IDENTITY"},
            reduce_key: {"mapping_id": "test_identity_reduce_v1", "mapping_type": "IDENTITY"},
        }
        metrics_a, _ = self._run_custom(cfg_a)
        metrics_b, _ = self._run_custom(cfg_b)
        a = float(metrics_a[0]["makespan_s"])
        b = float(metrics_b[0]["makespan_s"])
        self.assertLess(abs(a - b) / max(a, 1e-9), 0.02)

    def test_existing_transfer_paths_unchanged_under_identity_mapping(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE]

        cfg_disabled = copy.deepcopy(cfg)
        cfg_disabled["tiling_model_by_template"]["tpch_3op"]["enabled"] = False
        cfg_identity = copy.deepcopy(cfg)
        cfg_identity["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, reduce_key = self._tpch_transition_keys()
        cfg_identity["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            join_key: {"mapping_id": "test_identity_join_v1", "mapping_type": "IDENTITY"},
            reduce_key: {"mapping_id": "test_identity_reduce_v1", "mapping_type": "IDENTITY"},
        }
        m0, _ = self._run_custom(cfg_disabled)
        m1, _ = self._run_custom(cfg_identity)
        self.assertAlmostEqual(float(m0[0]["total_bytes_host_link"]), float(m1[0]["total_bytes_host_link"]), places=6)

    def test_multi_pim_scaling_improves_compute_heavy_kernel_throughput(self) -> None:
        cfg_small = copy.deepcopy(self.config)
        cfg_small["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg_small["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg_small["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg_small["size_multipliers"] = [1.0]
        cfg_small["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg_small["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, reduce_key = self._tpch_transition_keys()
        cfg_small["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            join_key: {"mapping_id": "test_identity_join_v1", "mapping_type": "IDENTITY"},
            reduce_key: {"mapping_id": "test_identity_reduce_v1", "mapping_type": "IDENTITY"},
        }
        cfg_small["memory_system_by_template"]["tpch_3op"]["enabled"] = False
        cfg_small["scenario_stage_endpoint_map_by_template"]["tpch_3op"]["pim_flowcxl_direct"] = [
            "pim0",
            "pim0",
            "pim0",
        ]
        cfg_big = copy.deepcopy(cfg_small)
        cfg_small["stage_defaults"]["pim_units"] = 8
        cfg_big["stage_defaults"]["pim_units"] = 64
        m_small, _ = self._run_custom(cfg_small)
        m_big, _ = self._run_custom(cfg_big)
        self.assertGreater(float(m_small[0]["makespan_s"]), float(m_big[0]["makespan_s"]))

    def test_barrier_heavy_mapping_shows_diminishing_returns_with_more_pim_units(self) -> None:
        cfg_id8 = copy.deepcopy(self.config)
        cfg_id8["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg_id8["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg_id8["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg_id8["size_multipliers"] = [1.0]
        cfg_id8["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg_id8["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        join_key, reduce_key = self._tpch_transition_keys()
        cfg_id8["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            join_key: {"mapping_id": "test_identity_join_v1", "mapping_type": "IDENTITY"},
            reduce_key: {"mapping_id": "test_identity_reduce_v1", "mapping_type": "IDENTITY"},
        }
        cfg_id8["memory_system_by_template"]["tpch_3op"]["enabled"] = False
        cfg_id8["scenario_stage_endpoint_map_by_template"]["tpch_3op"]["pim_flowcxl_direct"] = [
            "pim0",
            "pim0",
            "pim0",
        ]
        cfg_id64 = copy.deepcopy(cfg_id8)
        cfg_id8["stage_defaults"]["pim_units"] = 8
        cfg_id64["stage_defaults"]["pim_units"] = 64
        m_id8, _ = self._run_custom(cfg_id8)
        m_id64, _ = self._run_custom(cfg_id64)
        speedup_identity = float(m_id8[0]["makespan_s"]) / float(m_id64[0]["makespan_s"])

        cfg_bar8 = copy.deepcopy(cfg_id8)
        cfg_bar64 = copy.deepcopy(cfg_id64)
        # Default TPCH mapping includes shuffle + reduce barriers.
        cfg_bar8["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = copy.deepcopy(
            self.config["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"]
        )
        cfg_bar64["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = copy.deepcopy(
            self.config["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"]
        )
        m_bar8, _ = self._run_custom(cfg_bar8)
        m_bar64, _ = self._run_custom(cfg_bar64)
        speedup_barrier = float(m_bar8[0]["makespan_s"]) / float(m_bar64[0]["makespan_s"])
        self.assertGreater(speedup_barrier, 1.0)
        self.assertLess(speedup_barrier, 2.5)
        self.assertGreater(float(m_bar8[0]["total_barrier_wait_time_component_s"]), 0.0)

    def test_stage0_refill_policy_prevents_many_to_one_admission_starvation(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["max_inflight_tiles"] = 1
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["tiling_model_by_template"]["tpch_3op"]["admission_refill_policy"] = "stage0_output"
        join_key, reduce_key = self._tpch_transition_keys()
        cfg["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"] = {
            reduce_key: copy.deepcopy(
                self.config["tiling_model_by_template"]["tpch_3op"]["boundary_mappings"][reduce_key]
            ),
            join_key: {
                "mapping_id": "test_many_to_one_join_v1",
                "mapping_type": "GROUP_K_TO_1",
                "group_k": 4,
                "glue_type": "GLUE_COPY",
                "glue_device": "pim",
                "glue_fixed_s": 1e-7,
                "glue_compute_Bps": 8e10,
                "glue_mem_Bps": 2e11,
                "glue_transfer_path": "none",
            },
        }
        _, traces = self._run_custom(cfg)
        stage1_compute = [t for t in traces if t["op_type"] == "COMPUTE" and int(t["stage_id"]) == 1]
        stage2_compute = [t for t in traces if t["op_type"] == "COMPUTE" and int(t["stage_id"]) == 2]
        self.assertTrue(stage1_compute)
        self.assertTrue(stage2_compute)
        expected_stage2 = math.ceil(len(stage1_compute) / 4.0)
        self.assertEqual(len(stage2_compute), expected_stage2)

    def test_pim_mode_bank_vs_buffer_changes_service_time_deterministically(self) -> None:
        cfg_bank = copy.deepcopy(self.config)
        cfg_bank["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg_bank["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg_bank["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg_bank["size_multipliers"] = [1.0]
        cfg_bank["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg_bank["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg_bank["memory_system_by_template"]["tpch_3op"]["enabled"] = False
        cfg_bank["scenario_stage_endpoint_map_by_template"]["tpch_3op"]["pim_flowcxl_direct"] = [
            "pim0",
            "pim0",
            "pim0",
        ]
        cfg_buffer = copy.deepcopy(cfg_bank)
        for stage in ["scan_filter_project", "join", "groupby_agg"]:
            cfg_bank["pim_mode_by_stage_by_template"]["tpch_3op"][stage] = "BANK"
            cfg_buffer["pim_mode_by_stage_by_template"]["tpch_3op"][stage] = "BUFFER"
        m_bank, _ = self._run_custom(cfg_bank)
        m_buffer, _ = self._run_custom(cfg_buffer)
        self.assertLess(float(m_bank[0]["makespan_s"]), float(m_buffer[0]["makespan_s"]))

    def test_pim_mode_multipliers_affect_compute_and_mem_components(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        cfg["scenario_stage_endpoint_map_by_template"]["tpch_3op"]["pim_flowcxl_direct"] = [
            "pim0",
            "pim0",
            "pim0",
        ]
        cfg["memory_system_by_template"]["tpch_3op"]["enabled"] = True
        cfg["memory_system_by_template"]["tpch_3op"]["pim_system"]["queue_alpha"] = 0.0
        for stage in ["scan_filter_project", "join", "groupby_agg"]:
            cfg["memory_system_by_template"]["tpch_3op"]["pim_system"]["stages"][stage]["peak_bw_Bps"] = 1e14
        cfg_a = copy.deepcopy(cfg)
        cfg_b = copy.deepcopy(cfg)
        cfg_a["pim_mode_effects"]["BANK_GROUP"]["compute_multiplier"] = 0.9
        cfg_a["pim_mode_effects"]["BANK_GROUP"]["mem_multiplier"] = 0.9
        cfg_b["pim_mode_effects"]["BANK_GROUP"]["compute_multiplier"] = 0.5
        cfg_b["pim_mode_effects"]["BANK_GROUP"]["mem_multiplier"] = 0.5
        for stage in ["scan_filter_project", "join", "groupby_agg"]:
            cfg_a["pim_mode_by_stage_by_template"]["tpch_3op"][stage] = "BANK_GROUP"
            cfg_b["pim_mode_by_stage_by_template"]["tpch_3op"][stage] = "BANK_GROUP"
        m_a, _ = self._run_custom(cfg_a)
        m_b, _ = self._run_custom(cfg_b)
        self.assertLess(float(m_a[0]["makespan_s"]), float(m_b[0]["makespan_s"]))

    def test_mode_command_overhead_accounted_in_metrics(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE]
        cfg["workload_sweep"] = {
            "tpch_profiles": [sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
            "deepvariant_profiles": [],
        }
        cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["tiling_model_by_template"]["tpch_3op"]["enabled"] = True
        for stage in ["scan_filter_project", "join", "groupby_agg"]:
            cfg["pim_mode_by_stage_by_template"]["tpch_3op"][stage] = "BANK"
        cfg["pim_mode_effects"]["BANK"]["command_overhead_s"] = 5e-7
        metrics, _ = self._run_custom(cfg)
        self.assertGreater(float(metrics[0]["total_pim_mode_command_overhead_s"]), 0.0)


if __name__ == "__main__":
    unittest.main()
