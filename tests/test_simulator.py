"""Checks for directional links, access-pattern DRAM service, and materialization modeling."""

from __future__ import annotations

import copy
import unittest

import yaml

import sources
from simulator import compute_cpu_effective_mem_bw, generate_runs_from_config, scale_boundaries_exact


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
        metrics: list[dict] | None = None,
    ) -> dict:
        target = self.metrics if metrics is None else metrics
        for row in target:
            if (
                row["dataset_profile"] == dataset_profile
                and row["scenario"] == scenario
                and abs(float(row["stage_size_multiplier"]) - float(multiplier)) < 1e-12
            ):
                return row
        raise KeyError((dataset_profile, scenario, multiplier))

    def _ratio(self, metrics: list[dict], dataset_profile: str, multiplier: float = 1.0) -> float:
        bounce = self._find_row(
            dataset_profile=dataset_profile,
            scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
            multiplier=multiplier,
            metrics=metrics,
        )
        direct = self._find_row(
            dataset_profile=dataset_profile,
            scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
            multiplier=multiplier,
            metrics=metrics,
        )
        return float(bounce["makespan_s"]) / float(direct["makespan_s"])

    def _tpch_profiles(self) -> list[str]:
        return [
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        ]

    def _compute_traces(
        self,
        run_id: str,
        stage_name: str,
        stage_device: str = sources.DEVICE_CPU,
    ) -> list[dict]:
        return [
            row
            for row in self.traces
            if row["run_id"] == run_id
            and row["op_type"] == "COMPUTE"
            and row["stage_name"] == stage_name
            and row["stage_device"] == stage_device
        ]

    def test_access_pattern_schema_validation_tpch(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["cpu_access_pattern_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]["join"].pop(
            "mlp", None
        )
        with self.assertRaises((KeyError, ValueError)):
            generate_runs_from_config(cfg)

    def test_directional_host_links_are_required_for_new_schema(self) -> None:
        cfg = copy.deepcopy(self.config)
        link_profile = dict(cfg["link_profile"])
        link_profile.pop("host_h2d_link", None)
        link_profile.pop("host_d2h_link", None)
        link_profile.pop("host_link", None)
        cfg["link_profile"] = link_profile
        with self.assertRaises((KeyError, ValueError)):
            generate_runs_from_config(cfg)

    def test_directional_link_routing_uses_h2d_vs_d2h_paths(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        row = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, 1.0)
        run_id = row["run_id"]
        h2d_link = self.config["link_profile"]["host_h2d_link"]
        d2h_link = self.config["link_profile"]["host_d2h_link"]

        transfer_rows = [r for r in self.traces if r["run_id"] == run_id and r["op_type"] == "TRANSFER"]
        self.assertTrue(transfer_rows)

        ingress_or_stage = [r for r in transfer_rows if r["transfer_path"] in {"host_h2d_ingress", "host_h2d_stage"}]
        d2h = [r for r in transfer_rows if r["transfer_path"] == "host_d2h"]
        self.assertTrue(ingress_or_stage)
        self.assertTrue(d2h)
        self.assertTrue(all(r["link_type"] == h2d_link for r in ingress_or_stage))
        self.assertTrue(all(r["link_type"] == d2h_link for r in d2h))

    def test_cpu_bw_eff_sequential_uses_peak_cap(self) -> None:
        stage_cfg = self.config["cpu_access_pattern_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP][
            "scan_filter_project"
        ]
        bw = compute_cpu_effective_mem_bw(
            stage_name="scan_filter_project",
            cpu_units=32,
            bw_peak_Bps=120_000_000_000.0,
            access_pattern=stage_cfg["access_pattern"],
            row_hit_rate=float(stage_cfg["row_hit_rate"]),
            mlp=float(stage_cfg["mlp"]),
            avg_miss_latency_ns=float(stage_cfg["avg_miss_latency_ns"]),
            cacheline_bytes=float(self.config["dram_service_defaults"]["cacheline_bytes"]),
            cpu_random_access_penalty=1.0,
        )
        self.assertEqual(str(bw["cpu_mem_bound_mode"]), "peak_streaming")
        self.assertAlmostEqual(float(bw["cpu_bw_eff_stage_Bps"]), 120_000_000_000.0)
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        cpu_row = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        scan_compute_rows = self._compute_traces(run_id=cpu_row["run_id"], stage_name="scan_filter_project")
        self.assertTrue(scan_compute_rows)
        self.assertTrue(all(row["cpu_mem_bound_mode"] == "peak_streaming" for row in scan_compute_rows))

    def test_cpu_bw_eff_hash_can_be_latency_limited(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        cpu_row = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        join_compute_rows = self._compute_traces(run_id=cpu_row["run_id"], stage_name="join")
        groupby_compute_rows = self._compute_traces(run_id=cpu_row["run_id"], stage_name="groupby_agg")
        self.assertTrue(join_compute_rows)
        self.assertTrue(groupby_compute_rows)
        self.assertTrue(any(row["cpu_mem_bound_mode"] == "latency_limited" for row in join_compute_rows))
        self.assertTrue(any(row["cpu_mem_bound_mode"] == "latency_limited" for row in groupby_compute_rows))

    def test_row_hit_and_mlp_monotonicity(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        baseline_makespan = float(baseline["makespan_s"])

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_CPU_ONLY]
        access_cfg = cfg["cpu_access_pattern_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]["join"]
        access_cfg["row_hit_rate"] = min(0.99, float(access_cfg["row_hit_rate"]) + 0.20)
        access_cfg["mlp"] = float(access_cfg["mlp"]) * 2.0
        improved_metrics, _ = generate_runs_from_config(cfg)
        improved_row = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0, metrics=improved_metrics)
        self.assertLessEqual(float(improved_row["makespan_s"]), baseline_makespan)

    def test_legacy_penalty_still_applies_as_multiplier(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        baseline_makespan = float(baseline["makespan_s"])

        cfg = copy.deepcopy(self.config)
        penalty_map = cfg["cpu_random_access_penalty_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]
        penalty_map["scan_filter_project"] = 1.0
        penalty_map["join"] = 1.0
        penalty_map["groupby_agg"] = 1.0
        metrics_no_penalty, _ = generate_runs_from_config(cfg)
        no_penalty_row = self._find_row(
            profile,
            sources.SCENARIO_CPU_ONLY,
            1.0,
            metrics=metrics_no_penalty,
        )
        self.assertGreater(baseline_makespan, float(no_penalty_row["makespan_s"]))

    def test_materialize_ops_unchanged_tpch_cpu_only(self) -> None:
        for profile in self._tpch_profiles():
            for scenario in self.config["scenarios"]:
                row = self._find_row(profile, scenario, 1.0)
                run_id = row["run_id"]
                materialize_rows = [
                    trace_row
                    for trace_row in self.traces
                    if trace_row["run_id"] == run_id and trace_row["op_type"] == "MATERIALIZE"
                ]
                if scenario == sources.SCENARIO_CPU_ONLY:
                    self.assertTrue(materialize_rows)
                else:
                    self.assertFalse(materialize_rows)

    def test_materialize_bytes_match_breaker_boundaries(self) -> None:
        for profile in self._tpch_profiles():
            boundaries = sources.DATASET_PROFILES[profile]["boundaries_bytes"]
            for multiplier in self.config["size_multipliers"]:
                cpu = self._find_row(profile, sources.SCENARIO_CPU_ONLY, float(multiplier))
                bounce = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, float(multiplier))
                direct = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, float(multiplier))

                scaled = scale_boundaries_exact(boundaries, float(multiplier))
                expected = int(scaled[1] + scaled[2])
                self.assertEqual(int(cpu["total_cpu_materialize_bytes"]), expected)
                self.assertEqual(int(bounce["total_cpu_materialize_bytes"]), 0)
                self.assertEqual(int(direct["total_cpu_materialize_bytes"]), 0)

    def test_direct_not_slower_than_bounce_all_tpch_points(self) -> None:
        for profile in self._tpch_profiles():
            for multiplier in self.config["size_multipliers"]:
                ratio = self._ratio(self.metrics, dataset_profile=profile, multiplier=float(multiplier))
                self.assertGreaterEqual(ratio, 1.0)

    def test_tpch_high_profile_1x_bounce_direct_ratio_at_least_2x(self) -> None:
        ratio = self._ratio(self.metrics, dataset_profile=sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE, multiplier=1.0)
        self.assertGreaterEqual(ratio, 2.0)

    def test_tpch_high_profile_1x_cpu_direct_ratio_at_least_1p2(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        cpu = self._find_row(profile, sources.SCENARIO_CPU_ONLY, 1.0)
        direct = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0)
        ratio = float(cpu["makespan_s"]) / float(direct["makespan_s"])
        self.assertGreaterEqual(ratio, 1.2)

    def test_high_profile_bounce_dominant_lb_is_host_link_or_host_touch(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        bounce = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, 1.0)
        self.assertIn(str(bounce["dominant_lb_component"]), {"host_link", "host_touch"})

    def test_compute_sensitivity_increases_bounce_direct_ratio(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline_ratio = self._ratio(self.metrics, dataset_profile=profile, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        for stage_name in sources.TPCH_STAGE_NAMES:
            cfg["pim_speedup_vs_cpu_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP][stage_name] = (
                float(cfg["pim_speedup_vs_cpu_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP][stage_name])
                * 10.0
            )

        metrics_fast, _ = generate_runs_from_config(cfg)
        fast_ratio = self._ratio(metrics_fast, dataset_profile=profile, multiplier=1.0)
        self.assertGreater(fast_ratio, baseline_ratio)

    def test_host_link_sensitivity_increases_bounce_direct_ratio(self) -> None:
        profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline_ratio = self._ratio(self.metrics, dataset_profile=profile, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]

        h2d = cfg["link_profile"]["host_h2d_link"]
        d2h = cfg["link_profile"]["host_d2h_link"]
        old_h2d_bw = float(sources.LINKS[h2d]["bandwidth_Bps"])
        old_d2h_bw = float(sources.LINKS[d2h]["bandwidth_Bps"])
        try:
            sources.LINKS[h2d]["bandwidth_Bps"] = old_h2d_bw / 10.0
            sources.LINKS[d2h]["bandwidth_Bps"] = old_d2h_bw / 10.0
            metrics_slow, _ = generate_runs_from_config(cfg)
        finally:
            sources.LINKS[h2d]["bandwidth_Bps"] = old_h2d_bw
            sources.LINKS[d2h]["bandwidth_Bps"] = old_d2h_bw
        slow_ratio = self._ratio(metrics_slow, dataset_profile=profile, multiplier=1.0)
        self.assertGreater(slow_ratio, baseline_ratio)

    def test_metrics_schema_includes_materialize_columns(self) -> None:
        row = self.metrics[0]
        for key in [
            "total_cpu_materialize_bytes",
            "total_cpu_materialize_time_component_s",
            "cpu_materialize_energy_J",
            "total_cpu_mem_latency_bound_time_component_s",
            "total_cpu_mem_peak_bound_time_component_s",
        ]:
            self.assertIn(key, row)
        for forbidden in ["queue_wait_s", "utilization", "speedup_vs_cpu_only"]:
            self.assertNotIn(forbidden, row)

    def test_directional_links_unchanged_with_access_model(self) -> None:
        self.test_directional_link_routing_uses_h2d_vs_d2h_paths()

    def test_metrics_schema_includes_cpu_dram_service_columns(self) -> None:
        self.test_metrics_schema_includes_materialize_columns()

    def test_deepvariant_path_unchanged_by_default_for_new_knobs(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_DV_ILLUMINA_WGS_30X]
        cfg["size_multipliers"] = [1.0]
        metrics, traces = generate_runs_from_config(cfg)
        self.assertTrue(metrics)
        for row in metrics:
            self.assertEqual(row["pipeline_template"], sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE)
            self.assertFalse(bool(row["memory_ceiling_enabled"]))
            self.assertEqual(int(row["total_cpu_materialize_bytes"]), 0)
            self.assertEqual(float(row["total_cpu_materialize_time_component_s"]), 0.0)
            self.assertEqual(float(row["cpu_materialize_energy_J"]), 0.0)
            self.assertEqual(float(row["total_cpu_mem_latency_bound_time_component_s"]), 0.0)
            self.assertEqual(float(row["total_cpu_mem_peak_bound_time_component_s"]), 0.0)
        self.assertFalse(any(t["op_type"] == "MATERIALIZE" for t in traces))

    def test_legacy_host_link_compatibility_path(self) -> None:
        cfg = copy.deepcopy(self.config)
        cxl = cfg["link_profile"]["cxl_direct_link"]
        cfg["link_profile"] = {
            "host_link": sources.LINK_PCIE_GEN4_X16,
            "cxl_direct_link": cxl,
        }
        metrics, _ = generate_runs_from_config(cfg)
        self.assertTrue(metrics)


if __name__ == "__main__":
    unittest.main()
