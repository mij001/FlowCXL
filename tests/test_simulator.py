"""Checks for template-aware modeling with CPU/PIM memory ceilings."""

from __future__ import annotations

import copy
import unittest

import yaml

import sources
from simulator import (
    compute_bytes_touched,
    compute_stage_duration_components_s,
    generate_runs_from_config,
    scale_boundaries_exact,
)


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

    def test_tpch_memory_ceiling_keys_exist_and_validate(self) -> None:
        for key in [
            "enable_memory_ceiling_by_template",
            "cpu_mem_Bps_by_stage_by_template",
            "pim_mem_Bps_by_stage_by_template",
            "bytes_touched_factors_by_stage_by_template",
        ]:
            self.assertIn(key, self.config)

        cfg = copy.deepcopy(self.config)
        del cfg["cpu_mem_Bps_by_stage_by_template"]
        with self.assertRaises((KeyError, ValueError)):
            generate_runs_from_config(cfg)

        cfg2 = copy.deepcopy(self.config)
        del cfg2["bytes_touched_factors_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP][
            "join"
        ]["amplification_factor"]
        with self.assertRaises((KeyError, ValueError)):
            generate_runs_from_config(cfg2)

    def test_tpch_bytes_touched_formula_scan_join_agg(self) -> None:
        factors = self.config["bytes_touched_factors_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]
        bytes_in = 1_000
        bytes_out = 250
        for stage_name in sources.TPCH_STAGE_NAMES:
            stage_factors = factors[stage_name]
            expected = float(stage_factors["amplification_factor"]) * (
                float(stage_factors["input_factor"]) * bytes_in
                + float(stage_factors["output_factor"]) * bytes_out
            )
            actual = compute_bytes_touched(
                bytes_in=bytes_in,
                bytes_out=bytes_out,
                input_factor=float(stage_factors["input_factor"]),
                output_factor=float(stage_factors["output_factor"]),
                amplification_factor=float(stage_factors["amplification_factor"]),
            )
            self.assertAlmostEqual(actual, expected, places=9)

    def test_tpch_compute_duration_uses_max_compute_vs_mem(self) -> None:
        duration, compute_s, mem_s, _ = compute_stage_duration_components_s(
            bytes_in=1_000,
            bytes_out=500,
            compute_rate_Bps=100.0,
            memory_ceiling_enabled=True,
            memory_Bps_per_stage=10_000.0,
            stage_units=1,
            input_factor=1.0,
            output_factor=1.0,
            amplification_factor=1.0,
        )
        self.assertAlmostEqual(compute_s, 10.0, places=9)
        self.assertLess(mem_s, compute_s)
        self.assertAlmostEqual(duration, compute_s, places=9)

        duration2, compute_s2, mem_s2, _ = compute_stage_duration_components_s(
            bytes_in=1_000,
            bytes_out=500,
            compute_rate_Bps=10_000.0,
            memory_ceiling_enabled=True,
            memory_Bps_per_stage=100.0,
            stage_units=1,
            input_factor=2.0,
            output_factor=1.0,
            amplification_factor=1.0,
        )
        self.assertAlmostEqual(compute_s2, 0.1, places=9)
        self.assertGreater(mem_s2, compute_s2)
        self.assertAlmostEqual(duration2, mem_s2, places=9)

        duration3, compute_s3, mem_s3, bytes_touched3 = compute_stage_duration_components_s(
            bytes_in=1_000,
            bytes_out=500,
            compute_rate_Bps=100.0,
            memory_ceiling_enabled=False,
            memory_Bps_per_stage=1.0,
            stage_units=1,
            input_factor=5.0,
            output_factor=5.0,
            amplification_factor=5.0,
        )
        self.assertAlmostEqual(duration3, compute_s3, places=9)
        self.assertEqual(mem_s3, 0.0)
        self.assertEqual(bytes_touched3, 0.0)

    def test_tpch_host_touch_only_in_bounce(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            boundaries = sources.DATASET_PROFILES[profile_id]["boundaries_bytes"]
            for multiplier in self.config["size_multipliers"]:
                bounce = self._find_row(profile_id, sources.SCENARIO_PIM_HOST_BOUNCE, float(multiplier))
                direct = self._find_row(profile_id, sources.SCENARIO_PIM_FLOWCXL_DIRECT, float(multiplier))
                cpu = self._find_row(profile_id, sources.SCENARIO_CPU_ONLY, float(multiplier))

                scaled = scale_boundaries_exact(boundaries, float(multiplier))
                expected_touch_bytes = int(scaled[1] + scaled[2])

                self.assertEqual(int(bounce["total_bytes_host_touch"]), expected_touch_bytes)
                self.assertEqual(int(direct["total_bytes_host_touch"]), 0)
                self.assertEqual(int(cpu["total_bytes_host_touch"]), 0)

    def test_tpch_direct_not_slower_than_bounce_all_sizes(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            for multiplier in self.config["size_multipliers"]:
                ratio = self._ratio(self.metrics, dataset_profile=profile_id, multiplier=float(multiplier))
                self.assertGreaterEqual(ratio, 1.0)

    def test_tpch_high_profile_1x_ratio_at_least_2x(self) -> None:
        ratio = self._ratio(self.metrics, dataset_profile=sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE, multiplier=1.0)
        self.assertGreaterEqual(ratio, 2.0)

    def test_tpch_direct_beats_cpu_all_profiles_all_sizes(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            for multiplier in self.config["size_multipliers"]:
                cpu = self._find_row(profile_id, sources.SCENARIO_CPU_ONLY, float(multiplier))
                direct = self._find_row(profile_id, sources.SCENARIO_PIM_FLOWCXL_DIRECT, float(multiplier))
                self.assertLess(float(direct["makespan_s"]), float(cpu["makespan_s"]))

    def test_tpch_overlap_present_in_direct(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            direct_row = self._find_row(profile_id, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0)
            run_id = direct_row["run_id"]
            compute_rows = [row for row in self.traces if row["run_id"] == run_id and row["op_type"] == "COMPUTE"]
            stage1 = [row for row in compute_rows if int(row["stage_id"]) == 1]
            stage2 = [row for row in compute_rows if int(row["stage_id"]) == 2]
            self.assertTrue(stage1 and stage2)
            min_stage2_start = min(float(row["t_start"]) for row in stage2)
            max_stage1_end = max(float(row["t_end"]) for row in stage1)
            self.assertLess(min_stage2_start, max_stage1_end)

    def test_tpch_sensitivity_compute_faster_increases_ratio(self) -> None:
        profile_id = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline_ratio = self._ratio(self.metrics, dataset_profile=profile_id, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile_id]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        for stage_name in sources.TPCH_STAGE_NAMES:
            cfg["pim_speedup_vs_cpu_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP][stage_name] = (
                float(cfg["pim_speedup_vs_cpu_by_stage_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP][stage_name])
                * 10.0
            )

        metrics_fast, _ = generate_runs_from_config(cfg)
        fast_ratio = self._ratio(metrics_fast, dataset_profile=profile_id, multiplier=1.0)
        self.assertGreater(fast_ratio, baseline_ratio)

    def test_tpch_sensitivity_host_link_slower_increases_ratio(self) -> None:
        profile_id = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
        baseline_ratio = self._ratio(self.metrics, dataset_profile=profile_id, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile_id]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        host_link = cfg["link_profile"]["host_link"]
        old_bw = float(sources.LINKS[host_link]["bandwidth_Bps"])
        try:
            sources.LINKS[host_link]["bandwidth_Bps"] = old_bw / 10.0
            metrics_slow, _ = generate_runs_from_config(cfg)
        finally:
            sources.LINKS[host_link]["bandwidth_Bps"] = old_bw
        slow_ratio = self._ratio(metrics_slow, dataset_profile=profile_id, multiplier=1.0)
        self.assertGreater(slow_ratio, baseline_ratio)

    def test_pipeline_template_column_matches_profile_template(self) -> None:
        for row in self.metrics:
            expected = sources.DATASET_PROFILES[row["dataset_profile"]]["pipeline_template"]
            self.assertEqual(row["pipeline_template"], expected)

    def test_metrics_schema_stable_with_memory_columns(self) -> None:
        row = self.metrics[0]
        for key in [
            "memory_ceiling_enabled",
            "total_cpu_mem_time_component_s",
            "total_pim_mem_time_component_s",
            "total_compute_time_component_s",
        ]:
            self.assertIn(key, row)
        for forbidden in ["queue_wait_s", "utilization", "speedup_vs_cpu_only"]:
            self.assertNotIn(forbidden, row)

    def test_deepvariant_profile_still_loads_template(self) -> None:
        profile = sources.DATASET_PROFILES[sources.PROFILE_DV_ILLUMINA_WGS_30X]
        self.assertEqual(profile["pipeline_template"], sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE)
        self.assertEqual(tuple(profile["stage_names"]), sources.DEEPVARIANT_STAGE_NAMES)

    def test_deepvariant_single_run_executes_with_template_maps(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_DV_ILLUMINA_WGS_30X]
        cfg["size_multipliers"] = [1.0]
        metrics, _ = generate_runs_from_config(cfg)
        self.assertEqual(len(metrics), len(cfg["scenarios"]))
        self.assertTrue(all(row["pipeline_template"] == sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE for row in metrics))

    def test_deepvariant_memory_ceiling_disabled_by_default(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_DV_ILLUMINA_WGS_30X]
        cfg["size_multipliers"] = [1.0]
        metrics, _ = generate_runs_from_config(cfg)
        for row in metrics:
            self.assertFalse(bool(row["memory_ceiling_enabled"]))
            self.assertEqual(float(row["total_cpu_mem_time_component_s"]), 0.0)
            self.assertEqual(float(row["total_pim_mem_time_component_s"]), 0.0)

    def test_config_requires_template_specific_memory_maps(self) -> None:
        cfg = copy.deepcopy(self.config)
        del cfg["enable_memory_ceiling_by_template"]
        with self.assertRaises((KeyError, ValueError)):
            generate_runs_from_config(cfg)

    def test_legacy_flat_stage_map_keys_rejected_or_ignored_per_spec(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["scenario_stage_device_map"] = {
            "cpu_only": ["cpu", "cpu", "cpu"],
            "pim_host_bounce": ["pim", "pim", "pim"],
            "pim_flowcxl_direct": ["pim", "pim", "pim"],
        }
        with self.assertRaises(ValueError):
            generate_runs_from_config(cfg)


if __name__ == "__main__":
    unittest.main()
