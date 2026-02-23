"""Checks for template-aware TPC-H + DeepVariant overlap-first modeling."""

from __future__ import annotations

import copy
import unittest

import yaml

import sources
from simulator import generate_runs_from_config, scale_boundaries_exact


class SimulatorChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open("configs/runs.yaml", "r", encoding="utf-8") as handle:
            cls.config = yaml.safe_load(handle)
        cls.metrics, cls.traces = generate_runs_from_config(cls.config)

    def _find_row(self, dataset_profile: str, scenario: str, multiplier: float, metrics: list[dict] | None = None) -> dict:
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

    def test_tpch_profiles_are_three_stage_and_template_tagged(self) -> None:
        expected_profiles = {
            sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        }
        self.assertEqual(set(self.config["dataset_profiles"]), expected_profiles)
        for profile_id in expected_profiles:
            profile = sources.DATASET_PROFILES[profile_id]
            self.assertEqual(profile["pipeline_template"], sources.PIPELINE_TEMPLATE_TPCH_3OP)
            self.assertEqual(tuple(profile["stage_names"]), sources.TPCH_STAGE_NAMES)
            self.assertEqual(len(profile["boundaries_bytes"]), 4)

    def test_tpch_boundaries_derived_from_selectivity_and_fanout(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            profile = sources.DATASET_PROFILES[profile_id]
            params = profile["parameters"]

            rows_scan_in = max(1, int(round(float(params["scale_factor"]) * float(params["lineitem_rows_per_sf"]))))
            rows_scan_out = max(1, int(round(rows_scan_in * float(params["scan_selectivity"]))))
            rows_join_out = max(1, int(round(rows_scan_out * float(params["join_fanout"]))))
            rows_agg_out = max(1, int(round(rows_join_out * float(params["agg_reduction_ratio"]))))

            expected = [
                rows_scan_in * int(params["scan_input_row_bytes"]),
                rows_scan_out * int(params["scan_projected_row_bytes"]),
                rows_join_out * int(params["join_output_row_bytes"]),
                rows_agg_out * int(params["agg_output_row_bytes"]),
            ]
            self.assertEqual([int(x) for x in profile["boundaries_bytes"]], [int(x) for x in expected])

    def test_tpch_stage_map_is_pim_pim_pim_for_pim_scenarios(self) -> None:
        stage_map = self.config["scenario_stage_device_map_by_template"][sources.PIPELINE_TEMPLATE_TPCH_3OP]
        self.assertEqual(stage_map[sources.SCENARIO_CPU_ONLY], ["cpu", "cpu", "cpu"])
        self.assertEqual(stage_map[sources.SCENARIO_PIM_HOST_BOUNCE], ["pim", "pim", "pim"])
        self.assertEqual(stage_map[sources.SCENARIO_PIM_FLOWCXL_DIRECT], ["pim", "pim", "pim"])

        profile_id = sources.PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE
        for scenario, expected_devices in stage_map.items():
            row = self._find_row(profile_id, scenario, 1.0)
            run_id = row["run_id"]
            compute_rows = [r for r in self.traces if r["run_id"] == run_id and r["op_type"] == "COMPUTE"]
            by_stage = {int(r["stage_id"]): str(r["stage_device"]) for r in compute_rows}
            self.assertEqual(by_stage[1], expected_devices[0])
            self.assertEqual(by_stage[2], expected_devices[1])
            self.assertEqual(by_stage[3], expected_devices[2])

    def test_tpch_transfer_matrix_pim_to_pim_paths(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            bounce = self._find_row(profile_id, sources.SCENARIO_PIM_HOST_BOUNCE, 1.0)
            direct = self._find_row(profile_id, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0)
            cpu = self._find_row(profile_id, sources.SCENARIO_CPU_ONLY, 1.0)

            self.assertGreater(int(bounce["total_bytes_host_h2d_ingress"]), 0)
            self.assertGreater(int(bounce["total_bytes_host_h2d_stage"]), 0)
            self.assertGreater(int(bounce["total_bytes_host_d2h"]), 0)
            self.assertEqual(int(bounce["total_bytes_cxl_direct"]), 0)

            self.assertGreater(int(direct["total_bytes_host_h2d_ingress"]), 0)
            self.assertEqual(int(direct["total_bytes_host_h2d_stage"]), 0)
            self.assertGreater(int(direct["total_bytes_host_d2h"]), 0)
            self.assertGreater(int(direct["total_bytes_cxl_direct"]), 0)

            self.assertEqual(int(cpu["total_bytes_host_link"]), 0)
            self.assertEqual(int(cpu["total_bytes_cxl_direct"]), 0)
            self.assertEqual(int(cpu["total_bytes_host_touch"]), 0)

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

    def test_config_requires_template_specific_maps(self) -> None:
        cfg = copy.deepcopy(self.config)
        del cfg["scenario_stage_device_map_by_template"]
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
