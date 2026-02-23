"""Checks for DeepVariant-realistic overlap-first pipeline modeling."""

from __future__ import annotations

import copy
import unittest
from math import prod

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

    def test_deepvariant_profiles_are_three_stage(self) -> None:
        expected_profiles = {
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        }
        self.assertEqual(set(self.config["dataset_profiles"]), expected_profiles)
        for profile_id in expected_profiles:
            profile = sources.DATASET_PROFILES[profile_id]
            self.assertEqual(tuple(profile["stage_names"]), sources.DEEPVARIANT_STAGE_NAMES)
            self.assertEqual(len(profile["boundaries_bytes"]), 4)

    def test_tensor_materialization_boundary_uses_example_shape(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            profile = sources.DATASET_PROFILES[profile_id]
            params = profile["parameters"]
            num_examples = int(profile["num_examples_1x"])
            expected_x1 = (
                num_examples
                * prod(int(dim) for dim in params["example_shape"])
                * int(params["example_element_bytes"])
            )
            self.assertEqual(int(profile["boundaries_bytes"][1]), int(expected_x1))

    def test_stage_device_map_applies_per_scenario(self) -> None:
        stage_map = self.config["scenario_stage_device_map"]
        self.assertEqual(stage_map[sources.SCENARIO_CPU_ONLY], ["cpu", "cpu", "cpu"])
        self.assertEqual(stage_map[sources.SCENARIO_PIM_HOST_BOUNCE], ["pim", "pim", "cpu"])
        self.assertEqual(stage_map[sources.SCENARIO_PIM_FLOWCXL_DIRECT], ["pim", "pim", "cpu"])

        profile_id = self.config["dataset_profiles"][0]
        for scenario, expected_devices in stage_map.items():
            row = self._find_row(profile_id, scenario, 1.0)
            run_id = row["run_id"]
            compute_rows = [r for r in self.traces if r["run_id"] == run_id and r["op_type"] == "COMPUTE"]
            by_stage = {int(r["stage_id"]): str(r["stage_device"]) for r in compute_rows}
            self.assertEqual(by_stage[1], expected_devices[0])
            self.assertEqual(by_stage[2], expected_devices[1])
            self.assertEqual(by_stage[3], expected_devices[2])

    def test_transfer_path_matrix_cpu_pim_transitions(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            bounce = self._find_row(profile_id, sources.SCENARIO_PIM_HOST_BOUNCE, 1.0)
            direct = self._find_row(profile_id, sources.SCENARIO_PIM_FLOWCXL_DIRECT, 1.0)
            cpu = self._find_row(profile_id, sources.SCENARIO_CPU_ONLY, 1.0)

            self.assertGreater(int(bounce["total_bytes_host_h2d_ingress"]), 0)
            self.assertGreater(int(bounce["total_bytes_host_h2d_stage"]), 0)
            self.assertGreater(int(bounce["total_bytes_host_d2h"]), 0)
            self.assertEqual(int(direct["total_bytes_host_h2d_stage"]), 0)
            self.assertGreater(int(direct["total_bytes_host_h2d_ingress"]), 0)
            self.assertGreater(int(direct["total_bytes_host_d2h"]), 0)
            self.assertEqual(int(cpu["total_bytes_host_link"]), 0)
            self.assertEqual(int(cpu["total_bytes_cxl_direct"]), 0)

    def test_host_touch_only_on_pim_to_pim_bounce_transition(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            boundaries = sources.DATASET_PROFILES[profile_id]["boundaries_bytes"]
            for multiplier in self.config["size_multipliers"]:
                bounce = self._find_row(
                    dataset_profile=profile_id,
                    scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
                    multiplier=float(multiplier),
                )
                direct = self._find_row(
                    dataset_profile=profile_id,
                    scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
                    multiplier=float(multiplier),
                )
                cpu = self._find_row(
                    dataset_profile=profile_id,
                    scenario=sources.SCENARIO_CPU_ONLY,
                    multiplier=float(multiplier),
                )
                scaled = scale_boundaries_exact(boundaries, float(multiplier))
                expected_touch_bytes = int(scaled[1])
                self.assertEqual(int(bounce["total_bytes_host_touch"]), expected_touch_bytes)
                self.assertEqual(int(direct["total_bytes_host_touch"]), 0)
                self.assertEqual(int(cpu["total_bytes_host_touch"]), 0)

    def test_cpu_only_has_no_transfer_bytes(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            for multiplier in self.config["size_multipliers"]:
                cpu = self._find_row(profile_id, sources.SCENARIO_CPU_ONLY, float(multiplier))
                self.assertEqual(int(cpu["total_bytes_host_link"]), 0)
                self.assertEqual(int(cpu["total_bytes_cxl_direct"]), 0)
                self.assertEqual(int(cpu["total_bytes_host_touch"]), 0)
                self.assertEqual(int(cpu["total_bytes_moved"]), 0)

    def test_direct_not_slower_than_bounce_all_profiles_all_multipliers(self) -> None:
        for profile_id in self.config["dataset_profiles"]:
            for multiplier in self.config["size_multipliers"]:
                ratio = self._ratio(self.metrics, dataset_profile=profile_id, multiplier=float(multiplier))
                self.assertGreaterEqual(ratio, 1.0)

    def test_direct_strictly_better_at_least_one_point(self) -> None:
        strict = []
        for profile_id in self.config["dataset_profiles"]:
            for multiplier in self.config["size_multipliers"]:
                ratio = self._ratio(self.metrics, dataset_profile=profile_id, multiplier=float(multiplier))
                strict.append(ratio > 1.0 + 1e-9)
        self.assertTrue(any(strict))

    def test_overlap_make_examples_call_variants_direct(self) -> None:
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

    def test_streaming_window_limits_initial_admission(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_DV_ILLUMINA_WGS_30X]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_CPU_ONLY]
        cfg["max_inflight_tiles"] = 4
        metrics_small, traces_small = generate_runs_from_config(cfg)
        row = metrics_small[0]
        run_id = row["run_id"]

        first_ops = [r for r in traces_small if r["run_id"] == run_id and int(r["op_index"]) == 1]
        tile_ids_at_zero = {int(r["tile_id"]) for r in first_ops if abs(float(r["t_req"])) < 1e-15}
        later_tiles = [r for r in first_ops if float(r["t_req"]) > 0.0]

        self.assertEqual(len(tile_ids_at_zero), 4)
        self.assertGreater(len(later_tiles), 0)

    def test_compute_sensitivity_increases_bounce_direct_gap(self) -> None:
        profile_id = self.config["dataset_profiles"][0]
        baseline_ratio = self._ratio(self.metrics, dataset_profile=profile_id, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [profile_id]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        for stage_name in sources.DEEPVARIANT_STAGE_NAMES:
            cfg["pim_speedup_vs_cpu_by_stage"][stage_name] = float(
                cfg["pim_speedup_vs_cpu_by_stage"][stage_name]
            ) * 10.0

        metrics_fast, _ = generate_runs_from_config(cfg)
        fast_ratio = self._ratio(metrics_fast, dataset_profile=profile_id, multiplier=1.0)
        self.assertGreater(fast_ratio, baseline_ratio)

    def test_host_link_sensitivity_increases_bounce_direct_gap(self) -> None:
        profile_id = self.config["dataset_profiles"][0]
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

    def test_metrics_schema_stable_with_pipeline_template(self) -> None:
        expected = {
            "run_id",
            "dataset_profile",
            "stage_size_multiplier",
            "scenario",
            "num_stages",
            "num_tiles",
            "makespan_s",
            "total_energy_J",
            "compute_energy_J",
            "transfer_energy_J",
            "host_touch_energy_J",
            "total_bytes_host_link",
            "total_bytes_cxl_direct",
            "total_bytes_host_touch",
            "total_bytes_host_h2d_ingress",
            "total_bytes_host_h2d_stage",
            "total_bytes_host_d2h",
            "total_bytes_moved",
            "lb_compute_stage_max_s",
            "lb_host_h2d_ingress_s",
            "lb_host_h2d_stage_s",
            "lb_host_d2h_s",
            "lb_host_link_s",
            "lb_host_touch_s",
            "lb_cxl_direct_s",
            "dominant_lb_component",
            "pipeline_template",
        }
        legacy_forbidden = {
            "speedup_vs_chain",
            "queue_total_blocking_s",
            "queue_total_attributed_s",
            "bytes_pcie_h2d",
            "bytes_pcie_d2h",
            "bytes_cxl_h2d",
            "bytes_cxl_d2h",
        }
        for row in self.metrics:
            row_keys = set(row.keys())
            self.assertTrue(expected.issubset(row_keys))
            self.assertEqual(row["pipeline_template"], sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE)
            self.assertTrue(legacy_forbidden.isdisjoint(row_keys))
            self.assertFalse(any(key.startswith("util_") for key in row_keys))
            self.assertFalse(any(key.startswith("queue_") for key in row_keys))

    def test_legacy_profile_ids_absent_from_default_config(self) -> None:
        profile_ids = set(self.config["dataset_profiles"])
        self.assertNotIn("PROFILE_ONT_100Gbases", profile_ids)
        self.assertNotIn("PROFILE_ILLUMINA_NA12878", profile_ids)


if __name__ == "__main__":
    unittest.main()
