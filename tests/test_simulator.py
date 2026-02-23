"""Checks for overlap-first pipeline modeling."""

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

    def _ont_ratio(self, metrics: list[dict], multiplier: float = 1.0) -> float:
        bounce = self._find_row(
            dataset_profile=sources.PROFILE_ONT_100Gbases,
            scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
            multiplier=multiplier,
            metrics=metrics,
        )
        direct = self._find_row(
            dataset_profile=sources.PROFILE_ONT_100Gbases,
            scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
            multiplier=multiplier,
            metrics=metrics,
        )
        return float(bounce["makespan_s"]) / float(direct["makespan_s"])

    def test_overlap_direct_stage_pipeline(self) -> None:
        direct_row = self._find_row(
            dataset_profile=sources.PROFILE_ONT_100Gbases,
            scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
            multiplier=1.0,
        )
        run_id = direct_row["run_id"]
        compute_rows = [row for row in self.traces if row["run_id"] == run_id and row["op_type"] == "COMPUTE"]
        stage1 = [row for row in compute_rows if int(row["stage_id"]) == 1]
        stage2 = [row for row in compute_rows if int(row["stage_id"]) == 2]
        self.assertTrue(stage1 and stage2)
        min_stage2_start = min(float(row["t_start"]) for row in stage2)
        max_stage1_end = max(float(row["t_end"]) for row in stage1)
        self.assertLess(min_stage2_start, max_stage1_end)

    def test_streaming_window_limits_admission(self) -> None:
        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_ONT_100Gbases]
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

    def test_host_touch_applies_only_interstage_bounce(self) -> None:
        for dataset_profile in self.config["dataset_profiles"]:
            boundaries = sources.DATASET_PROFILES[dataset_profile]["boundaries_bytes"]
            for multiplier in self.config["size_multipliers"]:
                bounce = self._find_row(
                    dataset_profile=dataset_profile,
                    scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
                    multiplier=float(multiplier),
                )
                scaled = scale_boundaries_exact(boundaries, float(multiplier))
                expected_touch_bytes = sum(scaled[1:-1])
                self.assertEqual(int(bounce["total_bytes_host_touch"]), int(expected_touch_bytes))
                self.assertGreater(int(bounce["total_bytes_host_touch"]), 0)

            direct = self._find_row(
                dataset_profile=dataset_profile,
                scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
                multiplier=1.0,
            )
            cpu = self._find_row(
                dataset_profile=dataset_profile,
                scenario=sources.SCENARIO_CPU_ONLY,
                multiplier=1.0,
            )
            self.assertEqual(int(direct["total_bytes_host_touch"]), 0)
            self.assertEqual(int(cpu["total_bytes_host_touch"]), 0)

    def test_split_h2d_paths_accounting(self) -> None:
        for dataset_profile in self.config["dataset_profiles"]:
            for multiplier in self.config["size_multipliers"]:
                bounce = self._find_row(
                    dataset_profile=dataset_profile,
                    scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
                    multiplier=float(multiplier),
                )
                direct = self._find_row(
                    dataset_profile=dataset_profile,
                    scenario=sources.SCENARIO_PIM_FLOWCXL_DIRECT,
                    multiplier=float(multiplier),
                )
                self.assertGreater(int(bounce["total_bytes_host_h2d_ingress"]), 0)
                self.assertGreater(int(bounce["total_bytes_host_h2d_stage"]), 0)
                self.assertGreater(int(direct["total_bytes_host_h2d_ingress"]), 0)
                self.assertEqual(int(direct["total_bytes_host_h2d_stage"]), 0)

    def test_ont_gain_target_default(self) -> None:
        ratio = self._ont_ratio(self.metrics, multiplier=1.0)
        self.assertGreaterEqual(ratio, 1.05)

    def test_checkA_compute_sensitivity(self) -> None:
        baseline_ratio = self._ont_ratio(self.metrics, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_ONT_100Gbases]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        cfg["stage_defaults"]["pim_unit_compute_Bps"] = float(cfg["stage_defaults"]["pim_unit_compute_Bps"]) * 10.0

        metrics_fast, _ = generate_runs_from_config(cfg)
        fast_ratio = self._ont_ratio(metrics_fast, multiplier=1.0)
        self.assertGreater(fast_ratio, baseline_ratio)

    def test_checkA_link_sensitivity(self) -> None:
        baseline_ratio = self._ont_ratio(self.metrics, multiplier=1.0)

        cfg = copy.deepcopy(self.config)
        cfg["dataset_profiles"] = [sources.PROFILE_ONT_100Gbases]
        cfg["size_multipliers"] = [1.0]
        cfg["scenarios"] = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
        host_link = cfg["link_profile"]["host_link"]
        old_bw = float(sources.LINKS[host_link]["bandwidth_Bps"])
        try:
            sources.LINKS[host_link]["bandwidth_Bps"] = old_bw / 10.0
            metrics_slow, _ = generate_runs_from_config(cfg)
        finally:
            sources.LINKS[host_link]["bandwidth_Bps"] = old_bw
        slow_ratio = self._ont_ratio(metrics_slow, multiplier=1.0)
        self.assertGreater(slow_ratio, baseline_ratio)

    def test_metrics_schema_includes_new_lb_subfields(self) -> None:
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
            self.assertTrue(legacy_forbidden.isdisjoint(row_keys))
            self.assertFalse(any(key.startswith("util_") for key in row_keys))
            self.assertFalse(any(key.startswith("queue_") for key in row_keys))


if __name__ == "__main__":
    unittest.main()
