"""Checks for true host-bounce and bottleneck diagnostics."""

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

    def _rows(self, scenario: str, dataset_profile: str) -> list[dict]:
        return [
            row
            for row in self.metrics
            if row["scenario"] == scenario and row["dataset_profile"] == dataset_profile
        ]

    def _find_row(self, dataset_profile: str, scenario: str, multiplier: float) -> dict:
        for row in self.metrics:
            if (
                row["dataset_profile"] == dataset_profile
                and row["scenario"] == scenario
                and abs(float(row["stage_size_multiplier"]) - float(multiplier)) < 1e-12
            ):
                return row
        raise KeyError((dataset_profile, scenario, multiplier))

    def _ont_ratio(self, metrics: list[dict], multiplier: float = 1.0) -> float:
        bounce = None
        direct = None
        for row in metrics:
            if (
                row["dataset_profile"] == sources.PROFILE_ONT_100Gbases
                and abs(float(row["stage_size_multiplier"]) - float(multiplier)) < 1e-12
            ):
                if row["scenario"] == sources.SCENARIO_PIM_HOST_BOUNCE:
                    bounce = row
                elif row["scenario"] == sources.SCENARIO_PIM_FLOWCXL_DIRECT:
                    direct = row
        if bounce is None or direct is None:
            raise KeyError("missing ONT bounce/direct rows")
        return float(bounce["makespan_s"]) / float(direct["makespan_s"])

    def test_host_touch_applies_only_interstage_bounce(self) -> None:
        for dataset_profile in self.config["dataset_profiles"]:
            boundaries = sources.DATASET_PROFILES[dataset_profile]["boundaries_bytes"]
            for multiplier in self.config["size_multipliers"]:
                bounce_row = self._find_row(
                    dataset_profile=dataset_profile,
                    scenario=sources.SCENARIO_PIM_HOST_BOUNCE,
                    multiplier=float(multiplier),
                )
                scaled = scale_boundaries_exact(boundaries, float(multiplier))
                expected_touch_bytes = sum(scaled[1:-1])
                self.assertEqual(int(bounce_row["total_bytes_host_touch"]), int(expected_touch_bytes))
                self.assertGreater(int(bounce_row["total_bytes_host_touch"]), 0)

            for scenario in [sources.SCENARIO_CPU_ONLY, sources.SCENARIO_PIM_FLOWCXL_DIRECT]:
                for row in self._rows(scenario, dataset_profile):
                    self.assertEqual(int(row["total_bytes_host_touch"]), 0)

        bounce_touch_ops = [
            row for row in self.traces if row["scenario"] == sources.SCENARIO_PIM_HOST_BOUNCE and row["op_type"] == "HOST_TOUCH"
        ]
        non_bounce_touch_ops = [
            row for row in self.traces if row["scenario"] != sources.SCENARIO_PIM_HOST_BOUNCE and row["op_type"] == "HOST_TOUCH"
        ]
        self.assertGreater(len(bounce_touch_ops), 0)
        self.assertEqual(len(non_bounce_touch_ops), 0)

    def test_host_touch_energy_accounting(self) -> None:
        for dataset_profile in self.config["dataset_profiles"]:
            for row in self._rows(sources.SCENARIO_PIM_HOST_BOUNCE, dataset_profile):
                self.assertGreater(float(row["host_touch_energy_J"]), 0.0)
                self.assertGreaterEqual(float(row["transfer_energy_J"]), float(row["host_touch_energy_J"]))
            for scenario in [sources.SCENARIO_CPU_ONLY, sources.SCENARIO_PIM_FLOWCXL_DIRECT]:
                for row in self._rows(scenario, dataset_profile):
                    self.assertEqual(float(row["host_touch_energy_J"]), 0.0)

    def test_flowcxl_beats_bounce_with_touch_penalty(self) -> None:
        ont_improvement_found = False
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
                bounce_time = float(bounce["makespan_s"])
                direct_time = float(direct["makespan_s"])
                self.assertLessEqual(direct_time, bounce_time + 1e-12)
                if dataset_profile == sources.PROFILE_ONT_100Gbases and bounce_time > direct_time:
                    ont_improvement_found = True
        self.assertTrue(ont_improvement_found)

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

    def test_metrics_schema_includes_new_diagnostics(self) -> None:
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
            "total_bytes_moved",
            "lb_compute_stage_max_s",
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
