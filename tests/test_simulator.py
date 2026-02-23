"""Checks for the tiled stage-capacity pipeline model."""

from __future__ import annotations

import copy
import unittest

import yaml

import sources
from simulator import (
    compute_num_tiles,
    generate_runs_from_config,
    scale_boundaries_exact,
    simulate_configuration,
    tile_boundary_bytes,
)


class SimulatorChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open("configs/runs.yaml", "r", encoding="utf-8") as handle:
            cls.config = yaml.safe_load(handle)
        cls.metrics, _ = generate_runs_from_config(cls.config)

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

    def test_scaled_boundaries_conserve_bytes(self) -> None:
        tile_size = int(self.config["tile_size_bytes"])
        for dataset_profile, info in sources.DATASET_PROFILES.items():
            original = info["boundaries_bytes"]
            for multiplier in self.config["size_multipliers"]:
                scaled = scale_boundaries_exact(original, float(multiplier))
                self.assertEqual(sum(scaled), int(round(sum(original) * float(multiplier))))
                num_tiles = compute_num_tiles(scaled, tile_size)
                for boundary_value in scaled:
                    tiles = tile_boundary_bytes(boundary_value, num_tiles)
                    self.assertEqual(len(tiles), num_tiles)
                    self.assertEqual(sum(tiles), boundary_value)

    def test_parallel_units_reduce_makespan(self) -> None:
        dataset_profile = sources.PROFILE_ILLUMINA_NA12878
        boundaries = sources.DATASET_PROFILES[dataset_profile]["boundaries_bytes"]
        low_defaults = copy.deepcopy(self.config["stage_defaults"])
        high_defaults = copy.deepcopy(self.config["stage_defaults"])
        low_defaults["cpu_units"] = 1
        high_defaults["cpu_units"] = 64

        common = {
            "dataset_profile": dataset_profile,
            "boundaries_bytes": boundaries,
            "size_multiplier": 1.0,
            "scenario": sources.SCENARIO_CPU_ONLY,
            "tile_size_bytes": int(self.config["tile_size_bytes"]),
            "host_link": self.config["link_profile"]["host_link"],
            "cxl_direct_link": self.config["link_profile"]["cxl_direct_link"],
            "resource_capacity": self.config["resource_capacity"],
            "transfer_power_W": self.config["transfer_power_W"],
            "stage_overrides": {},
        }

        low_row, _ = simulate_configuration(run_id="low_units", stage_defaults=low_defaults, **common)
        high_row, _ = simulate_configuration(run_id="high_units", stage_defaults=high_defaults, **common)

        self.assertLessEqual(float(high_row["makespan_s"]), float(low_row["makespan_s"]))

    def test_flowcxl_beats_host_bounce(self) -> None:
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
                self.assertLessEqual(float(direct["makespan_s"]), float(bounce["makespan_s"]) + 1e-12)

    def test_cpu_only_has_no_transfer_bytes(self) -> None:
        for dataset_profile in self.config["dataset_profiles"]:
            for row in self._rows(sources.SCENARIO_CPU_ONLY, dataset_profile):
                self.assertEqual(int(row["total_bytes_host_link"]), 0)
                self.assertEqual(int(row["total_bytes_cxl_direct"]), 0)
                self.assertEqual(int(row["total_bytes_moved"]), 0)

    def test_energy_monotonic_with_size(self) -> None:
        for dataset_profile in self.config["dataset_profiles"]:
            for scenario in self.config["scenarios"]:
                rows = self._rows(scenario=scenario, dataset_profile=dataset_profile)
                rows = sorted(rows, key=lambda row: float(row["stage_size_multiplier"]))
                energies = [float(row["total_energy_J"]) for row in rows]
                for idx in range(1, len(energies)):
                    self.assertGreaterEqual(energies[idx] + 1e-12, energies[idx - 1])

    def test_metrics_schema(self) -> None:
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
            "total_bytes_host_link",
            "total_bytes_cxl_direct",
            "total_bytes_moved",
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
