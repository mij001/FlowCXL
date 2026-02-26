"""Optional narrative/regression gates for default story configuration."""

from __future__ import annotations

import os
import unittest

import yaml

import sources
from simulator import generate_runs_from_config


@unittest.skipUnless(
    os.getenv("FLOWCXL_ENABLE_STORY_GATES") == "1",
    "Set FLOWCXL_ENABLE_STORY_GATES=1 to run narrative threshold gates.",
)
class StoryGateChecks(unittest.TestCase):
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
    ) -> dict:
        for row in self.metrics:
            if (
                row["dataset_profile"] == dataset_profile
                and row["scenario"] == scenario
                and abs(float(row["stage_size_multiplier"]) - float(multiplier)) < 1e-12
                and str(row.get("workload_variant", "")) == workload_variant
            ):
                return row
        raise KeyError((dataset_profile, scenario, multiplier, workload_variant))

    def test_tpch_direct_not_slower_than_bounce_all_points(self) -> None:
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

    def test_tpch_high_profile_bounce_dominant_lb_is_host_link_or_host_touch(self) -> None:
        bounce = self._find_row(
            sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
            sources.SCENARIO_PIM_HOST_BOUNCE,
            1.0,
            "base",
        )
        self.assertIn(str(bounce["dominant_lb_component"]), {"host_link", "host_touch"})

    def test_dv_directional_and_strict_improvement(self) -> None:
        for profile in [
            sources.PROFILE_DV_ILLUMINA_WGS_30X,
            sources.PROFILE_DV_ILLUMINA_WES_100X,
        ]:
            has_strict = False
            for variant in ["base", "ingressless"]:
                for multiplier in self.config["size_multipliers"]:
                    bounce = self._find_row(profile, sources.SCENARIO_PIM_HOST_BOUNCE, float(multiplier), variant)
                    direct = self._find_row(profile, sources.SCENARIO_PIM_FLOWCXL_DIRECT, float(multiplier), variant)
                    self.assertLessEqual(float(direct["makespan_s"]), float(bounce["makespan_s"]))
                    if float(direct["makespan_s"]) < float(bounce["makespan_s"]):
                        has_strict = True
            self.assertTrue(has_strict, msg=f"no strict DV direct improvement for {profile}")


if __name__ == "__main__":
    unittest.main()
