"""Required checks for the contention-aware transfer model."""

from __future__ import annotations

import unittest

import yaml

import sources
from simulator import bytes_formula, generate_runs_from_config


class SimulatorChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        with open("configs/runs.yaml", "r", encoding="utf-8") as handle:
            cls.config = yaml.safe_load(handle)
        cls.metrics, _ = generate_runs_from_config(cls.config)
        cls.by_key = {
            (row["dataset_profile"], row["link_type"], row["num_chunks"], row["scenario"], row["shared_link"]): row
            for row in cls.metrics
        }

    def test_bytes_formula(self) -> None:
        for dataset_profile, info in sources.DATASET_PROFILES.items():
            boundaries = info["boundaries_bytes"]
            for num_chunks in [1, 8]:
                pcie_row = self.by_key[
                    (
                        dataset_profile,
                        sources.LINK_PCIE_GEN4_X16,
                        num_chunks,
                        sources.SCENARIO_PIM_NO_CXL_BOUNCE,
                        False,
                    )
                ]
                expected_bounce = bytes_formula(boundaries, sources.SCENARIO_PIM_NO_CXL_BOUNCE, num_chunks)
                expected_chain = bytes_formula(boundaries, sources.SCENARIO_PIM_CXL_CHAIN, num_chunks)

                self.assertEqual(int(pcie_row["total_bytes_moved"]), int(expected_bounce))
                for shared_link in [False, True]:
                    cxl_bounce_row = self.by_key[
                        (
                            dataset_profile,
                            sources.LINK_CXL_LOCAL,
                            num_chunks,
                            sources.SCENARIO_PIM_CXL_BOUNCE,
                            shared_link,
                        )
                    ]
                    cxl_chain_row = self.by_key[
                        (
                            dataset_profile,
                            sources.LINK_CXL_LOCAL,
                            num_chunks,
                            sources.SCENARIO_PIM_CXL_CHAIN,
                            shared_link,
                        )
                    ]
                    self.assertEqual(int(cxl_bounce_row["total_bytes_moved"]), int(expected_bounce))
                    self.assertEqual(int(cxl_chain_row["total_bytes_moved"]), int(expected_chain))

    def test_contention_monotonicity(self) -> None:
        for dataset_profile in sources.DATASET_PROFILES:
            cases = [
                (sources.LINK_PCIE_GEN4_X16, sources.SCENARIO_PIM_NO_CXL_BOUNCE, False),
                (sources.LINK_CXL_LOCAL, sources.SCENARIO_PIM_CXL_BOUNCE, False),
                (sources.LINK_CXL_LOCAL, sources.SCENARIO_PIM_CXL_BOUNCE, True),
                (sources.LINK_CXL_REMOTE, sources.SCENARIO_PIM_CXL_BOUNCE, False),
                (sources.LINK_CXL_REMOTE, sources.SCENARIO_PIM_CXL_BOUNCE, True),
                (sources.LINK_CXL_LOCAL, sources.SCENARIO_PIM_CXL_CHAIN, False),
                (sources.LINK_CXL_LOCAL, sources.SCENARIO_PIM_CXL_CHAIN, True),
                (sources.LINK_CXL_REMOTE, sources.SCENARIO_PIM_CXL_CHAIN, False),
                (sources.LINK_CXL_REMOTE, sources.SCENARIO_PIM_CXL_CHAIN, True),
            ]
            for link_type, scenario, shared_link in cases:
                row_k1 = self.by_key[(dataset_profile, link_type, 1, scenario, shared_link)]
                row_k8 = self.by_key[(dataset_profile, link_type, 8, scenario, shared_link)]
                self.assertGreaterEqual(float(row_k8["makespan_s"]), float(row_k1["makespan_s"]))

    def test_chain_saves_bytes(self) -> None:
        for dataset_profile in sources.DATASET_PROFILES:
            for link_type in [sources.LINK_CXL_LOCAL, sources.LINK_CXL_REMOTE]:
                for num_chunks in [1, 8]:
                    for shared_link in [False, True]:
                        bounce = self.by_key[
                            (dataset_profile, link_type, num_chunks, sources.SCENARIO_PIM_CXL_BOUNCE, shared_link)
                        ]
                        chain = self.by_key[
                            (dataset_profile, link_type, num_chunks, sources.SCENARIO_PIM_CXL_CHAIN, shared_link)
                        ]
                        self.assertLess(int(chain["total_bytes_moved"]), int(bounce["total_bytes_moved"]))


if __name__ == "__main__":
    unittest.main()
