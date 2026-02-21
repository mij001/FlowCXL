"""Required model checks."""

from __future__ import annotations

import unittest

import sources
from simulator import generate_fixed_runs, simulate_run


class SimulatorChecks(unittest.TestCase):
    def test_bytes_check(self) -> None:
        payload_bytes = sources.PAYLOAD_FASTQ_BYTES
        link = sources.LINKS["PCIe Gen4 x16"]

        bounce_row, _ = simulate_run(
            run_id="bytes_bounce",
            payload_name="FASTQ_100GB",
            payload_bytes=payload_bytes,
            link_type="PCIe Gen4 x16",
            scenario=sources.SCENARIO_BOUNCE,
            bandwidth_Bps=link["bandwidth_Bps"],
            latency_s=link["latency_s"],
            num_stages=sources.NUM_STAGES,
        )
        chain_row, _ = simulate_run(
            run_id="bytes_chain",
            payload_name="FASTQ_100GB",
            payload_bytes=payload_bytes,
            link_type="CXL Local",
            scenario=sources.SCENARIO_CHAIN,
            bandwidth_Bps=sources.LINKS["CXL Local"]["bandwidth_Bps"],
            latency_s=sources.LINKS["CXL Local"]["latency_s"],
            num_stages=sources.NUM_STAGES,
        )

        self.assertEqual(bounce_row["total_bytes_moved"], 2 * sources.NUM_STAGES * payload_bytes)
        self.assertEqual(chain_row["total_bytes_moved"], 2 * payload_bytes)

    def test_speedup_check(self) -> None:
        metrics, _ = generate_fixed_runs()

        by_key = {
            (row["payload_name"], row["link_type"], row["scenario"]): row
            for row in metrics
        }

        for payload_name, _ in sources.PAYLOADS:
            for link_type in ["CXL Local", "CXL Remote"]:
                bounce = by_key[(payload_name, link_type, sources.SCENARIO_BOUNCE)]
                chain = by_key[(payload_name, link_type, sources.SCENARIO_CHAIN)]
                expected_speedup = bounce["total_transfer_time_s"] / chain["total_transfer_time_s"]
                self.assertAlmostEqual(float(bounce["speedup_vs_chain"]), expected_speedup)


if __name__ == "__main__":
    unittest.main()
