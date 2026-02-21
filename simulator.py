"""Minimal transfer-only simulator for host-bounce vs Flow-CXL chain scenarios."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple

import sources


@dataclass(frozen=True)
class Transfer:
    transfer_index: int
    stage_index: int
    direction: str
    bytes_moved: int


def transfer_time_s(bytes_moved: int, bandwidth_Bps: float, latency_s: float) -> float:
    """Per-transfer equation: T = L + (B / BW)."""
    return latency_s + (bytes_moved / bandwidth_Bps)


def build_transfers(payload_bytes: int, scenario: str, num_stages: int = sources.NUM_STAGES) -> List[Transfer]:
    """Builds serial transfers for each scenario with queueing fixed to zero."""
    transfers: List[Transfer] = []

    if scenario == sources.SCENARIO_BOUNCE:
        next_index = 1
        for stage in range(1, num_stages + 1):
            transfers.append(
                Transfer(
                    transfer_index=next_index,
                    stage_index=stage,
                    direction="H2D",
                    bytes_moved=payload_bytes,
                )
            )
            next_index += 1
            transfers.append(
                Transfer(
                    transfer_index=next_index,
                    stage_index=stage,
                    direction="D2H",
                    bytes_moved=payload_bytes,
                )
            )
            next_index += 1
    elif scenario == sources.SCENARIO_CHAIN:
        transfers.append(
            Transfer(
                transfer_index=1,
                stage_index=1,
                direction="H2D",
                bytes_moved=payload_bytes,
            )
        )
        transfers.append(
            Transfer(
                transfer_index=2,
                stage_index=num_stages,
                direction="D2H",
                bytes_moved=payload_bytes,
            )
        )
    else:
        raise ValueError(f"Unknown scenario: {scenario}")

    return transfers


def simulate_run(
    run_id: str,
    payload_name: str,
    payload_bytes: int,
    link_type: str,
    scenario: str,
    bandwidth_Bps: float,
    latency_s: float,
    num_stages: int = sources.NUM_STAGES,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    """Simulates one serial run and returns aggregate metrics + transfer trace rows."""
    transfers = build_transfers(payload_bytes=payload_bytes, scenario=scenario, num_stages=num_stages)

    total_bytes_moved = sum(t.bytes_moved for t in transfers)
    total_transfer_time_s = 0.0
    trace_rows: List[Dict[str, object]] = []

    for transfer in transfers:
        t_seconds = transfer_time_s(
            bytes_moved=transfer.bytes_moved,
            bandwidth_Bps=bandwidth_Bps,
            latency_s=latency_s,
        )
        total_transfer_time_s += t_seconds

        trace = asdict(transfer)
        trace.update(
            {
                "run_id": run_id,
                "payload_name": payload_name,
                "payload_bytes": payload_bytes,
                "link_type": link_type,
                "scenario": scenario,
                "latency_s": latency_s,
                "bandwidth_Bps": bandwidth_Bps,
                "transfer_time_s": t_seconds,
            }
        )
        trace_rows.append(trace)

    metrics_row = {
        "run_id": run_id,
        "payload_name": payload_name,
        "payload_bytes": payload_bytes,
        "link_type": link_type,
        "scenario": scenario,
        "transfers_count": len(transfers),
        "total_bytes_moved": total_bytes_moved,
        "total_transfer_time_s": total_transfer_time_s,
        "speedup_vs_chain": None,
    }

    return metrics_row, trace_rows


def generate_fixed_runs() -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    """Generates the exact fixed 10 runs requested by the model specification."""
    metrics: List[Dict[str, object]] = []
    traces: List[Dict[str, object]] = []

    run_counter = 1
    for payload_name, payload_bytes in sources.PAYLOADS:
        for link_type, scenario in sources.RUN_LINK_SCENARIOS:
            link = sources.LINKS[link_type]
            run_id = f"run_{run_counter:02d}_{payload_name}_{link_type.replace(' ', '_').replace('/', '_')}_{scenario}"
            run_counter += 1

            row, trace_rows = simulate_run(
                run_id=run_id,
                payload_name=payload_name,
                payload_bytes=payload_bytes,
                link_type=link_type,
                scenario=scenario,
                bandwidth_Bps=link["bandwidth_Bps"],
                latency_s=link["latency_s"],
                num_stages=sources.NUM_STAGES,
            )
            metrics.append(row)
            traces.extend(trace_rows)

    chain_times = {
        (row["payload_name"], row["link_type"]): row["total_transfer_time_s"]
        for row in metrics
        if row["scenario"] == sources.SCENARIO_CHAIN
    }

    for row in metrics:
        if row["scenario"] != sources.SCENARIO_BOUNCE:
            continue
        key = (row["payload_name"], row["link_type"])
        chain_time = chain_times.get(key)
        if chain_time is None:
            continue
        row["speedup_vs_chain"] = row["total_transfer_time_s"] / chain_time

    return metrics, traces
