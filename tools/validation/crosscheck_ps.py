"""Cross-check CXL processor-sharing scheduler against an independent fluid PS solver."""

from __future__ import annotations

import argparse
import heapq
import math
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import sources
from simulator import CXLProcessorShareScheduler
from tools.validation.common import ensure_validation_config, load_yaml


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cross-check CXL processor-sharing model.")
    parser.add_argument("--config", default="configs/runs.yaml", help="Path to run config YAML.")
    parser.add_argument("--out", default="artifacts/validation", help="Validation output directory.")
    return parser.parse_args(argv)


def _generate_arrivals(payload_bytes: int, concurrency: int, pattern: str) -> List[Tuple[int, float, float]]:
    arrivals: List[Tuple[int, float, float]] = []
    if pattern == "burst":
        for idx in range(concurrency):
            arrivals.append((idx + 1, 0.0, float(payload_bytes)))
        return arrivals
    if pattern == "staggered":
        base_gap = (float(payload_bytes) / 100e9) * 0.2
        for idx in range(concurrency):
            arrivals.append((idx + 1, base_gap * idx, float(payload_bytes)))
        return arrivals
    raise ValueError(f"unknown arrival pattern: {pattern}")


def _reference_ps_solver(arrivals: List[Tuple[int, float, float]], bw_total_Bps: float) -> Dict[int, float]:
    remaining: Dict[int, float] = {}
    finish: Dict[int, float] = {}
    arrivals_sorted = sorted(arrivals, key=lambda item: item[1])
    next_idx = 0
    t_now = arrivals_sorted[0][1] if arrivals_sorted else 0.0
    guard_steps = 0

    while len(finish) < len(arrivals_sorted):
        guard_steps += 1
        if guard_steps > 1_000_000:
            raise RuntimeError("reference PS solver exceeded iteration guard")
        while next_idx < len(arrivals_sorted) and arrivals_sorted[next_idx][1] <= t_now + 1e-15:
            ident, _, bytes_total = arrivals_sorted[next_idx]
            remaining[ident] = float(bytes_total)
            next_idx += 1

        next_arrival_t = arrivals_sorted[next_idx][1] if next_idx < len(arrivals_sorted) else math.inf
        if not remaining:
            t_now = next_arrival_t
            continue

        per_rate = bw_total_Bps / float(len(remaining))
        min_finish_dt = min(rem / per_rate for rem in remaining.values())
        next_completion_t = t_now + min_finish_dt
        t_next = min(next_arrival_t, next_completion_t)
        dt = t_next - t_now
        if dt <= 1e-15:
            ident = min(remaining, key=lambda key: remaining[key])
            finish[ident] = t_now
            remaining.pop(ident, None)
            continue
        served = per_rate * dt
        for ident in list(remaining.keys()):
            remaining[ident] = max(0.0, remaining[ident] - served)
        t_now = t_next

        completed = [ident for ident, rem in remaining.items() if rem <= 1e-9]
        for ident in completed:
            finish[ident] = t_now
            remaining.pop(ident, None)

    return finish


def _scheduler_ps_solver(
    arrivals: List[Tuple[int, float, float]],
    bw_total_Bps: float,
    slots: int,
) -> Dict[int, float]:
    scheduler = CXLProcessorShareScheduler(bw_total_Bps=bw_total_Bps, slots=slots)
    arrivals_sorted = sorted(arrivals, key=lambda item: item[1])
    completion_heap: List[Tuple[float, int, int]] = []
    arrival_idx = 0
    finish: Dict[int, float] = {}
    total_transfers = len(arrivals_sorted)

    while len(finish) < total_transfers:
        next_arrival_t = arrivals_sorted[arrival_idx][1] if arrival_idx < len(arrivals_sorted) else math.inf
        next_completion_t = completion_heap[0][0] if completion_heap else math.inf
        if not math.isfinite(next_arrival_t) and not math.isfinite(next_completion_t):
            break

        if next_completion_t <= next_arrival_t:
            t_done, transfer_id, token = heapq.heappop(completion_heap)
            valid, new_events = scheduler.complete_if_valid(
                transfer_id=transfer_id,
                token=token,
                at_t=t_done,
            )
            if not valid:
                continue
            finish[transfer_id] = t_done
            completion_heap = list(new_events)
            heapq.heapify(completion_heap)
            continue

        if arrival_idx >= len(arrivals_sorted):
            break
        transfer_id, arrival_t, bytes_total = arrivals_sorted[arrival_idx]
        arrival_idx += 1
        admitted, events = scheduler.try_admit(
            transfer_id=transfer_id,
            bytes_total=int(bytes_total),
            at_t=arrival_t,
        )
        if not admitted:
            raise RuntimeError(
                "cross-check admission failed; increase slots or reduce concurrency for this run"
            )
        completion_heap = list(events)
        heapq.heapify(completion_heap)

    if len(finish) != total_transfers:
        raise RuntimeError("scheduler cross-check could not complete all transfers")
    return finish


def run_crosscheck(config: Dict[str, object], out_dir: Path) -> Dict[str, object]:
    validation = ensure_validation_config(config)
    cross_cfg = validation["crosscheck"]
    if not bool(cross_cfg.get("enabled", False)):
        return {"enabled": False, "reason": "validation.crosscheck.enabled=false"}
    if str(cross_cfg.get("reference_model")) != "processor_share":
        raise ValueError("validation.crosscheck.reference_model must be processor_share")

    cal_cfg = validation["calibration"]
    payloads = [int(v) for v in cal_cfg["payload_bytes"]]
    conc_levels = [int(v) for v in cal_cfg["concurrency_levels"]]
    system_id = str(validation["system_id"])

    cxl_link = str(config["link_profile"]["cxl_direct_link"])
    bw_total = float(sources.LINKS[cxl_link]["bandwidth_Bps"])
    slots = int(config["resource_capacity"]["cxl_direct_channels"]) * int(
        config["cxl_direct_concurrency"]["virtual_channels_per_channel"]
    )

    rows: List[Dict[str, object]] = []
    for pattern in ["burst", "staggered"]:
        for payload in payloads:
            for conc in conc_levels:
                arrivals = _generate_arrivals(payload_bytes=payload, concurrency=conc, pattern=pattern)
                ref_finish = _reference_ps_solver(arrivals=arrivals, bw_total_Bps=bw_total)
                sim_finish = _scheduler_ps_solver(
                    arrivals=arrivals,
                    bw_total_Bps=bw_total,
                    slots=max(slots, conc),
                )
                ids = sorted(ref_finish.keys())
                diffs = [abs(sim_finish[i] - ref_finish[i]) for i in ids]
                rels = [
                    (abs(sim_finish[i] - ref_finish[i]) / max(ref_finish[i], 1e-12)) * 100.0
                    for i in ids
                ]
                sim_makespan = max(sim_finish.values()) if sim_finish else 0.0
                ref_makespan = max(ref_finish.values()) if ref_finish else 0.0
                rows.append(
                    {
                        "system_id": system_id,
                        "pattern": pattern,
                        "payload_bytes": payload,
                        "concurrency": conc,
                        "sim_makespan_s": sim_makespan,
                        "ref_makespan_s": ref_makespan,
                        "mae_s": sum(diffs) / len(diffs) if diffs else 0.0,
                        "mape_percent": sum(rels) / len(rels) if rels else 0.0,
                        "max_abs_error_s": max(diffs) if diffs else 0.0,
                        "max_abs_pct_error": max(rels) if rels else 0.0,
                        "tolerance_mape_percent": float(cross_cfg["tolerance_mape_percent"]),
                        "passes_tolerance": (sum(rels) / len(rels) if rels else 0.0)
                        <= float(cross_cfg["tolerance_mape_percent"]),
                    }
                )

    out_dir.mkdir(parents=True, exist_ok=True)
    cross_df = pd.DataFrame(rows)
    out_path = out_dir / "cxl_ps_crosscheck.csv"
    cross_df.to_csv(out_path, index=False)
    return {
        "system_id": system_id,
        "crosscheck_csv": str(out_path),
        "max_mape_percent": float(cross_df["mape_percent"].max()),
    }


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config = load_yaml(Path(args.config))
    out_dir = Path(args.out)
    summary = run_crosscheck(config=config, out_dir=out_dir)
    print(f"Wrote {summary.get('crosscheck_csv', '')}")


if __name__ == "__main__":
    main()
