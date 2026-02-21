"""Contention-aware transfer simulator for PCIe/CXL host-link staging scenarios."""

from __future__ import annotations

import heapq
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import sources


@dataclass
class ResourceState:
    name: str
    next_free_time: float = 0.0
    busy_time: float = 0.0
    queue_time: float = 0.0


@dataclass(frozen=True)
class TransferOp:
    stage_id: int
    op_type: str
    direction: str
    bytes_moved: int


def pcie_transfer_time_s(bytes_moved: int) -> float:
    return sources.PCIE_FIXED_OVERHEAD_s + (bytes_moved / sources.PCIE4_X16_BW_Bps)


def cxl_transfer_time_s(bytes_moved: int, link_type: str) -> float:
    link = sources.LINKS[link_type]
    return link["latency_s"] + (bytes_moved / link["bandwidth_Bps"])


def schedule_on_resources(
    t_req: float,
    duration: float,
    resources_in_use: Iterable[ResourceState],
) -> Tuple[float, float, float, Dict[str, float], Dict[str, float]]:
    """Schedules one operation and attributes queueing to bottleneck resource(s)."""
    resources_list = list(resources_in_use)
    raw_waits = {resource.name: max(0.0, resource.next_free_time - t_req) for resource in resources_list}
    blocking_wait = max(raw_waits.values()) if raw_waits else 0.0

    t_start = t_req + blocking_wait
    t_end = t_start + duration

    attributed_waits = {resource.name: 0.0 for resource in resources_list}
    if blocking_wait > 0.0:
        winners = [
            resource
            for resource in resources_list
            if abs(raw_waits[resource.name] - blocking_wait) <= 1e-15
        ]
        share = blocking_wait / len(winners)
        for resource in winners:
            resource.queue_time += share
            attributed_waits[resource.name] = share

    for resource in resources_list:
        resource.next_free_time = t_end
        resource.busy_time += duration

    return t_start, t_end, blocking_wait, raw_waits, attributed_waits


def build_stage_operations(boundaries_bytes: List[int], scenario: str) -> List[TransferOp]:
    num_stages = len(boundaries_bytes) - 1
    operations: List[TransferOp] = []

    if scenario in (sources.SCENARIO_PIM_NO_CXL_BOUNCE, sources.SCENARIO_PIM_CXL_BOUNCE):
        for stage_id in range(1, num_stages + 1):
            operations.append(
                TransferOp(
                    stage_id=stage_id,
                    op_type="XFER_H2D",
                    direction="H2D",
                    bytes_moved=int(boundaries_bytes[stage_id - 1]),
                )
            )
            operations.append(
                TransferOp(
                    stage_id=stage_id,
                    op_type="XFER_D2H",
                    direction="D2H",
                    bytes_moved=int(boundaries_bytes[stage_id]),
                )
            )
    elif scenario == sources.SCENARIO_PIM_CXL_CHAIN:
        operations.append(
            TransferOp(
                stage_id=1,
                op_type="XFER_H2D",
                direction="H2D",
                bytes_moved=int(boundaries_bytes[0]),
            )
        )
        operations.append(
            TransferOp(
                stage_id=num_stages,
                op_type="XFER_D2H",
                direction="D2H",
                bytes_moved=int(boundaries_bytes[-1]),
            )
        )
    else:
        raise ValueError(f"Unknown scenario: {scenario}")

    return operations


def transfer_duration_s(scenario: str, link_type: str, bytes_moved: int) -> float:
    if scenario == sources.SCENARIO_PIM_NO_CXL_BOUNCE:
        return pcie_transfer_time_s(bytes_moved)
    if scenario in (sources.SCENARIO_PIM_CXL_BOUNCE, sources.SCENARIO_PIM_CXL_CHAIN):
        return cxl_transfer_time_s(bytes_moved, link_type)
    raise ValueError(f"Unknown scenario: {scenario}")


def resources_for_transfer(
    scenario: str,
    direction: str,
    shared_link: bool,
    resource_states: Dict[str, ResourceState],
) -> List[ResourceState]:
    if scenario == sources.SCENARIO_PIM_NO_CXL_BOUNCE:
        dma_resource = resource_states["dma_h2d"] if direction == "H2D" else resource_states["dma_d2h"]
        if shared_link:
            return [dma_resource, resource_states["pcie_shared"]]
        if direction == "H2D":
            return [dma_resource, resource_states["pcie_h2d"]]
        return [dma_resource, resource_states["pcie_d2h"]]

    if shared_link:
        return [resource_states["cxl_shared"]]

    if direction == "H2D":
        return [resource_states["cxl_h2d"]]
    return [resource_states["cxl_d2h"]]


def bytes_formula(boundaries_bytes: List[int], scenario: str, num_chunks: int) -> int:
    if scenario in (sources.SCENARIO_PIM_NO_CXL_BOUNCE, sources.SCENARIO_PIM_CXL_BOUNCE):
        per_chunk = sum(int(boundaries_bytes[i - 1]) + int(boundaries_bytes[i]) for i in range(1, len(boundaries_bytes)))
        return per_chunk * num_chunks
    if scenario == sources.SCENARIO_PIM_CXL_CHAIN:
        return (int(boundaries_bytes[0]) + int(boundaries_bytes[-1])) * num_chunks
    raise ValueError(f"Unknown scenario: {scenario}")


def simulate_configuration(
    run_id: str,
    dataset_profile: str,
    boundaries_bytes: List[int],
    link_type: str,
    scenario: str,
    num_chunks: int,
    shared_link: bool,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    if scenario == sources.SCENARIO_PIM_NO_CXL_BOUNCE and link_type != sources.LINK_PCIE_GEN4_X16:
        raise ValueError("pim_no_cxl_bounce must use PCIe Gen4 x16")
    if scenario in (sources.SCENARIO_PIM_CXL_BOUNCE, sources.SCENARIO_PIM_CXL_CHAIN) and link_type not in (
        sources.LINK_CXL_LOCAL,
        sources.LINK_CXL_REMOTE,
    ):
        raise ValueError("CXL scenarios must use CXL_LOCAL or CXL_REMOTE")

    num_stages = len(boundaries_bytes) - 1
    operations = build_stage_operations(boundaries_bytes=boundaries_bytes, scenario=scenario)
    resource_states = {name: ResourceState(name=name) for name in sources.RESOURCE_NAMES}

    bytes_counters = {
        "bytes_pcie_h2d": 0,
        "bytes_pcie_d2h": 0,
        "bytes_cxl_h2d": 0,
        "bytes_cxl_d2h": 0,
    }

    traces: List[Dict[str, object]] = []
    completion_times = [0.0 for _ in range(num_chunks)]
    next_op_index = [0 for _ in range(num_chunks)]
    request_heap: List[Tuple[float, int]] = [(0.0, chunk_id) for chunk_id in range(num_chunks)]
    heapq.heapify(request_heap)

    transfer_index = 0
    queue_total_blocking_s = 0.0

    while request_heap:
        t_req, chunk_id = heapq.heappop(request_heap)
        op_index = next_op_index[chunk_id]
        if op_index >= len(operations):
            continue

        op = operations[op_index]
        duration = transfer_duration_s(scenario=scenario, link_type=link_type, bytes_moved=op.bytes_moved)
        resources_in_use = resources_for_transfer(
            scenario=scenario,
            direction=op.direction,
            shared_link=shared_link,
            resource_states=resource_states,
        )
        t_start, t_end, blocking_wait, raw_waits, attributed_waits = schedule_on_resources(
            t_req=t_req,
            duration=duration,
            resources_in_use=resources_in_use,
        )
        queue_total_blocking_s += blocking_wait

        transfer_index += 1

        if scenario == sources.SCENARIO_PIM_NO_CXL_BOUNCE:
            key = "bytes_pcie_h2d" if op.direction == "H2D" else "bytes_pcie_d2h"
            bytes_counters[key] += op.bytes_moved
        else:
            key = "bytes_cxl_h2d" if op.direction == "H2D" else "bytes_cxl_d2h"
            bytes_counters[key] += op.bytes_moved

        for resource in resources_in_use:
            traces.append(
                {
                    "run_id": run_id,
                    "dataset_profile": dataset_profile,
                    "link_type": link_type,
                    "shared_link": shared_link,
                    "scenario": scenario,
                    "num_chunks": num_chunks,
                    "transfer_index": transfer_index,
                    "t_req": t_req,
                    "t_start": t_start,
                    "t_end": t_end,
                    "duration": duration,
                    "blocking_wait": blocking_wait,
                    "resource_wait_raw": raw_waits[resource.name],
                    "queue_wait_attributed": attributed_waits[resource.name],
                    "resource": resource.name,
                    "op_type": op.op_type,
                    "bytes": op.bytes_moved,
                    "chunk_id": chunk_id,
                    "stage_id": op.stage_id,
                }
            )

        next_op_index[chunk_id] = op_index + 1
        completion_times[chunk_id] = t_end
        if next_op_index[chunk_id] < len(operations):
            heapq.heappush(request_heap, (t_end, chunk_id))

    makespan_s = max(completion_times) if completion_times else 0.0
    total_bytes_moved = (
        bytes_counters["bytes_pcie_h2d"]
        + bytes_counters["bytes_pcie_d2h"]
        + bytes_counters["bytes_cxl_h2d"]
        + bytes_counters["bytes_cxl_d2h"]
    )

    metrics_row: Dict[str, object] = {
        "run_id": run_id,
        "dataset_profile": dataset_profile,
        "link_type": link_type,
        "shared_link": shared_link,
        "num_chunks": num_chunks,
        "scenario": scenario,
        "num_stages": num_stages,
        "transfers_count": num_chunks * len(operations),
        "makespan_s": makespan_s,
        "bytes_pcie_h2d": bytes_counters["bytes_pcie_h2d"],
        "bytes_pcie_d2h": bytes_counters["bytes_pcie_d2h"],
        "bytes_cxl_h2d": bytes_counters["bytes_cxl_h2d"],
        "bytes_cxl_d2h": bytes_counters["bytes_cxl_d2h"],
        "total_bytes_moved": total_bytes_moved,
        "speedup_vs_chain": None,
        "queue_total_blocking_s": queue_total_blocking_s,
    }

    queue_total_attributed_s = 0.0
    for resource_name in sources.RESOURCE_NAMES:
        state = resource_states[resource_name]
        queue_field = f"queue_{resource_name}_s"
        util_field = f"util_{resource_name}"
        metrics_row[queue_field] = state.queue_time
        metrics_row[util_field] = (state.busy_time / makespan_s) if makespan_s > 0 else 0.0
        queue_total_attributed_s += state.queue_time

    metrics_row["queue_total_attributed_s"] = queue_total_attributed_s

    expected_bytes = bytes_formula(boundaries_bytes=boundaries_bytes, scenario=scenario, num_chunks=num_chunks)
    if int(total_bytes_moved) != int(expected_bytes):
        raise AssertionError(
            f"Bytes mismatch for {run_id}: got {total_bytes_moved}, expected {expected_bytes}"
        )

    return metrics_row, traces


def generate_runs_from_config(config: Dict[str, object]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    metrics: List[Dict[str, object]] = []
    traces: List[Dict[str, object]] = []

    datasets = config["dataset_profiles"]
    num_chunks_values = config["num_chunks"]
    scenario_matrix = config["scenario_matrix"]

    run_counter = 1
    for dataset_profile in datasets:
        boundaries = sources.DATASET_PROFILES[dataset_profile]["boundaries_bytes"]
        for num_chunks in num_chunks_values:
            for scenario_entry in scenario_matrix:
                scenario = scenario_entry["scenario"]
                shared_modes = scenario_entry.get("shared_link_modes", [False])
                for link_type in scenario_entry["links"]:
                    for shared_link in shared_modes:
                        mode_label = "shared" if shared_link else "duplex"
                        run_id = (
                            f"run_{run_counter:03d}_{dataset_profile}_{link_type}_{scenario}_{mode_label}_k{num_chunks}"
                            .replace(" ", "_")
                            .replace("/", "_")
                        )
                        run_counter += 1
                        row, trace_rows = simulate_configuration(
                            run_id=run_id,
                            dataset_profile=dataset_profile,
                            boundaries_bytes=boundaries,
                            link_type=link_type,
                            scenario=scenario,
                            num_chunks=int(num_chunks),
                            shared_link=bool(shared_link),
                        )
                        metrics.append(row)
                        traces.extend(trace_rows)

    chain_times = {
        (row["dataset_profile"], row["link_type"], row["num_chunks"], row["shared_link"]): row["makespan_s"]
        for row in metrics
        if row["scenario"] == sources.SCENARIO_PIM_CXL_CHAIN
    }

    for row in metrics:
        if row["scenario"] != sources.SCENARIO_PIM_CXL_BOUNCE:
            continue
        key = (row["dataset_profile"], row["link_type"], row["num_chunks"], row["shared_link"])
        chain_time = chain_times.get(key)
        if chain_time is None:
            continue
        row["speedup_vs_chain"] = row["makespan_s"] / chain_time

    return metrics, traces
