"""Tiled stage-capacity simulator for CPU-only and PIM pipeline scenarios."""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Dict, List, Sequence, Tuple

import sources


@dataclass(frozen=True)
class StageConfig:
    cpu_units: int
    cpu_unit_compute_Bps: float
    cpu_unit_power_W: float
    pim_units: int
    pim_unit_compute_Bps: float
    pim_unit_power_W: float
    host_touch_Bps: float
    host_touch_fixed_s: float


@dataclass(frozen=True)
class TileOperation:
    op_type: str
    stage_id: int
    boundary_index: int
    transfer_path: str = ""


@dataclass
class ResourcePool:
    name: str
    capacity: int
    power_W: float
    next_free_time_by_slot: List[float] = field(init=False)
    busy_time_s: float = 0.0

    def __post_init__(self) -> None:
        if self.capacity <= 0:
            raise ValueError(f"ResourcePool capacity must be >= 1 for {self.name}")
        self.next_free_time_by_slot = [0.0 for _ in range(self.capacity)]

    def schedule(self, t_req: float, duration_s: float) -> Tuple[float, float, float, int]:
        slot_idx = min(range(self.capacity), key=lambda idx: self.next_free_time_by_slot[idx])
        t_start = max(t_req, self.next_free_time_by_slot[slot_idx])
        t_end = t_start + duration_s
        wait_s = t_start - t_req
        self.next_free_time_by_slot[slot_idx] = t_end
        self.busy_time_s += duration_s
        return t_start, t_end, wait_s, slot_idx


def scale_boundaries_exact(boundaries_bytes: Sequence[int], multiplier: float) -> List[int]:
    if multiplier <= 0:
        raise ValueError("size multiplier must be > 0")

    raw_scaled = [float(value) * multiplier for value in boundaries_bytes]
    floor_scaled = [int(math.floor(value)) for value in raw_scaled]
    target_sum = int(round(sum(boundaries_bytes) * multiplier))
    remainder = target_sum - sum(floor_scaled)

    if remainder > 0:
        order = sorted(range(len(raw_scaled)), key=lambda idx: raw_scaled[idx] - floor_scaled[idx], reverse=True)
        for idx in order[:remainder]:
            floor_scaled[idx] += 1
    elif remainder < 0:
        order = sorted(range(len(raw_scaled)), key=lambda idx: raw_scaled[idx] - floor_scaled[idx])
        for idx in order[: abs(remainder)]:
            if floor_scaled[idx] > 0:
                floor_scaled[idx] -= 1

    if sum(floor_scaled) != target_sum:
        raise AssertionError("scaled boundary sum mismatch")
    return floor_scaled


def compute_num_tiles(boundaries_bytes: Sequence[int], tile_size_bytes: int) -> int:
    if tile_size_bytes <= 0:
        raise ValueError("tile_size_bytes must be > 0")
    max_boundary = max(boundaries_bytes) if boundaries_bytes else 0
    return max(1, int(math.ceil(max_boundary / tile_size_bytes)))


def tile_boundary_bytes(total_bytes: int, num_tiles: int) -> List[int]:
    if num_tiles <= 0:
        raise ValueError("num_tiles must be > 0")
    if total_bytes < 0:
        raise ValueError("total_bytes must be >= 0")
    base = total_bytes // num_tiles
    remainder = total_bytes % num_tiles
    tiled = [base + 1 for _ in range(remainder)] + [base for _ in range(num_tiles - remainder)]
    if sum(tiled) != total_bytes:
        raise AssertionError("tile partition sum mismatch")
    return tiled


def transfer_duration_s(bytes_moved: int, link_type: str) -> float:
    if link_type not in sources.LINKS:
        raise ValueError(f"unknown link type: {link_type}")
    link = sources.LINKS[link_type]
    return float(link["latency_s"]) + (bytes_moved / float(link["bandwidth_Bps"]))


def compute_duration_s(bytes_moved: int, compute_rate_Bps: float) -> float:
    if compute_rate_Bps <= 0:
        raise ValueError("compute rate must be > 0")
    return bytes_moved / compute_rate_Bps


def host_touch_duration_s(bytes_moved: int, touch_Bps: float, touch_fixed_s: float) -> float:
    if touch_Bps <= 0:
        raise ValueError("host touch bandwidth must be > 0")
    if touch_fixed_s < 0:
        raise ValueError("host touch fixed overhead must be >= 0")
    return touch_fixed_s + (bytes_moved / touch_Bps)


def _build_tile_operations(num_stages: int, scenario: str) -> List[TileOperation]:
    operations: List[TileOperation] = []

    if scenario == sources.SCENARIO_CPU_ONLY:
        for stage_id in range(1, num_stages + 1):
            operations.append(
                TileOperation(op_type="COMPUTE", stage_id=stage_id, boundary_index=stage_id - 1)
            )
        return operations

    operations.append(
        TileOperation(op_type="TRANSFER", stage_id=1, boundary_index=0, transfer_path="host_h2d")
    )

    for stage_id in range(1, num_stages + 1):
        operations.append(TileOperation(op_type="COMPUTE", stage_id=stage_id, boundary_index=stage_id - 1))
        if stage_id < num_stages:
            if scenario == sources.SCENARIO_PIM_HOST_BOUNCE:
                operations.append(
                    TileOperation(
                        op_type="TRANSFER",
                        stage_id=stage_id,
                        boundary_index=stage_id,
                        transfer_path="host_d2h",
                    )
                )
                operations.append(
                    TileOperation(
                        op_type="HOST_TOUCH",
                        stage_id=stage_id,
                        boundary_index=stage_id,
                        transfer_path="host_touch",
                    )
                )
                operations.append(
                    TileOperation(
                        op_type="TRANSFER",
                        stage_id=stage_id + 1,
                        boundary_index=stage_id,
                        transfer_path="host_h2d",
                    )
                )
            elif scenario == sources.SCENARIO_PIM_FLOWCXL_DIRECT:
                operations.append(
                    TileOperation(
                        op_type="TRANSFER",
                        stage_id=stage_id,
                        boundary_index=stage_id,
                        transfer_path="cxl_direct",
                    )
                )
            else:
                raise ValueError(f"unknown scenario: {scenario}")
        else:
            operations.append(
                TileOperation(
                    op_type="TRANSFER",
                    stage_id=stage_id,
                    boundary_index=num_stages,
                    transfer_path="host_d2h",
                )
            )
    return operations


def _stage_overrides_for_dataset(
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]], dataset_profile: str
) -> Dict[object, Dict[str, object]]:
    return stage_overrides.get(dataset_profile, {})


def _build_stage_configs(
    dataset_profile: str,
    num_stages: int,
    stage_defaults: Dict[str, object],
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]],
) -> List[StageConfig]:
    required_keys = [
        "cpu_units",
        "cpu_unit_compute_Bps",
        "cpu_unit_power_W",
        "pim_units",
        "pim_unit_compute_Bps",
        "pim_unit_power_W",
        "host_touch_Bps",
        "host_touch_fixed_s",
    ]
    missing = [key for key in required_keys if key not in stage_defaults]
    if missing:
        raise KeyError(f"missing stage defaults: {missing}")

    dataset_overrides = _stage_overrides_for_dataset(stage_overrides=stage_overrides, dataset_profile=dataset_profile)

    stage_configs: List[StageConfig] = []
    for stage_id in range(1, num_stages + 1):
        merged = dict(stage_defaults)
        stage_override = dataset_overrides.get(stage_id, dataset_overrides.get(str(stage_id), {}))
        if stage_override:
            merged.update(stage_override)
        stage_configs.append(
            StageConfig(
                cpu_units=int(merged["cpu_units"]),
                cpu_unit_compute_Bps=float(merged["cpu_unit_compute_Bps"]),
                cpu_unit_power_W=float(merged["cpu_unit_power_W"]),
                pim_units=int(merged["pim_units"]),
                pim_unit_compute_Bps=float(merged["pim_unit_compute_Bps"]),
                pim_unit_power_W=float(merged["pim_unit_power_W"]),
                host_touch_Bps=float(merged["host_touch_Bps"]),
                host_touch_fixed_s=float(merged["host_touch_fixed_s"]),
            )
        )
    return stage_configs


def _validate_config(config: Dict[str, object]) -> None:
    required_top_level = [
        "dataset_profiles",
        "size_multipliers",
        "tile_size_bytes",
        "scenarios",
        "link_profile",
        "resource_capacity",
        "stage_defaults",
        "transfer_power_W",
    ]
    missing = [key for key in required_top_level if key not in config]
    if missing:
        raise KeyError(f"missing config keys: {missing}")

    link_profile = config["link_profile"]
    if "host_link" not in link_profile or "cxl_direct_link" not in link_profile:
        raise KeyError("link_profile must include host_link and cxl_direct_link")

    resource_capacity = config["resource_capacity"]
    for key in ["host_h2d_channels", "host_d2h_channels", "cxl_direct_channels", "host_touch_channels"]:
        if key not in resource_capacity:
            raise KeyError(f"resource_capacity missing key: {key}")
        if int(resource_capacity[key]) <= 0:
            raise ValueError(f"{key} must be > 0")

    transfer_power_W = config["transfer_power_W"]
    for key in ["host_h2d_channel", "host_d2h_channel", "cxl_direct_channel", "host_touch_channel"]:
        if key not in transfer_power_W:
            raise KeyError(f"transfer_power_W missing key: {key}")
        if float(transfer_power_W[key]) < 0.0:
            raise ValueError(f"{key} must be >= 0")

    stage_defaults = config["stage_defaults"]
    for key in ["host_touch_Bps", "host_touch_fixed_s"]:
        if key not in stage_defaults:
            raise KeyError(f"stage_defaults missing key: {key}")
    if float(stage_defaults["host_touch_Bps"]) <= 0:
        raise ValueError("host_touch_Bps must be > 0")
    if float(stage_defaults["host_touch_fixed_s"]) < 0:
        raise ValueError("host_touch_fixed_s must be >= 0")

    for scenario in config["scenarios"]:
        if scenario not in sources.SCENARIOS:
            raise ValueError(f"unknown scenario in config: {scenario}")

    for dataset_profile in config["dataset_profiles"]:
        if dataset_profile not in sources.DATASET_PROFILES:
            raise ValueError(f"unknown dataset profile: {dataset_profile}")

    if int(config["tile_size_bytes"]) <= 0:
        raise ValueError("tile_size_bytes must be > 0")


def _pool_lower_bound_s(resource: ResourcePool) -> float:
    return resource.busy_time_s / float(resource.capacity)


def _dominant_lb_component(
    lb_compute_stage_max_s: float,
    lb_host_link_s: float,
    lb_host_touch_s: float,
    lb_cxl_direct_s: float,
) -> str:
    components = [
        ("compute_stage_max", lb_compute_stage_max_s),
        ("host_link", lb_host_link_s),
        ("host_touch", lb_host_touch_s),
        ("cxl_direct", lb_cxl_direct_s),
    ]
    components.sort(key=lambda item: item[1], reverse=True)
    return components[0][0]


def simulate_configuration(
    run_id: str,
    dataset_profile: str,
    boundaries_bytes: Sequence[int],
    size_multiplier: float,
    scenario: str,
    tile_size_bytes: int,
    host_link: str,
    cxl_direct_link: str,
    resource_capacity: Dict[str, object],
    stage_defaults: Dict[str, object],
    transfer_power_W: Dict[str, object],
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]],
    trace_max_tiles: int | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    if scenario not in sources.SCENARIOS:
        raise ValueError(f"unknown scenario: {scenario}")
    if host_link not in sources.LINKS:
        raise ValueError(f"unknown host link: {host_link}")
    if cxl_direct_link not in sources.LINKS:
        raise ValueError(f"unknown cxl direct link: {cxl_direct_link}")

    scaled_boundaries = scale_boundaries_exact(boundaries_bytes=boundaries_bytes, multiplier=size_multiplier)
    num_stages = len(scaled_boundaries) - 1
    if num_stages <= 0:
        raise ValueError("dataset profile must contain at least two boundaries")
    num_tiles = compute_num_tiles(boundaries_bytes=scaled_boundaries, tile_size_bytes=tile_size_bytes)
    tiled_boundaries = [tile_boundary_bytes(total_bytes=value, num_tiles=num_tiles) for value in scaled_boundaries]

    stage_configs = _build_stage_configs(
        dataset_profile=dataset_profile,
        num_stages=num_stages,
        stage_defaults=stage_defaults,
        stage_overrides=stage_overrides,
    )

    host_h2d_pool = ResourcePool(
        name="host_h2d",
        capacity=int(resource_capacity["host_h2d_channels"]),
        power_W=float(transfer_power_W["host_h2d_channel"]),
    )
    host_d2h_pool = ResourcePool(
        name="host_d2h",
        capacity=int(resource_capacity["host_d2h_channels"]),
        power_W=float(transfer_power_W["host_d2h_channel"]),
    )
    cxl_direct_pool = ResourcePool(
        name="cxl_direct",
        capacity=int(resource_capacity["cxl_direct_channels"]),
        power_W=float(transfer_power_W["cxl_direct_channel"]),
    )
    host_touch_pool = ResourcePool(
        name="host_touch",
        capacity=int(resource_capacity["host_touch_channels"]),
        power_W=float(transfer_power_W["host_touch_channel"]),
    )

    compute_pools: List[ResourcePool] = []
    for stage_id in range(1, num_stages + 1):
        stage_cfg = stage_configs[stage_id - 1]
        if scenario == sources.SCENARIO_CPU_ONLY:
            compute_pools.append(
                ResourcePool(
                    name=f"cpu_stage_{stage_id}",
                    capacity=stage_cfg.cpu_units,
                    power_W=stage_cfg.cpu_unit_power_W,
                )
            )
        else:
            compute_pools.append(
                ResourcePool(
                    name=f"pim_stage_{stage_id}",
                    capacity=stage_cfg.pim_units,
                    power_W=stage_cfg.pim_unit_power_W,
                )
            )

    operations = _build_tile_operations(num_stages=num_stages, scenario=scenario)

    traces: List[Dict[str, object]] = []
    completion_times = [0.0 for _ in range(num_tiles)]
    next_op_index = [0 for _ in range(num_tiles)]
    request_heap: List[Tuple[float, int]] = [(0.0, tile_id) for tile_id in range(num_tiles)]
    heapq.heapify(request_heap)

    total_bytes_host_link = 0
    total_bytes_cxl_direct = 0
    total_bytes_host_touch = 0

    while request_heap:
        t_req, tile_id = heapq.heappop(request_heap)
        op_index = next_op_index[tile_id]
        if op_index >= len(operations):
            continue

        operation = operations[op_index]
        stage_cfg = stage_configs[operation.stage_id - 1]
        bytes_moved = int(tiled_boundaries[operation.boundary_index][tile_id])

        if operation.op_type == "COMPUTE":
            if scenario == sources.SCENARIO_CPU_ONLY:
                compute_rate = stage_cfg.cpu_unit_compute_Bps
            else:
                compute_rate = stage_cfg.pim_unit_compute_Bps
            duration_s = compute_duration_s(bytes_moved=bytes_moved, compute_rate_Bps=compute_rate)
            pool = compute_pools[operation.stage_id - 1]
            link_used = ""
            transfer_path = ""
        elif operation.op_type == "HOST_TOUCH":
            duration_s = host_touch_duration_s(
                bytes_moved=bytes_moved,
                touch_Bps=stage_cfg.host_touch_Bps,
                touch_fixed_s=stage_cfg.host_touch_fixed_s,
            )
            pool = host_touch_pool
            link_used = ""
            transfer_path = operation.transfer_path
            total_bytes_host_touch += bytes_moved
        else:
            if operation.transfer_path == "host_h2d":
                pool = host_h2d_pool
                link_used = host_link
                total_bytes_host_link += bytes_moved
            elif operation.transfer_path == "host_d2h":
                pool = host_d2h_pool
                link_used = host_link
                total_bytes_host_link += bytes_moved
            elif operation.transfer_path == "cxl_direct":
                pool = cxl_direct_pool
                link_used = cxl_direct_link
                total_bytes_cxl_direct += bytes_moved
            else:
                raise ValueError(f"unknown transfer path: {operation.transfer_path}")
            transfer_path = operation.transfer_path
            duration_s = transfer_duration_s(bytes_moved=bytes_moved, link_type=link_used)

        t_start, t_end, wait_s, slot_idx = pool.schedule(t_req=t_req, duration_s=duration_s)
        if trace_max_tiles is None or tile_id < trace_max_tiles:
            traces.append(
                {
                    "run_id": run_id,
                    "dataset_profile": dataset_profile,
                    "stage_size_multiplier": size_multiplier,
                    "scenario": scenario,
                    "tile_id": tile_id,
                    "op_index": op_index + 1,
                    "stage_id": operation.stage_id,
                    "op_type": operation.op_type,
                    "transfer_path": transfer_path,
                    "resource": pool.name,
                    "resource_slot": slot_idx,
                    "link_type": link_used,
                    "bytes": bytes_moved,
                    "t_req": t_req,
                    "t_start": t_start,
                    "t_end": t_end,
                    "duration_s": duration_s,
                    "wait_s": wait_s,
                }
            )

        next_op_index[tile_id] = op_index + 1
        completion_times[tile_id] = t_end
        if next_op_index[tile_id] < len(operations):
            heapq.heappush(request_heap, (t_end, tile_id))

    makespan_s = max(completion_times) if completion_times else 0.0

    compute_energy_J = sum(pool.busy_time_s * pool.power_W for pool in compute_pools)
    host_touch_energy_J = host_touch_pool.busy_time_s * host_touch_pool.power_W
    transfer_energy_J = sum(
        pool.busy_time_s * pool.power_W for pool in [host_h2d_pool, host_d2h_pool, cxl_direct_pool]
    ) + host_touch_energy_J
    total_energy_J = compute_energy_J + transfer_energy_J

    lb_compute_stage_max_s = max(_pool_lower_bound_s(pool) for pool in compute_pools) if compute_pools else 0.0
    lb_host_link_s = max(_pool_lower_bound_s(host_h2d_pool), _pool_lower_bound_s(host_d2h_pool))
    lb_host_touch_s = _pool_lower_bound_s(host_touch_pool)
    lb_cxl_direct_s = _pool_lower_bound_s(cxl_direct_pool)
    dominant_lb_component = _dominant_lb_component(
        lb_compute_stage_max_s=lb_compute_stage_max_s,
        lb_host_link_s=lb_host_link_s,
        lb_host_touch_s=lb_host_touch_s,
        lb_cxl_direct_s=lb_cxl_direct_s,
    )

    metrics_row: Dict[str, object] = {
        "run_id": run_id,
        "dataset_profile": dataset_profile,
        "stage_size_multiplier": size_multiplier,
        "scenario": scenario,
        "num_stages": num_stages,
        "num_tiles": num_tiles,
        "makespan_s": makespan_s,
        "total_energy_J": total_energy_J,
        "compute_energy_J": compute_energy_J,
        "transfer_energy_J": transfer_energy_J,
        "host_touch_energy_J": host_touch_energy_J,
        "total_bytes_host_link": total_bytes_host_link,
        "total_bytes_cxl_direct": total_bytes_cxl_direct,
        "total_bytes_host_touch": total_bytes_host_touch,
        "total_bytes_moved": total_bytes_host_link + total_bytes_cxl_direct,
        "lb_compute_stage_max_s": lb_compute_stage_max_s,
        "lb_host_link_s": lb_host_link_s,
        "lb_host_touch_s": lb_host_touch_s,
        "lb_cxl_direct_s": lb_cxl_direct_s,
        "dominant_lb_component": dominant_lb_component,
    }
    return metrics_row, traces


def generate_runs_from_config(config: Dict[str, object]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    _validate_config(config)

    metrics: List[Dict[str, object]] = []
    traces: List[Dict[str, object]] = []

    dataset_profiles = config["dataset_profiles"]
    size_multipliers = config["size_multipliers"]
    scenarios = config["scenarios"]
    tile_size_bytes = int(config["tile_size_bytes"])
    host_link = config["link_profile"]["host_link"]
    cxl_direct_link = config["link_profile"]["cxl_direct_link"]
    resource_capacity = config["resource_capacity"]
    stage_defaults = config["stage_defaults"]
    transfer_power_W = config["transfer_power_W"]
    stage_overrides = config.get("stage_overrides", {})
    trace_max_tiles_raw = config.get("trace_max_tiles", 512)
    trace_max_tiles: int | None
    if trace_max_tiles_raw is None:
        trace_max_tiles = None
    else:
        trace_max_tiles = int(trace_max_tiles_raw)
        if trace_max_tiles < 0:
            trace_max_tiles = None

    run_counter = 1
    for dataset_profile in dataset_profiles:
        boundaries = sources.DATASET_PROFILES[dataset_profile]["boundaries_bytes"]
        for size_multiplier in size_multipliers:
            for scenario in scenarios:
                multiplier_token = str(size_multiplier).replace(".", "p")
                run_id = (
                    f"run_{run_counter:03d}_{dataset_profile}_{scenario}_m{multiplier_token}"
                    .replace(" ", "_")
                    .replace("/", "_")
                )
                run_counter += 1

                row, trace_rows = simulate_configuration(
                    run_id=run_id,
                    dataset_profile=dataset_profile,
                    boundaries_bytes=boundaries,
                    size_multiplier=float(size_multiplier),
                    scenario=scenario,
                    tile_size_bytes=tile_size_bytes,
                    host_link=host_link,
                    cxl_direct_link=cxl_direct_link,
                    resource_capacity=resource_capacity,
                    stage_defaults=stage_defaults,
                    transfer_power_W=transfer_power_W,
                    stage_overrides=stage_overrides,
                    trace_max_tiles=trace_max_tiles,
                )
                metrics.append(row)
                traces.extend(trace_rows)

    return metrics, traces
