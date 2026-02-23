"""Tiled stage-capacity simulator for mixed pipeline templates."""

from __future__ import annotations

import heapq
import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Sequence, Tuple

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


def _stage_overrides_for_dataset(
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]], dataset_profile: str
) -> Dict[object, Dict[str, object]]:
    return stage_overrides.get(dataset_profile, {})


def _profile_stage_names(profile: Mapping[str, object]) -> List[str]:
    stage_names = profile.get("stage_names")
    if not isinstance(stage_names, Sequence):
        raise ValueError("profile is missing stage_names sequence")
    normalized = [str(name) for name in stage_names]
    if not normalized:
        raise ValueError("profile stage_names cannot be empty")
    return normalized


def _profile_template(profile: Mapping[str, object]) -> str:
    template = profile.get("pipeline_template")
    if not isinstance(template, str) or not template:
        raise ValueError("profile is missing pipeline_template")
    return template


def _stage_device_map_for_scenario(
    scenario_stage_device_map: Mapping[str, Sequence[str]],
    scenario: str,
    num_stages: int,
) -> List[str]:
    if scenario not in scenario_stage_device_map:
        raise KeyError(f"scenario_stage_device_map missing scenario: {scenario}")
    stage_devices = [str(device).strip().lower() for device in scenario_stage_device_map[scenario]]
    if len(stage_devices) != num_stages:
        raise ValueError(
            f"scenario {scenario} has {len(stage_devices)} stage devices, expected {num_stages}"
        )
    allowed = {sources.DEVICE_CPU, sources.DEVICE_PIM}
    invalid = [device for device in stage_devices if device not in allowed]
    if invalid:
        raise ValueError(f"invalid stage devices for scenario {scenario}: {invalid}")
    return stage_devices


def _derive_compute_rates_for_stages(
    dataset_profile: str,
    boundaries_bytes: Sequence[int],
    stage_names: Sequence[str],
    pipeline_template: str,
    stage_defaults: Mapping[str, object],
    dataset_stage_overrides: Mapping[object, Mapping[str, object]],
    pim_speedup_vs_cpu_by_stage: Mapping[str, object],
    cpu_stage_unit_compute_Bps: Mapping[str, object],
) -> List[Tuple[float, float]]:
    profile = sources.DATASET_PROFILES[dataset_profile]
    rates: List[Tuple[float, float]] = []

    if pipeline_template == sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE:
        params = profile.get("parameters")
        if not isinstance(params, Mapping):
            raise ValueError("deepvariant profile missing parameters")
        runtime_total_s = float(params["cpu_reference_total_runtime_s_1x"])
        stage_shares = params["cpu_stage_time_share_1x"]
        if runtime_total_s <= 0:
            raise ValueError("cpu_reference_total_runtime_s_1x must be > 0")

        for stage_id, stage_name in enumerate(stage_names, start=1):
            if stage_name not in stage_shares:
                raise KeyError(f"cpu_stage_time_share_1x missing stage {stage_name}")
            if stage_name not in pim_speedup_vs_cpu_by_stage:
                raise KeyError(f"pim speedup map missing stage {stage_name}")

            stage_share = float(stage_shares[stage_name])
            stage_runtime_s = runtime_total_s * stage_share
            if stage_runtime_s <= 0:
                raise ValueError(f"stage runtime must be > 0 for {stage_name}")

            stage_override = dataset_stage_overrides.get(stage_id, dataset_stage_overrides.get(str(stage_id), {}))
            cpu_units = int(stage_override.get("cpu_units", stage_defaults["cpu_units"]))
            if cpu_units <= 0:
                raise ValueError(f"cpu_units must be > 0 for stage {stage_name}")

            stage_input_bytes = float(boundaries_bytes[stage_id - 1])
            if stage_input_bytes <= 0:
                cpu_rate = float(cpu_stage_unit_compute_Bps.get(stage_name, stage_defaults["cpu_unit_compute_Bps"]))
            else:
                cpu_rate = stage_input_bytes / (float(cpu_units) * stage_runtime_s)
            pim_speedup = float(pim_speedup_vs_cpu_by_stage[stage_name])
            if pim_speedup <= 0:
                raise ValueError(f"pim speedup for {stage_name} must be > 0")
            pim_rate = cpu_rate * pim_speedup
            rates.append((cpu_rate, pim_rate))
        return rates

    if pipeline_template == sources.PIPELINE_TEMPLATE_TPCH_3OP:
        for stage_name in stage_names:
            if stage_name not in cpu_stage_unit_compute_Bps:
                raise KeyError(f"cpu_stage_unit_compute_Bps missing stage {stage_name} for template {pipeline_template}")
            if stage_name not in pim_speedup_vs_cpu_by_stage:
                raise KeyError(f"pim speedup map missing stage {stage_name} for template {pipeline_template}")
            cpu_rate = float(cpu_stage_unit_compute_Bps[stage_name])
            if cpu_rate <= 0:
                raise ValueError(f"cpu stage rate must be > 0 for {stage_name}")
            pim_speedup = float(pim_speedup_vs_cpu_by_stage[stage_name])
            if pim_speedup <= 0:
                raise ValueError(f"pim speedup must be > 0 for {stage_name}")
            rates.append((cpu_rate, cpu_rate * pim_speedup))
        return rates

    raise ValueError(f"unsupported pipeline_template: {pipeline_template}")


def _build_stage_configs(
    dataset_profile: str,
    boundaries_bytes: Sequence[int],
    stage_names: Sequence[str],
    pipeline_template: str,
    stage_defaults: Dict[str, object],
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]],
    pim_speedup_vs_cpu_by_stage: Mapping[str, object],
    cpu_stage_unit_compute_Bps: Mapping[str, object],
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

    num_stages = len(stage_names)
    if len(boundaries_bytes) != num_stages + 1:
        raise ValueError("boundaries size must be num_stages + 1")

    dataset_overrides = _stage_overrides_for_dataset(stage_overrides=stage_overrides, dataset_profile=dataset_profile)
    derived_rates = _derive_compute_rates_for_stages(
        dataset_profile=dataset_profile,
        boundaries_bytes=boundaries_bytes,
        stage_names=stage_names,
        pipeline_template=pipeline_template,
        stage_defaults=stage_defaults,
        dataset_stage_overrides=dataset_overrides,
        pim_speedup_vs_cpu_by_stage=pim_speedup_vs_cpu_by_stage,
        cpu_stage_unit_compute_Bps=cpu_stage_unit_compute_Bps,
    )

    stage_configs: List[StageConfig] = []
    for stage_id in range(1, num_stages + 1):
        stage_name = stage_names[stage_id - 1]
        merged = dict(stage_defaults)
        stage_override = dataset_overrides.get(stage_id, dataset_overrides.get(str(stage_id), {}))
        if stage_override:
            merged.update(stage_override)

        derived_cpu_rate, derived_pim_rate = derived_rates[stage_id - 1]
        if "cpu_unit_compute_Bps" not in stage_override:
            merged["cpu_unit_compute_Bps"] = derived_cpu_rate
        if "pim_unit_compute_Bps" not in stage_override:
            merged["pim_unit_compute_Bps"] = derived_pim_rate

        cfg = StageConfig(
            cpu_units=int(merged["cpu_units"]),
            cpu_unit_compute_Bps=float(merged["cpu_unit_compute_Bps"]),
            cpu_unit_power_W=float(merged["cpu_unit_power_W"]),
            pim_units=int(merged["pim_units"]),
            pim_unit_compute_Bps=float(merged["pim_unit_compute_Bps"]),
            pim_unit_power_W=float(merged["pim_unit_power_W"]),
            host_touch_Bps=float(merged["host_touch_Bps"]),
            host_touch_fixed_s=float(merged["host_touch_fixed_s"]),
        )
        if cfg.cpu_units <= 0:
            raise ValueError(f"cpu_units must be > 0 at stage {stage_id} ({stage_name})")
        if cfg.pim_units <= 0:
            raise ValueError(f"pim_units must be > 0 at stage {stage_id} ({stage_name})")
        stage_configs.append(cfg)
    return stage_configs


def _build_tile_operations(
    scenario: str,
    stage_devices: Sequence[str],
) -> List[TileOperation]:
    num_stages = len(stage_devices)
    operations: List[TileOperation] = []

    if stage_devices[0] == sources.DEVICE_PIM:
        operations.append(
            TileOperation(op_type="TRANSFER", stage_id=1, boundary_index=0, transfer_path="host_h2d_ingress")
        )

    for stage_id in range(1, num_stages + 1):
        operations.append(TileOperation(op_type="COMPUTE", stage_id=stage_id, boundary_index=stage_id - 1))
        if stage_id >= num_stages:
            continue

        src = stage_devices[stage_id - 1]
        dst = stage_devices[stage_id]
        boundary_index = stage_id

        if src == sources.DEVICE_CPU and dst == sources.DEVICE_CPU:
            continue
        if src == sources.DEVICE_CPU and dst == sources.DEVICE_PIM:
            operations.append(
                TileOperation(
                    op_type="TRANSFER",
                    stage_id=stage_id + 1,
                    boundary_index=boundary_index,
                    transfer_path="host_h2d_stage",
                )
            )
            continue
        if src == sources.DEVICE_PIM and dst == sources.DEVICE_CPU:
            operations.append(
                TileOperation(
                    op_type="TRANSFER",
                    stage_id=stage_id,
                    boundary_index=boundary_index,
                    transfer_path="host_d2h",
                )
            )
            continue
        if src == sources.DEVICE_PIM and dst == sources.DEVICE_PIM:
            if scenario == sources.SCENARIO_PIM_HOST_BOUNCE:
                operations.append(
                    TileOperation(
                        op_type="TRANSFER",
                        stage_id=stage_id,
                        boundary_index=boundary_index,
                        transfer_path="host_d2h",
                    )
                )
                operations.append(
                    TileOperation(
                        op_type="HOST_TOUCH",
                        stage_id=stage_id,
                        boundary_index=boundary_index,
                        transfer_path="host_touch",
                    )
                )
                operations.append(
                    TileOperation(
                        op_type="TRANSFER",
                        stage_id=stage_id + 1,
                        boundary_index=boundary_index,
                        transfer_path="host_h2d_stage",
                    )
                )
            elif scenario == sources.SCENARIO_PIM_FLOWCXL_DIRECT:
                operations.append(
                    TileOperation(
                        op_type="TRANSFER",
                        stage_id=stage_id,
                        boundary_index=boundary_index,
                        transfer_path="cxl_direct",
                    )
                )
            else:
                raise ValueError(
                    f"scenario {scenario} cannot route PIM->PIM transition at stage {stage_id}->{stage_id + 1}"
                )
            continue
        raise ValueError(f"unknown stage-device transition: {src}->{dst}")

    if stage_devices[-1] == sources.DEVICE_PIM:
        operations.append(
            TileOperation(
                op_type="TRANSFER",
                stage_id=num_stages,
                boundary_index=num_stages,
                transfer_path="host_d2h",
            )
        )

    return operations


def _validate_config(config: Dict[str, object]) -> None:
    required_top_level = [
        "dataset_profiles",
        "size_multipliers",
        "tile_size_bytes",
        "max_inflight_tiles",
        "scenarios",
        "link_profile",
        "resource_capacity",
        "stage_defaults",
        "transfer_power_W",
        "scenario_stage_device_map_by_template",
        "pim_speedup_vs_cpu_by_stage_by_template",
        "cpu_stage_unit_compute_Bps_by_template",
    ]
    missing = [key for key in required_top_level if key not in config]
    if missing:
        raise KeyError(f"missing config keys: {missing}")

    legacy_flat_keys = [
        "scenario_stage_device_map",
        "pim_speedup_vs_cpu_by_stage",
        "cpu_stage_unit_compute_Bps",
    ]
    legacy_present = [key for key in legacy_flat_keys if key in config]
    if legacy_present:
        raise ValueError(f"legacy flat template keys are not allowed: {legacy_present}")

    link_profile = config["link_profile"]
    if "host_link" not in link_profile or "cxl_direct_link" not in link_profile:
        raise KeyError("link_profile must include host_link and cxl_direct_link")

    resource_capacity = config["resource_capacity"]
    for key in [
        "host_h2d_ingress_channels",
        "host_h2d_stage_channels",
        "host_d2h_channels",
        "cxl_direct_channels",
        "host_touch_channels",
    ]:
        if key not in resource_capacity:
            raise KeyError(f"resource_capacity missing key: {key}")
        if int(resource_capacity[key]) <= 0:
            raise ValueError(f"{key} must be > 0")

    transfer_power_W = config["transfer_power_W"]
    for key in [
        "host_h2d_ingress_channel",
        "host_h2d_stage_channel",
        "host_d2h_channel",
        "cxl_direct_channel",
        "host_touch_channel",
    ]:
        if key not in transfer_power_W:
            raise KeyError(f"transfer_power_W missing key: {key}")
        if float(transfer_power_W[key]) < 0.0:
            raise ValueError(f"{key} must be >= 0")

    stage_defaults = config["stage_defaults"]
    for key in [
        "cpu_units",
        "cpu_unit_compute_Bps",
        "cpu_unit_power_W",
        "pim_units",
        "pim_unit_compute_Bps",
        "pim_unit_power_W",
        "host_touch_Bps",
        "host_touch_fixed_s",
    ]:
        if key not in stage_defaults:
            raise KeyError(f"stage_defaults missing key: {key}")
    if int(stage_defaults["cpu_units"]) <= 0:
        raise ValueError("cpu_units must be > 0")
    if int(stage_defaults["pim_units"]) <= 0:
        raise ValueError("pim_units must be > 0")
    if float(stage_defaults["host_touch_Bps"]) <= 0:
        raise ValueError("host_touch_Bps must be > 0")
    if float(stage_defaults["host_touch_fixed_s"]) < 0:
        raise ValueError("host_touch_fixed_s must be >= 0")

    for scenario in config["scenarios"]:
        if scenario not in sources.SCENARIOS:
            raise ValueError(f"unknown scenario in config: {scenario}")

    dataset_profiles = config["dataset_profiles"]
    template_to_stage_names: Dict[str, List[str]] = {}
    for dataset_profile in dataset_profiles:
        if dataset_profile not in sources.DATASET_PROFILES:
            raise ValueError(f"unknown dataset profile: {dataset_profile}")
        profile = sources.DATASET_PROFILES[dataset_profile]
        stage_names = _profile_stage_names(profile)
        boundaries = profile.get("boundaries_bytes")
        if not isinstance(boundaries, Sequence):
            raise ValueError(f"profile {dataset_profile} missing boundaries_bytes")
        if len(stage_names) != len(boundaries) - 1:
            raise ValueError(
                f"profile {dataset_profile} stage_names length {len(stage_names)} does not match boundaries"
            )
        template = _profile_template(profile)
        if template in template_to_stage_names and template_to_stage_names[template] != stage_names:
            raise ValueError(f"inconsistent stage_names for template {template}")
        template_to_stage_names[template] = stage_names

    scenario_stage_device_map_by_template = config["scenario_stage_device_map_by_template"]
    pim_speedup_by_template = config["pim_speedup_vs_cpu_by_stage_by_template"]
    cpu_stage_rates_by_template = config["cpu_stage_unit_compute_Bps_by_template"]
    for mapping_name, mapping in [
        ("scenario_stage_device_map_by_template", scenario_stage_device_map_by_template),
        ("pim_speedup_vs_cpu_by_stage_by_template", pim_speedup_by_template),
        ("cpu_stage_unit_compute_Bps_by_template", cpu_stage_rates_by_template),
    ]:
        if not isinstance(mapping, Mapping):
            raise ValueError(f"{mapping_name} must be a map")

    for template, stage_names in template_to_stage_names.items():
        if template not in scenario_stage_device_map_by_template:
            raise KeyError(f"scenario_stage_device_map_by_template missing template {template}")
        if template not in pim_speedup_by_template:
            raise KeyError(f"pim_speedup_vs_cpu_by_stage_by_template missing template {template}")
        if template not in cpu_stage_rates_by_template:
            raise KeyError(f"cpu_stage_unit_compute_Bps_by_template missing template {template}")

        scenario_map = scenario_stage_device_map_by_template[template]
        if not isinstance(scenario_map, Mapping):
            raise ValueError(f"scenario_stage_device_map_by_template[{template}] must be a map")
        for scenario in config["scenarios"]:
            if scenario not in scenario_map:
                raise KeyError(f"scenario map for template {template} missing scenario {scenario}")
            devices = [str(device).lower() for device in scenario_map[scenario]]
            if len(devices) != len(stage_names):
                raise ValueError(
                    f"template {template} scenario {scenario} has {len(devices)} devices, expected {len(stage_names)}"
                )
            for device in devices:
                if device not in {sources.DEVICE_CPU, sources.DEVICE_PIM}:
                    raise ValueError(f"invalid device {device} in template {template} scenario {scenario}")

        speedup_map = pim_speedup_by_template[template]
        rate_map = cpu_stage_rates_by_template[template]
        if not isinstance(speedup_map, Mapping):
            raise ValueError(f"pim speedup map for template {template} must be a map")
        if not isinstance(rate_map, Mapping):
            raise ValueError(f"cpu stage rate map for template {template} must be a map")
        for stage_name in stage_names:
            if stage_name not in speedup_map:
                raise KeyError(f"template {template} speedup map missing stage {stage_name}")
            if float(speedup_map[stage_name]) <= 0:
                raise ValueError(f"template {template} speedup for {stage_name} must be > 0")
            if stage_name not in rate_map:
                raise KeyError(f"template {template} cpu stage rate map missing stage {stage_name}")
            if float(rate_map[stage_name]) <= 0:
                raise ValueError(f"template {template} cpu stage rate for {stage_name} must be > 0")

    if int(config["tile_size_bytes"]) <= 0:
        raise ValueError("tile_size_bytes must be > 0")
    if int(config["max_inflight_tiles"]) <= 0:
        raise ValueError("max_inflight_tiles must be > 0")


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
    stage_names: Sequence[str],
    pipeline_template: str,
    size_multiplier: float,
    scenario: str,
    tile_size_bytes: int,
    max_inflight_tiles: int,
    host_link: str,
    cxl_direct_link: str,
    resource_capacity: Dict[str, object],
    stage_defaults: Dict[str, object],
    transfer_power_W: Dict[str, object],
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]],
    scenario_stage_device_map_by_template: Mapping[str, Mapping[str, Sequence[str]]],
    pim_speedup_vs_cpu_by_stage_by_template: Mapping[str, Mapping[str, object]],
    cpu_stage_unit_compute_Bps_by_template: Mapping[str, Mapping[str, object]],
    trace_max_tiles: int | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    if scenario not in sources.SCENARIOS:
        raise ValueError(f"unknown scenario: {scenario}")
    if host_link not in sources.LINKS:
        raise ValueError(f"unknown host link: {host_link}")
    if cxl_direct_link not in sources.LINKS:
        raise ValueError(f"unknown cxl direct link: {cxl_direct_link}")

    num_stages = len(boundaries_bytes) - 1
    if len(stage_names) != num_stages:
        raise ValueError(
            f"stage_names length {len(stage_names)} does not match boundaries-derived stages {num_stages}"
        )
    if pipeline_template not in scenario_stage_device_map_by_template:
        raise KeyError(f"missing scenario stage map for template {pipeline_template}")
    if pipeline_template not in pim_speedup_vs_cpu_by_stage_by_template:
        raise KeyError(f"missing pim speedup map for template {pipeline_template}")
    if pipeline_template not in cpu_stage_unit_compute_Bps_by_template:
        raise KeyError(f"missing cpu stage rate map for template {pipeline_template}")

    stage_devices = _stage_device_map_for_scenario(
        scenario_stage_device_map=scenario_stage_device_map_by_template[pipeline_template],
        scenario=scenario,
        num_stages=num_stages,
    )

    scaled_boundaries = scale_boundaries_exact(boundaries_bytes=boundaries_bytes, multiplier=size_multiplier)
    if num_stages <= 0:
        raise ValueError("dataset profile must contain at least two boundaries")
    num_tiles = compute_num_tiles(boundaries_bytes=scaled_boundaries, tile_size_bytes=tile_size_bytes)
    tiled_boundaries = [tile_boundary_bytes(total_bytes=value, num_tiles=num_tiles) for value in scaled_boundaries]

    stage_configs = _build_stage_configs(
        dataset_profile=dataset_profile,
        boundaries_bytes=boundaries_bytes,
        stage_names=stage_names,
        pipeline_template=pipeline_template,
        stage_defaults=stage_defaults,
        stage_overrides=stage_overrides,
        pim_speedup_vs_cpu_by_stage=pim_speedup_vs_cpu_by_stage_by_template[pipeline_template],
        cpu_stage_unit_compute_Bps=cpu_stage_unit_compute_Bps_by_template[pipeline_template],
    )

    host_h2d_ingress_pool = ResourcePool(
        name="host_h2d_ingress",
        capacity=int(resource_capacity["host_h2d_ingress_channels"]),
        power_W=float(transfer_power_W["host_h2d_ingress_channel"]),
    )
    host_h2d_stage_pool = ResourcePool(
        name="host_h2d_stage",
        capacity=int(resource_capacity["host_h2d_stage_channels"]),
        power_W=float(transfer_power_W["host_h2d_stage_channel"]),
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
        stage_device = stage_devices[stage_id - 1]
        if stage_device == sources.DEVICE_CPU:
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

    operations = _build_tile_operations(scenario=scenario, stage_devices=stage_devices)

    traces: List[Dict[str, object]] = []
    completion_times = [0.0 for _ in range(num_tiles)]
    next_op_index = [0 for _ in range(num_tiles)]

    inflight_seed = min(int(max_inflight_tiles), num_tiles)
    next_tile_to_admit = inflight_seed
    request_heap: List[Tuple[float, int]] = [(0.0, tile_id) for tile_id in range(inflight_seed)]
    heapq.heapify(request_heap)

    total_bytes_host_link = 0
    total_bytes_cxl_direct = 0
    total_bytes_host_touch = 0
    total_bytes_host_h2d_ingress = 0
    total_bytes_host_h2d_stage = 0
    total_bytes_host_d2h = 0

    while request_heap:
        t_req, tile_id = heapq.heappop(request_heap)
        op_index = next_op_index[tile_id]
        if op_index >= len(operations):
            continue

        operation = operations[op_index]
        stage_cfg = stage_configs[operation.stage_id - 1]
        stage_device = stage_devices[operation.stage_id - 1]
        bytes_moved = int(tiled_boundaries[operation.boundary_index][tile_id])

        if operation.op_type == "COMPUTE":
            if stage_device == sources.DEVICE_CPU:
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
            if operation.transfer_path == "host_h2d_ingress":
                pool = host_h2d_ingress_pool
                link_used = host_link
                total_bytes_host_link += bytes_moved
                total_bytes_host_h2d_ingress += bytes_moved
            elif operation.transfer_path == "host_h2d_stage":
                pool = host_h2d_stage_pool
                link_used = host_link
                total_bytes_host_link += bytes_moved
                total_bytes_host_h2d_stage += bytes_moved
            elif operation.transfer_path == "host_d2h":
                pool = host_d2h_pool
                link_used = host_link
                total_bytes_host_link += bytes_moved
                total_bytes_host_d2h += bytes_moved
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
                    "pipeline_template": pipeline_template,
                    "tile_id": tile_id,
                    "op_index": op_index + 1,
                    "stage_id": operation.stage_id,
                    "stage_name": stage_names[operation.stage_id - 1],
                    "stage_device": stage_device,
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
        elif next_tile_to_admit < num_tiles:
            heapq.heappush(request_heap, (t_end, next_tile_to_admit))
            next_tile_to_admit += 1

    makespan_s = max(completion_times) if completion_times else 0.0

    compute_energy_J = sum(pool.busy_time_s * pool.power_W for pool in compute_pools)
    host_touch_energy_J = host_touch_pool.busy_time_s * host_touch_pool.power_W
    transfer_energy_J = sum(
        pool.busy_time_s * pool.power_W
        for pool in [
            host_h2d_ingress_pool,
            host_h2d_stage_pool,
            host_d2h_pool,
            cxl_direct_pool,
        ]
    ) + host_touch_energy_J
    total_energy_J = compute_energy_J + transfer_energy_J

    lb_compute_stage_max_s = max(_pool_lower_bound_s(pool) for pool in compute_pools) if compute_pools else 0.0
    lb_host_h2d_ingress_s = _pool_lower_bound_s(host_h2d_ingress_pool)
    lb_host_h2d_stage_s = _pool_lower_bound_s(host_h2d_stage_pool)
    lb_host_d2h_s = _pool_lower_bound_s(host_d2h_pool)
    lb_host_link_s = max(lb_host_h2d_ingress_s, lb_host_h2d_stage_s, lb_host_d2h_s)
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
        "total_bytes_host_h2d_ingress": total_bytes_host_h2d_ingress,
        "total_bytes_host_h2d_stage": total_bytes_host_h2d_stage,
        "total_bytes_host_d2h": total_bytes_host_d2h,
        "total_bytes_moved": total_bytes_host_link + total_bytes_cxl_direct,
        "lb_compute_stage_max_s": lb_compute_stage_max_s,
        "lb_host_h2d_ingress_s": lb_host_h2d_ingress_s,
        "lb_host_h2d_stage_s": lb_host_h2d_stage_s,
        "lb_host_d2h_s": lb_host_d2h_s,
        "lb_host_link_s": lb_host_link_s,
        "lb_host_touch_s": lb_host_touch_s,
        "lb_cxl_direct_s": lb_cxl_direct_s,
        "dominant_lb_component": dominant_lb_component,
        "pipeline_template": pipeline_template,
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
    max_inflight_tiles = int(config["max_inflight_tiles"])
    host_link = config["link_profile"]["host_link"]
    cxl_direct_link = config["link_profile"]["cxl_direct_link"]
    resource_capacity = config["resource_capacity"]
    stage_defaults = config["stage_defaults"]
    transfer_power_W = config["transfer_power_W"]
    scenario_stage_device_map_by_template = config["scenario_stage_device_map_by_template"]
    pim_speedup_vs_cpu_by_stage_by_template = config["pim_speedup_vs_cpu_by_stage_by_template"]
    cpu_stage_unit_compute_Bps_by_template = config["cpu_stage_unit_compute_Bps_by_template"]
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
        profile = sources.DATASET_PROFILES[dataset_profile]
        boundaries = profile["boundaries_bytes"]
        stage_names = _profile_stage_names(profile)
        pipeline_template = _profile_template(profile)

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
                    stage_names=stage_names,
                    pipeline_template=pipeline_template,
                    size_multiplier=float(size_multiplier),
                    scenario=scenario,
                    tile_size_bytes=tile_size_bytes,
                    max_inflight_tiles=max_inflight_tiles,
                    host_link=host_link,
                    cxl_direct_link=cxl_direct_link,
                    resource_capacity=resource_capacity,
                    stage_defaults=stage_defaults,
                    transfer_power_W=transfer_power_W,
                    stage_overrides=stage_overrides,
                    scenario_stage_device_map_by_template=scenario_stage_device_map_by_template,
                    pim_speedup_vs_cpu_by_stage_by_template=pim_speedup_vs_cpu_by_stage_by_template,
                    cpu_stage_unit_compute_Bps_by_template=cpu_stage_unit_compute_Bps_by_template,
                    trace_max_tiles=trace_max_tiles,
                )
                metrics.append(row)
                traces.extend(trace_rows)

    return metrics, traces
