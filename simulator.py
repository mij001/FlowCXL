"""Tiled stage-capacity simulator for mixed pipeline templates."""

from __future__ import annotations

import heapq
import math
import warnings
import copy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Sequence, Tuple

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
class StageMemoryServiceConfig:
    access_pattern: str
    row_hit_rate: float
    mlp: float
    avg_miss_latency_ns: float
    peak_bw_Bps: float
    penalty_multiplier: float = 1.0


@dataclass(frozen=True)
class MaterializationPolicyConfig:
    boundaries_by_engine: Dict[str, List[int]]
    materialize_Bps: float
    fixed_s: float
    scenarios: List[str]


@dataclass(frozen=True)
class CPUBaselineSystemConfig:
    baseline_engine: str
    dram_channels: int
    banks_per_channel: int
    cacheline_bytes: float
    queueing_model: str
    queue_alpha: float
    rho_cap: float
    stages: Dict[str, StageMemoryServiceConfig]
    materialization_policy: MaterializationPolicyConfig


@dataclass(frozen=True)
class PIMSystemConfig:
    enabled: bool
    dram_channels: int
    banks_per_channel: int
    cacheline_bytes: float
    queueing_model: str
    queue_alpha: float
    rho_cap: float
    stages: Dict[str, StageMemoryServiceConfig]


@dataclass(frozen=True)
class TileOperation:
    op_type: str
    stage_id: int
    boundary_index: int
    transfer_path: str = ""
    src_stage_id: int = 0
    dst_stage_id: int = 0


@dataclass(frozen=True)
class PIMRetentionConfig:
    enabled: bool
    applies_to_scenarios: Tuple[str, ...]
    same_endpoint_short_circuit: bool
    retain_fixed_s: float
    retain_metadata_bytes: int
    retain_local_BW_Bps: float
    pim_retention_capacity_bytes: int
    overflow_policy: str


@dataclass(frozen=True)
class CXLDirectConcurrencyConfig:
    virtual_channels_per_channel: int
    dma_outstanding_per_vc: int
    full_bw_outstanding_threshold: int
    dma_issue_fixed_s: float


@dataclass(frozen=True)
class CXLTopologyConfig:
    enabled: bool
    mode: str
    max_stripes: int
    num_physical_links: int
    applies_to_links: Tuple[str, ...]


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


def compute_bytes_touched(
    bytes_in: int,
    bytes_out: int,
    input_factor: float,
    output_factor: float,
    amplification_factor: float,
) -> float:
    if bytes_in < 0 or bytes_out < 0:
        raise ValueError("bytes_in and bytes_out must be >= 0")
    if input_factor < 0 or output_factor < 0:
        raise ValueError("input/output factors must be >= 0")
    if amplification_factor <= 0:
        raise ValueError("amplification_factor must be > 0")
    return amplification_factor * ((input_factor * float(bytes_in)) + (output_factor * float(bytes_out)))


def compute_stage_duration_components_s(
    bytes_in: int,
    bytes_out: int,
    compute_rate_Bps: float,
    memory_ceiling_enabled: bool,
    memory_Bps_per_stage: float,
    stage_units: int,
    input_factor: float,
    output_factor: float,
    amplification_factor: float,
) -> Tuple[float, float, float, float]:
    compute_component_s = compute_duration_s(bytes_moved=bytes_in, compute_rate_Bps=compute_rate_Bps)
    if not memory_ceiling_enabled:
        return compute_component_s, compute_component_s, 0.0, 0.0

    if memory_Bps_per_stage <= 0:
        raise ValueError("memory_Bps_per_stage must be > 0 when memory ceiling is enabled")
    if stage_units <= 0:
        raise ValueError("stage_units must be > 0 when memory ceiling is enabled")

    bytes_touched = compute_bytes_touched(
        bytes_in=bytes_in,
        bytes_out=bytes_out,
        input_factor=input_factor,
        output_factor=output_factor,
        amplification_factor=amplification_factor,
    )
    effective_memory_Bps_per_unit = memory_Bps_per_stage / float(stage_units)
    memory_component_s = bytes_touched / effective_memory_Bps_per_unit
    return max(compute_component_s, memory_component_s), compute_component_s, memory_component_s, bytes_touched


def compute_cpu_effective_mem_bw(
    stage_name: str,
    cpu_units: int,
    bw_peak_Bps: float,
    access_pattern: str,
    row_hit_rate: float,
    mlp: float,
    avg_miss_latency_ns: float,
    cacheline_bytes: float,
    cpu_random_access_penalty: float,
) -> Dict[str, float | str]:
    if cpu_units <= 0:
        raise ValueError(f"cpu_units must be > 0 for stage {stage_name}")
    if bw_peak_Bps <= 0:
        raise ValueError(f"bw_peak_Bps must be > 0 for stage {stage_name}")
    if access_pattern not in sources.ACCESS_PATTERNS:
        raise ValueError(f"unknown access pattern {access_pattern} for stage {stage_name}")
    if not 0.0 <= row_hit_rate <= 1.0:
        raise ValueError(f"row_hit_rate must be in [0,1] for stage {stage_name}")
    if mlp <= 0:
        raise ValueError(f"mlp must be > 0 for stage {stage_name}")
    if avg_miss_latency_ns <= 0:
        raise ValueError(f"avg_miss_latency_ns must be > 0 for stage {stage_name}")
    if cacheline_bytes <= 0:
        raise ValueError("cacheline_bytes must be > 0")
    if cpu_random_access_penalty <= 0:
        raise ValueError("cpu_random_access_penalty must be > 0")

    miss_fraction = max(1e-6, 1.0 - row_hit_rate)
    latency_s = avg_miss_latency_ns * 1e-9
    bw_latency_Bps = (mlp * cacheline_bytes) / (latency_s * miss_fraction)

    if access_pattern == sources.ACCESS_PATTERN_SEQUENTIAL_SCAN:
        bw_service_Bps = bw_peak_Bps
        mem_bound_mode = "peak_streaming"
    else:
        bw_service_Bps = min(bw_peak_Bps, bw_latency_Bps)
        mem_bound_mode = "latency_limited" if bw_latency_Bps < bw_peak_Bps else "peak_streaming"

    bw_eff_stage_Bps = bw_service_Bps / cpu_random_access_penalty
    bw_eff_per_unit_Bps = bw_eff_stage_Bps / float(cpu_units)

    return {
        "cpu_access_pattern": access_pattern,
        "cpu_row_hit_rate": row_hit_rate,
        "cpu_mlp": mlp,
        "cpu_avg_miss_latency_ns": avg_miss_latency_ns,
        "cpu_bw_peak_Bps": bw_peak_Bps,
        "cpu_bw_latency_Bps": bw_latency_Bps,
        "cpu_bw_eff_stage_Bps": bw_eff_stage_Bps,
        "cpu_bw_eff_per_unit_Bps": bw_eff_per_unit_Bps,
        "cpu_mem_bound_mode": mem_bound_mode,
    }


def host_touch_duration_s(bytes_moved: int, touch_Bps: float, touch_fixed_s: float) -> float:
    if touch_Bps <= 0:
        raise ValueError("host touch bandwidth must be > 0")
    if touch_fixed_s < 0:
        raise ValueError("host touch fixed overhead must be >= 0")
    return touch_fixed_s + (bytes_moved / touch_Bps)


def materialize_duration_s(bytes_moved: int, materialize_Bps: float, fixed_s: float) -> float:
    if materialize_Bps <= 0:
        raise ValueError("materialize_Bps must be > 0")
    if fixed_s < 0:
        raise ValueError("materialize fixed_s must be >= 0")
    return fixed_s + (bytes_moved / materialize_Bps)


def retain_duration_s(retain_fixed_s: float, retain_metadata_bytes: int, retain_local_BW_Bps: float) -> float:
    if retain_fixed_s < 0:
        raise ValueError("retain_fixed_s must be >= 0")
    if retain_metadata_bytes < 0:
        raise ValueError("retain_metadata_bytes must be >= 0")
    if retain_local_BW_Bps <= 0:
        raise ValueError("retain_local_BW_Bps must be > 0")
    return retain_fixed_s + (float(retain_metadata_bytes) / retain_local_BW_Bps)


def _active_slots_at_time(pool: ResourcePool, t_point: float) -> int:
    return sum(1 for free_t in pool.next_free_time_by_slot if free_t > t_point)


def _resolve_host_link_names(link_profile: Mapping[str, object]) -> Tuple[str, str]:
    if "host_h2d_link" in link_profile or "host_d2h_link" in link_profile:
        if "host_h2d_link" not in link_profile or "host_d2h_link" not in link_profile:
            raise KeyError("link_profile must include both host_h2d_link and host_d2h_link when directional keys are used")
        return str(link_profile["host_h2d_link"]), str(link_profile["host_d2h_link"])

    if "host_link" in link_profile:
        legacy = str(link_profile["host_link"])
        return legacy, legacy

    raise KeyError(
        "link_profile must include host_h2d_link+host_d2h_link or legacy host_link"
    )


def _template_to_stage_names_from_config(config: Mapping[str, object]) -> Dict[str, List[str]]:
    dataset_profiles = config["dataset_profiles"]
    template_to_stage_names: Dict[str, List[str]] = {}
    for dataset_profile in dataset_profiles:
        profile = sources.DATASET_PROFILES[dataset_profile]
        stage_names = _profile_stage_names(profile)
        template = _profile_template(profile)
        if template in template_to_stage_names and template_to_stage_names[template] != stage_names:
            raise ValueError(f"inconsistent stage_names for template {template}")
        template_to_stage_names[template] = stage_names
    return template_to_stage_names


def _coerce_stage_memory_cfg(
    cfg: Mapping[str, object],
    *,
    stage_name: str,
    require_penalty: bool,
) -> StageMemoryServiceConfig:
    required = ["access_pattern", "row_hit_rate", "mlp", "avg_miss_latency_ns", "peak_bw_Bps"]
    missing = [key for key in required if key not in cfg]
    if missing:
        raise KeyError(f"stage memory config for {stage_name} missing keys: {missing}")
    access_pattern = str(cfg["access_pattern"])
    if access_pattern not in sources.ACCESS_PATTERNS:
        raise ValueError(f"invalid access_pattern for {stage_name}: {access_pattern}")
    row_hit_rate = float(cfg["row_hit_rate"])
    if row_hit_rate < 0.0 or row_hit_rate > 1.0:
        raise ValueError(f"row_hit_rate for {stage_name} must be in [0,1]")
    mlp = float(cfg["mlp"])
    if mlp <= 0.0:
        raise ValueError(f"mlp for {stage_name} must be > 0")
    avg_miss_latency_ns = float(cfg["avg_miss_latency_ns"])
    if avg_miss_latency_ns <= 0.0:
        raise ValueError(f"avg_miss_latency_ns for {stage_name} must be > 0")
    peak_bw_Bps = float(cfg["peak_bw_Bps"])
    if peak_bw_Bps <= 0.0:
        raise ValueError(f"peak_bw_Bps for {stage_name} must be > 0")
    if require_penalty and "penalty_multiplier" not in cfg:
        raise KeyError(f"stage memory config for {stage_name} missing penalty_multiplier")
    penalty_multiplier = float(cfg.get("penalty_multiplier", 1.0))
    if penalty_multiplier <= 0.0:
        raise ValueError(f"penalty_multiplier for {stage_name} must be > 0")
    return StageMemoryServiceConfig(
        access_pattern=access_pattern,
        row_hit_rate=row_hit_rate,
        mlp=mlp,
        avg_miss_latency_ns=avg_miss_latency_ns,
        peak_bw_Bps=peak_bw_Bps,
        penalty_multiplier=penalty_multiplier,
    )


def _coerce_materialization_policy(
    cfg: Mapping[str, object],
    *,
    stage_count: int,
) -> MaterializationPolicyConfig:
    required = ["boundaries_by_engine", "materialize_Bps", "fixed_s", "scenarios"]
    missing = [key for key in required if key not in cfg]
    if missing:
        raise KeyError(f"materialization_policy missing keys: {missing}")

    boundaries_raw = cfg["boundaries_by_engine"]
    if not isinstance(boundaries_raw, Mapping):
        raise ValueError("materialization_policy.boundaries_by_engine must be a map")
    boundaries_by_engine: Dict[str, List[int]] = {}
    for engine in sources.CPU_BASELINE_ENGINES:
        raw = boundaries_raw.get(engine, [])
        if not isinstance(raw, Sequence):
            raise ValueError(f"materialization boundaries for engine {engine} must be a list")
        boundaries: List[int] = []
        for boundary in raw:
            boundary_int = int(boundary)
            if boundary_int <= 0 or boundary_int >= stage_count:
                raise ValueError(
                    f"materialization boundary {boundary_int} for engine {engine} must be in [1, {stage_count - 1}]"
                )
            boundaries.append(boundary_int)
        boundaries_by_engine[engine] = sorted(set(boundaries))

    materialize_Bps = float(cfg["materialize_Bps"])
    fixed_s = float(cfg["fixed_s"])
    if materialize_Bps <= 0:
        raise ValueError("materialization_policy.materialize_Bps must be > 0")
    if fixed_s < 0:
        raise ValueError("materialization_policy.fixed_s must be >= 0")

    scenarios_raw = cfg["scenarios"]
    if not isinstance(scenarios_raw, Sequence):
        raise ValueError("materialization_policy.scenarios must be a list")
    scenarios: List[str] = []
    for scenario in scenarios_raw:
        scenario_name = str(scenario)
        if scenario_name not in sources.SCENARIOS:
            raise ValueError(f"materialization_policy has unknown scenario {scenario_name}")
        scenarios.append(scenario_name)

    return MaterializationPolicyConfig(
        boundaries_by_engine=boundaries_by_engine,
        materialize_Bps=materialize_Bps,
        fixed_s=fixed_s,
        scenarios=scenarios,
    )


def _build_default_memory_system_template(
    *,
    stage_names: Sequence[str],
) -> Dict[str, object]:
    return {
        "enabled": False,
        "cpu_baseline_system": {"baseline_engine": sources.CPU_ENGINE_VECTORIZED_PIPELINE},
        "pim_system": {"enabled": False},
        "_stage_names": list(stage_names),
    }


def _normalize_memory_system_config(
    *,
    config: Mapping[str, object],
    template_to_stage_names: Mapping[str, Sequence[str]],
    warn_deprecated: bool,
) -> Dict[str, Dict[str, object]]:
    memory_system_by_template_raw = config.get("memory_system_by_template")
    if memory_system_by_template_raw is not None:
        if not isinstance(memory_system_by_template_raw, Mapping):
            raise ValueError("memory_system_by_template must be a map")
        normalized: Dict[str, Dict[str, object]] = {}
        for template, stage_names in template_to_stage_names.items():
            if template not in memory_system_by_template_raw:
                raise KeyError(f"memory_system_by_template missing template {template}")
            template_cfg = memory_system_by_template_raw[template]
            if not isinstance(template_cfg, Mapping):
                raise ValueError(f"memory_system_by_template[{template}] must be a map")
            merged = dict(template_cfg)
            merged["_stage_names"] = list(stage_names)
            normalized[template] = merged
        return normalized
    return _normalize_memory_system_config_from_legacy_keys(
        config=config,
        template_to_stage_names=template_to_stage_names,
        warn_deprecated=warn_deprecated,
    )


def _normalize_memory_system_config_from_legacy_keys(
    *,
    config: Mapping[str, object],
    template_to_stage_names: Mapping[str, Sequence[str]],
    warn_deprecated: bool,
) -> Dict[str, Dict[str, object]]:
    legacy_keys = [
        "enable_memory_ceiling_by_template",
        "dram_service_defaults",
        "cpu_mem_Bps_by_stage_by_template",
        "pim_mem_Bps_by_stage_by_template",
        "cpu_access_pattern_by_stage_by_template",
        "cpu_random_access_penalty_by_stage_by_template",
        "cpu_materialization_by_template",
    ]
    missing = [key for key in legacy_keys if key not in config]
    if missing:
        raise KeyError(
            "memory_system_by_template missing and cannot normalize from legacy keys; missing: "
            + ", ".join(missing)
        )

    if warn_deprecated:
        warnings.warn(
            "memory_system_by_template missing and deprecated legacy keys were used for normalization.",
            UserWarning,
            stacklevel=2,
        )

    enable_map = config["enable_memory_ceiling_by_template"]
    dram_defaults = config["dram_service_defaults"]
    cpu_mem_map = config["cpu_mem_Bps_by_stage_by_template"]
    pim_mem_map = config["pim_mem_Bps_by_stage_by_template"]
    cpu_access_map = config["cpu_access_pattern_by_stage_by_template"]
    cpu_penalty_map = config["cpu_random_access_penalty_by_stage_by_template"]
    cpu_mat_map = config["cpu_materialization_by_template"]
    if not isinstance(dram_defaults, Mapping):
        raise ValueError("legacy dram_service_defaults must be a map")
    cacheline_bytes = float(dram_defaults.get("cacheline_bytes", 64.0))
    if cacheline_bytes <= 0:
        raise ValueError("legacy dram_service_defaults.cacheline_bytes must be > 0")

    normalized: Dict[str, Dict[str, object]] = {}
    for template, stage_names in template_to_stage_names.items():
        if template not in enable_map:
            raise KeyError(f"legacy enable_memory_ceiling_by_template missing template {template}")
        if template not in cpu_mem_map:
            raise KeyError(f"legacy cpu_mem_Bps_by_stage_by_template missing template {template}")
        if template not in pim_mem_map:
            raise KeyError(f"legacy pim_mem_Bps_by_stage_by_template missing template {template}")
        if template not in cpu_access_map:
            raise KeyError(f"legacy cpu_access_pattern_by_stage_by_template missing template {template}")
        if template not in cpu_penalty_map:
            raise KeyError(f"legacy cpu_random_access_penalty_by_stage_by_template missing template {template}")
        if template not in cpu_mat_map:
            raise KeyError(f"legacy cpu_materialization_by_template missing template {template}")

        cpu_stages: Dict[str, Dict[str, object]] = {}
        pim_stages: Dict[str, Dict[str, object]] = {}
        for stage_name in stage_names:
            access_cfg = cpu_access_map[template][stage_name]
            cpu_stages[stage_name] = {
                "access_pattern": access_cfg["access_pattern"],
                "row_hit_rate": access_cfg["row_hit_rate"],
                "mlp": access_cfg["mlp"],
                "avg_miss_latency_ns": access_cfg["avg_miss_latency_ns"],
                "peak_bw_Bps": cpu_mem_map[template][stage_name],
                "penalty_multiplier": cpu_penalty_map[template][stage_name],
            }
            pim_stages[stage_name] = {
                "access_pattern": access_cfg["access_pattern"],
                "row_hit_rate": access_cfg["row_hit_rate"],
                "mlp": access_cfg["mlp"],
                "avg_miss_latency_ns": access_cfg["avg_miss_latency_ns"],
                "peak_bw_Bps": pim_mem_map[template][stage_name],
            }

        legacy_mat = cpu_mat_map[template]
        blocker_boundaries = list(legacy_mat.get("breaker_boundaries", []))
        mat_scenarios = list(legacy_mat.get("scenarios", []))
        mat_enabled = bool(legacy_mat.get("enabled", False))
        if not mat_enabled:
            blocker_boundaries = []
            mat_scenarios = []

        normalized[template] = {
            "enabled": bool(enable_map[template]),
            "_stage_names": list(stage_names),
            "cpu_baseline_system": {
                "baseline_engine": sources.CPU_ENGINE_VECTORIZED_PIPELINE,
                "dram_channels": 8,
                "banks_per_channel": 16,
                "cacheline_bytes": cacheline_bytes,
                "queueing_model": "utilization_penalty",
                "queue_alpha": 0.35,
                "rho_cap": 0.95,
                "stages": cpu_stages,
                "materialization_policy": {
                    "boundaries_by_engine": {
                        sources.CPU_ENGINE_VECTORIZED_PIPELINE: [],
                        sources.CPU_ENGINE_BLOCKING_VOLCANO: blocker_boundaries,
                    },
                    "materialize_Bps": legacy_mat["materialize_Bps"],
                    "fixed_s": legacy_mat["fixed_s"],
                    "scenarios": mat_scenarios,
                },
            },
            "pim_system": {
                "enabled": bool(enable_map[template]),
                "dram_channels": 16,
                "banks_per_channel": 16,
                "cacheline_bytes": cacheline_bytes,
                "queueing_model": "utilization_penalty",
                "queue_alpha": 0.20,
                "rho_cap": 0.95,
                "stages": pim_stages,
            },
        }
    return normalized


def _deep_merge_config(base: Mapping[str, object], patch: Mapping[str, object]) -> Dict[str, object]:
    merged: Dict[str, object] = copy.deepcopy(dict(base))
    for key, value in patch.items():
        if (
            key in merged
            and isinstance(merged[key], Mapping)
            and isinstance(value, Mapping)
        ):
            merged[key] = _deep_merge_config(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _normalize_workload_variants(config: Mapping[str, object]) -> List[Dict[str, object]]:
    raw = config.get("workload_variants")
    if raw is None:
        return [{"name": "base", "overrides": {}}]
    if not isinstance(raw, Sequence):
        raise ValueError("workload_variants must be a list")
    variants: List[Dict[str, object]] = []
    seen_names: set[str] = set()
    for entry in raw:
        if not isinstance(entry, Mapping):
            raise ValueError("each workload_variants entry must be a map")
        name = str(entry.get("name", "")).strip()
        if not name:
            raise ValueError("workload_variants entry requires non-empty name")
        if name in seen_names:
            raise ValueError(f"duplicate workload variant name: {name}")
        seen_names.add(name)
        overrides = entry.get("overrides", {})
        if not isinstance(overrides, Mapping):
            raise ValueError(f"workload variant {name} overrides must be a map")
        variants.append({"name": name, "overrides": copy.deepcopy(dict(overrides))})
    if not variants:
        raise ValueError("workload_variants must include at least one entry")
    return variants


def _normalize_workload_sweep(config: Mapping[str, object]) -> Dict[str, List[str]]:
    dataset_profiles = config["dataset_profiles"]
    if not isinstance(dataset_profiles, Sequence):
        raise ValueError("dataset_profiles must be a list")

    def classify_profiles(profiles: Sequence[object]) -> Dict[str, List[str]]:
        tpch_profiles: List[str] = []
        deepvariant_profiles: List[str] = []
        for profile_raw in profiles:
            profile_id = str(profile_raw)
            if profile_id not in sources.DATASET_PROFILES:
                raise ValueError(f"unknown dataset profile: {profile_id}")
            profile = sources.DATASET_PROFILES[profile_id]
            template = _profile_template(profile)
            if template == sources.PIPELINE_TEMPLATE_TPCH_3OP:
                tpch_profiles.append(profile_id)
            elif template == sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE:
                deepvariant_profiles.append(profile_id)
            else:
                raise ValueError(f"unsupported pipeline template in workload sweep: {template}")
        return {
            "tpch_profiles": tpch_profiles,
            "deepvariant_profiles": deepvariant_profiles,
        }

    workload_sweep = config.get("workload_sweep")
    if workload_sweep is None:
        return classify_profiles(dataset_profiles)
    if not isinstance(workload_sweep, Mapping):
        raise ValueError("workload_sweep must be a map")

    tpch_profiles_raw = workload_sweep.get("tpch_profiles", [])
    deepvariant_profiles_raw = workload_sweep.get("deepvariant_profiles", [])
    if not isinstance(tpch_profiles_raw, Sequence) or not isinstance(deepvariant_profiles_raw, Sequence):
        raise ValueError("workload_sweep profile lists must be sequences")

    tpch_profiles: List[str] = []
    for profile_raw in tpch_profiles_raw:
        profile_id = str(profile_raw)
        if profile_id not in sources.DATASET_PROFILES:
            raise ValueError(f"unknown dataset profile: {profile_id}")
        template = _profile_template(sources.DATASET_PROFILES[profile_id])
        if template != sources.PIPELINE_TEMPLATE_TPCH_3OP:
            raise ValueError(f"workload_sweep.tpch_profiles contains non-tpch profile {profile_id}")
        tpch_profiles.append(profile_id)

    deepvariant_profiles: List[str] = []
    for profile_raw in deepvariant_profiles_raw:
        profile_id = str(profile_raw)
        if profile_id not in sources.DATASET_PROFILES:
            raise ValueError(f"unknown dataset profile: {profile_id}")
        template = _profile_template(sources.DATASET_PROFILES[profile_id])
        if template != sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE:
            raise ValueError(
                f"workload_sweep.deepvariant_profiles contains non-deepvariant profile {profile_id}"
            )
        deepvariant_profiles.append(profile_id)

    normalized = {
        "tpch_profiles": tpch_profiles,
        "deepvariant_profiles": deepvariant_profiles,
    }
    sweep_union = normalized["tpch_profiles"] + normalized["deepvariant_profiles"]
    if len(set(sweep_union)) != len(sweep_union):
        raise ValueError("workload_sweep contains duplicate profile ids")

    dataset_profile_order = [str(value) for value in dataset_profiles]
    if dataset_profile_order and dataset_profile_order != sweep_union:
        return classify_profiles(dataset_profiles)
    return normalized


def _normalize_ingress_resident_scenarios_by_template(
    *,
    config: Mapping[str, object],
    template_to_stage_names: Mapping[str, Sequence[str]],
) -> Dict[str, Tuple[str, ...]]:
    raw = config.get("ingress_resident_scenarios_by_template")
    if raw is None:
        return {template: tuple() for template in template_to_stage_names}
    if not isinstance(raw, Mapping):
        raise ValueError("ingress_resident_scenarios_by_template must be a map")

    normalized: Dict[str, Tuple[str, ...]] = {}
    for template in template_to_stage_names:
        values = raw.get(template, [])
        if not isinstance(values, Sequence):
            raise ValueError(f"ingress_resident_scenarios_by_template[{template}] must be a list")
        scenarios: List[str] = []
        for value in values:
            scenario = str(value)
            if scenario not in sources.SCENARIOS:
                raise ValueError(
                    f"ingress_resident_scenarios_by_template[{template}] has unknown scenario {scenario}"
                )
            if scenario not in scenarios:
                scenarios.append(scenario)
        normalized[template] = tuple(scenarios)
    return normalized


def _workload_family_from_template(pipeline_template: str) -> str:
    if pipeline_template == sources.PIPELINE_TEMPLATE_TPCH_3OP:
        return "tpch"
    if pipeline_template == sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE:
        return "deepvariant"
    return "unknown"


def _validate_memory_system_config(
    *,
    memory_system_by_template: Mapping[str, Mapping[str, object]],
    template_to_stage_names: Mapping[str, Sequence[str]],
) -> None:
    for template, stage_names in template_to_stage_names.items():
        if template not in memory_system_by_template:
            raise KeyError(f"memory_system_by_template missing template {template}")
        template_cfg = memory_system_by_template[template]
        if not isinstance(template_cfg, Mapping):
            raise ValueError(f"memory_system_by_template[{template}] must be a map")
        if "enabled" not in template_cfg:
            raise KeyError(f"memory_system_by_template[{template}] missing enabled")
        enabled = bool(template_cfg["enabled"])

        if "cpu_baseline_system" not in template_cfg:
            raise KeyError(f"memory_system_by_template[{template}] missing cpu_baseline_system")
        if "pim_system" not in template_cfg:
            raise KeyError(f"memory_system_by_template[{template}] missing pim_system")

        cpu_system = template_cfg["cpu_baseline_system"]
        pim_system = template_cfg["pim_system"]
        if not isinstance(cpu_system, Mapping):
            raise ValueError(f"cpu_baseline_system for template {template} must be a map")
        if not isinstance(pim_system, Mapping):
            raise ValueError(f"pim_system for template {template} must be a map")

        baseline_engine = str(cpu_system.get("baseline_engine", ""))
        if baseline_engine not in sources.CPU_BASELINE_ENGINES:
            raise ValueError(f"invalid cpu baseline engine for template {template}: {baseline_engine}")
        pim_enabled = bool(pim_system.get("enabled", False))

        if not enabled:
            continue

        for system_name, system_cfg, require_penalty in [
            ("cpu_baseline_system", cpu_system, True),
            ("pim_system", pim_system, False),
        ]:
            if system_name == "pim_system" and not pim_enabled:
                continue
            for key in ["dram_channels", "banks_per_channel", "cacheline_bytes", "queueing_model", "queue_alpha", "rho_cap", "stages"]:
                if key not in system_cfg:
                    raise KeyError(f"{system_name} for template {template} missing key {key}")
            if int(system_cfg["dram_channels"]) <= 0:
                raise ValueError(f"{system_name}.dram_channels must be > 0 for template {template}")
            if int(system_cfg["banks_per_channel"]) <= 0:
                raise ValueError(f"{system_name}.banks_per_channel must be > 0 for template {template}")
            if float(system_cfg["cacheline_bytes"]) <= 0:
                raise ValueError(f"{system_name}.cacheline_bytes must be > 0 for template {template}")
            queueing_model = str(system_cfg["queueing_model"])
            if queueing_model != "utilization_penalty":
                raise ValueError(f"{system_name}.queueing_model must be utilization_penalty for template {template}")
            if float(system_cfg["queue_alpha"]) < 0:
                raise ValueError(f"{system_name}.queue_alpha must be >= 0 for template {template}")
            rho_cap = float(system_cfg["rho_cap"])
            if rho_cap <= 0.0 or rho_cap >= 1.0:
                raise ValueError(f"{system_name}.rho_cap must be in (0,1) for template {template}")
            stages_cfg = system_cfg["stages"]
            if not isinstance(stages_cfg, Mapping):
                raise ValueError(f"{system_name}.stages must be a map for template {template}")
            for stage_name in stage_names:
                if stage_name not in stages_cfg:
                    raise KeyError(f"{system_name}.stages missing {stage_name} for template {template}")
                _coerce_stage_memory_cfg(
                    stages_cfg[stage_name],
                    stage_name=f"{template}:{stage_name}",
                    require_penalty=require_penalty,
                )

        if "materialization_policy" not in cpu_system:
            raise KeyError(f"cpu_baseline_system.materialization_policy missing for template {template}")
        _coerce_materialization_policy(
            cpu_system["materialization_policy"],
            stage_count=len(stage_names),
        )


def _build_system_configs_for_template(
    *,
    memory_system_cfg: Mapping[str, object],
    stage_names: Sequence[str],
) -> Tuple[bool, CPUBaselineSystemConfig, PIMSystemConfig]:
    enabled = bool(memory_system_cfg["enabled"])
    cpu_raw = memory_system_cfg["cpu_baseline_system"]
    pim_raw = memory_system_cfg["pim_system"]

    baseline_engine = str(cpu_raw.get("baseline_engine", sources.CPU_ENGINE_VECTORIZED_PIPELINE))
    if baseline_engine not in sources.CPU_BASELINE_ENGINES:
        raise ValueError(f"invalid baseline_engine: {baseline_engine}")

    if enabled:
        cpu_stages = {
            stage_name: _coerce_stage_memory_cfg(
                cpu_raw["stages"][stage_name],
                stage_name=stage_name,
                require_penalty=True,
            )
            for stage_name in stage_names
        }
        cpu_materialization = _coerce_materialization_policy(
            cpu_raw["materialization_policy"],
            stage_count=len(stage_names),
        )
        cpu_cfg = CPUBaselineSystemConfig(
            baseline_engine=baseline_engine,
            dram_channels=int(cpu_raw["dram_channels"]),
            banks_per_channel=int(cpu_raw["banks_per_channel"]),
            cacheline_bytes=float(cpu_raw["cacheline_bytes"]),
            queueing_model=str(cpu_raw["queueing_model"]),
            queue_alpha=float(cpu_raw["queue_alpha"]),
            rho_cap=float(cpu_raw["rho_cap"]),
            stages=cpu_stages,
            materialization_policy=cpu_materialization,
        )

        pim_enabled = bool(pim_raw.get("enabled", False))
        pim_stages: Dict[str, StageMemoryServiceConfig] = {}
        if pim_enabled:
            pim_stages = {
                stage_name: _coerce_stage_memory_cfg(
                    pim_raw["stages"][stage_name],
                    stage_name=stage_name,
                    require_penalty=False,
                )
                for stage_name in stage_names
            }
        pim_cfg = PIMSystemConfig(
            enabled=pim_enabled,
            dram_channels=int(pim_raw.get("dram_channels", 1)),
            banks_per_channel=int(pim_raw.get("banks_per_channel", 1)),
            cacheline_bytes=float(pim_raw.get("cacheline_bytes", 64.0)),
            queueing_model=str(pim_raw.get("queueing_model", "utilization_penalty")),
            queue_alpha=float(pim_raw.get("queue_alpha", 0.0)),
            rho_cap=float(pim_raw.get("rho_cap", 0.95)),
            stages=pim_stages,
        )
        return enabled, cpu_cfg, pim_cfg

    cpu_cfg = CPUBaselineSystemConfig(
        baseline_engine=baseline_engine,
        dram_channels=1,
        banks_per_channel=1,
        cacheline_bytes=64.0,
        queueing_model="utilization_penalty",
        queue_alpha=0.0,
        rho_cap=0.95,
        stages={},
        materialization_policy=MaterializationPolicyConfig(
            boundaries_by_engine={
                sources.CPU_ENGINE_VECTORIZED_PIPELINE: [],
                sources.CPU_ENGINE_BLOCKING_VOLCANO: [],
            },
            materialize_Bps=1.0,
            fixed_s=0.0,
            scenarios=[],
        ),
    )
    pim_cfg = PIMSystemConfig(
        enabled=False,
        dram_channels=1,
        banks_per_channel=1,
        cacheline_bytes=64.0,
        queueing_model="utilization_penalty",
        queue_alpha=0.0,
        rho_cap=0.95,
        stages={},
    )
    return enabled, cpu_cfg, pim_cfg


def _compute_stage_memory_service(
    *,
    stage_cfg: StageMemoryServiceConfig,
    stage_units: int,
    bytes_touched: float,
    compute_component_s: float,
    cacheline_bytes: float,
    queueing_model: str,
    queue_alpha: float,
    rho_cap: float,
) -> Dict[str, float | str]:
    miss_fraction = max(1e-6, 1.0 - stage_cfg.row_hit_rate)
    latency_s = stage_cfg.avg_miss_latency_ns * 1e-9
    bw_latency_Bps = (stage_cfg.mlp * cacheline_bytes) / (latency_s * miss_fraction)
    if stage_cfg.access_pattern == sources.ACCESS_PATTERN_SEQUENTIAL_SCAN:
        bw_service_no_penalty = stage_cfg.peak_bw_Bps
        mem_bound_mode = "peak_streaming"
    else:
        bw_service_no_penalty = min(stage_cfg.peak_bw_Bps, bw_latency_Bps)
        mem_bound_mode = "latency_limited" if bw_latency_Bps < stage_cfg.peak_bw_Bps else "peak_streaming"

    bw_service_Bps = bw_service_no_penalty / stage_cfg.penalty_multiplier
    if queueing_model != "utilization_penalty":
        raise ValueError(f"unsupported queueing model: {queueing_model}")
    offered_Bps = bytes_touched / max(compute_component_s, 1e-12)
    rho = min(rho_cap, max(0.0, offered_Bps / max(bw_service_Bps, 1e-12)))
    rho = min(rho, 0.999999)
    queue_multiplier = 1.0 + (queue_alpha * (rho / max(1e-12, 1.0 - rho)))

    bw_eff_stage_Bps = bw_service_Bps / queue_multiplier
    bw_eff_per_unit_Bps = bw_eff_stage_Bps / float(stage_units)
    mem_service_time_s = bytes_touched / (bw_service_Bps / float(stage_units))
    mem_total_time_s = bytes_touched / bw_eff_per_unit_Bps
    mem_queue_delay_s = max(0.0, mem_total_time_s - mem_service_time_s)

    return {
        "bw_latency_Bps": bw_latency_Bps,
        "bw_service_Bps": bw_service_Bps,
        "bw_eff_stage_Bps": bw_eff_stage_Bps,
        "bw_eff_per_unit_Bps": bw_eff_per_unit_Bps,
        "queue_multiplier": queue_multiplier,
        "rho": rho,
        "mem_service_time_s": mem_service_time_s,
        "mem_total_time_s": mem_total_time_s,
        "mem_queue_delay_s": mem_queue_delay_s,
        "mem_bound_mode": mem_bound_mode,
    }


def _normalize_endpoint_map(
    *,
    config: Mapping[str, object],
    template_to_stage_names: Mapping[str, Sequence[str]],
    scenarios: Sequence[str],
    scenario_stage_device_map_by_template: Mapping[str, Mapping[str, Sequence[str]]],
    warn_defaults: bool,
) -> Tuple[str, Dict[str, Dict[str, List[str]]]]:
    default_policy = str(config.get("default_pim_endpoint_policy", "colocate"))
    if default_policy not in {"colocate", "spread"}:
        raise ValueError("default_pim_endpoint_policy must be one of: colocate, spread")

    derived_map: Dict[str, Dict[str, List[str]]] = {}
    for template, stage_names in template_to_stage_names.items():
        derived_map[template] = {}
        device_map = scenario_stage_device_map_by_template[template]
        for scenario in scenarios:
            devices = [str(device).strip().lower() for device in device_map[scenario]]
            if len(devices) != len(stage_names):
                raise ValueError(
                    f"endpoint derivation mismatch for template {template} scenario {scenario}: "
                    f"{len(devices)} devices vs {len(stage_names)} stages"
                )
            endpoints: List[str] = []
            pim_counter = 0
            for device in devices:
                if device == sources.DEVICE_CPU:
                    endpoints.append("cpu0")
                elif default_policy == "colocate":
                    endpoints.append("pim0")
                else:
                    endpoints.append(f"pim{pim_counter}")
                    pim_counter += 1
            derived_map[template][scenario] = endpoints

    explicit_raw = config.get("scenario_stage_endpoint_map_by_template")
    if explicit_raw is None:
        if warn_defaults:
            warnings.warn(
                "scenario_stage_endpoint_map_by_template missing; derived from stage-device map and default_pim_endpoint_policy.",
                UserWarning,
                stacklevel=2,
            )
        return default_policy, derived_map
    if not isinstance(explicit_raw, Mapping):
        raise ValueError("scenario_stage_endpoint_map_by_template must be a map")

    merged: Dict[str, Dict[str, List[str]]] = {
        template: {scenario: list(values) for scenario, values in scenario_map.items()}
        for template, scenario_map in derived_map.items()
    }
    for template, stage_names in template_to_stage_names.items():
        if template not in explicit_raw:
            continue
        template_map = explicit_raw[template]
        if not isinstance(template_map, Mapping):
            raise ValueError(f"scenario_stage_endpoint_map_by_template[{template}] must be a map")
        for scenario in scenarios:
            if scenario not in template_map:
                continue
            endpoints_raw = template_map[scenario]
            if not isinstance(endpoints_raw, Sequence):
                raise ValueError(
                    f"endpoint map for template {template} scenario {scenario} must be a sequence"
                )
            endpoints = [str(value).strip() for value in endpoints_raw]
            if len(endpoints) != len(stage_names):
                raise ValueError(
                    f"endpoint map for template {template} scenario {scenario} has {len(endpoints)} entries, "
                    f"expected {len(stage_names)}"
                )
            if any(not endpoint for endpoint in endpoints):
                raise ValueError(
                    f"endpoint map for template {template} scenario {scenario} contains empty endpoint id"
                )
            merged[template][scenario] = endpoints
    return default_policy, merged


def _normalize_pim_retention_config(config: Mapping[str, object], warn_defaults: bool) -> PIMRetentionConfig:
    defaults: Dict[str, object] = {
        "enabled": True,
        "applies_to_scenarios": [sources.SCENARIO_PIM_FLOWCXL_DIRECT, sources.SCENARIO_PIM_HOST_BOUNCE],
        "same_endpoint_short_circuit": True,
        "retain_fixed_s": 3e-7,
        "retain_metadata_bytes": 4096,
        "retain_local_BW_Bps": 200e9,
        "pim_retention_capacity_bytes": 34_359_738_368,
        "overflow_policy": "fallback_transfer",
    }
    raw = config.get("pim_retention")
    if raw is None:
        if warn_defaults:
            warnings.warn("pim_retention missing; using defaults.", UserWarning, stacklevel=2)
        raw = defaults
    if not isinstance(raw, Mapping):
        raise ValueError("pim_retention must be a map")
    merged = dict(defaults)
    merged.update(raw)
    applies = tuple(str(value) for value in merged["applies_to_scenarios"])
    for scenario in applies:
        if scenario not in sources.SCENARIOS:
            raise ValueError(f"pim_retention.applies_to_scenarios has unknown scenario {scenario}")
    overflow_policy = str(merged["overflow_policy"])
    if overflow_policy != "fallback_transfer":
        raise ValueError("pim_retention.overflow_policy must be fallback_transfer")
    cfg = PIMRetentionConfig(
        enabled=bool(merged["enabled"]),
        applies_to_scenarios=applies,
        same_endpoint_short_circuit=bool(merged["same_endpoint_short_circuit"]),
        retain_fixed_s=float(merged["retain_fixed_s"]),
        retain_metadata_bytes=int(merged["retain_metadata_bytes"]),
        retain_local_BW_Bps=float(merged["retain_local_BW_Bps"]),
        pim_retention_capacity_bytes=int(merged["pim_retention_capacity_bytes"]),
        overflow_policy=overflow_policy,
    )
    if cfg.retain_fixed_s < 0:
        raise ValueError("pim_retention.retain_fixed_s must be >= 0")
    if cfg.retain_metadata_bytes < 0:
        raise ValueError("pim_retention.retain_metadata_bytes must be >= 0")
    if cfg.retain_local_BW_Bps <= 0:
        raise ValueError("pim_retention.retain_local_BW_Bps must be > 0")
    if cfg.pim_retention_capacity_bytes <= 0:
        raise ValueError("pim_retention.pim_retention_capacity_bytes must be > 0")
    return cfg


def _normalize_cxl_direct_concurrency_config(
    config: Mapping[str, object], warn_defaults: bool
) -> CXLDirectConcurrencyConfig:
    defaults: Dict[str, object] = {
        "virtual_channels_per_channel": 4,
        "dma_outstanding_per_vc": 16,
        "full_bw_outstanding_threshold": 8,
        "dma_issue_fixed_s": 2e-7,
    }
    raw = config.get("cxl_direct_concurrency")
    if raw is None:
        if warn_defaults:
            warnings.warn("cxl_direct_concurrency missing; using defaults.", UserWarning, stacklevel=2)
        raw = defaults
    if not isinstance(raw, Mapping):
        raise ValueError("cxl_direct_concurrency must be a map")
    merged = dict(defaults)
    merged.update(raw)
    cfg = CXLDirectConcurrencyConfig(
        virtual_channels_per_channel=int(merged["virtual_channels_per_channel"]),
        dma_outstanding_per_vc=int(merged["dma_outstanding_per_vc"]),
        full_bw_outstanding_threshold=int(merged["full_bw_outstanding_threshold"]),
        dma_issue_fixed_s=float(merged["dma_issue_fixed_s"]),
    )
    if cfg.virtual_channels_per_channel <= 0:
        raise ValueError("cxl_direct_concurrency.virtual_channels_per_channel must be > 0")
    if cfg.dma_outstanding_per_vc <= 0:
        raise ValueError("cxl_direct_concurrency.dma_outstanding_per_vc must be > 0")
    if cfg.full_bw_outstanding_threshold <= 0:
        raise ValueError("cxl_direct_concurrency.full_bw_outstanding_threshold must be > 0")
    if cfg.dma_issue_fixed_s < 0:
        raise ValueError("cxl_direct_concurrency.dma_issue_fixed_s must be >= 0")
    return cfg


def _normalize_cxl_topology_config(config: Mapping[str, object], warn_defaults: bool) -> CXLTopologyConfig:
    defaults: Dict[str, object] = {
        "enabled": True,
        "mode": "dynamic_striping",
        "max_stripes": 4,
        "num_physical_links": 4,
        "applies_to_links": [sources.LINK_CXL_SWITCH],
    }
    raw = config.get("cxl_topology")
    if raw is None:
        if warn_defaults:
            warnings.warn("cxl_topology missing; using defaults.", UserWarning, stacklevel=2)
        raw = defaults
    if not isinstance(raw, Mapping):
        raise ValueError("cxl_topology must be a map")
    merged = dict(defaults)
    merged.update(raw)
    mode = str(merged["mode"])
    if mode != "dynamic_striping":
        raise ValueError("cxl_topology.mode must be dynamic_striping")
    applies_to_links = tuple(str(value) for value in merged["applies_to_links"])
    for link_name in applies_to_links:
        if link_name not in sources.LINKS:
            raise ValueError(f"cxl_topology.applies_to_links has unknown link {link_name}")
    cfg = CXLTopologyConfig(
        enabled=bool(merged["enabled"]),
        mode=mode,
        max_stripes=int(merged["max_stripes"]),
        num_physical_links=int(merged["num_physical_links"]),
        applies_to_links=applies_to_links,
    )
    if cfg.max_stripes <= 0:
        raise ValueError("cxl_topology.max_stripes must be > 0")
    if cfg.num_physical_links <= 0:
        raise ValueError("cxl_topology.num_physical_links must be > 0")
    return cfg


def _stage_overrides_for_dataset(
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]], dataset_profile: str
) -> Dict[object, Dict[str, object]]:
    return stage_overrides.get(dataset_profile, {})


def _profile_execution_stage_names(profile: Mapping[str, object]) -> List[str]:
    stage_names = profile.get("stage_names")
    if not isinstance(stage_names, Sequence):
        raise ValueError("profile is missing stage_names sequence")
    normalized = [str(name) for name in stage_names]
    if not normalized:
        raise ValueError("profile stage_names cannot be empty")
    return normalized


def _profile_public_stage_names(profile: Mapping[str, object]) -> List[str]:
    public_stage_names = profile.get("public_stage_names")
    if public_stage_names is None:
        return _profile_execution_stage_names(profile)
    if not isinstance(public_stage_names, Sequence):
        raise ValueError("profile public_stage_names must be a sequence when present")
    normalized = [str(name) for name in public_stage_names]
    if not normalized:
        raise ValueError("profile public_stage_names cannot be empty")
    return normalized


def _profile_stage_names(profile: Mapping[str, object]) -> List[str]:
    # Backward-compatible alias for existing call sites.
    return _profile_execution_stage_names(profile)


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


def _stage_endpoint_map_for_scenario(
    scenario_stage_endpoint_map: Mapping[str, Sequence[str]],
    scenario: str,
    num_stages: int,
) -> List[str]:
    if scenario not in scenario_stage_endpoint_map:
        raise KeyError(f"scenario_stage_endpoint_map missing scenario: {scenario}")
    endpoints = [str(endpoint).strip() for endpoint in scenario_stage_endpoint_map[scenario]]
    if len(endpoints) != num_stages:
        raise ValueError(
            f"scenario {scenario} has {len(endpoints)} stage endpoints, expected {num_stages}"
        )
    if any(not endpoint for endpoint in endpoints):
        raise ValueError(f"scenario {scenario} has empty endpoint ids")
    return endpoints


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

        make_examples_share = float(stage_shares["make_examples"])
        call_variants_share = float(stage_shares["call_variants"])
        postprocess_share = float(stage_shares["postprocess_variants"])
        make_examples_frontend_fraction = float(
            params.get("make_examples_frontend_fraction_of_make_examples", 0.45)
        )
        call_variants_infer_fraction = float(
            params.get("call_variants_infer_fraction_of_call_variants", 0.85)
        )
        if not 0.0 <= make_examples_frontend_fraction <= 1.0:
            raise ValueError("make_examples_frontend_fraction_of_make_examples must be in [0,1]")
        if not 0.0 <= call_variants_infer_fraction <= 1.0:
            raise ValueError("call_variants_infer_fraction_of_call_variants must be in [0,1]")

        kernel_stage_shares = {
            "make_examples_frontend": make_examples_share * make_examples_frontend_fraction,
            "make_examples_tensorize": make_examples_share * (1.0 - make_examples_frontend_fraction),
            "call_variants_infer": call_variants_share * call_variants_infer_fraction,
            "call_variants_post": call_variants_share * (1.0 - call_variants_infer_fraction),
            "postprocess_variants": postprocess_share,
        }

        using_kernel_split = all(stage_name in kernel_stage_shares for stage_name in stage_names)
        if using_kernel_split:
            share_sum = sum(kernel_stage_shares[stage_name] for stage_name in stage_names)
            if share_sum <= 0:
                raise ValueError("deepvariant kernel stage shares must sum to > 0")
            stage_share_map = {
                stage_name: kernel_stage_shares[stage_name] / share_sum for stage_name in stage_names
            }
        else:
            # Backward compatibility for custom 3-stage profiles.
            stage_share_map = {}
            for stage_name in stage_names:
                if stage_name not in stage_shares:
                    raise KeyError(f"cpu_stage_time_share_1x missing stage {stage_name}")
                stage_share_map[stage_name] = float(stage_shares[stage_name])
            share_sum = sum(stage_share_map.values())
            if share_sum <= 0:
                raise ValueError("deepvariant stage shares must sum to > 0")
            stage_share_map = {
                stage_name: stage_share_map[stage_name] / share_sum for stage_name in stage_names
            }

        for stage_id, stage_name in enumerate(stage_names, start=1):
            if stage_name not in pim_speedup_vs_cpu_by_stage:
                raise KeyError(f"pim speedup map missing stage {stage_name}")
            stage_share = float(stage_share_map[stage_name])
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
    stage_endpoints: Sequence[str],
    materialization_policy: MaterializationPolicyConfig,
    baseline_engine: str,
    ingress_resident: bool,
) -> List[TileOperation]:
    num_stages = len(stage_devices)
    if len(stage_endpoints) != num_stages:
        raise ValueError("stage_endpoints length must match stage_devices length")
    operations: List[TileOperation] = []
    materialization_scenarios = set(materialization_policy.scenarios)
    breaker_boundaries = set(materialization_policy.boundaries_by_engine.get(baseline_engine, []))

    skip_first_host_to_pim_transfer = bool(ingress_resident)
    if stage_devices[0] == sources.DEVICE_PIM:
        if skip_first_host_to_pim_transfer:
            skip_first_host_to_pim_transfer = False
        else:
            operations.append(
                TileOperation(op_type="TRANSFER", stage_id=1, boundary_index=0, transfer_path="host_h2d_ingress")
            )

    for stage_id in range(1, num_stages + 1):
        operations.append(TileOperation(op_type="COMPUTE", stage_id=stage_id, boundary_index=stage_id - 1))
        if (
            scenario in materialization_scenarios
            and stage_id in breaker_boundaries
            and stage_id < num_stages
        ):
            operations.append(
                TileOperation(
                    op_type="MATERIALIZE",
                    stage_id=stage_id,
                    boundary_index=stage_id,
                    transfer_path="cpu_materialize",
                )
            )
        if stage_id >= num_stages:
            continue

        src = stage_devices[stage_id - 1]
        dst = stage_devices[stage_id]
        boundary_index = stage_id

        if src == sources.DEVICE_CPU and dst == sources.DEVICE_CPU:
            continue
        if src == sources.DEVICE_CPU and dst == sources.DEVICE_PIM:
            if skip_first_host_to_pim_transfer:
                skip_first_host_to_pim_transfer = False
            else:
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
            if scenario not in {sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT}:
                raise ValueError(
                    f"scenario {scenario} cannot route PIM->PIM transition at stage {stage_id}->{stage_id + 1}"
                )
            operations.append(
                TileOperation(
                    op_type="PIM_HANDOFF",
                    stage_id=stage_id,
                    boundary_index=boundary_index,
                    transfer_path="pim_handoff",
                    src_stage_id=stage_id,
                    dst_stage_id=stage_id + 1,
                )
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


def _validate_config(
    config: Dict[str, object],
    *,
    warn_deprecated_memory_keys: bool = False,
) -> Dict[str, Dict[str, object]]:
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
        "ingress_resident_scenarios_by_template",
        "pim_speedup_vs_cpu_by_stage_by_template",
        "cpu_stage_unit_compute_Bps_by_template",
        "bytes_touched_factors_by_stage_by_template",
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
    host_h2d_link, host_d2h_link = _resolve_host_link_names(link_profile)
    if "cxl_direct_link" not in link_profile:
        raise KeyError("link_profile must include cxl_direct_link")
    cxl_direct_link = str(link_profile["cxl_direct_link"])
    for link_name, link_id in [
        ("host_h2d_link", host_h2d_link),
        ("host_d2h_link", host_d2h_link),
        ("cxl_direct_link", cxl_direct_link),
    ]:
        if link_id not in sources.LINKS:
            raise ValueError(f"{link_name} references unknown link: {link_id}")

    resource_capacity = config["resource_capacity"]
    for key in [
        "host_h2d_ingress_channels",
        "host_h2d_stage_channels",
        "host_d2h_channels",
        "cxl_direct_channels",
        "host_touch_channels",
        "cpu_materialize_channels",
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
        "cpu_materialize_channel",
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
        _profile_public_stage_names(profile)
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
    bytes_touched_factors_by_stage_by_template = config["bytes_touched_factors_by_stage_by_template"]
    for mapping_name, mapping in [
        ("scenario_stage_device_map_by_template", scenario_stage_device_map_by_template),
        ("pim_speedup_vs_cpu_by_stage_by_template", pim_speedup_by_template),
        ("cpu_stage_unit_compute_Bps_by_template", cpu_stage_rates_by_template),
        ("bytes_touched_factors_by_stage_by_template", bytes_touched_factors_by_stage_by_template),
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
        if template not in bytes_touched_factors_by_stage_by_template:
            raise KeyError(f"bytes_touched_factors_by_stage_by_template missing template {template}")

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
        bytes_touched_map = bytes_touched_factors_by_stage_by_template[template]
        if not isinstance(speedup_map, Mapping):
            raise ValueError(f"pim speedup map for template {template} must be a map")
        if not isinstance(rate_map, Mapping):
            raise ValueError(f"cpu stage rate map for template {template} must be a map")
        if not isinstance(bytes_touched_map, Mapping):
            raise ValueError(f"bytes_touched map for template {template} must be a map")
        for stage_name in stage_names:
            if stage_name not in speedup_map:
                raise KeyError(f"template {template} speedup map missing stage {stage_name}")
            if float(speedup_map[stage_name]) <= 0:
                raise ValueError(f"template {template} speedup for {stage_name} must be > 0")
            if stage_name not in rate_map:
                raise KeyError(f"template {template} cpu stage rate map missing stage {stage_name}")
            if float(rate_map[stage_name]) <= 0:
                raise ValueError(f"template {template} cpu stage rate for {stage_name} must be > 0")
            if stage_name not in bytes_touched_map:
                raise KeyError(f"template {template} bytes_touched map missing stage {stage_name}")
            factors = bytes_touched_map[stage_name]
            if not isinstance(factors, Mapping):
                raise ValueError(f"bytes_touched factors for {template}:{stage_name} must be a map")
            for factor_key in ["input_factor", "output_factor", "amplification_factor"]:
                if factor_key not in factors:
                    raise KeyError(f"bytes_touched factors for {template}:{stage_name} missing {factor_key}")
            if float(factors["input_factor"]) < 0 or float(factors["output_factor"]) < 0:
                raise ValueError(f"bytes_touched input/output factors must be >= 0 for {template}:{stage_name}")
            if float(factors["amplification_factor"]) <= 0:
                raise ValueError(f"bytes_touched amplification_factor must be > 0 for {template}:{stage_name}")

    memory_system_by_template = _normalize_memory_system_config(
        config=config,
        template_to_stage_names=template_to_stage_names,
        warn_deprecated=warn_deprecated_memory_keys,
    )
    _validate_memory_system_config(
        memory_system_by_template=memory_system_by_template,
        template_to_stage_names=template_to_stage_names,
    )

    _normalize_endpoint_map(
        config=config,
        template_to_stage_names=template_to_stage_names,
        scenarios=config["scenarios"],
        scenario_stage_device_map_by_template=scenario_stage_device_map_by_template,
        warn_defaults=False,
    )
    _normalize_pim_retention_config(config=config, warn_defaults=False)
    _normalize_cxl_direct_concurrency_config(config=config, warn_defaults=False)
    _normalize_cxl_topology_config(config=config, warn_defaults=False)
    _normalize_ingress_resident_scenarios_by_template(
        config=config,
        template_to_stage_names=template_to_stage_names,
    )

    if int(config["tile_size_bytes"]) <= 0:
        raise ValueError("tile_size_bytes must be > 0")
    if int(config["max_inflight_tiles"]) <= 0:
        raise ValueError("max_inflight_tiles must be > 0")

    return memory_system_by_template


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
    public_stage_names: Sequence[str],
    stage_names: Sequence[str],
    pipeline_template: str,
    size_multiplier: float,
    scenario: str,
    tile_size_bytes: int,
    max_inflight_tiles: int,
    host_h2d_link: str,
    host_d2h_link: str,
    cxl_direct_link: str,
    resource_capacity: Dict[str, object],
    stage_defaults: Dict[str, object],
    transfer_power_W: Dict[str, object],
    stage_overrides: Dict[str, Dict[object, Dict[str, object]]],
    scenario_stage_device_map_by_template: Mapping[str, Mapping[str, Sequence[str]]],
    scenario_stage_endpoint_map_by_template: Mapping[str, Mapping[str, Sequence[str]]],
    ingress_resident_scenarios_by_template: Mapping[str, Sequence[str]],
    pim_speedup_vs_cpu_by_stage_by_template: Mapping[str, Mapping[str, object]],
    cpu_stage_unit_compute_Bps_by_template: Mapping[str, Mapping[str, object]],
    memory_system_by_template: Mapping[str, Mapping[str, object]],
    pim_retention: PIMRetentionConfig,
    cxl_direct_concurrency: CXLDirectConcurrencyConfig,
    cxl_topology: CXLTopologyConfig,
    bytes_touched_factors_by_stage_by_template: Mapping[str, Mapping[str, Mapping[str, object]]],
    workload_family: str,
    workload_profile: str,
    workload_variant: str,
    baseline_id: str,
    trace_max_tiles: int | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    if scenario not in sources.SCENARIOS:
        raise ValueError(f"unknown scenario: {scenario}")
    if host_h2d_link not in sources.LINKS:
        raise ValueError(f"unknown host H2D link: {host_h2d_link}")
    if host_d2h_link not in sources.LINKS:
        raise ValueError(f"unknown host D2H link: {host_d2h_link}")
    if cxl_direct_link not in sources.LINKS:
        raise ValueError(f"unknown cxl direct link: {cxl_direct_link}")

    num_stages = len(boundaries_bytes) - 1
    num_public_stages = len(public_stage_names)
    if len(stage_names) != num_stages:
        raise ValueError(
            f"stage_names length {len(stage_names)} does not match boundaries-derived stages {num_stages}"
        )
    if num_public_stages <= 0:
        raise ValueError("public_stage_names must contain at least one stage")
    if pipeline_template not in scenario_stage_device_map_by_template:
        raise KeyError(f"missing scenario stage map for template {pipeline_template}")
    if pipeline_template not in scenario_stage_endpoint_map_by_template:
        raise KeyError(f"missing scenario endpoint map for template {pipeline_template}")
    if pipeline_template not in ingress_resident_scenarios_by_template:
        raise KeyError(f"missing ingress resident scenario map for template {pipeline_template}")
    if pipeline_template not in pim_speedup_vs_cpu_by_stage_by_template:
        raise KeyError(f"missing pim speedup map for template {pipeline_template}")
    if pipeline_template not in cpu_stage_unit_compute_Bps_by_template:
        raise KeyError(f"missing cpu stage rate map for template {pipeline_template}")
    if pipeline_template not in memory_system_by_template:
        raise KeyError(f"missing memory system map for template {pipeline_template}")
    if pipeline_template not in bytes_touched_factors_by_stage_by_template:
        raise KeyError(f"missing bytes_touched map for template {pipeline_template}")

    stage_devices = _stage_device_map_for_scenario(
        scenario_stage_device_map=scenario_stage_device_map_by_template[pipeline_template],
        scenario=scenario,
        num_stages=num_stages,
    )
    stage_endpoints = _stage_endpoint_map_for_scenario(
        scenario_stage_endpoint_map=scenario_stage_endpoint_map_by_template[pipeline_template],
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
    memory_system_enabled, cpu_baseline_cfg, pim_system_cfg = _build_system_configs_for_template(
        memory_system_cfg=memory_system_by_template[pipeline_template],
        stage_names=stage_names,
    )
    bytes_touched_factors_by_stage = bytes_touched_factors_by_stage_by_template[pipeline_template]
    cpu_baseline_engine = cpu_baseline_cfg.baseline_engine
    cxl_direct_stream_slots = int(resource_capacity["cxl_direct_channels"]) * int(
        cxl_direct_concurrency.virtual_channels_per_channel
    )
    cxl_power_per_stream_slot = float(transfer_power_W["cxl_direct_channel"]) / float(
        cxl_direct_concurrency.virtual_channels_per_channel
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
        capacity=cxl_direct_stream_slots,
        power_W=cxl_power_per_stream_slot,
    )
    host_touch_pool = ResourcePool(
        name="host_touch",
        capacity=int(resource_capacity["host_touch_channels"]),
        power_W=float(transfer_power_W["host_touch_channel"]),
    )
    cpu_materialize_pool = ResourcePool(
        name="cpu_materialize",
        capacity=int(resource_capacity["cpu_materialize_channels"]),
        power_W=float(transfer_power_W["cpu_materialize_channel"]),
    )
    active_pim_endpoints = sorted(
        {
            stage_endpoints[stage_id - 1]
            for stage_id in range(1, num_stages + 1)
            if stage_devices[stage_id - 1] == sources.DEVICE_PIM
        }
    )
    retain_pools: Dict[str, ResourcePool] = {
        endpoint: ResourcePool(name=f"retain_{endpoint}", capacity=1, power_W=0.0)
        for endpoint in active_pim_endpoints
    }

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

    operations = _build_tile_operations(
        scenario=scenario,
        stage_devices=stage_devices,
        stage_endpoints=stage_endpoints,
        materialization_policy=cpu_baseline_cfg.materialization_policy,
        baseline_engine=cpu_baseline_engine,
        ingress_resident=scenario in set(ingress_resident_scenarios_by_template[pipeline_template]),
    )
    active_direct_endpoints: set[str] = set()
    if scenario == sources.SCENARIO_PIM_FLOWCXL_DIRECT:
        for operation in operations:
            if operation.op_type != "PIM_HANDOFF":
                continue
            src_id = operation.src_stage_id or operation.stage_id
            dst_id = operation.dst_stage_id if operation.dst_stage_id > 0 else src_id + 1
            if dst_id <= 0 or dst_id > num_stages:
                continue
            src_endpoint = stage_endpoints[src_id - 1]
            dst_endpoint = stage_endpoints[dst_id - 1]
            if src_endpoint != dst_endpoint:
                active_direct_endpoints.add(src_endpoint)
                active_direct_endpoints.add(dst_endpoint)
    active_direct_endpoint_count = len(active_direct_endpoints)
    cxl_striping_factor = 1
    cxl_link = sources.LINKS[cxl_direct_link]
    supports_dynamic_striping = bool(cxl_link.get("supports_dynamic_striping", False))
    if (
        cxl_topology.enabled
        and cxl_topology.mode == "dynamic_striping"
        and supports_dynamic_striping
        and cxl_direct_link in cxl_topology.applies_to_links
    ):
        endpoint_factor = max(1, active_direct_endpoint_count)
        cxl_striping_factor = min(
            cxl_topology.max_stripes,
            cxl_topology.num_physical_links,
            endpoint_factor,
        )

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
    total_bytes_pim_retained = 0
    total_retain_fallback_bytes = 0
    total_cpu_materialize_bytes = 0
    total_compute_time_component_s = 0.0
    total_cpu_mem_time_component_s = 0.0
    total_cpu_mem_latency_bound_time_component_s = 0.0
    total_cpu_mem_peak_bound_time_component_s = 0.0
    total_pim_mem_time_component_s = 0.0
    total_cpu_mem_service_time_component_s = 0.0
    total_cpu_mem_queue_delay_component_s = 0.0
    total_pim_mem_service_time_component_s = 0.0
    total_pim_mem_queue_delay_component_s = 0.0
    total_cpu_materialize_time_component_s = 0.0
    total_retain_handoff_time_component_s = 0.0
    total_cxl_dma_issue_time_component_s = 0.0

    while request_heap:
        t_req, tile_id = heapq.heappop(request_heap)
        op_index = next_op_index[tile_id]
        if op_index >= len(operations):
            continue

        operation = operations[op_index]
        stage_cfg = stage_configs[operation.stage_id - 1]
        stage_device = stage_devices[operation.stage_id - 1]
        bytes_moved = int(tiled_boundaries[operation.boundary_index][tile_id])
        compute_component_s = 0.0
        memory_component_s = 0.0
        bytes_touched = 0.0
        cpu_access_pattern = ""
        cpu_row_hit_rate = 0.0
        cpu_mlp = 0.0
        cpu_avg_miss_latency_ns = 0.0
        cpu_bw_peak_Bps = 0.0
        cpu_bw_latency_Bps = 0.0
        cpu_bw_eff_stage_Bps = 0.0
        cpu_bw_eff_per_unit_Bps = 0.0
        cpu_mem_bound_mode = ""
        memory_system_role = ""
        mem_service_Bps = 0.0
        mem_queue_multiplier = 1.0
        mem_rho = 0.0
        mem_service_time_s = 0.0
        mem_queue_delay_s = 0.0
        stage_src_endpoint = stage_endpoints[operation.stage_id - 1]
        stage_dst_endpoint = stage_src_endpoint
        handoff_mode = ""
        retention_capacity_blocked = False
        cxl_active_streams = 0
        cxl_bw_share_Bps = 0.0
        cxl_issue_overhead_s = 0.0
        cxl_striping_factor_trace = 1

        event_specs: List[Dict[str, object]] = []

        if operation.op_type == "COMPUTE":
            stage_name = stage_names[operation.stage_id - 1]
            bytes_in = bytes_moved
            bytes_out = int(tiled_boundaries[operation.stage_id][tile_id])
            factors = bytes_touched_factors_by_stage[stage_name]
            if stage_device == sources.DEVICE_CPU:
                compute_rate = stage_cfg.cpu_unit_compute_Bps
                stage_units = stage_cfg.cpu_units
                memory_system_role = "cpu_baseline"
                stage_service_cfg = cpu_baseline_cfg.stages.get(stage_name)
                cacheline_bytes = cpu_baseline_cfg.cacheline_bytes
                queueing_model = cpu_baseline_cfg.queueing_model
                queue_alpha = cpu_baseline_cfg.queue_alpha
                rho_cap = cpu_baseline_cfg.rho_cap
            else:
                compute_rate = stage_cfg.pim_unit_compute_Bps
                stage_units = stage_cfg.pim_units
                memory_system_role = "pim_system"
                stage_service_cfg = pim_system_cfg.stages.get(stage_name) if pim_system_cfg.enabled else None
                cacheline_bytes = pim_system_cfg.cacheline_bytes
                queueing_model = pim_system_cfg.queueing_model
                queue_alpha = pim_system_cfg.queue_alpha
                rho_cap = pim_system_cfg.rho_cap

            compute_component_s = compute_duration_s(bytes_moved=bytes_in, compute_rate_Bps=compute_rate)
            bytes_touched = compute_bytes_touched(
                bytes_in=bytes_in,
                bytes_out=bytes_out,
                input_factor=float(factors["input_factor"]),
                output_factor=float(factors["output_factor"]),
                amplification_factor=float(factors["amplification_factor"]),
            )
            if memory_system_enabled and stage_service_cfg is not None:
                mem_stats = _compute_stage_memory_service(
                    stage_cfg=stage_service_cfg,
                    stage_units=stage_units,
                    bytes_touched=bytes_touched,
                    compute_component_s=compute_component_s,
                    cacheline_bytes=cacheline_bytes,
                    queueing_model=queueing_model,
                    queue_alpha=queue_alpha,
                    rho_cap=rho_cap,
                )
                memory_component_s = float(mem_stats["mem_total_time_s"])
                mem_service_time_s = float(mem_stats["mem_service_time_s"])
                mem_queue_delay_s = float(mem_stats["mem_queue_delay_s"])
                mem_service_Bps = float(mem_stats["bw_service_Bps"])
                mem_queue_multiplier = float(mem_stats["queue_multiplier"])
                mem_rho = float(mem_stats["rho"])
                duration_s = max(compute_component_s, memory_component_s)

                if stage_device == sources.DEVICE_CPU:
                    cpu_access_pattern = stage_service_cfg.access_pattern
                    cpu_row_hit_rate = stage_service_cfg.row_hit_rate
                    cpu_mlp = stage_service_cfg.mlp
                    cpu_avg_miss_latency_ns = stage_service_cfg.avg_miss_latency_ns
                    cpu_bw_peak_Bps = stage_service_cfg.peak_bw_Bps
                    cpu_bw_latency_Bps = float(mem_stats["bw_latency_Bps"])
                    cpu_bw_eff_stage_Bps = float(mem_stats["bw_eff_stage_Bps"])
                    cpu_bw_eff_per_unit_Bps = float(mem_stats["bw_eff_per_unit_Bps"])
                    cpu_mem_bound_mode = str(mem_stats["mem_bound_mode"])
            else:
                duration_s = compute_component_s
                memory_component_s = 0.0
            total_compute_time_component_s += compute_component_s
            if memory_system_enabled and stage_service_cfg is not None:
                if stage_device == sources.DEVICE_CPU:
                    total_cpu_mem_time_component_s += memory_component_s
                    total_cpu_mem_service_time_component_s += mem_service_time_s
                    total_cpu_mem_queue_delay_component_s += mem_queue_delay_s
                    if cpu_mem_bound_mode == "latency_limited":
                        total_cpu_mem_latency_bound_time_component_s += memory_component_s
                    else:
                        total_cpu_mem_peak_bound_time_component_s += memory_component_s
                else:
                    total_pim_mem_time_component_s += memory_component_s
                    total_pim_mem_service_time_component_s += mem_service_time_s
                    total_pim_mem_queue_delay_component_s += mem_queue_delay_s
            event_specs.append(
                {
                    "op_type": "COMPUTE",
                    "stage_id": operation.stage_id,
                    "pool": compute_pools[operation.stage_id - 1],
                    "duration_s": duration_s,
                    "transfer_path": "",
                    "link_used": "",
                    "bytes": bytes_moved,
                }
            )
        elif operation.op_type == "MATERIALIZE":
            duration_s = materialize_duration_s(
                bytes_moved=bytes_moved,
                materialize_Bps=cpu_baseline_cfg.materialization_policy.materialize_Bps,
                fixed_s=cpu_baseline_cfg.materialization_policy.fixed_s,
            )
            total_cpu_materialize_bytes += bytes_moved
            total_cpu_materialize_time_component_s += duration_s
            event_specs.append(
                {
                    "op_type": "MATERIALIZE",
                    "stage_id": operation.stage_id,
                    "pool": cpu_materialize_pool,
                    "duration_s": duration_s,
                    "transfer_path": operation.transfer_path,
                    "link_used": "",
                    "bytes": bytes_moved,
                }
            )
        elif operation.op_type == "HOST_TOUCH":
            duration_s = host_touch_duration_s(
                bytes_moved=bytes_moved,
                touch_Bps=stage_cfg.host_touch_Bps,
                touch_fixed_s=stage_cfg.host_touch_fixed_s,
            )
            total_bytes_host_touch += bytes_moved
            event_specs.append(
                {
                    "op_type": "HOST_TOUCH",
                    "stage_id": operation.stage_id,
                    "pool": host_touch_pool,
                    "duration_s": duration_s,
                    "transfer_path": operation.transfer_path,
                    "link_used": "",
                    "bytes": bytes_moved,
                }
            )
        elif operation.op_type == "PIM_HANDOFF":
            src_stage_id = operation.src_stage_id if operation.src_stage_id > 0 else operation.stage_id
            dst_stage_id = operation.dst_stage_id if operation.dst_stage_id > 0 else src_stage_id + 1
            if src_stage_id <= 0 or src_stage_id > num_stages or dst_stage_id <= 0 or dst_stage_id > num_stages:
                raise ValueError(f"invalid PIM_HANDOFF stage ids: {src_stage_id}->{dst_stage_id}")
            src_endpoint = stage_endpoints[src_stage_id - 1]
            dst_endpoint = stage_endpoints[dst_stage_id - 1]
            stage_src_endpoint = src_endpoint
            stage_dst_endpoint = dst_endpoint

            retain_allowed = (
                pim_retention.enabled
                and pim_retention.same_endpoint_short_circuit
                and scenario in pim_retention.applies_to_scenarios
                and stage_devices[src_stage_id - 1] == sources.DEVICE_PIM
                and stage_devices[dst_stage_id - 1] == sources.DEVICE_PIM
                and src_endpoint == dst_endpoint
            )
            if retain_allowed:
                boundary_total_bytes = int(scaled_boundaries[operation.boundary_index])
                if boundary_total_bytes <= pim_retention.pim_retention_capacity_bytes:
                    handoff_mode = "retain"
                    retain_duration = retain_duration_s(
                        retain_fixed_s=pim_retention.retain_fixed_s,
                        retain_metadata_bytes=pim_retention.retain_metadata_bytes,
                        retain_local_BW_Bps=pim_retention.retain_local_BW_Bps,
                    )
                    total_bytes_pim_retained += bytes_moved
                    total_retain_handoff_time_component_s += retain_duration
                    retain_pool = retain_pools[src_endpoint]
                    event_specs.append(
                        {
                            "op_type": "PIM_HANDOFF",
                            "stage_id": src_stage_id,
                            "pool": retain_pool,
                            "duration_s": retain_duration,
                            "transfer_path": "retain",
                            "link_used": "",
                            "bytes": bytes_moved,
                        }
                    )
                else:
                    retention_capacity_blocked = True
                    total_retain_fallback_bytes += bytes_moved

            if not event_specs:
                if scenario == sources.SCENARIO_PIM_FLOWCXL_DIRECT:
                    handoff_mode = "transfer_direct"
                    earliest_slot_free = min(cxl_direct_pool.next_free_time_by_slot)
                    t_direct_start_snapshot = max(t_req, earliest_slot_free)
                    active_direct_streams = _active_slots_at_time(cxl_direct_pool, t_direct_start_snapshot)
                    cxl_active_streams = active_direct_streams + 1
                    cxl_striping_factor_trace = cxl_striping_factor
                    total_cxl_bw_Bps = float(cxl_link["bandwidth_Bps"]) * float(cxl_striping_factor_trace)
                    cxl_bw_share_Bps = total_cxl_bw_Bps / float(max(1, cxl_active_streams))
                    u_out = min(
                        1.0,
                        float(cxl_direct_concurrency.dma_outstanding_per_vc)
                        / float(cxl_direct_concurrency.full_bw_outstanding_threshold),
                    )
                    cxl_issue_overhead_s = cxl_direct_concurrency.dma_issue_fixed_s / max(u_out, 1e-6)
                    total_cxl_dma_issue_time_component_s += cxl_issue_overhead_s
                    direct_duration = (
                        float(cxl_link["latency_s"])
                        + cxl_issue_overhead_s
                        + (bytes_moved / cxl_bw_share_Bps)
                    )
                    total_bytes_cxl_direct += bytes_moved
                    event_specs.append(
                        {
                            "op_type": "TRANSFER",
                            "stage_id": src_stage_id,
                            "pool": cxl_direct_pool,
                            "duration_s": direct_duration,
                            "transfer_path": "cxl_direct",
                            "link_used": cxl_direct_link,
                            "bytes": bytes_moved,
                        }
                    )
                elif scenario == sources.SCENARIO_PIM_HOST_BOUNCE:
                    handoff_mode = "transfer_bounce"
                    d2h_duration = transfer_duration_s(bytes_moved=bytes_moved, link_type=host_d2h_link)
                    touch_duration = host_touch_duration_s(
                        bytes_moved=bytes_moved,
                        touch_Bps=stage_cfg.host_touch_Bps,
                        touch_fixed_s=stage_cfg.host_touch_fixed_s,
                    )
                    h2d_duration = transfer_duration_s(bytes_moved=bytes_moved, link_type=host_h2d_link)
                    total_bytes_host_link += bytes_moved * 2
                    total_bytes_host_d2h += bytes_moved
                    total_bytes_host_h2d_stage += bytes_moved
                    total_bytes_host_touch += bytes_moved
                    event_specs.extend(
                        [
                            {
                                "op_type": "TRANSFER",
                                "stage_id": src_stage_id,
                                "pool": host_d2h_pool,
                                "duration_s": d2h_duration,
                                "transfer_path": "host_d2h",
                                "link_used": host_d2h_link,
                                "bytes": bytes_moved,
                            },
                            {
                                "op_type": "HOST_TOUCH",
                                "stage_id": src_stage_id,
                                "pool": host_touch_pool,
                                "duration_s": touch_duration,
                                "transfer_path": "host_touch",
                                "link_used": "",
                                "bytes": bytes_moved,
                            },
                            {
                                "op_type": "TRANSFER",
                                "stage_id": dst_stage_id,
                                "pool": host_h2d_stage_pool,
                                "duration_s": h2d_duration,
                                "transfer_path": "host_h2d_stage",
                                "link_used": host_h2d_link,
                                "bytes": bytes_moved,
                            },
                        ]
                    )
                else:
                    raise ValueError(f"PIM_HANDOFF is not valid for scenario {scenario}")
        else:
            if operation.transfer_path == "host_h2d_ingress":
                total_bytes_host_link += bytes_moved
                total_bytes_host_h2d_ingress += bytes_moved
                stage_src_endpoint = "host0"
                stage_dst_endpoint = stage_endpoints[operation.stage_id - 1]
                event_specs.append(
                    {
                        "op_type": "TRANSFER",
                        "stage_id": operation.stage_id,
                        "pool": host_h2d_ingress_pool,
                        "duration_s": transfer_duration_s(bytes_moved=bytes_moved, link_type=host_h2d_link),
                        "transfer_path": "host_h2d_ingress",
                        "link_used": host_h2d_link,
                        "bytes": bytes_moved,
                    }
                )
            elif operation.transfer_path == "host_h2d_stage":
                total_bytes_host_link += bytes_moved
                total_bytes_host_h2d_stage += bytes_moved
                stage_src_endpoint = "host0"
                stage_dst_endpoint = stage_endpoints[operation.stage_id - 1]
                event_specs.append(
                    {
                        "op_type": "TRANSFER",
                        "stage_id": operation.stage_id,
                        "pool": host_h2d_stage_pool,
                        "duration_s": transfer_duration_s(bytes_moved=bytes_moved, link_type=host_h2d_link),
                        "transfer_path": "host_h2d_stage",
                        "link_used": host_h2d_link,
                        "bytes": bytes_moved,
                    }
                )
            elif operation.transfer_path == "host_d2h":
                total_bytes_host_link += bytes_moved
                total_bytes_host_d2h += bytes_moved
                stage_src_endpoint = stage_endpoints[operation.stage_id - 1]
                stage_dst_endpoint = "host0"
                event_specs.append(
                    {
                        "op_type": "TRANSFER",
                        "stage_id": operation.stage_id,
                        "pool": host_d2h_pool,
                        "duration_s": transfer_duration_s(bytes_moved=bytes_moved, link_type=host_d2h_link),
                        "transfer_path": "host_d2h",
                        "link_used": host_d2h_link,
                        "bytes": bytes_moved,
                    }
                )
            elif operation.transfer_path == "cxl_direct":
                total_bytes_cxl_direct += bytes_moved
                event_specs.append(
                    {
                        "op_type": "TRANSFER",
                        "stage_id": operation.stage_id,
                        "pool": cxl_direct_pool,
                        "duration_s": transfer_duration_s(bytes_moved=bytes_moved, link_type=cxl_direct_link),
                        "transfer_path": "cxl_direct",
                        "link_used": cxl_direct_link,
                        "bytes": bytes_moved,
                    }
                )
            else:
                raise ValueError(f"unknown transfer path: {operation.transfer_path}")

        if not event_specs:
            raise AssertionError(f"no event generated for op_type={operation.op_type}")

        t_cursor = t_req
        t_end = t_req
        for event in event_specs:
            pool = event["pool"]
            duration_s = float(event["duration_s"])
            transfer_path = str(event["transfer_path"])
            link_used = str(event["link_used"])
            event_stage_id = int(event["stage_id"])
            event_stage_device = stage_devices[event_stage_id - 1]
            event_bytes = int(event["bytes"])
            event_op_type = str(event["op_type"])
            event_t_req = t_cursor

            t_start, t_end, wait_s, slot_idx = pool.schedule(t_req=t_cursor, duration_s=duration_s)
            t_cursor = t_end

            if trace_max_tiles is None or tile_id < trace_max_tiles:
                traces.append(
                    {
                        "run_id": run_id,
                        "dataset_profile": dataset_profile,
                        "stage_size_multiplier": size_multiplier,
                        "scenario": scenario,
                        "pipeline_template": pipeline_template,
                        "workload_family": workload_family,
                        "workload_profile": workload_profile,
                        "workload_variant": workload_variant,
                        "baseline_id": baseline_id,
                        "tile_id": tile_id,
                        "op_index": op_index + 1,
                        "stage_id": event_stage_id,
                        "stage_name": stage_names[event_stage_id - 1],
                        "stage_device": event_stage_device,
                        "op_type": event_op_type,
                        "transfer_path": transfer_path,
                        "resource": pool.name,
                        "resource_slot": slot_idx,
                        "link_type": link_used,
                        "bytes": event_bytes,
                        "t_req": event_t_req,
                        "t_start": t_start,
                        "t_end": t_end,
                        "duration_s": duration_s,
                        "wait_s": wait_s,
                        "compute_component_s": compute_component_s if event_op_type == "COMPUTE" else 0.0,
                        "memory_component_s": memory_component_s if event_op_type == "COMPUTE" else 0.0,
                        "bytes_touched": bytes_touched if event_op_type == "COMPUTE" else 0.0,
                        "cpu_access_pattern": cpu_access_pattern if event_op_type == "COMPUTE" else "",
                        "cpu_row_hit_rate": cpu_row_hit_rate if event_op_type == "COMPUTE" else 0.0,
                        "cpu_mlp": cpu_mlp if event_op_type == "COMPUTE" else 0.0,
                        "cpu_avg_miss_latency_ns": cpu_avg_miss_latency_ns if event_op_type == "COMPUTE" else 0.0,
                        "cpu_bw_peak_Bps": cpu_bw_peak_Bps if event_op_type == "COMPUTE" else 0.0,
                        "cpu_bw_latency_Bps": cpu_bw_latency_Bps if event_op_type == "COMPUTE" else 0.0,
                        "cpu_bw_eff_stage_Bps": cpu_bw_eff_stage_Bps if event_op_type == "COMPUTE" else 0.0,
                        "cpu_bw_eff_per_unit_Bps": cpu_bw_eff_per_unit_Bps if event_op_type == "COMPUTE" else 0.0,
                        "cpu_mem_bound_mode": cpu_mem_bound_mode if event_op_type == "COMPUTE" else "",
                        "memory_ceiling_enabled": memory_system_enabled,
                        "memory_system_role": memory_system_role if event_op_type == "COMPUTE" else "",
                        "mem_service_Bps": mem_service_Bps if event_op_type == "COMPUTE" else 0.0,
                        "mem_queue_multiplier": mem_queue_multiplier if event_op_type == "COMPUTE" else 1.0,
                        "mem_rho": mem_rho if event_op_type == "COMPUTE" else 0.0,
                        "mem_service_time_s": mem_service_time_s if event_op_type == "COMPUTE" else 0.0,
                        "mem_queue_delay_s": mem_queue_delay_s if event_op_type == "COMPUTE" else 0.0,
                        "cpu_baseline_engine": cpu_baseline_engine,
                        "stage_src_endpoint": stage_src_endpoint,
                        "stage_dst_endpoint": stage_dst_endpoint,
                        "handoff_mode": handoff_mode,
                        "retention_capacity_blocked": retention_capacity_blocked,
                        "cxl_active_streams": cxl_active_streams,
                        "cxl_bw_share_Bps": cxl_bw_share_Bps,
                        "cxl_issue_overhead_s": cxl_issue_overhead_s,
                        "cxl_striping_factor": cxl_striping_factor_trace,
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
    cpu_materialize_energy_J = cpu_materialize_pool.busy_time_s * cpu_materialize_pool.power_W
    compute_energy_J += cpu_materialize_energy_J
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

    compute_lbs = [_pool_lower_bound_s(pool) for pool in compute_pools]
    compute_lbs.append(_pool_lower_bound_s(cpu_materialize_pool))
    lb_compute_stage_max_s = max(compute_lbs) if compute_lbs else 0.0
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
        "num_stages": num_public_stages,
        "num_kernels": num_stages,
        "num_tiles": num_tiles,
        "makespan_s": makespan_s,
        "total_energy_J": total_energy_J,
        "compute_energy_J": compute_energy_J,
        "transfer_energy_J": transfer_energy_J,
        "host_touch_energy_J": host_touch_energy_J,
        "cpu_materialize_energy_J": cpu_materialize_energy_J,
        "total_bytes_host_link": total_bytes_host_link,
        "total_bytes_cxl_direct": total_bytes_cxl_direct,
        "total_bytes_host_touch": total_bytes_host_touch,
        "total_bytes_host_h2d_ingress": total_bytes_host_h2d_ingress,
        "total_bytes_host_h2d_stage": total_bytes_host_h2d_stage,
        "total_bytes_host_d2h": total_bytes_host_d2h,
        "total_bytes_pim_retained": total_bytes_pim_retained,
        "total_retain_fallback_bytes": total_retain_fallback_bytes,
        "total_cpu_materialize_bytes": total_cpu_materialize_bytes,
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
        "workload_family": workload_family,
        "workload_profile": workload_profile,
        "workload_variant": workload_variant,
        "baseline_id": baseline_id,
        "memory_ceiling_enabled": memory_system_enabled,
        "cpu_baseline_engine": cpu_baseline_engine,
        "total_cpu_mem_time_component_s": total_cpu_mem_time_component_s,
        "total_cpu_mem_latency_bound_time_component_s": total_cpu_mem_latency_bound_time_component_s,
        "total_cpu_mem_peak_bound_time_component_s": total_cpu_mem_peak_bound_time_component_s,
        "total_pim_mem_time_component_s": total_pim_mem_time_component_s,
        "total_cpu_mem_service_time_component_s": total_cpu_mem_service_time_component_s,
        "total_cpu_mem_queue_delay_component_s": total_cpu_mem_queue_delay_component_s,
        "total_pim_mem_service_time_component_s": total_pim_mem_service_time_component_s,
        "total_pim_mem_queue_delay_component_s": total_pim_mem_queue_delay_component_s,
        "total_compute_time_component_s": total_compute_time_component_s,
        "total_cpu_materialize_time_component_s": total_cpu_materialize_time_component_s,
        "total_retain_handoff_time_component_s": total_retain_handoff_time_component_s,
        "cxl_direct_stream_slots": cxl_direct_stream_slots,
        "cxl_active_direct_endpoints": active_direct_endpoint_count,
        "cxl_effective_striping_factor": cxl_striping_factor,
        "total_cxl_dma_issue_time_component_s": total_cxl_dma_issue_time_component_s,
    }
    return metrics_row, traces


def generate_runs_from_config(config: Dict[str, object]) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    workload_sweep = _normalize_workload_sweep(config)
    workload_variants = _normalize_workload_variants(config)

    tpch_profiles = workload_sweep["tpch_profiles"]
    deepvariant_profiles = workload_sweep["deepvariant_profiles"]
    run_profiles = tpch_profiles + deepvariant_profiles
    if not run_profiles:
        raise ValueError("no profiles selected for run matrix; check workload_sweep or dataset_profiles")

    size_multipliers = [float(value) for value in config["size_multipliers"]]
    scenarios = [str(value) for value in config["scenarios"]]

    metrics: List[Dict[str, object]] = []
    traces: List[Dict[str, object]] = []
    run_counter = 1

    for variant in workload_variants:
        variant_name = str(variant["name"])
        variant_overrides = variant["overrides"]
        merged_config = _deep_merge_config(config, variant_overrides)
        merged_config["dataset_profiles"] = list(run_profiles)
        merged_config["size_multipliers"] = list(size_multipliers)
        merged_config["scenarios"] = list(scenarios)

        memory_system_by_template = _validate_config(
            merged_config,
            warn_deprecated_memory_keys=True,
        )
        template_to_stage_names = _template_to_stage_names_from_config(merged_config)

        tile_size_bytes = int(merged_config["tile_size_bytes"])
        max_inflight_tiles = int(merged_config["max_inflight_tiles"])
        host_h2d_link, host_d2h_link = _resolve_host_link_names(merged_config["link_profile"])
        cxl_direct_link = merged_config["link_profile"]["cxl_direct_link"]
        resource_capacity = merged_config["resource_capacity"]
        stage_defaults = merged_config["stage_defaults"]
        transfer_power_W = merged_config["transfer_power_W"]
        scenario_stage_device_map_by_template = merged_config["scenario_stage_device_map_by_template"]

        _, scenario_stage_endpoint_map_by_template = _normalize_endpoint_map(
            config=merged_config,
            template_to_stage_names=template_to_stage_names,
            scenarios=scenarios,
            scenario_stage_device_map_by_template=scenario_stage_device_map_by_template,
            warn_defaults=True,
        )
        ingress_resident_scenarios_by_template = _normalize_ingress_resident_scenarios_by_template(
            config=merged_config,
            template_to_stage_names=template_to_stage_names,
        )
        pim_retention_cfg = _normalize_pim_retention_config(config=merged_config, warn_defaults=True)
        cxl_direct_concurrency_cfg = _normalize_cxl_direct_concurrency_config(
            config=merged_config,
            warn_defaults=True,
        )
        cxl_topology_cfg = _normalize_cxl_topology_config(config=merged_config, warn_defaults=True)
        pim_speedup_vs_cpu_by_stage_by_template = merged_config["pim_speedup_vs_cpu_by_stage_by_template"]
        cpu_stage_unit_compute_Bps_by_template = merged_config["cpu_stage_unit_compute_Bps_by_template"]
        bytes_touched_factors_by_stage_by_template = merged_config["bytes_touched_factors_by_stage_by_template"]
        stage_overrides = merged_config.get("stage_overrides", {})
        trace_max_tiles_raw = merged_config.get("trace_max_tiles", 512)
        trace_max_tiles: int | None
        if trace_max_tiles_raw is None:
            trace_max_tiles = None
        else:
            trace_max_tiles = int(trace_max_tiles_raw)
            if trace_max_tiles < 0:
                trace_max_tiles = None

        for workload_profile in run_profiles:
            profile = sources.DATASET_PROFILES[workload_profile]
            boundaries = profile["boundaries_bytes"]
            public_stage_names = _profile_public_stage_names(profile)
            stage_names = _profile_stage_names(profile)
            pipeline_template = _profile_template(profile)
            workload_family = _workload_family_from_template(pipeline_template)
            for size_multiplier in size_multipliers:
                multiplier_token = str(size_multiplier).replace(".", "p")
                baseline_id = f"{workload_family}|{workload_profile}|{variant_name}|m{multiplier_token}"
                for scenario in scenarios:
                    run_id = (
                        f"run_{run_counter:03d}_{workload_profile}_{variant_name}_{scenario}_m{multiplier_token}"
                        .replace(" ", "_")
                        .replace("/", "_")
                    )
                    run_counter += 1

                    row, trace_rows = simulate_configuration(
                        run_id=run_id,
                        dataset_profile=workload_profile,
                        boundaries_bytes=boundaries,
                        public_stage_names=public_stage_names,
                        stage_names=stage_names,
                        pipeline_template=pipeline_template,
                        size_multiplier=float(size_multiplier),
                        scenario=scenario,
                        tile_size_bytes=tile_size_bytes,
                        max_inflight_tiles=max_inflight_tiles,
                        host_h2d_link=host_h2d_link,
                        host_d2h_link=host_d2h_link,
                        cxl_direct_link=cxl_direct_link,
                        resource_capacity=resource_capacity,
                        stage_defaults=stage_defaults,
                        transfer_power_W=transfer_power_W,
                        stage_overrides=stage_overrides,
                        scenario_stage_device_map_by_template=scenario_stage_device_map_by_template,
                        scenario_stage_endpoint_map_by_template=scenario_stage_endpoint_map_by_template,
                        ingress_resident_scenarios_by_template=ingress_resident_scenarios_by_template,
                        pim_speedup_vs_cpu_by_stage_by_template=pim_speedup_vs_cpu_by_stage_by_template,
                        cpu_stage_unit_compute_Bps_by_template=cpu_stage_unit_compute_Bps_by_template,
                        memory_system_by_template=memory_system_by_template,
                        pim_retention=pim_retention_cfg,
                        cxl_direct_concurrency=cxl_direct_concurrency_cfg,
                        cxl_topology=cxl_topology_cfg,
                        bytes_touched_factors_by_stage_by_template=bytes_touched_factors_by_stage_by_template,
                        workload_family=workload_family,
                        workload_profile=workload_profile,
                        workload_variant=variant_name,
                        baseline_id=baseline_id,
                        trace_max_tiles=trace_max_tiles,
                    )
                    metrics.append(row)
                    traces.extend(trace_rows)

    return metrics, traces
