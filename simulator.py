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


@dataclass(frozen=True)
class PIMModeEffect:
    compute_multiplier: float
    mem_multiplier: float
    command_overhead_s: float


@dataclass(frozen=True)
class TileDomain:
    domain_id: str
    boundary_index: int
    tile_count: int
    tile_bytes: Tuple[int, ...]
    logical_kind: str = ""


@dataclass(frozen=True)
class BoundaryMappingSpec:
    transition_key: str
    mapping_id: str
    mapping_type: str
    group_k: int
    split_m: int
    partitions: int | str
    glue_type: str
    glue_device: str
    glue_fixed_s: float
    glue_compute_Bps: float
    glue_mem_Bps: float
    glue_transfer_path: str
    output_amplification: float


@dataclass(frozen=True)
class TilingTemplateConfig:
    enabled: bool
    admission_refill_policy: str
    glue_resource_mode: str
    stage_kernel_class: Dict[str, str]
    kernel_tiling_policy_by_class: Dict[str, Dict[str, float]]
    boundary_mappings: Dict[str, BoundaryMappingSpec]
    boundary_index_to_transition_key: Dict[int, str]


@dataclass
class BoundaryAggregationState:
    expected_count: int
    received_count: int = 0
    first_contrib_t: float = math.inf
    latest_contrib_t: float = 0.0
    bytes_in: float = 0.0


@dataclass
class RepartitionBarrierState:
    producer_count: int
    received_producer_count: int = 0
    first_contrib_t: float = math.inf
    latest_contrib_t: float = 0.0
    total_bytes_in: float = 0.0

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


class CXLProcessorShareScheduler:
    """Symmetric processor-sharing model for direct CXL data service."""

    def __init__(self, *, bw_total_Bps: float, slots: int) -> None:
        if bw_total_Bps <= 0:
            raise ValueError("bw_total_Bps must be > 0")
        if slots <= 0:
            raise ValueError("slots must be > 0")
        self.bw_total_Bps = float(bw_total_Bps)
        self.slots = int(slots)
        self.now_t = 0.0
        self.busy_slot_time_s = 0.0
        self._token_counter = 0
        self._active: Dict[int, Dict[str, float | int]] = {}

    def _advance(self, to_t: float) -> None:
        if to_t < self.now_t:
            to_t = self.now_t
        if not self._active:
            self.now_t = to_t
            return
        dt = to_t - self.now_t
        if dt <= 0:
            return
        active_count = len(self._active)
        served_per_transfer = (self.bw_total_Bps * dt) / float(active_count)
        for state in self._active.values():
            state["remaining_bytes"] = max(0.0, state["remaining_bytes"] - served_per_transfer)
        self.busy_slot_time_s += dt * float(active_count)
        self.now_t = to_t

    def _reschedule_completions(self) -> List[Tuple[float, int, int]]:
        events: List[Tuple[float, int, int]] = []
        if not self._active:
            return events
        per_transfer_Bps = self.bw_total_Bps / float(len(self._active))
        for transfer_id, state in self._active.items():
            remaining_bytes = max(0.0, float(state["remaining_bytes"]))
            t_complete = self.now_t + (remaining_bytes / per_transfer_Bps)
            self._token_counter += 1
            token = self._token_counter
            state["token"] = token
            state["scheduled_complete_t"] = t_complete
            events.append((t_complete, transfer_id, token))
        return events

    def next_completion_time(self, *, at_t: float | None = None) -> float:
        if at_t is not None:
            self._advance(at_t)
        if not self._active:
            return math.inf
        per_transfer_Bps = self.bw_total_Bps / float(len(self._active))
        min_remaining = min(float(state["remaining_bytes"]) for state in self._active.values())
        return self.now_t + (min_remaining / per_transfer_Bps)

    def active_count(self, *, at_t: float | None = None) -> int:
        if at_t is not None:
            self._advance(at_t)
        return len(self._active)

    def try_admit(
        self,
        *,
        transfer_id: int,
        bytes_total: int,
        at_t: float,
    ) -> Tuple[bool, List[Tuple[float, int, int]]]:
        self._advance(at_t)
        if len(self._active) >= self.slots:
            return False, []
        self._active[transfer_id] = {
            "remaining_bytes": float(max(0, bytes_total)),
            "token": -1,
            "scheduled_complete_t": at_t,
        }
        return True, self._reschedule_completions()

    def complete_if_valid(
        self,
        *,
        transfer_id: int,
        token: int,
        at_t: float,
    ) -> Tuple[bool, List[Tuple[float, int, int]]]:
        self._advance(at_t)
        state = self._active.get(transfer_id)
        if state is None:
            return False, []
        current_token = int(state.get("token", -1))
        if current_token != token:
            return False, []
        del self._active[transfer_id]
        return True, self._reschedule_completions()


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


def transfer_duration_s(
    bytes_moved: int,
    link_type: str,
    *,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
) -> float:
    link_catalog = sources.LINKS if links_catalog is None else links_catalog
    if link_type not in link_catalog:
        raise ValueError(f"unknown link type: {link_type}")
    link = link_catalog[link_type]
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
) -> Dict[str, Dict[str, object]]:
    if "memory_system_by_template" not in config:
        raise KeyError(
            "missing required config key: memory_system_by_template. "
            "Legacy memory keys are no longer supported."
        )
    memory_system_by_template_raw = config["memory_system_by_template"]
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


def _normalize_pim_mode_effects(config: Mapping[str, object]) -> Dict[str, PIMModeEffect]:
    defaults: Dict[str, Dict[str, float]] = {
        sources.PIM_MODE_NONE: {"compute_multiplier": 1.0, "mem_multiplier": 1.0, "command_overhead_s": 0.0},
        sources.PIM_MODE_BANK: {"compute_multiplier": 1.0, "mem_multiplier": 1.0, "command_overhead_s": 0.0},
        sources.PIM_MODE_BANK_GROUP: {
            "compute_multiplier": 0.8,
            "mem_multiplier": 0.85,
            "command_overhead_s": 1e-7,
        },
        sources.PIM_MODE_BUFFER: {
            "compute_multiplier": 0.6,
            "mem_multiplier": 0.65,
            "command_overhead_s": 2e-7,
        },
    }
    raw = config.get("pim_mode_effects")
    if raw is None:
        raw = defaults
    if not isinstance(raw, Mapping):
        raise ValueError("pim_mode_effects must be a map")
    normalized: Dict[str, PIMModeEffect] = {}
    for mode in sources.PIM_MODES:
        merged = dict(defaults[mode])
        mode_raw = raw.get(mode, {})
        if mode_raw:
            if not isinstance(mode_raw, Mapping):
                raise ValueError(f"pim_mode_effects[{mode}] must be a map")
            merged.update(mode_raw)
        effect = PIMModeEffect(
            compute_multiplier=float(merged["compute_multiplier"]),
            mem_multiplier=float(merged["mem_multiplier"]),
            command_overhead_s=float(merged["command_overhead_s"]),
        )
        if effect.compute_multiplier <= 0:
            raise ValueError(f"pim_mode_effects[{mode}].compute_multiplier must be > 0")
        if effect.mem_multiplier <= 0:
            raise ValueError(f"pim_mode_effects[{mode}].mem_multiplier must be > 0")
        if effect.command_overhead_s < 0:
            raise ValueError(f"pim_mode_effects[{mode}].command_overhead_s must be >= 0")
        normalized[mode] = effect
    return normalized


def _normalize_pim_mode_by_stage_by_template(
    *,
    config: Mapping[str, object],
    template_to_stage_names: Mapping[str, Sequence[str]],
) -> Dict[str, Dict[str, str]]:
    raw = config.get("pim_mode_by_stage_by_template")
    normalized: Dict[str, Dict[str, str]] = {}
    for template, stage_names in template_to_stage_names.items():
        normalized[template] = {stage_name: sources.PIM_MODE_NONE for stage_name in stage_names}
    if raw is None:
        return normalized
    if not isinstance(raw, Mapping):
        raise ValueError("pim_mode_by_stage_by_template must be a map")
    for template, stage_names in template_to_stage_names.items():
        template_raw = raw.get(template, {})
        if not isinstance(template_raw, Mapping):
            raise ValueError(f"pim_mode_by_stage_by_template[{template}] must be a map")
        for stage_name in stage_names:
            mode = str(template_raw.get(stage_name, sources.PIM_MODE_NONE))
            if mode not in sources.PIM_MODES:
                raise ValueError(
                    f"pim_mode_by_stage_by_template[{template}][{stage_name}] has invalid mode {mode}"
                )
            normalized[template][stage_name] = mode
    return normalized


def _default_kernel_class_for_stage(stage_name: str) -> str:
    lower = stage_name.lower()
    if "scan" in lower or "make_examples_frontend" in lower:
        return sources.KERNEL_CLASS_STREAM_SIMD
    if "tensorize" in lower:
        return sources.KERNEL_CLASS_PACK_TENSORIZE
    if "infer" in lower:
        return sources.KERNEL_CLASS_CNN_INFER
    if "join" in lower:
        return sources.KERNEL_CLASS_HASH_JOIN_PROBE
    if "groupby" in lower or "reduce" in lower:
        return sources.KERNEL_CLASS_REDUCE_PARTIAL
    if "post" in lower:
        return sources.KERNEL_CLASS_POSTPROC
    return sources.KERNEL_CLASS_STREAM_SIMD


def _boundary_transition_key(stage_names: Sequence[str], boundary_idx: int) -> str:
    if boundary_idx <= 0 or boundary_idx >= len(stage_names):
        raise ValueError(f"invalid boundary index for transition key: {boundary_idx}")
    return f"{stage_names[boundary_idx - 1]}->{stage_names[boundary_idx]}"


def _identity_boundary_mapping(*, transition_key: str) -> BoundaryMappingSpec:
    return BoundaryMappingSpec(
        transition_key=transition_key,
        mapping_id=f"auto_identity_{transition_key}",
        mapping_type=sources.MAPPING_IDENTITY,
        group_k=1,
        split_m=1,
        partitions=1,
        glue_type=sources.GLUE_COPY,
        glue_device=sources.DEVICE_PIM,
        glue_fixed_s=0.0,
        glue_compute_Bps=1e30,
        glue_mem_Bps=1e30,
        glue_transfer_path="none",
        output_amplification=1.0,
    )


def _normalize_tiling_model_by_template(
    *,
    config: Mapping[str, object],
    template_to_stage_names: Mapping[str, Sequence[str]],
) -> Dict[str, TilingTemplateConfig]:
    raw = config.get("tiling_model_by_template", {})
    if not isinstance(raw, Mapping):
        raise ValueError("tiling_model_by_template must be a map")

    normalized: Dict[str, TilingTemplateConfig] = {}
    for template, stage_names in template_to_stage_names.items():
        template_raw = raw.get(template, {})
        if template_raw and not isinstance(template_raw, Mapping):
            raise ValueError(f"tiling_model_by_template[{template}] must be a map")
        template_raw = dict(template_raw) if isinstance(template_raw, Mapping) else {}
        enabled = bool(template_raw.get("enabled", False))
        refill = str(template_raw.get("admission_refill_policy", "stage0_output"))
        if refill not in {"stage0_output", "pipeline_complete"}:
            raise ValueError(
                f"tiling_model_by_template[{template}].admission_refill_policy must be "
                "stage0_output or pipeline_complete"
            )
        glue_resource_mode = str(
            template_raw.get(
                "glue_resource_mode",
                sources.GLUE_RESOURCE_MODE_SHARED_CONSUMER_COMPUTE,
            )
        )
        if glue_resource_mode not in sources.GLUE_RESOURCE_MODES:
            raise ValueError(
                f"tiling_model_by_template[{template}].glue_resource_mode must be one of "
                f"{list(sources.GLUE_RESOURCE_MODES)}"
            )

        stage_kernel_raw = template_raw.get("stage_kernel_class", {})
        if stage_kernel_raw and not isinstance(stage_kernel_raw, Mapping):
            raise ValueError(f"tiling_model_by_template[{template}].stage_kernel_class must be a map")
        stage_kernel_class: Dict[str, str] = {}
        for stage_name in stage_names:
            kernel_class = str(
                (stage_kernel_raw.get(stage_name) if isinstance(stage_kernel_raw, Mapping) else None)
                or _default_kernel_class_for_stage(stage_name)
            )
            if kernel_class not in sources.KERNEL_CLASSES:
                raise ValueError(
                    f"tiling_model_by_template[{template}].stage_kernel_class[{stage_name}] invalid: {kernel_class}"
                )
            stage_kernel_class[stage_name] = kernel_class

        kernel_policy_raw = template_raw.get("kernel_tiling_policy_by_class", {})
        if kernel_policy_raw and not isinstance(kernel_policy_raw, Mapping):
            raise ValueError(f"tiling_model_by_template[{template}].kernel_tiling_policy_by_class must be a map")
        kernel_tiling_policy_by_class: Dict[str, Dict[str, float]] = {}
        for kernel_class in sources.KERNEL_CLASSES:
            class_policy_raw = kernel_policy_raw.get(kernel_class, {}) if isinstance(kernel_policy_raw, Mapping) else {}
            if class_policy_raw and not isinstance(class_policy_raw, Mapping):
                raise ValueError(
                    f"tiling_model_by_template[{template}].kernel_tiling_policy_by_class[{kernel_class}] must be a map"
                )
            target_tile_bytes = float(class_policy_raw.get("target_tile_bytes", 268_435_456.0))
            if target_tile_bytes <= 0:
                raise ValueError(
                    f"tiling_model_by_template[{template}].kernel_tiling_policy_by_class[{kernel_class}] "
                    "target_tile_bytes must be > 0"
                )
            kernel_tiling_policy_by_class[kernel_class] = {"target_tile_bytes": target_tile_bytes}

        boundary_raw = template_raw.get("boundary_mappings", {})
        if boundary_raw and not isinstance(boundary_raw, Mapping):
            raise ValueError(f"tiling_model_by_template[{template}].boundary_mappings must be a map")
        boundary_mappings: Dict[str, BoundaryMappingSpec] = {}
        boundary_index_to_transition_key: Dict[int, str] = {}
        num_stages = len(stage_names)
        warned_explicit_mapping_authority = False
        expected_transition_keys = {
            _boundary_transition_key(stage_names=stage_names, boundary_idx=boundary_idx)
            for boundary_idx in range(1, num_stages)
        }
        if boundary_raw:
            numeric_like_keys = []
            raw_key_strings = set()
            for raw_key in boundary_raw.keys():
                if isinstance(raw_key, int):
                    numeric_like_keys.append(str(raw_key))
                    continue
                key_str = str(raw_key)
                raw_key_strings.add(key_str)
                if key_str.strip().isdigit():
                    numeric_like_keys.append(key_str)
            if numeric_like_keys:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings uses numeric keys "
                    f"{numeric_like_keys}; use transition keys like 'join->groupby_agg'"
                )
            if raw_key_strings != expected_transition_keys:
                missing = sorted(expected_transition_keys - raw_key_strings)
                extra = sorted(raw_key_strings - expected_transition_keys)
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings keys must exactly match "
                    f"stage transitions. Missing={missing}, extra={extra}"
                )
        for boundary_idx in range(1, num_stages):
            transition_key = _boundary_transition_key(stage_names=stage_names, boundary_idx=boundary_idx)
            boundary_index_to_transition_key[boundary_idx] = transition_key
            entry_raw = boundary_raw.get(transition_key, {})
            if entry_raw and not isinstance(entry_raw, Mapping):
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] must be a map"
                )
            if not entry_raw:
                boundary_mappings[transition_key] = _identity_boundary_mapping(
                    transition_key=transition_key
                )
                continue
            if (
                not warned_explicit_mapping_authority
                and enabled
                and any(key in entry_raw for key in ("group_k", "split_m", "partitions"))
            ):
                warnings.warn(
                    f"tiling_model_by_template[{template}] uses explicit mapping parameters; "
                    "these override kernel_tiling_policy_by_class.target_tile_bytes for boundary cardinality.",
                    RuntimeWarning,
                    stacklevel=2,
                )
                warned_explicit_mapping_authority = True
            mapping_id = str(entry_raw.get("mapping_id", "")).strip()
            if not mapping_id:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] "
                    "requires non-empty mapping_id"
                )
            mapping_type = str(entry_raw.get("mapping_type", sources.MAPPING_IDENTITY))
            if mapping_type not in sources.MAPPING_TYPES:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] invalid mapping_type "
                    f"{mapping_type}"
                )
            group_k = int(entry_raw.get("group_k", 1))
            split_m = int(entry_raw.get("split_m", 1))
            partitions: int | str = entry_raw.get("partitions", 1)
            if isinstance(partitions, str):
                partitions = partitions.strip()
                if partitions != "pim_units":
                    raise ValueError(
                        f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}].partitions "
                        "must be integer or 'pim_units'"
                    )
            else:
                partitions = int(partitions)
                if partitions <= 0:
                    raise ValueError(
                        f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}].partitions "
                        "must be > 0"
                    )
            glue_type = str(entry_raw.get("glue_type", sources.GLUE_COPY))
            if glue_type not in sources.GLUE_TYPES:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] invalid glue_type "
                    f"{glue_type}"
                )
            glue_device = str(entry_raw.get("glue_device", sources.DEVICE_PIM)).lower()
            if glue_device not in {sources.DEVICE_CPU, sources.DEVICE_PIM}:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] invalid glue_device "
                    f"{glue_device}"
                )
            glue_transfer_path = str(entry_raw.get("glue_transfer_path", "none"))
            if glue_transfer_path not in {"none", "host_h2d_stage", "host_d2h", "cxl_direct"}:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] invalid glue_transfer_path "
                    f"{glue_transfer_path}"
                )
            spec = BoundaryMappingSpec(
                transition_key=transition_key,
                mapping_id=mapping_id,
                mapping_type=mapping_type,
                group_k=group_k,
                split_m=split_m,
                partitions=partitions,
                glue_type=glue_type,
                glue_device=glue_device,
                glue_fixed_s=float(entry_raw.get("glue_fixed_s", 0.0)),
                glue_compute_Bps=float(entry_raw.get("glue_compute_Bps", 1e30)),
                glue_mem_Bps=float(entry_raw.get("glue_mem_Bps", 1e30)),
                glue_transfer_path=glue_transfer_path,
                output_amplification=float(entry_raw.get("output_amplification", 1.0)),
            )
            if spec.group_k <= 0:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}].group_k must be > 0"
                )
            if spec.split_m <= 0:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}].split_m must be > 0"
                )
            if spec.glue_fixed_s < 0:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}].glue_fixed_s "
                    "must be >= 0"
                )
            if spec.glue_compute_Bps <= 0 or spec.glue_mem_Bps <= 0:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}] glue Bps must be > 0"
                )
            if spec.output_amplification <= 0:
                raise ValueError(
                    f"tiling_model_by_template[{template}].boundary_mappings[{transition_key}].output_amplification "
                    "must be > 0"
                )
            boundary_mappings[transition_key] = spec

        normalized[template] = TilingTemplateConfig(
            enabled=enabled,
            admission_refill_policy=refill,
            glue_resource_mode=glue_resource_mode,
            stage_kernel_class=stage_kernel_class,
            kernel_tiling_policy_by_class=kernel_tiling_policy_by_class,
            boundary_mappings=boundary_mappings,
            boundary_index_to_transition_key=boundary_index_to_transition_key,
        )
    return normalized


def _resolve_boundary_partition_count(
    *,
    partitions: int | str,
    producer_count: int,
    src_stage_cfg: StageConfig,
) -> int:
    if isinstance(partitions, str):
        if partitions != "pim_units":
            raise ValueError(f"unsupported partitions token {partitions}")
        resolved = int(src_stage_cfg.pim_units)
    else:
        resolved = int(partitions)
    resolved = max(1, resolved)
    return min(resolved, max(1, producer_count))


def _build_tile_domains_and_mappings(
    *,
    scaled_boundaries: Sequence[int],
    stage_names: Sequence[str],
    tiling_cfg: TilingTemplateConfig,
    stage_configs: Sequence[StageConfig],
) -> Tuple[List[TileDomain], Dict[int, BoundaryMappingSpec]]:
    num_stages = len(stage_names)
    if len(scaled_boundaries) != num_stages + 1:
        raise ValueError("scaled_boundaries must have len(stage_names)+1")
    boundary_domains: List[TileDomain] = []
    boundary_mappings_by_index: Dict[int, BoundaryMappingSpec] = {}

    stage0_class = tiling_cfg.stage_kernel_class[stage_names[0]]
    stage0_target_bytes = float(
        tiling_cfg.kernel_tiling_policy_by_class[stage0_class]["target_tile_bytes"]
    )
    input_tile_count = max(1, int(math.ceil(float(scaled_boundaries[0]) / stage0_target_bytes)))
    boundary_domains.append(
        TileDomain(
            domain_id="d0",
            boundary_index=0,
            tile_count=input_tile_count,
            tile_bytes=tuple(tile_boundary_bytes(scaled_boundaries[0], input_tile_count)),
            logical_kind="boundary_0",
        )
    )

    for boundary_idx in range(1, num_stages):
        producer_count = boundary_domains[boundary_idx - 1].tile_count
        transition_key = tiling_cfg.boundary_index_to_transition_key.get(boundary_idx)
        if transition_key is None:
            transition_key = _boundary_transition_key(stage_names=stage_names, boundary_idx=boundary_idx)
        mapping = tiling_cfg.boundary_mappings.get(
            transition_key,
            _identity_boundary_mapping(transition_key=transition_key),
        )
        if mapping.mapping_type == sources.MAPPING_IDENTITY:
            consumer_count = producer_count
        elif mapping.mapping_type == sources.MAPPING_GROUP_K_TO_1:
            consumer_count = max(1, int(math.ceil(float(producer_count) / float(mapping.group_k))))
        elif mapping.mapping_type == sources.MAPPING_SPLIT_1_TO_M:
            consumer_count = max(1, int(producer_count * mapping.split_m))
        elif mapping.mapping_type == sources.MAPPING_REPARTITION_HASH:
            consumer_count = _resolve_boundary_partition_count(
                partitions=mapping.partitions,
                producer_count=producer_count,
                src_stage_cfg=stage_configs[boundary_idx - 1],
            )
        else:
            raise ValueError(f"unknown mapping type {mapping.mapping_type}")

        boundary_domains.append(
            TileDomain(
                domain_id=f"d{boundary_idx}",
                boundary_index=boundary_idx,
                tile_count=consumer_count,
                tile_bytes=tuple(tile_boundary_bytes(scaled_boundaries[boundary_idx], consumer_count)),
                logical_kind=f"boundary_{boundary_idx}",
            )
        )
        boundary_mappings_by_index[boundary_idx] = mapping

    final_boundary_idx = num_stages
    final_producer_count = boundary_domains[-1].tile_count
    boundary_domains.append(
        TileDomain(
            domain_id=f"d{final_boundary_idx}",
            boundary_index=final_boundary_idx,
            tile_count=final_producer_count,
            tile_bytes=tuple(tile_boundary_bytes(scaled_boundaries[final_boundary_idx], final_producer_count)),
            logical_kind=f"boundary_{final_boundary_idx}",
        )
    )
    return boundary_domains, boundary_mappings_by_index


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


def _normalize_cxl_topology_config(
    config: Mapping[str, object],
    warn_defaults: bool,
    *,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
) -> CXLTopologyConfig:
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
    link_catalog = sources.LINKS if links_catalog is None else links_catalog
    mode = str(merged["mode"])
    if mode != "dynamic_striping":
        raise ValueError("cxl_topology.mode must be dynamic_striping")
    applies_to_links = tuple(str(value) for value in merged["applies_to_links"])
    for link_name in applies_to_links:
        if link_name not in link_catalog:
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
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
) -> Dict[str, Dict[str, object]]:
    link_catalog = sources.LINKS if links_catalog is None else links_catalog
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
        "memory_system_by_template",
        "pim_mode_by_stage_by_template",
        "pim_mode_effects",
        "validation",
    ]
    missing = [key for key in required_top_level if key not in config]
    if missing:
        raise KeyError(f"missing config keys: {missing}")

    legacy_flat_keys = [
        "scenario_stage_device_map",
        "pim_speedup_vs_cpu_by_stage",
        "cpu_stage_unit_compute_Bps",
        "enable_memory_ceiling_by_template",
        "dram_service_defaults",
        "cpu_mem_Bps_by_stage_by_template",
        "pim_mem_Bps_by_stage_by_template",
        "cpu_random_access_penalty_by_stage_by_template",
        "cpu_access_pattern_by_stage_by_template",
        "cpu_materialization_by_template",
    ]
    legacy_present = [key for key in legacy_flat_keys if key in config]
    if legacy_present:
        raise ValueError(
            "legacy keys are not supported; use memory_system_by_template instead. "
            f"Found: {legacy_present}"
        )

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
        if link_id not in link_catalog:
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
    _normalize_cxl_topology_config(config=config, warn_defaults=False, links_catalog=link_catalog)
    _normalize_ingress_resident_scenarios_by_template(
        config=config,
        template_to_stage_names=template_to_stage_names,
    )
    _normalize_tiling_model_by_template(
        config=config,
        template_to_stage_names=template_to_stage_names,
    )
    _normalize_pim_mode_by_stage_by_template(
        config=config,
        template_to_stage_names=template_to_stage_names,
    )
    _normalize_pim_mode_effects(config)

    if int(config["tile_size_bytes"]) <= 0:
        raise ValueError("tile_size_bytes must be > 0")
    if int(config["max_inflight_tiles"]) <= 0:
        raise ValueError("max_inflight_tiles must be > 0")

    validation_cfg = config["validation"]
    if not isinstance(validation_cfg, Mapping):
        raise ValueError("validation must be a map")
    if not str(validation_cfg.get("system_id", "")).strip():
        raise ValueError("validation.system_id is required")
    for key in ["calibration", "crosscheck", "sensitivity", "energy"]:
        if key not in validation_cfg or not isinstance(validation_cfg[key], Mapping):
            raise ValueError(f"validation.{key} must be a map")

    return memory_system_by_template


def _glue_bytes_touched(glue_type: str, bytes_in: float, bytes_out: float) -> float:
    if glue_type == sources.GLUE_REDUCE:
        return max(0.0, bytes_in + bytes_out)
    if glue_type == sources.GLUE_SHUFFLE:
        return max(0.0, bytes_in + bytes_out)
    return max(0.0, bytes_in + bytes_out)


def _glue_core_duration_s(
    *,
    spec: BoundaryMappingSpec,
    bytes_in: float,
    bytes_out: float,
) -> float:
    touched = _glue_bytes_touched(spec.glue_type, bytes_in, bytes_out)
    compute_s = touched / max(spec.glue_compute_Bps, 1e-12)
    mem_s = touched / max(spec.glue_mem_Bps, 1e-12)
    return spec.glue_fixed_s + max(compute_s, mem_s)


def _mapping_contributions_for_producer(
    *,
    spec: BoundaryMappingSpec,
    producer_tile_id: int,
    producer_tile_count: int,
    consumer_tile_count: int,
    producer_bytes_out: int,
    src_stage_cfg: StageConfig,
) -> List[Tuple[int, int]]:
    if spec.mapping_type == sources.MAPPING_IDENTITY:
        consumer_id = min(producer_tile_id, consumer_tile_count - 1)
        return [(consumer_id, int(producer_bytes_out))]

    if spec.mapping_type == sources.MAPPING_GROUP_K_TO_1:
        consumer_id = min(producer_tile_id // max(1, spec.group_k), consumer_tile_count - 1)
        return [(consumer_id, int(producer_bytes_out))]

    if spec.mapping_type == sources.MAPPING_SPLIT_1_TO_M:
        split_m = max(1, spec.split_m)
        split_bytes = tile_boundary_bytes(int(producer_bytes_out), split_m)
        out: List[Tuple[int, int]] = []
        base = producer_tile_id * split_m
        for idx, value in enumerate(split_bytes):
            consumer_id = min(base + idx, consumer_tile_count - 1)
            out.append((consumer_id, int(value)))
        return out

    if spec.mapping_type == sources.MAPPING_REPARTITION_HASH:
        amplified = int(round(float(producer_bytes_out) * spec.output_amplification))
        # v1 materialized shuffle barrier: aggregate once per producer, then release all partitions together.
        return [(0, int(amplified))]

    raise ValueError(f"unsupported mapping_type {spec.mapping_type}")


def _expected_contributions_per_consumer(
    *,
    spec: BoundaryMappingSpec,
    producer_tile_count: int,
    consumer_tile_count: int,
) -> List[int]:
    if spec.mapping_type == sources.MAPPING_IDENTITY:
        return [1 for _ in range(consumer_tile_count)]
    if spec.mapping_type == sources.MAPPING_GROUP_K_TO_1:
        out: List[int] = []
        group_k = max(1, spec.group_k)
        for consumer_id in range(consumer_tile_count):
            remaining = max(0, producer_tile_count - (consumer_id * group_k))
            out.append(max(1, min(group_k, remaining)))
        return out
    if spec.mapping_type == sources.MAPPING_SPLIT_1_TO_M:
        return [1 for _ in range(consumer_tile_count)]
    if spec.mapping_type == sources.MAPPING_REPARTITION_HASH:
        return [max(1, producer_tile_count) for _ in range(consumer_tile_count)]
    raise ValueError(f"unsupported mapping_type {spec.mapping_type}")


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


def _simulate_configuration_linear(
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
    pim_mode_by_stage_by_template: Mapping[str, Mapping[str, str]],
    pim_mode_effects: Mapping[str, PIMModeEffect],
    tiling_model_by_template: Mapping[str, TilingTemplateConfig],
    workload_family: str,
    workload_profile: str,
    workload_variant: str,
    baseline_id: str,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
    trace_max_tiles: int | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    link_catalog = sources.LINKS if links_catalog is None else links_catalog
    if scenario not in sources.SCENARIOS:
        raise ValueError(f"unknown scenario: {scenario}")
    if host_h2d_link not in link_catalog:
        raise ValueError(f"unknown host H2D link: {host_h2d_link}")
    if host_d2h_link not in link_catalog:
        raise ValueError(f"unknown host D2H link: {host_d2h_link}")
    if cxl_direct_link not in link_catalog:
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
    if pipeline_template not in pim_mode_by_stage_by_template:
        raise KeyError(f"missing pim_mode map for template {pipeline_template}")
    if pipeline_template not in tiling_model_by_template:
        raise KeyError(f"missing tiling model map for template {pipeline_template}")

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
    stage_pim_modes = pim_mode_by_stage_by_template[pipeline_template]
    stage_kernel_class = tiling_model_by_template[pipeline_template].stage_kernel_class
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
    cxl_link = link_catalog[cxl_direct_link]
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
    cxl_total_bw_Bps = float(cxl_link["bandwidth_Bps"]) * float(cxl_striping_factor)
    cxl_processor_scheduler = CXLProcessorShareScheduler(
        bw_total_Bps=cxl_total_bw_Bps,
        slots=cxl_direct_stream_slots,
    )
    cxl_direct_completion_heap: List[Tuple[float, int, int]] = []
    pending_direct_transfers: Dict[Tuple[int, int], Dict[str, object]] = {}
    active_direct_transfers: Dict[int, Dict[str, object]] = {}
    next_direct_transfer_id = 1

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
    total_pim_mode_command_overhead_s = 0.0

    while request_heap or cxl_direct_completion_heap:
        next_req_t = request_heap[0][0] if request_heap else math.inf
        next_direct_done_t = cxl_direct_completion_heap[0][0] if cxl_direct_completion_heap else math.inf

        if next_direct_done_t <= next_req_t:
            t_done, transfer_id, token = heapq.heappop(cxl_direct_completion_heap)
            transfer_info = active_direct_transfers.get(transfer_id)
            if transfer_info is None:
                continue
            valid, new_completion_events = cxl_processor_scheduler.complete_if_valid(
                transfer_id=transfer_id,
                token=token,
                at_t=t_done,
            )
            if not valid:
                continue
            for t_evt, tr_id, tr_token in new_completion_events:
                heapq.heappush(cxl_direct_completion_heap, (t_evt, tr_id, tr_token))

            direct_record = active_direct_transfers.pop(transfer_id)
            tile_id = int(direct_record["tile_id"])
            op_index = int(direct_record["op_index"])
            t_req = float(direct_record["t_req"])
            t_start = float(direct_record["t_start"])
            t_end = float(t_done)
            wait_s = max(0.0, t_start - t_req)
            duration_s = max(0.0, t_end - t_start)
            stage_id = int(direct_record["stage_id"])
            stage_device = str(direct_record["stage_device"])

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
                        "stage_id": stage_id,
                        "stage_name": stage_names[stage_id - 1],
                        "stage_device": stage_device,
                        "op_type": "TRANSFER",
                        "transfer_path": "cxl_direct",
                        "resource": cxl_direct_pool.name,
                        "resource_slot": -1,
                        "link_type": cxl_direct_link,
                        "bytes": int(direct_record["bytes"]),
                        "t_req": t_req,
                        "t_start": t_start,
                        "t_end": t_end,
                        "duration_s": duration_s,
                        "wait_s": wait_s,
                        "compute_component_s": 0.0,
                        "memory_component_s": 0.0,
                        "bytes_touched": 0.0,
                        "cpu_access_pattern": "",
                        "cpu_row_hit_rate": 0.0,
                        "cpu_mlp": 0.0,
                        "cpu_avg_miss_latency_ns": 0.0,
                        "cpu_bw_peak_Bps": 0.0,
                        "cpu_bw_latency_Bps": 0.0,
                        "cpu_bw_eff_stage_Bps": 0.0,
                        "cpu_bw_eff_per_unit_Bps": 0.0,
                        "cpu_mem_bound_mode": "",
                        "memory_ceiling_enabled": memory_system_enabled,
                        "memory_system_role": "",
                        "mem_service_Bps": 0.0,
                        "mem_queue_multiplier": 1.0,
                        "mem_rho": 0.0,
                        "mem_service_time_s": 0.0,
                        "mem_queue_delay_s": 0.0,
                        "cpu_baseline_engine": cpu_baseline_engine,
                        "stage_src_endpoint": str(direct_record["stage_src_endpoint"]),
                        "stage_dst_endpoint": str(direct_record["stage_dst_endpoint"]),
                        "handoff_mode": str(direct_record["handoff_mode"]),
                        "retention_capacity_blocked": bool(direct_record["retention_capacity_blocked"]),
                        "cxl_active_streams": int(direct_record["cxl_active_streams"]),
                        "cxl_bw_share_Bps": float(direct_record["cxl_bw_share_Bps"]),
                        "cxl_effective_bw_Bps": (
                            float(direct_record["bytes"]) / max(duration_s, 1e-12)
                            if duration_s > 0
                            else 0.0
                        ),
                        "cxl_issue_overhead_s": float(direct_record["cxl_issue_overhead_s"]),
                        "cxl_striping_factor": int(direct_record["cxl_striping_factor"]),
                        "domain_in_id": str(direct_record.get("domain_in_id", "")),
                        "domain_out_id": str(direct_record.get("domain_out_id", "")),
                        "domain_in_tile_id": int(direct_record.get("domain_in_tile_id", tile_id)),
                        "domain_out_tile_id": int(direct_record.get("domain_out_tile_id", tile_id)),
                        "mapping_type": str(direct_record.get("mapping_type", sources.MAPPING_IDENTITY)),
                        "mapping_id": str(direct_record.get("mapping_id", "")),
                        "kernel_class": str(direct_record.get("kernel_class", "")),
                        "glue_type": str(direct_record.get("glue_type", "")),
                        "barrier_dependency_wait_s": float(direct_record.get("barrier_dependency_wait_s", 0.0)),
                        "glue_queue_wait_s": float(direct_record.get("glue_queue_wait_s", 0.0)),
                        "barrier_total_wait_s": float(direct_record.get("barrier_total_wait_s", 0.0)),
                        "barrier_wait_s": float(direct_record.get("barrier_wait_s", 0.0)),
                        "aggregation_expected": int(direct_record.get("aggregation_expected", 0)),
                        "aggregation_received": int(direct_record.get("aggregation_received", 0)),
                        "pim_mode": str(direct_record.get("pim_mode", sources.PIM_MODE_NONE)),
                        "pim_mode_compute_multiplier": float(
                            direct_record.get("pim_mode_compute_multiplier", 1.0)
                        ),
                        "pim_mode_mem_multiplier": float(
                            direct_record.get("pim_mode_mem_multiplier", 1.0)
                        ),
                        "pim_mode_command_overhead_s": float(
                            direct_record.get("pim_mode_command_overhead_s", 0.0)
                        ),
                    }
                )

            next_op_index[tile_id] = op_index + 1
            completion_times[tile_id] = t_end
            if next_op_index[tile_id] < len(operations):
                heapq.heappush(request_heap, (t_end, tile_id))
            elif next_tile_to_admit < num_tiles:
                heapq.heappush(request_heap, (t_end, next_tile_to_admit))
                next_tile_to_admit += 1
            continue

        if not request_heap:
            break
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
        domain_in_id = f"d{operation.boundary_index}"
        domain_out_id = f"d{operation.boundary_index}"
        domain_in_tile_id = tile_id
        domain_out_tile_id = tile_id
        mapping_type = sources.MAPPING_IDENTITY
        kernel_class = stage_kernel_class.get(stage_names[operation.stage_id - 1], "")
        glue_type = ""
        barrier_wait_s = 0.0
        aggregation_expected = 0
        aggregation_received = 0
        pim_mode = sources.PIM_MODE_NONE
        pim_mode_compute_multiplier = 1.0
        pim_mode_mem_multiplier = 1.0
        pim_mode_command_overhead_s = 0.0

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
                pim_mode = stage_pim_modes.get(stage_name, sources.PIM_MODE_NONE)
                mode_effect = pim_mode_effects[pim_mode]
                pim_mode_compute_multiplier = mode_effect.compute_multiplier
                pim_mode_mem_multiplier = mode_effect.mem_multiplier
                pim_mode_command_overhead_s = mode_effect.command_overhead_s
                compute_rate = stage_cfg.pim_unit_compute_Bps * pim_mode_compute_multiplier
                stage_units = stage_cfg.pim_units
                memory_system_role = "pim_system"
                stage_service_cfg = pim_system_cfg.stages.get(stage_name) if pim_system_cfg.enabled else None
                if stage_service_cfg is not None and pim_mode_mem_multiplier != 1.0:
                    stage_service_cfg = StageMemoryServiceConfig(
                        access_pattern=stage_service_cfg.access_pattern,
                        row_hit_rate=stage_service_cfg.row_hit_rate,
                        mlp=stage_service_cfg.mlp,
                        avg_miss_latency_ns=stage_service_cfg.avg_miss_latency_ns,
                        peak_bw_Bps=stage_service_cfg.peak_bw_Bps * pim_mode_mem_multiplier,
                        penalty_multiplier=stage_service_cfg.penalty_multiplier,
                    )
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
            if stage_device == sources.DEVICE_PIM and pim_mode_command_overhead_s > 0:
                duration_s += pim_mode_command_overhead_s
                total_pim_mode_command_overhead_s += pim_mode_command_overhead_s
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
                    pending_key = (tile_id, op_index)
                    pending = pending_direct_transfers.get(pending_key)
                    if pending is None:
                        u_out = min(
                            1.0,
                            float(cxl_direct_concurrency.dma_outstanding_per_vc)
                            / float(cxl_direct_concurrency.full_bw_outstanding_threshold),
                        )
                        issue_overhead_s = cxl_direct_concurrency.dma_issue_fixed_s / max(u_out, 1e-6)
                        ready_t = t_req + float(cxl_link["latency_s"]) + issue_overhead_s
                        total_cxl_dma_issue_time_component_s += issue_overhead_s
                        pending = {
                            "tile_id": tile_id,
                            "op_index": op_index,
                            "stage_id": src_stage_id,
                            "stage_device": stage_devices[src_stage_id - 1],
                            "bytes": bytes_moved,
                            "stage_src_endpoint": src_endpoint,
                            "stage_dst_endpoint": dst_endpoint,
                            "handoff_mode": handoff_mode,
                            "retention_capacity_blocked": retention_capacity_blocked,
                            "t_req": t_req,
                            "ready_t": ready_t,
                            "cxl_issue_overhead_s": issue_overhead_s,
                            "cxl_striping_factor": cxl_striping_factor,
                        }
                        pending_direct_transfers[pending_key] = pending

                    admit_t = max(t_req, float(pending["ready_t"]))
                    active_before = cxl_processor_scheduler.active_count(at_t=admit_t)
                    if active_before >= cxl_direct_stream_slots:
                        retry_t = cxl_processor_scheduler.next_completion_time(at_t=admit_t)
                        if not math.isfinite(retry_t):
                            retry_t = admit_t
                        heapq.heappush(request_heap, (max(admit_t, retry_t), tile_id))
                        continue

                    transfer_id = next_direct_transfer_id
                    next_direct_transfer_id += 1
                    admitted, new_completion_events = cxl_processor_scheduler.try_admit(
                        transfer_id=transfer_id,
                        bytes_total=bytes_moved,
                        at_t=admit_t,
                    )
                    if not admitted:
                        retry_t = cxl_processor_scheduler.next_completion_time(at_t=admit_t)
                        if not math.isfinite(retry_t):
                            retry_t = admit_t
                        heapq.heappush(request_heap, (max(admit_t, retry_t), tile_id))
                        continue

                    cxl_active_streams = active_before + 1
                    cxl_bw_share_Bps = cxl_total_bw_Bps / float(max(1, cxl_active_streams))
                    cxl_issue_overhead_s = float(pending["cxl_issue_overhead_s"])
                    cxl_striping_factor_trace = int(pending["cxl_striping_factor"])
                    total_bytes_cxl_direct += bytes_moved

                    active_direct_transfers[transfer_id] = {
                        "tile_id": tile_id,
                        "op_index": op_index,
                        "stage_id": src_stage_id,
                        "stage_device": stage_devices[src_stage_id - 1],
                        "bytes": bytes_moved,
                        "stage_src_endpoint": src_endpoint,
                        "stage_dst_endpoint": dst_endpoint,
                        "handoff_mode": handoff_mode,
                        "retention_capacity_blocked": retention_capacity_blocked,
                        "t_req": float(pending["t_req"]),
                        "t_start": admit_t,
                        "cxl_active_streams": cxl_active_streams,
                        "cxl_bw_share_Bps": cxl_bw_share_Bps,
                        "cxl_issue_overhead_s": cxl_issue_overhead_s,
                        "cxl_striping_factor": cxl_striping_factor_trace,
                        "domain_in_id": domain_in_id,
                        "domain_out_id": domain_out_id,
                        "domain_in_tile_id": domain_in_tile_id,
                        "domain_out_tile_id": domain_out_tile_id,
                        "mapping_type": mapping_type,
                        "mapping_id": "",
                        "kernel_class": kernel_class,
                        "glue_type": glue_type,
                        "barrier_dependency_wait_s": 0.0,
                        "glue_queue_wait_s": 0.0,
                        "barrier_total_wait_s": barrier_wait_s,
                        "barrier_wait_s": barrier_wait_s,
                        "aggregation_expected": aggregation_expected,
                        "aggregation_received": aggregation_received,
                        "pim_mode": pim_mode,
                        "pim_mode_compute_multiplier": pim_mode_compute_multiplier,
                        "pim_mode_mem_multiplier": pim_mode_mem_multiplier,
                        "pim_mode_command_overhead_s": pim_mode_command_overhead_s,
                    }
                    pending_direct_transfers.pop(pending_key, None)
                    for t_evt, tr_id, tr_token in new_completion_events:
                        heapq.heappush(cxl_direct_completion_heap, (t_evt, tr_id, tr_token))
                    continue
                elif scenario == sources.SCENARIO_PIM_HOST_BOUNCE:
                    handoff_mode = "transfer_bounce"
                    d2h_duration = transfer_duration_s(
                        bytes_moved=bytes_moved,
                        link_type=host_d2h_link,
                        links_catalog=link_catalog,
                    )
                    touch_duration = host_touch_duration_s(
                        bytes_moved=bytes_moved,
                        touch_Bps=stage_cfg.host_touch_Bps,
                        touch_fixed_s=stage_cfg.host_touch_fixed_s,
                    )
                    h2d_duration = transfer_duration_s(
                        bytes_moved=bytes_moved,
                        link_type=host_h2d_link,
                        links_catalog=link_catalog,
                    )
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
                        "duration_s": transfer_duration_s(
                            bytes_moved=bytes_moved,
                            link_type=host_h2d_link,
                            links_catalog=link_catalog,
                        ),
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
                        "duration_s": transfer_duration_s(
                            bytes_moved=bytes_moved,
                            link_type=host_h2d_link,
                            links_catalog=link_catalog,
                        ),
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
                        "duration_s": transfer_duration_s(
                            bytes_moved=bytes_moved,
                            link_type=host_d2h_link,
                            links_catalog=link_catalog,
                        ),
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
                        "duration_s": transfer_duration_s(
                            bytes_moved=bytes_moved,
                            link_type=cxl_direct_link,
                            links_catalog=link_catalog,
                        ),
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
                        "cxl_effective_bw_Bps": (
                            (event_bytes / duration_s)
                            if event_op_type == "TRANSFER"
                            and transfer_path == "cxl_direct"
                            and duration_s > 0
                            else 0.0
                        ),
                        "cxl_issue_overhead_s": cxl_issue_overhead_s,
                        "cxl_striping_factor": cxl_striping_factor_trace,
                        "domain_in_id": domain_in_id,
                        "domain_out_id": domain_out_id,
                        "domain_in_tile_id": domain_in_tile_id,
                        "domain_out_tile_id": domain_out_tile_id,
                        "mapping_type": mapping_type,
                        "mapping_id": "",
                        "kernel_class": kernel_class,
                        "glue_type": glue_type,
                        "barrier_dependency_wait_s": 0.0,
                        "glue_queue_wait_s": 0.0,
                        "barrier_total_wait_s": barrier_wait_s,
                        "barrier_wait_s": barrier_wait_s,
                        "aggregation_expected": aggregation_expected,
                        "aggregation_received": aggregation_received,
                        "pim_mode": pim_mode,
                        "pim_mode_compute_multiplier": pim_mode_compute_multiplier,
                        "pim_mode_mem_multiplier": pim_mode_mem_multiplier,
                        "pim_mode_command_overhead_s": pim_mode_command_overhead_s,
                    }
                )

        next_op_index[tile_id] = op_index + 1
        completion_times[tile_id] = t_end
        if next_op_index[tile_id] < len(operations):
            heapq.heappush(request_heap, (t_end, tile_id))
        elif next_tile_to_admit < num_tiles:
            heapq.heappush(request_heap, (t_end, next_tile_to_admit))
            next_tile_to_admit += 1

    if active_direct_transfers:
        raise AssertionError("direct transfer state left active at simulation end")
    cxl_direct_pool.busy_time_s = cxl_processor_scheduler.busy_slot_time_s

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
        "cxl_bw_model": "processor_share",
        "retile_enabled": False,
        "num_tile_domains": num_stages + 1,
        "total_glue_copy_bytes": 0,
        "total_glue_reduce_bytes": 0,
        "total_glue_shuffle_bytes": 0,
        "total_glue_time_component_s": 0.0,
        "total_glue_transfer_time_component_s": 0.0,
        "total_barrier_dependency_wait_time_component_s": 0.0,
        "total_glue_queue_wait_time_component_s": 0.0,
        "total_barrier_wait_time_component_s": 0.0,
        "lb_glue_s": 0.0,
        "total_pim_mode_command_overhead_s": total_pim_mode_command_overhead_s,
        "mapping_ids_used": "",
    }
    return metrics_row, traces


def _simulate_configuration_retile(
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
    pim_mode_by_stage_by_template: Mapping[str, Mapping[str, str]],
    pim_mode_effects: Mapping[str, PIMModeEffect],
    tiling_model_by_template: Mapping[str, TilingTemplateConfig],
    workload_family: str,
    workload_profile: str,
    workload_variant: str,
    baseline_id: str,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
    trace_max_tiles: int | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    link_catalog = sources.LINKS if links_catalog is None else links_catalog
    num_stages = len(boundaries_bytes) - 1
    num_public_stages = len(public_stage_names)
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
    tiling_cfg = tiling_model_by_template[pipeline_template]
    boundary_domains, boundary_mappings = _build_tile_domains_and_mappings(
        scaled_boundaries=scaled_boundaries,
        stage_names=stage_names,
        tiling_cfg=tiling_cfg,
        stage_configs=stage_configs,
    )
    for boundary_idx, mapping in boundary_mappings.items():
        if tiling_cfg.glue_resource_mode == sources.GLUE_RESOURCE_MODE_SHARED_CONSUMER_COMPUTE:
            consumer_stage_id = boundary_idx + 1
            consumer_device = stage_devices[consumer_stage_id - 1]
            if mapping.glue_device != consumer_device:
                raise ValueError(
                    f"shared_consumer_compute requires glue_device={consumer_device} for "
                    f"mapping {mapping.transition_key} (mapping_id={mapping.mapping_id})"
                )
    stage_pim_modes = pim_mode_by_stage_by_template[pipeline_template]
    stage_kernel_class = tiling_cfg.stage_kernel_class
    bytes_touched_factors_by_stage = bytes_touched_factors_by_stage_by_template[pipeline_template]

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
    glue_cpu_pool = ResourcePool(
        name="glue_cpu",
        capacity=max(1, int(stage_defaults["cpu_units"])),
        power_W=float(stage_defaults["cpu_unit_power_W"]),
    )
    glue_pim_pool = ResourcePool(
        name="glue_pim",
        capacity=max(1, int(stage_defaults["pim_units"])),
        power_W=float(stage_defaults["pim_unit_power_W"]),
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
        cfg = stage_configs[stage_id - 1]
        if stage_devices[stage_id - 1] == sources.DEVICE_CPU:
            compute_pools.append(
                ResourcePool(
                    name=f"cpu_stage_{stage_id}",
                    capacity=cfg.cpu_units,
                    power_W=cfg.cpu_unit_power_W,
                )
            )
        else:
            compute_pools.append(
                ResourcePool(
                    name=f"pim_stage_{stage_id}",
                    capacity=cfg.pim_units,
                    power_W=cfg.pim_unit_power_W,
                )
            )

    # Topology/striping setup.
    active_direct_endpoints: set[str] = set()
    if scenario == sources.SCENARIO_PIM_FLOWCXL_DIRECT:
        for boundary_idx in range(1, num_stages):
            if stage_devices[boundary_idx - 1] == sources.DEVICE_PIM and stage_devices[boundary_idx] == sources.DEVICE_PIM:
                src_endpoint = stage_endpoints[boundary_idx - 1]
                dst_endpoint = stage_endpoints[boundary_idx]
                if src_endpoint != dst_endpoint:
                    active_direct_endpoints.add(src_endpoint)
                    active_direct_endpoints.add(dst_endpoint)
    active_direct_endpoint_count = len(active_direct_endpoints)
    cxl_striping_factor = 1
    cxl_link = link_catalog[cxl_direct_link]
    supports_dynamic_striping = bool(cxl_link.get("supports_dynamic_striping", False))
    if (
        cxl_topology.enabled
        and cxl_topology.mode == "dynamic_striping"
        and supports_dynamic_striping
        and cxl_direct_link in cxl_topology.applies_to_links
    ):
        cxl_striping_factor = min(
            cxl_topology.max_stripes,
            cxl_topology.num_physical_links,
            max(1, active_direct_endpoint_count),
        )
    cxl_total_bw_Bps = float(cxl_link["bandwidth_Bps"]) * float(cxl_striping_factor)
    cxl_processor_scheduler = CXLProcessorShareScheduler(
        bw_total_Bps=cxl_total_bw_Bps,
        slots=cxl_direct_stream_slots,
    )

    traces: List[Dict[str, object]] = []
    stage_work_heap: List[Tuple[float, int, int, int]] = []
    direct_request_heap: List[Tuple[float, int]] = []
    direct_completion_heap: List[Tuple[float, int, int]] = []
    pending_direct_transfers: Dict[int, Dict[str, object]] = {}
    active_direct_transfers: Dict[int, Dict[str, object]] = {}
    next_pending_direct_id = 1
    next_direct_transfer_id = 1
    heap_counter = 0

    num_input_tiles = boundary_domains[0].tile_count
    inflight_seed = min(max(1, int(max_inflight_tiles)), num_input_tiles)
    next_tile_to_admit = inflight_seed
    for tile_id in range(inflight_seed):
        heap_counter += 1
        heapq.heappush(stage_work_heap, (0.0, heap_counter, 1, tile_id))

    stage_output_bytes: Dict[int, List[int]] = {}
    for stage_id in range(1, num_stages + 1):
        producer_count = boundary_domains[stage_id - 1].tile_count
        stage_output_bytes[stage_id] = tile_boundary_bytes(
            total_bytes=scaled_boundaries[stage_id], num_tiles=producer_count
        )

    boundary_states: Dict[int, List[BoundaryAggregationState]] = {}
    repartition_states: Dict[int, RepartitionBarrierState] = {}
    for boundary_idx in range(1, num_stages):
        spec = boundary_mappings[boundary_idx]
        producer_count = boundary_domains[boundary_idx - 1].tile_count
        consumer_count = boundary_domains[boundary_idx].tile_count
        if spec.mapping_type == sources.MAPPING_REPARTITION_HASH:
            repartition_states[boundary_idx] = RepartitionBarrierState(producer_count=producer_count)
            boundary_states[boundary_idx] = []
        else:
            expected = _expected_contributions_per_consumer(
                spec=spec,
                producer_tile_count=producer_count,
                consumer_tile_count=consumer_count,
            )
            boundary_states[boundary_idx] = [
                BoundaryAggregationState(expected_count=expected[idx]) for idx in range(consumer_count)
            ]

    completed_terminal_items = 0
    makespan_s = 0.0
    cpu_baseline_engine = cpu_baseline_cfg.baseline_engine
    ingress_resident = scenario in set(ingress_resident_scenarios_by_template[pipeline_template])
    ingress_skip_seen: set[int] = set()

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
    total_pim_mode_command_overhead_s = 0.0
    total_glue_copy_bytes = 0
    total_glue_reduce_bytes = 0
    total_glue_shuffle_bytes = 0
    total_glue_time_component_s = 0.0
    total_glue_transfer_time_component_s = 0.0
    total_barrier_dependency_wait_time_component_s = 0.0
    total_glue_queue_wait_time_component_s = 0.0
    total_barrier_wait_time_component_s = 0.0
    mapping_ids_used = sorted({mapping.mapping_id for mapping in boundary_mappings.values()})

    def _trace_event(
        *,
        tile_id: int,
        stage_id: int,
        op_type: str,
        bytes_value: int,
        t_req: float,
        t_start: float,
        t_end: float,
        wait_s: float,
        duration_s: float,
        stage_name: str,
        stage_device: str,
        transfer_path: str = "",
        resource_name: str = "",
        resource_slot: int = -1,
        link_type: str = "",
        compute_component_s: float = 0.0,
        memory_component_s: float = 0.0,
        bytes_touched: float = 0.0,
        cpu_access_pattern: str = "",
        cpu_row_hit_rate: float = 0.0,
        cpu_mlp: float = 0.0,
        cpu_avg_miss_latency_ns: float = 0.0,
        cpu_bw_peak_Bps: float = 0.0,
        cpu_bw_latency_Bps: float = 0.0,
        cpu_bw_eff_stage_Bps: float = 0.0,
        cpu_bw_eff_per_unit_Bps: float = 0.0,
        cpu_mem_bound_mode: str = "",
        memory_system_role: str = "",
        mem_service_Bps: float = 0.0,
        mem_queue_multiplier: float = 1.0,
        mem_rho: float = 0.0,
        mem_service_time_s: float = 0.0,
        mem_queue_delay_s: float = 0.0,
        stage_src_endpoint: str = "",
        stage_dst_endpoint: str = "",
        handoff_mode: str = "",
        retention_capacity_blocked: bool = False,
        cxl_active_streams: int = 0,
        cxl_bw_share_Bps: float = 0.0,
        cxl_issue_overhead_s: float = 0.0,
        cxl_striping_factor_trace: int = 1,
        domain_in_id: str = "",
        domain_out_id: str = "",
        domain_in_tile_id: int = 0,
        domain_out_tile_id: int = 0,
        mapping_type: str = sources.MAPPING_IDENTITY,
        mapping_id: str = "",
        kernel_class: str = "",
        glue_type: str = "",
        barrier_dependency_wait_s: float = 0.0,
        glue_queue_wait_s: float = 0.0,
        barrier_total_wait_s: float = 0.0,
        barrier_wait_s: float = 0.0,
        aggregation_expected: int = 0,
        aggregation_received: int = 0,
        pim_mode: str = sources.PIM_MODE_NONE,
        pim_mode_compute_multiplier: float = 1.0,
        pim_mode_mem_multiplier: float = 1.0,
        pim_mode_command_overhead_s: float = 0.0,
    ) -> None:
        if trace_max_tiles is not None and tile_id >= trace_max_tiles:
            return
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
                "op_index": 1,
                "stage_id": stage_id,
                "stage_name": stage_name,
                "stage_device": stage_device,
                "op_type": op_type,
                "transfer_path": transfer_path,
                "resource": resource_name,
                "resource_slot": resource_slot,
                "link_type": link_type,
                "bytes": bytes_value,
                "t_req": t_req,
                "t_start": t_start,
                "t_end": t_end,
                "duration_s": duration_s,
                "wait_s": wait_s,
                "compute_component_s": compute_component_s,
                "memory_component_s": memory_component_s,
                "bytes_touched": bytes_touched,
                "cpu_access_pattern": cpu_access_pattern,
                "cpu_row_hit_rate": cpu_row_hit_rate,
                "cpu_mlp": cpu_mlp,
                "cpu_avg_miss_latency_ns": cpu_avg_miss_latency_ns,
                "cpu_bw_peak_Bps": cpu_bw_peak_Bps,
                "cpu_bw_latency_Bps": cpu_bw_latency_Bps,
                "cpu_bw_eff_stage_Bps": cpu_bw_eff_stage_Bps,
                "cpu_bw_eff_per_unit_Bps": cpu_bw_eff_per_unit_Bps,
                "cpu_mem_bound_mode": cpu_mem_bound_mode,
                "memory_ceiling_enabled": memory_system_enabled,
                "memory_system_role": memory_system_role,
                "mem_service_Bps": mem_service_Bps,
                "mem_queue_multiplier": mem_queue_multiplier,
                "mem_rho": mem_rho,
                "mem_service_time_s": mem_service_time_s,
                "mem_queue_delay_s": mem_queue_delay_s,
                "cpu_baseline_engine": cpu_baseline_engine,
                "stage_src_endpoint": stage_src_endpoint,
                "stage_dst_endpoint": stage_dst_endpoint,
                "handoff_mode": handoff_mode,
                "retention_capacity_blocked": retention_capacity_blocked,
                "cxl_active_streams": cxl_active_streams,
                "cxl_bw_share_Bps": cxl_bw_share_Bps,
                "cxl_effective_bw_Bps": (bytes_value / duration_s) if duration_s > 0 and transfer_path == "cxl_direct" else 0.0,
                "cxl_issue_overhead_s": cxl_issue_overhead_s,
                "cxl_striping_factor": cxl_striping_factor_trace,
                "domain_in_id": domain_in_id,
                "domain_out_id": domain_out_id,
                "domain_in_tile_id": domain_in_tile_id,
                "domain_out_tile_id": domain_out_tile_id,
                "mapping_type": mapping_type,
                "mapping_id": mapping_id,
                "kernel_class": kernel_class,
                "glue_type": glue_type,
                "barrier_dependency_wait_s": barrier_dependency_wait_s,
                "glue_queue_wait_s": glue_queue_wait_s,
                "barrier_total_wait_s": barrier_total_wait_s,
                "barrier_wait_s": barrier_wait_s if barrier_wait_s > 0.0 else barrier_total_wait_s,
                "aggregation_expected": aggregation_expected,
                "aggregation_received": aggregation_received,
                "pim_mode": pim_mode,
                "pim_mode_compute_multiplier": pim_mode_compute_multiplier,
                "pim_mode_mem_multiplier": pim_mode_mem_multiplier,
                "pim_mode_command_overhead_s": pim_mode_command_overhead_s,
            }
        )

    def _schedule_pool_event(
        *,
        pool: ResourcePool,
        t_req: float,
        duration_s: float,
    ) -> Tuple[float, float, float, int]:
        return pool.schedule(t_req=t_req, duration_s=duration_s)

    def _admit_next_input(admit_t: float) -> None:
        nonlocal heap_counter, next_tile_to_admit
        if next_tile_to_admit >= num_input_tiles:
            return
        heap_counter += 1
        heapq.heappush(stage_work_heap, (admit_t, heap_counter, 1, next_tile_to_admit))
        next_tile_to_admit += 1

    def _release_stage_consumer(stage_id: int, tile_id: int, release_t: float) -> None:
        nonlocal heap_counter
        heap_counter += 1
        heapq.heappush(stage_work_heap, (release_t, heap_counter, stage_id, tile_id))

    def _schedule_boundary_glue_and_release(
        *,
        boundary_idx: int,
        consumer_id: int,
        bytes_in_value: int,
        first_contrib_t: float,
        latest_contrib_t: float,
        aggregation_expected: int,
        aggregation_received: int,
        producer_tile_id: int,
        stage_id: int,
        stage_name: str,
        stage_device: str,
        mapping: BoundaryMappingSpec,
        kernel_class: str,
        pim_mode: str,
        pim_mode_compute_multiplier: float,
        pim_mode_mem_multiplier: float,
        pim_mode_command_overhead_s: float,
        domain_in_id: str,
        domain_out_id: str,
    ) -> None:
        nonlocal total_glue_time_component_s
        nonlocal total_glue_transfer_time_component_s
        nonlocal total_glue_copy_bytes, total_glue_reduce_bytes, total_glue_shuffle_bytes
        nonlocal total_barrier_dependency_wait_time_component_s
        nonlocal total_glue_queue_wait_time_component_s
        nonlocal total_barrier_wait_time_component_s

        barrier_dependency_wait_s = max(0.0, latest_contrib_t - first_contrib_t)
        total_barrier_dependency_wait_time_component_s += barrier_dependency_wait_s
        glue_req_t = latest_contrib_t
        bytes_out = int(boundary_domains[boundary_idx].tile_bytes[consumer_id])
        need_glue = (
            mapping.mapping_type != sources.MAPPING_IDENTITY
            or mapping.glue_fixed_s > 0.0
            or mapping.glue_transfer_path != "none"
        )
        glue_end_t = glue_req_t
        glue_queue_wait_s = 0.0
        barrier_total_wait_s = barrier_dependency_wait_s
        if need_glue:
            if tiling_cfg.glue_resource_mode == sources.GLUE_RESOURCE_MODE_SHARED_CONSUMER_COMPUTE:
                glue_pool = compute_pools[boundary_idx]
            else:
                glue_pool = glue_pim_pool if mapping.glue_device == sources.DEVICE_PIM else glue_cpu_pool
            glue_core_duration = _glue_core_duration_s(
                spec=mapping,
                bytes_in=float(bytes_in_value),
                bytes_out=float(bytes_out),
            )
            t_glue_start, t_glue_end, glue_wait_s, glue_slot = _schedule_pool_event(
                pool=glue_pool,
                t_req=glue_req_t,
                duration_s=glue_core_duration,
            )
            glue_end_t = t_glue_end
            total_glue_time_component_s += glue_core_duration
            if mapping.glue_type == sources.GLUE_COPY:
                total_glue_copy_bytes += int(bytes_in_value)
            elif mapping.glue_type == sources.GLUE_REDUCE:
                total_glue_reduce_bytes += int(bytes_in_value)
            elif mapping.glue_type == sources.GLUE_SHUFFLE:
                total_glue_shuffle_bytes += int(bytes_in_value)
            glue_queue_wait_s = max(0.0, t_glue_start - glue_req_t)
            total_glue_queue_wait_time_component_s += glue_queue_wait_s
            barrier_total_wait_s = barrier_dependency_wait_s + glue_queue_wait_s
            _trace_event(
                tile_id=consumer_id,
                stage_id=stage_id,
                op_type=mapping.glue_type,
                bytes_value=int(bytes_in_value),
                t_req=glue_req_t,
                t_start=t_glue_start,
                t_end=t_glue_end,
                wait_s=glue_wait_s,
                duration_s=glue_core_duration,
                stage_name=stage_name,
                stage_device=mapping.glue_device,
                transfer_path="",
                resource_name=glue_pool.name,
                resource_slot=glue_slot,
                domain_in_id=domain_in_id,
                domain_out_id=domain_out_id,
                domain_in_tile_id=producer_tile_id,
                domain_out_tile_id=consumer_id,
                mapping_type=mapping.mapping_type,
                mapping_id=mapping.mapping_id,
                kernel_class=kernel_class,
                glue_type=mapping.glue_type,
                barrier_dependency_wait_s=barrier_dependency_wait_s,
                glue_queue_wait_s=glue_queue_wait_s,
                barrier_total_wait_s=barrier_total_wait_s,
                barrier_wait_s=barrier_total_wait_s,
                aggregation_expected=aggregation_expected,
                aggregation_received=aggregation_received,
                pim_mode=pim_mode,
                pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                pim_mode_command_overhead_s=pim_mode_command_overhead_s,
            )

            if mapping.glue_transfer_path != "none":
                if mapping.glue_transfer_path == "host_h2d_stage":
                    transfer_pool = host_h2d_stage_pool
                    transfer_link = host_h2d_link
                elif mapping.glue_transfer_path == "host_d2h":
                    transfer_pool = host_d2h_pool
                    transfer_link = host_d2h_link
                elif mapping.glue_transfer_path == "cxl_direct":
                    transfer_pool = cxl_direct_pool
                    transfer_link = cxl_direct_link
                else:
                    raise ValueError(f"unsupported glue_transfer_path {mapping.glue_transfer_path}")
                transfer_duration = transfer_duration_s(
                    bytes_moved=bytes_out,
                    link_type=transfer_link,
                    links_catalog=link_catalog,
                )
                t_tr_start, t_tr_end, tr_wait_s, tr_slot = _schedule_pool_event(
                    pool=transfer_pool,
                    t_req=glue_end_t,
                    duration_s=transfer_duration,
                )
                glue_end_t = t_tr_end
                total_glue_transfer_time_component_s += transfer_duration
                _trace_event(
                    tile_id=consumer_id,
                    stage_id=stage_id,
                    op_type="TRANSFER",
                    bytes_value=bytes_out,
                    t_req=t_glue_end,
                    t_start=t_tr_start,
                    t_end=t_tr_end,
                    wait_s=tr_wait_s,
                    duration_s=transfer_duration,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    transfer_path=mapping.glue_transfer_path,
                    resource_name=transfer_pool.name,
                    resource_slot=tr_slot,
                    link_type=transfer_link,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                    domain_in_tile_id=producer_tile_id,
                    domain_out_tile_id=consumer_id,
                    mapping_type=mapping.mapping_type,
                    mapping_id=mapping.mapping_id,
                    kernel_class=kernel_class,
                    glue_type=mapping.glue_type,
                    barrier_dependency_wait_s=0.0,
                    glue_queue_wait_s=0.0,
                    barrier_total_wait_s=0.0,
                    barrier_wait_s=0.0,
                    aggregation_expected=aggregation_expected,
                    aggregation_received=aggregation_received,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
        else:
            barrier_total_wait_s = barrier_dependency_wait_s

        total_barrier_wait_time_component_s += barrier_total_wait_s
        _release_stage_consumer(stage_id=boundary_idx + 1, tile_id=consumer_id, release_t=glue_end_t)

    def _handle_boundary_arrival(
        *,
        boundary_idx: int,
        consumer_id: int,
        bytes_in_value: int,
        t_arrival: float,
        producer_tile_id: int,
        stage_id: int,
        stage_name: str,
        stage_device: str,
        mapping: BoundaryMappingSpec,
        kernel_class: str,
        pim_mode: str,
        pim_mode_compute_multiplier: float,
        pim_mode_mem_multiplier: float,
        pim_mode_command_overhead_s: float,
        domain_in_id: str,
        domain_out_id: str,
    ) -> None:
        if mapping.mapping_type == sources.MAPPING_REPARTITION_HASH:
            barrier_state = repartition_states[boundary_idx]
            barrier_state.received_producer_count += 1
            barrier_state.total_bytes_in += float(bytes_in_value)
            barrier_state.first_contrib_t = min(barrier_state.first_contrib_t, t_arrival)
            barrier_state.latest_contrib_t = max(barrier_state.latest_contrib_t, t_arrival)
            if barrier_state.received_producer_count < barrier_state.producer_count:
                return
            consumer_count = boundary_domains[boundary_idx].tile_count
            consumer_bytes = tile_boundary_bytes(
                int(round(barrier_state.total_bytes_in)),
                consumer_count,
            )
            for consumer_idx, consumer_bytes_in in enumerate(consumer_bytes):
                _schedule_boundary_glue_and_release(
                    boundary_idx=boundary_idx,
                    consumer_id=consumer_idx,
                    bytes_in_value=int(consumer_bytes_in),
                    first_contrib_t=barrier_state.first_contrib_t,
                    latest_contrib_t=barrier_state.latest_contrib_t,
                    aggregation_expected=barrier_state.producer_count,
                    aggregation_received=barrier_state.received_producer_count,
                    producer_tile_id=-1,
                    stage_id=stage_id,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    mapping=mapping,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                )
            return

        state = boundary_states[boundary_idx][consumer_id]
        state.received_count += 1
        state.bytes_in += float(bytes_in_value)
        state.first_contrib_t = min(state.first_contrib_t, t_arrival)
        state.latest_contrib_t = max(state.latest_contrib_t, t_arrival)
        if state.received_count < state.expected_count:
            return
        _schedule_boundary_glue_and_release(
            boundary_idx=boundary_idx,
            consumer_id=consumer_id,
            bytes_in_value=int(round(state.bytes_in)),
            first_contrib_t=state.first_contrib_t,
            latest_contrib_t=state.latest_contrib_t,
            aggregation_expected=state.expected_count,
            aggregation_received=state.received_count,
            producer_tile_id=producer_tile_id,
            stage_id=stage_id,
            stage_name=stage_name,
            stage_device=stage_device,
            mapping=mapping,
            kernel_class=kernel_class,
            pim_mode=pim_mode,
            pim_mode_compute_multiplier=pim_mode_compute_multiplier,
            pim_mode_mem_multiplier=pim_mode_mem_multiplier,
            pim_mode_command_overhead_s=pim_mode_command_overhead_s,
            domain_in_id=domain_in_id,
            domain_out_id=domain_out_id,
        )

    while stage_work_heap or direct_request_heap or direct_completion_heap:
        next_stage_t = stage_work_heap[0][0] if stage_work_heap else math.inf
        next_req_t = direct_request_heap[0][0] if direct_request_heap else math.inf
        next_done_t = direct_completion_heap[0][0] if direct_completion_heap else math.inf

        if next_done_t <= next_req_t and next_done_t <= next_stage_t:
            t_done, transfer_id, token = heapq.heappop(direct_completion_heap)
            transfer_info = active_direct_transfers.get(transfer_id)
            if transfer_info is None:
                continue
            valid, rescheduled = cxl_processor_scheduler.complete_if_valid(
                transfer_id=transfer_id, token=token, at_t=t_done
            )
            if not valid:
                continue
            for t_evt, tr_id, tr_token in rescheduled:
                heapq.heappush(direct_completion_heap, (t_evt, tr_id, tr_token))
            active_direct_transfers.pop(transfer_id, None)
            t_req = float(transfer_info["t_req"])
            t_start = float(transfer_info["t_start"])
            duration_s = max(0.0, float(t_done) - t_start)
            _trace_event(
                tile_id=int(transfer_info["producer_tile_id"]),
                stage_id=int(transfer_info["stage_id"]),
                op_type="TRANSFER",
                bytes_value=int(transfer_info["bytes"]),
                t_req=t_req,
                t_start=t_start,
                t_end=float(t_done),
                wait_s=max(0.0, t_start - t_req),
                duration_s=duration_s,
                stage_name=str(transfer_info["stage_name"]),
                stage_device=str(transfer_info["stage_device"]),
                transfer_path="cxl_direct",
                resource_name=cxl_direct_pool.name,
                resource_slot=-1,
                link_type=cxl_direct_link,
                stage_src_endpoint=str(transfer_info["stage_src_endpoint"]),
                stage_dst_endpoint=str(transfer_info["stage_dst_endpoint"]),
                handoff_mode="transfer_direct",
                retention_capacity_blocked=bool(transfer_info["retention_capacity_blocked"]),
                cxl_active_streams=int(transfer_info["cxl_active_streams"]),
                cxl_bw_share_Bps=float(transfer_info["cxl_bw_share_Bps"]),
                cxl_issue_overhead_s=float(transfer_info["cxl_issue_overhead_s"]),
                cxl_striping_factor_trace=int(transfer_info["cxl_striping_factor"]),
                domain_in_id=str(transfer_info["domain_in_id"]),
                domain_out_id=str(transfer_info["domain_out_id"]),
                domain_in_tile_id=int(transfer_info["domain_in_tile_id"]),
                domain_out_tile_id=int(transfer_info["domain_out_tile_id"]),
                mapping_type=str(transfer_info["mapping_type"]),
                mapping_id=str(transfer_info.get("mapping_id", "")),
                kernel_class=str(transfer_info["kernel_class"]),
                glue_type=str(transfer_info["glue_type"]),
                barrier_dependency_wait_s=float(transfer_info.get("barrier_dependency_wait_s", 0.0)),
                glue_queue_wait_s=float(transfer_info.get("glue_queue_wait_s", 0.0)),
                barrier_total_wait_s=float(transfer_info.get("barrier_total_wait_s", 0.0)),
                barrier_wait_s=float(transfer_info["barrier_wait_s"]),
                aggregation_expected=int(transfer_info["aggregation_expected"]),
                aggregation_received=int(transfer_info["aggregation_received"]),
                pim_mode=str(transfer_info["pim_mode"]),
                pim_mode_compute_multiplier=float(transfer_info["pim_mode_compute_multiplier"]),
                pim_mode_mem_multiplier=float(transfer_info["pim_mode_mem_multiplier"]),
                pim_mode_command_overhead_s=float(transfer_info["pim_mode_command_overhead_s"]),
            )
            _handle_boundary_arrival(
                boundary_idx=int(transfer_info["boundary_idx"]),
                consumer_id=int(transfer_info["consumer_id"]),
                bytes_in_value=int(transfer_info["bytes"]),
                t_arrival=float(t_done),
                producer_tile_id=int(transfer_info["producer_tile_id"]),
                stage_id=int(transfer_info["stage_id"]),
                stage_name=str(transfer_info["stage_name"]),
                stage_device=str(transfer_info["stage_device"]),
                mapping=boundary_mappings[int(transfer_info["boundary_idx"])],
                kernel_class=str(transfer_info["kernel_class"]),
                pim_mode=str(transfer_info["pim_mode"]),
                pim_mode_compute_multiplier=float(transfer_info["pim_mode_compute_multiplier"]),
                pim_mode_mem_multiplier=float(transfer_info["pim_mode_mem_multiplier"]),
                pim_mode_command_overhead_s=float(transfer_info["pim_mode_command_overhead_s"]),
                domain_in_id=str(transfer_info["domain_in_id"]),
                domain_out_id=str(transfer_info["domain_out_id"]),
            )
            continue

        if next_req_t <= next_stage_t:
            t_ready, pending_id = heapq.heappop(direct_request_heap)
            pending = pending_direct_transfers.get(pending_id)
            if pending is None:
                continue
            admit_t = max(float(t_ready), float(pending["ready_t"]))
            active_before = cxl_processor_scheduler.active_count(at_t=admit_t)
            if active_before >= cxl_direct_stream_slots:
                retry_t = cxl_processor_scheduler.next_completion_time(at_t=admit_t)
                if not math.isfinite(retry_t):
                    retry_t = admit_t
                heapq.heappush(direct_request_heap, (max(admit_t, retry_t), pending_id))
                continue
            transfer_id = next_direct_transfer_id
            next_direct_transfer_id += 1
            admitted, rescheduled = cxl_processor_scheduler.try_admit(
                transfer_id=transfer_id,
                bytes_total=int(pending["bytes"]),
                at_t=admit_t,
            )
            if not admitted:
                retry_t = cxl_processor_scheduler.next_completion_time(at_t=admit_t)
                if not math.isfinite(retry_t):
                    retry_t = admit_t
                heapq.heappush(direct_request_heap, (max(admit_t, retry_t), pending_id))
                continue
            for t_evt, tr_id, tr_token in rescheduled:
                heapq.heappush(direct_completion_heap, (t_evt, tr_id, tr_token))
            active_direct_transfers[transfer_id] = {
                **pending,
                "t_start": admit_t,
                "cxl_active_streams": active_before + 1,
                "cxl_bw_share_Bps": cxl_total_bw_Bps / float(max(1, active_before + 1)),
            }
            pending_direct_transfers.pop(pending_id, None)
            total_bytes_cxl_direct += int(pending["bytes"])
            continue

        if not stage_work_heap:
            break

        t_req, _, stage_id, tile_id = heapq.heappop(stage_work_heap)
        stage_name = stage_names[stage_id - 1]
        stage_device = stage_devices[stage_id - 1]
        kernel_class = stage_kernel_class.get(stage_name, _default_kernel_class_for_stage(stage_name))
        bytes_in = int(boundary_domains[stage_id - 1].tile_bytes[tile_id])
        bytes_out = int(stage_output_bytes[stage_id][tile_id])
        stage_cfg = stage_configs[stage_id - 1]
        stage_src_endpoint = stage_endpoints[stage_id - 1]

        t_cursor = float(t_req)

        if stage_id == 1 and stage_device == sources.DEVICE_PIM:
            if not (ingress_resident and tile_id not in ingress_skip_seen):
                ingress_duration = transfer_duration_s(
                    bytes_moved=bytes_in,
                    link_type=host_h2d_link,
                    links_catalog=link_catalog,
                )
                t_start, t_end, wait_s, slot = _schedule_pool_event(
                    pool=host_h2d_ingress_pool,
                    t_req=t_cursor,
                    duration_s=ingress_duration,
                )
                total_bytes_host_link += bytes_in
                total_bytes_host_h2d_ingress += bytes_in
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id,
                    op_type="TRANSFER",
                    bytes_value=bytes_in,
                    t_req=t_cursor,
                    t_start=t_start,
                    t_end=t_end,
                    wait_s=wait_s,
                    duration_s=ingress_duration,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    transfer_path="host_h2d_ingress",
                    resource_name=host_h2d_ingress_pool.name,
                    resource_slot=slot,
                    link_type=host_h2d_link,
                    stage_src_endpoint="host0",
                    stage_dst_endpoint=stage_src_endpoint,
                    domain_in_id=boundary_domains[0].domain_id,
                    domain_out_id=boundary_domains[0].domain_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=tile_id,
                    mapping_type=sources.MAPPING_IDENTITY,
                    kernel_class=kernel_class,
                )
                t_cursor = t_end
            else:
                ingress_skip_seen.add(tile_id)

        factors = bytes_touched_factors_by_stage[stage_name]
        compute_rate = stage_cfg.cpu_unit_compute_Bps if stage_device == sources.DEVICE_CPU else stage_cfg.pim_unit_compute_Bps
        stage_units = stage_cfg.cpu_units if stage_device == sources.DEVICE_CPU else stage_cfg.pim_units
        memory_system_role = "cpu_baseline" if stage_device == sources.DEVICE_CPU else "pim_system"
        stage_service_cfg = (
            cpu_baseline_cfg.stages.get(stage_name)
            if stage_device == sources.DEVICE_CPU
            else (pim_system_cfg.stages.get(stage_name) if pim_system_cfg.enabled else None)
        )
        cacheline_bytes = cpu_baseline_cfg.cacheline_bytes if stage_device == sources.DEVICE_CPU else pim_system_cfg.cacheline_bytes
        queueing_model = cpu_baseline_cfg.queueing_model if stage_device == sources.DEVICE_CPU else pim_system_cfg.queueing_model
        queue_alpha = cpu_baseline_cfg.queue_alpha if stage_device == sources.DEVICE_CPU else pim_system_cfg.queue_alpha
        rho_cap = cpu_baseline_cfg.rho_cap if stage_device == sources.DEVICE_CPU else pim_system_cfg.rho_cap
        pim_mode = stage_pim_modes.get(stage_name, sources.PIM_MODE_NONE)
        mode_effect = pim_mode_effects[pim_mode]
        pim_mode_compute_multiplier = mode_effect.compute_multiplier
        pim_mode_mem_multiplier = mode_effect.mem_multiplier
        pim_mode_command_overhead_s = mode_effect.command_overhead_s if stage_device == sources.DEVICE_PIM else 0.0
        if stage_device == sources.DEVICE_PIM:
            compute_rate *= pim_mode_compute_multiplier
            if stage_service_cfg is not None:
                stage_service_cfg = StageMemoryServiceConfig(
                    access_pattern=stage_service_cfg.access_pattern,
                    row_hit_rate=stage_service_cfg.row_hit_rate,
                    mlp=stage_service_cfg.mlp,
                    avg_miss_latency_ns=stage_service_cfg.avg_miss_latency_ns,
                    peak_bw_Bps=stage_service_cfg.peak_bw_Bps * pim_mode_mem_multiplier,
                    penalty_multiplier=stage_service_cfg.penalty_multiplier,
                )

        compute_component_s = compute_duration_s(bytes_moved=bytes_in, compute_rate_Bps=compute_rate)
        bytes_touched = compute_bytes_touched(
            bytes_in=bytes_in,
            bytes_out=bytes_out,
            input_factor=float(factors["input_factor"]),
            output_factor=float(factors["output_factor"]),
            amplification_factor=float(factors["amplification_factor"]),
        )
        memory_component_s = 0.0
        mem_service_time_s = 0.0
        mem_queue_delay_s = 0.0
        mem_service_Bps = 0.0
        mem_queue_multiplier = 1.0
        mem_rho = 0.0
        cpu_access_pattern = ""
        cpu_row_hit_rate = 0.0
        cpu_mlp = 0.0
        cpu_avg_miss_latency_ns = 0.0
        cpu_bw_peak_Bps = 0.0
        cpu_bw_latency_Bps = 0.0
        cpu_bw_eff_stage_Bps = 0.0
        cpu_bw_eff_per_unit_Bps = 0.0
        cpu_mem_bound_mode = ""

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

        duration_s = max(compute_component_s, memory_component_s)
        if stage_device == sources.DEVICE_PIM and pim_mode_command_overhead_s > 0:
            duration_s += pim_mode_command_overhead_s
            total_pim_mode_command_overhead_s += pim_mode_command_overhead_s
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

        t_start, t_end, wait_s, slot = _schedule_pool_event(
            pool=compute_pools[stage_id - 1],
            t_req=t_cursor,
            duration_s=duration_s,
        )
        _trace_event(
            tile_id=tile_id,
            stage_id=stage_id,
            op_type="COMPUTE",
            bytes_value=bytes_in,
            t_req=t_cursor,
            t_start=t_start,
            t_end=t_end,
            wait_s=wait_s,
            duration_s=duration_s,
            stage_name=stage_name,
            stage_device=stage_device,
            transfer_path="",
            resource_name=compute_pools[stage_id - 1].name,
            resource_slot=slot,
            compute_component_s=compute_component_s,
            memory_component_s=memory_component_s,
            bytes_touched=bytes_touched,
            cpu_access_pattern=cpu_access_pattern,
            cpu_row_hit_rate=cpu_row_hit_rate,
            cpu_mlp=cpu_mlp,
            cpu_avg_miss_latency_ns=cpu_avg_miss_latency_ns,
            cpu_bw_peak_Bps=cpu_bw_peak_Bps,
            cpu_bw_latency_Bps=cpu_bw_latency_Bps,
            cpu_bw_eff_stage_Bps=cpu_bw_eff_stage_Bps,
            cpu_bw_eff_per_unit_Bps=cpu_bw_eff_per_unit_Bps,
            cpu_mem_bound_mode=cpu_mem_bound_mode,
            memory_system_role=memory_system_role,
            mem_service_Bps=mem_service_Bps,
            mem_queue_multiplier=mem_queue_multiplier,
            mem_rho=mem_rho,
            mem_service_time_s=mem_service_time_s,
            mem_queue_delay_s=mem_queue_delay_s,
            stage_src_endpoint=stage_src_endpoint,
            stage_dst_endpoint=stage_src_endpoint,
            domain_in_id=boundary_domains[stage_id - 1].domain_id,
            domain_out_id=boundary_domains[stage_id].domain_id,
            domain_in_tile_id=tile_id,
            domain_out_tile_id=tile_id,
            mapping_type=sources.MAPPING_IDENTITY,
            kernel_class=kernel_class,
            pim_mode=pim_mode,
            pim_mode_compute_multiplier=pim_mode_compute_multiplier,
            pim_mode_mem_multiplier=pim_mode_mem_multiplier,
            pim_mode_command_overhead_s=pim_mode_command_overhead_s,
        )
        t_cursor = t_end

        if (
            scenario in set(cpu_baseline_cfg.materialization_policy.scenarios)
            and stage_id in set(cpu_baseline_cfg.materialization_policy.boundaries_by_engine.get(cpu_baseline_engine, []))
            and stage_id < num_stages
        ):
            mat_duration = materialize_duration_s(
                bytes_moved=bytes_out,
                materialize_Bps=cpu_baseline_cfg.materialization_policy.materialize_Bps,
                fixed_s=cpu_baseline_cfg.materialization_policy.fixed_s,
            )
            t_m_start, t_m_end, m_wait_s, m_slot = _schedule_pool_event(
                pool=cpu_materialize_pool,
                t_req=t_cursor,
                duration_s=mat_duration,
            )
            total_cpu_materialize_bytes += bytes_out
            total_cpu_materialize_time_component_s += mat_duration
            _trace_event(
                tile_id=tile_id,
                stage_id=stage_id,
                op_type="MATERIALIZE",
                bytes_value=bytes_out,
                t_req=t_cursor,
                t_start=t_m_start,
                t_end=t_m_end,
                wait_s=m_wait_s,
                duration_s=mat_duration,
                stage_name=stage_name,
                stage_device=stage_device,
                transfer_path="cpu_materialize",
                resource_name=cpu_materialize_pool.name,
                resource_slot=m_slot,
                stage_src_endpoint=stage_src_endpoint,
                stage_dst_endpoint=stage_src_endpoint,
                domain_in_id=boundary_domains[stage_id].domain_id,
                domain_out_id=boundary_domains[stage_id].domain_id,
                domain_in_tile_id=tile_id,
                domain_out_tile_id=tile_id,
                mapping_type=sources.MAPPING_IDENTITY,
                kernel_class=kernel_class,
                pim_mode=pim_mode,
                pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                pim_mode_command_overhead_s=pim_mode_command_overhead_s,
            )
            t_cursor = t_m_end

        if stage_id == num_stages:
            terminal_end = t_cursor
            if stage_device == sources.DEVICE_PIM:
                d2h_duration = transfer_duration_s(
                    bytes_moved=bytes_out,
                    link_type=host_d2h_link,
                    links_catalog=link_catalog,
                )
                t_tr_start, t_tr_end, tr_wait_s, tr_slot = _schedule_pool_event(
                    pool=host_d2h_pool,
                    t_req=t_cursor,
                    duration_s=d2h_duration,
                )
                total_bytes_host_link += bytes_out
                total_bytes_host_d2h += bytes_out
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id,
                    op_type="TRANSFER",
                    bytes_value=bytes_out,
                    t_req=t_cursor,
                    t_start=t_tr_start,
                    t_end=t_tr_end,
                    wait_s=tr_wait_s,
                    duration_s=d2h_duration,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    transfer_path="host_d2h",
                    resource_name=host_d2h_pool.name,
                    resource_slot=tr_slot,
                    link_type=host_d2h_link,
                    stage_src_endpoint=stage_src_endpoint,
                    stage_dst_endpoint="host0",
                    domain_in_id=boundary_domains[stage_id].domain_id,
                    domain_out_id=boundary_domains[stage_id].domain_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=tile_id,
                    mapping_type=sources.MAPPING_IDENTITY,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
                terminal_end = t_tr_end
            makespan_s = max(makespan_s, terminal_end)
            completed_terminal_items += 1
            if tiling_cfg.admission_refill_policy == "pipeline_complete":
                _admit_next_input(terminal_end)
            continue

        boundary_idx = stage_id
        mapping = boundary_mappings[boundary_idx]
        src_device = stage_devices[stage_id - 1]
        dst_device = stage_devices[stage_id]
        src_endpoint = stage_endpoints[stage_id - 1]
        dst_endpoint = stage_endpoints[stage_id]
        domain_in_id = boundary_domains[boundary_idx - 1].domain_id
        domain_out_id = boundary_domains[boundary_idx].domain_id
        contributions = _mapping_contributions_for_producer(
            spec=mapping,
            producer_tile_id=tile_id,
            producer_tile_count=boundary_domains[boundary_idx - 1].tile_count,
            consumer_tile_count=boundary_domains[boundary_idx].tile_count,
            producer_bytes_out=bytes_out,
            src_stage_cfg=stage_cfg,
        )

        for consumer_id, contribution_bytes in contributions:
            t_contrib_req = t_cursor
            if src_device == sources.DEVICE_CPU and dst_device == sources.DEVICE_CPU:
                _handle_boundary_arrival(
                    boundary_idx=boundary_idx,
                    consumer_id=consumer_id,
                    bytes_in_value=contribution_bytes,
                    t_arrival=t_contrib_req,
                    producer_tile_id=tile_id,
                    stage_id=stage_id,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    mapping=mapping,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                )
                continue

            if src_device == sources.DEVICE_CPU and dst_device == sources.DEVICE_PIM:
                skip_first = ingress_resident and tile_id not in ingress_skip_seen
                if skip_first:
                    ingress_skip_seen.add(tile_id)
                    _handle_boundary_arrival(
                        boundary_idx=boundary_idx,
                        consumer_id=consumer_id,
                        bytes_in_value=contribution_bytes,
                        t_arrival=t_contrib_req,
                        producer_tile_id=tile_id,
                        stage_id=stage_id,
                        stage_name=stage_name,
                        stage_device=stage_device,
                        mapping=mapping,
                        kernel_class=kernel_class,
                        pim_mode=pim_mode,
                        pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                        pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                        pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                        domain_in_id=domain_in_id,
                        domain_out_id=domain_out_id,
                    )
                    continue
                h2d_duration = transfer_duration_s(
                    bytes_moved=contribution_bytes,
                    link_type=host_h2d_link,
                    links_catalog=link_catalog,
                )
                t_tr_start, t_tr_end, tr_wait_s, tr_slot = _schedule_pool_event(
                    pool=host_h2d_stage_pool,
                    t_req=t_contrib_req,
                    duration_s=h2d_duration,
                )
                total_bytes_host_link += contribution_bytes
                total_bytes_host_h2d_stage += contribution_bytes
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id + 1,
                    op_type="TRANSFER",
                    bytes_value=contribution_bytes,
                    t_req=t_contrib_req,
                    t_start=t_tr_start,
                    t_end=t_tr_end,
                    wait_s=tr_wait_s,
                    duration_s=h2d_duration,
                    stage_name=stage_names[stage_id],
                    stage_device=dst_device,
                    transfer_path="host_h2d_stage",
                    resource_name=host_h2d_stage_pool.name,
                    resource_slot=tr_slot,
                    link_type=host_h2d_link,
                    stage_src_endpoint="host0",
                    stage_dst_endpoint=dst_endpoint,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=consumer_id,
                    mapping_type=mapping.mapping_type,
                    mapping_id=mapping.mapping_id,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
                _handle_boundary_arrival(
                    boundary_idx=boundary_idx,
                    consumer_id=consumer_id,
                    bytes_in_value=contribution_bytes,
                    t_arrival=t_tr_end,
                    producer_tile_id=tile_id,
                    stage_id=stage_id,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    mapping=mapping,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                )
                continue

            if src_device == sources.DEVICE_PIM and dst_device == sources.DEVICE_CPU:
                d2h_duration = transfer_duration_s(
                    bytes_moved=contribution_bytes,
                    link_type=host_d2h_link,
                    links_catalog=link_catalog,
                )
                t_tr_start, t_tr_end, tr_wait_s, tr_slot = _schedule_pool_event(
                    pool=host_d2h_pool,
                    t_req=t_contrib_req,
                    duration_s=d2h_duration,
                )
                total_bytes_host_link += contribution_bytes
                total_bytes_host_d2h += contribution_bytes
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id,
                    op_type="TRANSFER",
                    bytes_value=contribution_bytes,
                    t_req=t_contrib_req,
                    t_start=t_tr_start,
                    t_end=t_tr_end,
                    wait_s=tr_wait_s,
                    duration_s=d2h_duration,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    transfer_path="host_d2h",
                    resource_name=host_d2h_pool.name,
                    resource_slot=tr_slot,
                    link_type=host_d2h_link,
                    stage_src_endpoint=src_endpoint,
                    stage_dst_endpoint="host0",
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=consumer_id,
                    mapping_type=mapping.mapping_type,
                    mapping_id=mapping.mapping_id,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
                _handle_boundary_arrival(
                    boundary_idx=boundary_idx,
                    consumer_id=consumer_id,
                    bytes_in_value=contribution_bytes,
                    t_arrival=t_tr_end,
                    producer_tile_id=tile_id,
                    stage_id=stage_id,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    mapping=mapping,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                )
                continue

            # PIM->PIM
            retain_allowed = (
                pim_retention.enabled
                and pim_retention.same_endpoint_short_circuit
                and scenario in pim_retention.applies_to_scenarios
                and src_endpoint == dst_endpoint
            )
            retention_capacity_blocked = False
            if retain_allowed:
                boundary_total_bytes = int(scaled_boundaries[boundary_idx])
                if boundary_total_bytes <= pim_retention.pim_retention_capacity_bytes:
                    retain_duration = retain_duration_s(
                        retain_fixed_s=pim_retention.retain_fixed_s,
                        retain_metadata_bytes=pim_retention.retain_metadata_bytes,
                        retain_local_BW_Bps=pim_retention.retain_local_BW_Bps,
                    )
                    retain_pool = retain_pools[src_endpoint]
                    t_rt_start, t_rt_end, rt_wait_s, rt_slot = _schedule_pool_event(
                        pool=retain_pool,
                        t_req=t_contrib_req,
                        duration_s=retain_duration,
                    )
                    total_bytes_pim_retained += contribution_bytes
                    total_retain_handoff_time_component_s += retain_duration
                    _trace_event(
                        tile_id=tile_id,
                        stage_id=stage_id,
                        op_type="PIM_HANDOFF",
                        bytes_value=contribution_bytes,
                        t_req=t_contrib_req,
                        t_start=t_rt_start,
                        t_end=t_rt_end,
                        wait_s=rt_wait_s,
                        duration_s=retain_duration,
                        stage_name=stage_name,
                        stage_device=stage_device,
                        transfer_path="retain",
                        resource_name=retain_pool.name,
                        resource_slot=rt_slot,
                        stage_src_endpoint=src_endpoint,
                        stage_dst_endpoint=dst_endpoint,
                        handoff_mode="retain",
                        domain_in_id=domain_in_id,
                        domain_out_id=domain_out_id,
                        domain_in_tile_id=tile_id,
                        domain_out_tile_id=consumer_id,
                        mapping_type=mapping.mapping_type,
                        mapping_id=mapping.mapping_id,
                        kernel_class=kernel_class,
                        pim_mode=pim_mode,
                        pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                        pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                        pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                    )
                    _handle_boundary_arrival(
                        boundary_idx=boundary_idx,
                        consumer_id=consumer_id,
                        bytes_in_value=contribution_bytes,
                        t_arrival=t_rt_end,
                        producer_tile_id=tile_id,
                        stage_id=stage_id,
                        stage_name=stage_name,
                        stage_device=stage_device,
                        mapping=mapping,
                        kernel_class=kernel_class,
                        pim_mode=pim_mode,
                        pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                        pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                        pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                        domain_in_id=domain_in_id,
                        domain_out_id=domain_out_id,
                    )
                    continue
                retention_capacity_blocked = True
                total_retain_fallback_bytes += contribution_bytes

            if scenario == sources.SCENARIO_PIM_HOST_BOUNCE:
                d2h_duration = transfer_duration_s(
                    bytes_moved=contribution_bytes,
                    link_type=host_d2h_link,
                    links_catalog=link_catalog,
                )
                touch_duration = host_touch_duration_s(
                    bytes_moved=contribution_bytes,
                    touch_Bps=stage_cfg.host_touch_Bps,
                    touch_fixed_s=stage_cfg.host_touch_fixed_s,
                )
                h2d_duration = transfer_duration_s(
                    bytes_moved=contribution_bytes,
                    link_type=host_h2d_link,
                    links_catalog=link_catalog,
                )
                t_d2h_s, t_d2h_e, d2h_wait, d2h_slot = _schedule_pool_event(
                    pool=host_d2h_pool,
                    t_req=t_contrib_req,
                    duration_s=d2h_duration,
                )
                t_touch_s, t_touch_e, touch_wait, touch_slot = _schedule_pool_event(
                    pool=host_touch_pool,
                    t_req=t_d2h_e,
                    duration_s=touch_duration,
                )
                t_h2d_s, t_h2d_e, h2d_wait, h2d_slot = _schedule_pool_event(
                    pool=host_h2d_stage_pool,
                    t_req=t_touch_e,
                    duration_s=h2d_duration,
                )
                total_bytes_host_link += contribution_bytes * 2
                total_bytes_host_d2h += contribution_bytes
                total_bytes_host_h2d_stage += contribution_bytes
                total_bytes_host_touch += contribution_bytes
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id,
                    op_type="TRANSFER",
                    bytes_value=contribution_bytes,
                    t_req=t_contrib_req,
                    t_start=t_d2h_s,
                    t_end=t_d2h_e,
                    wait_s=d2h_wait,
                    duration_s=d2h_duration,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    transfer_path="host_d2h",
                    resource_name=host_d2h_pool.name,
                    resource_slot=d2h_slot,
                    link_type=host_d2h_link,
                    stage_src_endpoint=src_endpoint,
                    stage_dst_endpoint="host0",
                    handoff_mode="transfer_bounce",
                    retention_capacity_blocked=retention_capacity_blocked,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=consumer_id,
                    mapping_type=mapping.mapping_type,
                    mapping_id=mapping.mapping_id,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id,
                    op_type="HOST_TOUCH",
                    bytes_value=contribution_bytes,
                    t_req=t_d2h_e,
                    t_start=t_touch_s,
                    t_end=t_touch_e,
                    wait_s=touch_wait,
                    duration_s=touch_duration,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    transfer_path="host_touch",
                    resource_name=host_touch_pool.name,
                    resource_slot=touch_slot,
                    stage_src_endpoint="host0",
                    stage_dst_endpoint="host0",
                    handoff_mode="transfer_bounce",
                    retention_capacity_blocked=retention_capacity_blocked,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=consumer_id,
                    mapping_type=mapping.mapping_type,
                    mapping_id=mapping.mapping_id,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
                _trace_event(
                    tile_id=tile_id,
                    stage_id=stage_id + 1,
                    op_type="TRANSFER",
                    bytes_value=contribution_bytes,
                    t_req=t_touch_e,
                    t_start=t_h2d_s,
                    t_end=t_h2d_e,
                    wait_s=h2d_wait,
                    duration_s=h2d_duration,
                    stage_name=stage_names[stage_id],
                    stage_device=dst_device,
                    transfer_path="host_h2d_stage",
                    resource_name=host_h2d_stage_pool.name,
                    resource_slot=h2d_slot,
                    link_type=host_h2d_link,
                    stage_src_endpoint="host0",
                    stage_dst_endpoint=dst_endpoint,
                    handoff_mode="transfer_bounce",
                    retention_capacity_blocked=retention_capacity_blocked,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                    domain_in_tile_id=tile_id,
                    domain_out_tile_id=consumer_id,
                    mapping_type=mapping.mapping_type,
                    mapping_id=mapping.mapping_id,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                )
                _handle_boundary_arrival(
                    boundary_idx=boundary_idx,
                    consumer_id=consumer_id,
                    bytes_in_value=contribution_bytes,
                    t_arrival=t_h2d_e,
                    producer_tile_id=tile_id,
                    stage_id=stage_id,
                    stage_name=stage_name,
                    stage_device=stage_device,
                    mapping=mapping,
                    kernel_class=kernel_class,
                    pim_mode=pim_mode,
                    pim_mode_compute_multiplier=pim_mode_compute_multiplier,
                    pim_mode_mem_multiplier=pim_mode_mem_multiplier,
                    pim_mode_command_overhead_s=pim_mode_command_overhead_s,
                    domain_in_id=domain_in_id,
                    domain_out_id=domain_out_id,
                )
            else:
                u_out = min(
                    1.0,
                    float(cxl_direct_concurrency.dma_outstanding_per_vc)
                    / float(cxl_direct_concurrency.full_bw_outstanding_threshold),
                )
                issue_overhead_s = cxl_direct_concurrency.dma_issue_fixed_s / max(u_out, 1e-6)
                ready_t = t_contrib_req + float(cxl_link["latency_s"]) + issue_overhead_s
                total_cxl_dma_issue_time_component_s += issue_overhead_s
                pending_id = next_pending_direct_id
                next_pending_direct_id += 1
                if mapping.mapping_type == sources.MAPPING_REPARTITION_HASH:
                    aggregation_expected = repartition_states[boundary_idx].producer_count
                    aggregation_received = repartition_states[boundary_idx].received_producer_count
                else:
                    aggregation_expected = boundary_states[boundary_idx][consumer_id].expected_count
                    aggregation_received = boundary_states[boundary_idx][consumer_id].received_count
                pending_direct_transfers[pending_id] = {
                    "ready_t": ready_t,
                    "t_req": t_contrib_req,
                    "bytes": contribution_bytes,
                    "producer_tile_id": tile_id,
                    "consumer_id": consumer_id,
                    "boundary_idx": boundary_idx,
                    "stage_id": stage_id,
                    "stage_name": stage_name,
                    "stage_device": stage_device,
                    "stage_src_endpoint": src_endpoint,
                    "stage_dst_endpoint": dst_endpoint,
                    "retention_capacity_blocked": retention_capacity_blocked,
                    "cxl_issue_overhead_s": issue_overhead_s,
                    "cxl_striping_factor": cxl_striping_factor,
                    "domain_in_id": domain_in_id,
                    "domain_out_id": domain_out_id,
                    "domain_in_tile_id": tile_id,
                    "domain_out_tile_id": consumer_id,
                    "mapping_type": mapping.mapping_type,
                    "mapping_id": mapping.mapping_id,
                    "kernel_class": kernel_class,
                    "glue_type": "",
                    "barrier_dependency_wait_s": 0.0,
                    "glue_queue_wait_s": 0.0,
                    "barrier_total_wait_s": 0.0,
                    "barrier_wait_s": 0.0,
                    "aggregation_expected": aggregation_expected,
                    "aggregation_received": aggregation_received,
                    "pim_mode": pim_mode,
                    "pim_mode_compute_multiplier": pim_mode_compute_multiplier,
                    "pim_mode_mem_multiplier": pim_mode_mem_multiplier,
                    "pim_mode_command_overhead_s": pim_mode_command_overhead_s,
                }
                heapq.heappush(direct_request_heap, (ready_t, pending_id))

        if tiling_cfg.admission_refill_policy == "stage0_output" and stage_id == 1:
            _admit_next_input(t_cursor)

    if active_direct_transfers:
        raise AssertionError("direct transfer state left active at simulation end")
    cxl_direct_pool.busy_time_s = cxl_processor_scheduler.busy_slot_time_s

    compute_energy_J = sum(pool.busy_time_s * pool.power_W for pool in compute_pools)
    compute_energy_J += cpu_materialize_pool.busy_time_s * cpu_materialize_pool.power_W
    if tiling_cfg.glue_resource_mode == sources.GLUE_RESOURCE_MODE_DEDICATED_POOL:
        compute_energy_J += glue_cpu_pool.busy_time_s * glue_cpu_pool.power_W
        compute_energy_J += glue_pim_pool.busy_time_s * glue_pim_pool.power_W
    cpu_materialize_energy_J = cpu_materialize_pool.busy_time_s * cpu_materialize_pool.power_W
    host_touch_energy_J = host_touch_pool.busy_time_s * host_touch_pool.power_W
    transfer_energy_J = (
        host_h2d_ingress_pool.busy_time_s * host_h2d_ingress_pool.power_W
        + host_h2d_stage_pool.busy_time_s * host_h2d_stage_pool.power_W
        + host_d2h_pool.busy_time_s * host_d2h_pool.power_W
        + cxl_direct_pool.busy_time_s * cxl_direct_pool.power_W
        + host_touch_energy_J
    )
    total_energy_J = compute_energy_J + transfer_energy_J

    compute_lbs = [_pool_lower_bound_s(pool) for pool in compute_pools]
    compute_lbs.append(_pool_lower_bound_s(cpu_materialize_pool))
    lb_compute_stage_max_s = max(compute_lbs) if compute_lbs else 0.0
    if tiling_cfg.glue_resource_mode == sources.GLUE_RESOURCE_MODE_DEDICATED_POOL:
        lb_glue_s = max(_pool_lower_bound_s(glue_cpu_pool), _pool_lower_bound_s(glue_pim_pool))
    else:
        lb_glue_s = 0.0
    lb_host_h2d_ingress_s = _pool_lower_bound_s(host_h2d_ingress_pool)
    lb_host_h2d_stage_s = _pool_lower_bound_s(host_h2d_stage_pool)
    lb_host_d2h_s = _pool_lower_bound_s(host_d2h_pool)
    lb_host_link_s = max(lb_host_h2d_ingress_s, lb_host_h2d_stage_s, lb_host_d2h_s)
    lb_host_touch_s = _pool_lower_bound_s(host_touch_pool)
    lb_cxl_direct_s = _pool_lower_bound_s(cxl_direct_pool)
    dominant_candidates = [
        ("compute_stage_max", lb_compute_stage_max_s),
        ("host_link", lb_host_link_s),
        ("host_touch", lb_host_touch_s),
        ("cxl_direct", lb_cxl_direct_s),
        ("glue", lb_glue_s),
    ]
    dominant_lb_component = sorted(dominant_candidates, key=lambda item: item[1], reverse=True)[0][0]

    metrics_row: Dict[str, object] = {
        "run_id": run_id,
        "dataset_profile": dataset_profile,
        "stage_size_multiplier": size_multiplier,
        "scenario": scenario,
        "num_stages": num_public_stages,
        "num_kernels": num_stages,
        "num_tiles": num_input_tiles,
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
        "cxl_bw_model": "processor_share",
        "retile_enabled": True,
        "num_tile_domains": len(boundary_domains),
        "total_glue_copy_bytes": total_glue_copy_bytes,
        "total_glue_reduce_bytes": total_glue_reduce_bytes,
        "total_glue_shuffle_bytes": total_glue_shuffle_bytes,
        "total_glue_time_component_s": total_glue_time_component_s,
        "total_glue_transfer_time_component_s": total_glue_transfer_time_component_s,
        "total_barrier_dependency_wait_time_component_s": total_barrier_dependency_wait_time_component_s,
        "total_glue_queue_wait_time_component_s": total_glue_queue_wait_time_component_s,
        "total_barrier_wait_time_component_s": total_barrier_wait_time_component_s,
        "lb_glue_s": lb_glue_s,
        "total_pim_mode_command_overhead_s": total_pim_mode_command_overhead_s,
        "mapping_ids_used": "|".join(mapping_ids_used),
    }
    return metrics_row, traces


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
    pim_mode_by_stage_by_template: Mapping[str, Mapping[str, str]],
    pim_mode_effects: Mapping[str, PIMModeEffect],
    tiling_model_by_template: Mapping[str, TilingTemplateConfig],
    workload_family: str,
    workload_profile: str,
    workload_variant: str,
    baseline_id: str,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
    trace_max_tiles: int | None = None,
) -> Tuple[Dict[str, object], List[Dict[str, object]]]:
    tiling_cfg = tiling_model_by_template[pipeline_template]
    if tiling_cfg.enabled:
        return _simulate_configuration_retile(
            run_id=run_id,
            dataset_profile=dataset_profile,
            boundaries_bytes=boundaries_bytes,
            public_stage_names=public_stage_names,
            stage_names=stage_names,
            pipeline_template=pipeline_template,
            size_multiplier=size_multiplier,
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
            pim_retention=pim_retention,
            cxl_direct_concurrency=cxl_direct_concurrency,
            cxl_topology=cxl_topology,
            bytes_touched_factors_by_stage_by_template=bytes_touched_factors_by_stage_by_template,
            pim_mode_by_stage_by_template=pim_mode_by_stage_by_template,
            pim_mode_effects=pim_mode_effects,
            tiling_model_by_template=tiling_model_by_template,
            workload_family=workload_family,
            workload_profile=workload_profile,
            workload_variant=workload_variant,
            baseline_id=baseline_id,
            links_catalog=links_catalog,
            trace_max_tiles=trace_max_tiles,
        )
    return _simulate_configuration_linear(
        run_id=run_id,
        dataset_profile=dataset_profile,
        boundaries_bytes=boundaries_bytes,
        public_stage_names=public_stage_names,
        stage_names=stage_names,
        pipeline_template=pipeline_template,
        size_multiplier=size_multiplier,
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
        pim_retention=pim_retention,
        cxl_direct_concurrency=cxl_direct_concurrency,
        cxl_topology=cxl_topology,
        bytes_touched_factors_by_stage_by_template=bytes_touched_factors_by_stage_by_template,
        pim_mode_by_stage_by_template=pim_mode_by_stage_by_template,
        pim_mode_effects=pim_mode_effects,
        tiling_model_by_template=tiling_model_by_template,
        workload_family=workload_family,
        workload_profile=workload_profile,
        workload_variant=workload_variant,
        baseline_id=baseline_id,
        links_catalog=links_catalog,
        trace_max_tiles=trace_max_tiles,
    )


def resolve_variant_configs(
    config: Dict[str, object],
    *,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
) -> Dict[str, Dict[str, object]]:
    workload_sweep = _normalize_workload_sweep(config)
    workload_variants = _normalize_workload_variants(config)

    tpch_profiles = workload_sweep["tpch_profiles"]
    deepvariant_profiles = workload_sweep["deepvariant_profiles"]
    run_profiles = tpch_profiles + deepvariant_profiles
    if not run_profiles:
        raise ValueError("no profiles selected for run matrix; check workload_sweep or dataset_profiles")

    size_multipliers = [float(value) for value in config["size_multipliers"]]
    scenarios = [str(value) for value in config["scenarios"]]

    resolved: Dict[str, Dict[str, object]] = {}
    for variant in workload_variants:
        variant_name = str(variant["name"])
        variant_overrides = variant["overrides"]
        merged_config = _deep_merge_config(config, variant_overrides)
        merged_config["dataset_profiles"] = list(run_profiles)
        merged_config["size_multipliers"] = list(size_multipliers)
        merged_config["scenarios"] = list(scenarios)
        _validate_config(merged_config, links_catalog=links_catalog)
        resolved[variant_name] = merged_config
    return resolved


def generate_runs_from_config(
    config: Dict[str, object],
    *,
    links_catalog: Mapping[str, Mapping[str, object]] | None = None,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
    resolved_variant_configs = resolve_variant_configs(config, links_catalog=links_catalog)

    metrics: List[Dict[str, object]] = []
    traces: List[Dict[str, object]] = []
    run_counter = 1

    for variant_name, merged_config in resolved_variant_configs.items():
        run_profiles = [str(value) for value in merged_config["dataset_profiles"]]
        size_multipliers = [float(value) for value in merged_config["size_multipliers"]]
        scenarios = [str(value) for value in merged_config["scenarios"]]

        memory_system_by_template = _validate_config(merged_config, links_catalog=links_catalog)
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
        tiling_model_by_template = _normalize_tiling_model_by_template(
            config=merged_config,
            template_to_stage_names=template_to_stage_names,
        )
        pim_mode_by_stage_by_template = _normalize_pim_mode_by_stage_by_template(
            config=merged_config,
            template_to_stage_names=template_to_stage_names,
        )
        pim_mode_effects = _normalize_pim_mode_effects(merged_config)
        pim_retention_cfg = _normalize_pim_retention_config(config=merged_config, warn_defaults=True)
        cxl_direct_concurrency_cfg = _normalize_cxl_direct_concurrency_config(
            config=merged_config,
            warn_defaults=True,
        )
        cxl_topology_cfg = _normalize_cxl_topology_config(
            config=merged_config,
            warn_defaults=True,
            links_catalog=links_catalog,
        )
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
                        pim_mode_by_stage_by_template=pim_mode_by_stage_by_template,
                        pim_mode_effects=pim_mode_effects,
                        tiling_model_by_template=tiling_model_by_template,
                        workload_family=workload_family,
                        workload_profile=workload_profile,
                        workload_variant=variant_name,
                        baseline_id=baseline_id,
                        links_catalog=links_catalog,
                        trace_max_tiles=trace_max_tiles,
                    )
                    metrics.append(row)
                    traces.extend(trace_rows)

    return metrics, traces
