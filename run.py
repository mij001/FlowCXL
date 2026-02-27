"""Runner for tiled stage-capacity FlowCXL experiments."""

from __future__ import annotations

import argparse
import copy
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd
import yaml

import sources
from simulator import generate_runs_from_config, resolve_variant_configs

BASE_METRICS_COLUMNS = [
    "run_id",
    "dataset_profile",
    "stage_size_multiplier",
    "scenario",
    "num_stages",
    "num_kernels",
    "num_tiles",
    "makespan_s",
    "total_energy_J",
    "compute_energy_J",
    "transfer_energy_J",
    "host_touch_energy_J",
    "total_bytes_host_link",
    "total_bytes_cxl_direct",
    "total_bytes_host_touch",
    "total_bytes_host_h2d_ingress",
    "total_bytes_host_h2d_stage",
    "total_bytes_host_d2h",
    "total_bytes_moved",
    "lb_compute_stage_max_s",
    "lb_host_h2d_ingress_s",
    "lb_host_h2d_stage_s",
    "lb_host_d2h_s",
    "lb_host_link_s",
    "lb_host_touch_s",
    "lb_cxl_direct_s",
    "dominant_lb_component",
    "pipeline_template",
    "memory_ceiling_enabled",
    "total_cpu_mem_time_component_s",
    "total_cpu_mem_latency_bound_time_component_s",
    "total_cpu_mem_peak_bound_time_component_s",
    "total_pim_mem_time_component_s",
    "total_compute_time_component_s",
    "total_cpu_materialize_bytes",
    "total_cpu_materialize_time_component_s",
    "cpu_materialize_energy_J",
    "cpu_baseline_engine",
    "total_cpu_mem_service_time_component_s",
    "total_cpu_mem_queue_delay_component_s",
    "total_pim_mem_service_time_component_s",
    "total_pim_mem_queue_delay_component_s",
    "total_bytes_pim_retained",
    "total_retain_fallback_bytes",
    "total_retain_handoff_time_component_s",
    "cxl_direct_stream_slots",
    "cxl_active_direct_endpoints",
    "cxl_effective_striping_factor",
    "total_cxl_dma_issue_time_component_s",
    "cxl_bw_model",
    "retile_enabled",
    "num_tile_domains",
    "total_glue_copy_bytes",
    "total_glue_reduce_bytes",
    "total_glue_shuffle_bytes",
    "total_glue_time_component_s",
    "total_glue_transfer_time_component_s",
    "total_barrier_wait_time_component_s",
    "lb_glue_s",
    "total_pim_mode_command_overhead_s",
    "workload_family",
    "workload_profile",
    "workload_variant",
    "baseline_id",
]
DEFAULT_TRACE_YAML_MAX_EVENTS = 2000


def _sample_yaml_events(traces: List[Dict[str, object]], max_events: int) -> List[Dict[str, object]]:
    if max_events <= 0 or not traces:
        return []

    scenarios = sorted({str(row.get("scenario", "")) for row in traces})
    if not scenarios:
        return traces[:max_events]

    quota = max(1, max_events // len(scenarios))
    selected: List[Dict[str, object]] = []
    selected_ids: set[int] = set()
    used_per_scenario = {scenario: 0 for scenario in scenarios}

    for idx, row in enumerate(traces):
        scenario = str(row.get("scenario", ""))
        if scenario in used_per_scenario and used_per_scenario[scenario] < quota:
            selected.append(row)
            selected_ids.add(idx)
            used_per_scenario[scenario] += 1
            if len(selected) >= max_events:
                return selected

    for idx, row in enumerate(traces):
        if len(selected) >= max_events:
            break
        if idx not in selected_ids:
            selected.append(row)

    return selected


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run FlowCXL simulations.")
    parser.add_argument("--config", default="configs/runs.yaml", help="Path to run config YAML.")
    parser.add_argument("--artifacts-dir", default="artifacts", help="Artifacts output directory.")
    parser.add_argument(
        "--trace-yaml-max-events",
        type=int,
        default=DEFAULT_TRACE_YAML_MAX_EVENTS,
        help="Maximum number of trace events stored in traces.yaml.",
    )
    parser.add_argument(
        "--validation-overlay",
        default=None,
        help="Optional YAML overlay with config/link-constant overrides from validation.",
    )
    return parser.parse_args(argv)


def metrics_columns() -> List[str]:
    return list(BASE_METRICS_COLUMNS)


def load_config(config_path: Path) -> Dict[str, object]:
    with config_path.open("r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    if not isinstance(config, dict):
        raise ValueError("config root must be a mapping")
    return config


def _deep_merge(base: Dict[str, object], patch: Dict[str, object]) -> Dict[str, object]:
    merged = copy.deepcopy(base)
    for key, value in patch.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _apply_validation_overlay(
    config: Dict[str, object],
    overlay: Dict[str, object],
) -> Tuple[Dict[str, object], Dict[str, Dict[str, object]]]:
    resolved_links: Dict[str, Dict[str, object]] = {
        str(link_id): dict(link_cfg) for link_id, link_cfg in sources.LINKS.items()
    }
    effective_overlay = copy.deepcopy(overlay)
    link_overrides = effective_overlay.pop("link_constant_overrides", {})
    if link_overrides:
        if not isinstance(link_overrides, dict):
            raise ValueError("validation overlay link_constant_overrides must be a map")
        for link_id, link_patch in link_overrides.items():
            if link_id not in resolved_links:
                raise ValueError(f"validation overlay references unknown link id: {link_id}")
            if not isinstance(link_patch, dict):
                raise ValueError(f"validation overlay for {link_id} must be a map")
            current = dict(resolved_links[link_id])
            current.update(link_patch)
            resolved_links[link_id] = current
    return _deep_merge(config, effective_overlay), resolved_links


def _write_yaml(path: Path, payload: Dict[str, object]) -> None:
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"config file not found: {config_path}")

    config = load_config(config_path)
    config_input_snapshot = copy.deepcopy(config)
    links_catalog: Dict[str, Dict[str, object]] = {
        str(link_id): dict(link_cfg) for link_id, link_cfg in sources.LINKS.items()
    }
    overlay_payload: Dict[str, object] | None = None
    if args.validation_overlay:
        overlay_path = Path(args.validation_overlay)
        if not overlay_path.exists():
            raise FileNotFoundError(f"validation overlay not found: {overlay_path}")
        with overlay_path.open("r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle) or {}
        if not isinstance(loaded, dict):
            raise ValueError("validation overlay root must be a mapping")
        overlay_payload = loaded
        config, links_catalog = _apply_validation_overlay(config=config, overlay=overlay_payload)

    artifacts_dir = Path(args.artifacts_dir)
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    _write_yaml(artifacts_dir / "config_input.yaml", config_input_snapshot)
    if overlay_payload is not None:
        _write_yaml(artifacts_dir / "config_validation_overlay.yaml", overlay_payload)
    resolved_variant_configs = resolve_variant_configs(config, links_catalog=links_catalog)
    for variant_name, resolved_cfg in resolved_variant_configs.items():
        resolved_name = variant_name.replace("/", "_").replace(" ", "_")
        _write_yaml(artifacts_dir / f"config_resolved_{resolved_name}.yaml", resolved_cfg)

    metrics, traces = generate_runs_from_config(config, links_catalog=links_catalog)

    metrics_df = pd.DataFrame(metrics)
    metrics_df = metrics_df.reindex(columns=metrics_columns())
    metrics_path = artifacts_dir / "metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)

    run_matrix_cols = [
        "run_id",
        "workload_family",
        "workload_profile",
        "workload_variant",
        "dataset_profile",
        "pipeline_template",
        "scenario",
        "stage_size_multiplier",
        "baseline_id",
    ]
    run_matrix_df = metrics_df[run_matrix_cols].copy()
    run_matrix_path = artifacts_dir / "run_matrix.csv"
    run_matrix_df.to_csv(run_matrix_path, index=False)

    traces_df = pd.DataFrame(traces)
    traces_csv_path = artifacts_dir / "traces.csv"
    traces_df.to_csv(traces_csv_path, index=False)

    traces_yaml_path = artifacts_dir / "traces.yaml"
    yaml_events = _sample_yaml_events(traces=traces, max_events=int(args.trace_yaml_max_events))
    with traces_yaml_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "total_events": len(traces),
                "yaml_events": len(yaml_events),
                "truncated": len(traces) > len(yaml_events),
                "events": yaml_events,
            },
            handle,
            sort_keys=False,
        )

    print(f"Wrote {metrics_path} ({len(metrics_df)} rows)")
    print(f"Wrote {run_matrix_path} ({len(run_matrix_df)} rows)")
    print(f"Wrote {traces_csv_path} ({len(traces_df)} rows)")
    print(f"Wrote {traces_yaml_path} ({len(yaml_events)} events in YAML, total events {len(traces)})")


if __name__ == "__main__":
    main()
