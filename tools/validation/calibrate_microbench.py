"""Run microbenchmark calibration fitting from measured CSV inputs."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Tuple

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import sources
from tools.validation.common import (
    TRANSFER_CALIBRATION_PATHS,
    ensure_calibration_config,
    ensure_validation_config,
    load_yaml,
    save_yaml,
)


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run microbenchmark calibration fitting.")
    parser.add_argument("--config", default="configs/runs.yaml", help="Path to run config YAML.")
    parser.add_argument("--out", default="artifacts/validation", help="Validation output directory.")
    return parser.parse_args(argv)


def _sim_time_for_path(
    *,
    path: str,
    payload_bytes: int,
    concurrency: int,
    config: Dict[str, object],
) -> float:
    link_profile = config["link_profile"]
    stage_defaults = config["stage_defaults"]
    cxl_concurrency = config["cxl_direct_concurrency"]
    cxl_topology = config["cxl_topology"]
    host_h2d_link = str(link_profile["host_h2d_link"])
    host_d2h_link = str(link_profile["host_d2h_link"])
    cxl_direct_link = str(link_profile["cxl_direct_link"])
    h2d = sources.LINKS[host_h2d_link]
    d2h = sources.LINKS[host_d2h_link]
    cxl = sources.LINKS[cxl_direct_link]

    if path == "host_h2d":
        return float(h2d["latency_s"]) + (payload_bytes / float(h2d["bandwidth_Bps"]))
    if path == "host_d2h":
        return float(d2h["latency_s"]) + (payload_bytes / float(d2h["bandwidth_Bps"]))
    if path == "bounce":
        touch = float(stage_defaults["host_touch_fixed_s"]) + (
            payload_bytes / float(stage_defaults["host_touch_Bps"])
        )
        return (
            float(d2h["latency_s"]) + (payload_bytes / float(d2h["bandwidth_Bps"]))
            + touch
            + float(h2d["latency_s"]) + (payload_bytes / float(h2d["bandwidth_Bps"]))
        )
    if path == "direct":
        striping = 1
        if (
            bool(cxl_topology["enabled"])
            and cxl_direct_link in list(cxl_topology["applies_to_links"])
            and bool(cxl.get("supports_dynamic_striping", False))
        ):
            striping = min(
                int(cxl_topology["max_stripes"]),
                int(cxl_topology["num_physical_links"]),
                max(1, concurrency),
            )
        bw_total = float(cxl["bandwidth_Bps"]) * float(striping)
        share_bw = bw_total / float(max(1, concurrency))
        u_out = min(
            1.0,
            float(cxl_concurrency["dma_outstanding_per_vc"])
            / float(cxl_concurrency["full_bw_outstanding_threshold"]),
        )
        issue = float(cxl_concurrency["dma_issue_fixed_s"]) / max(u_out, 1e-6)
        return float(cxl["latency_s"]) + issue + (payload_bytes / share_bw)
    if path == "host_touch":
        return float(stage_defaults["host_touch_fixed_s"]) + (
            payload_bytes / float(stage_defaults["host_touch_Bps"])
        )
    raise ValueError(f"unsupported calibration path: {path}")


def _linear_fit(x_vals: List[float], y_vals: List[float]) -> Tuple[float, float, float]:
    if len(x_vals) != len(y_vals) or len(x_vals) < 2:
        raise ValueError("need at least two points for linear fit")
    x_mean = sum(x_vals) / len(x_vals)
    y_mean = sum(y_vals) / len(y_vals)
    cov = sum((x - x_mean) * (y - y_mean) for x, y in zip(x_vals, y_vals))
    var_x = sum((x - x_mean) ** 2 for x in x_vals)
    if var_x <= 0:
        raise ValueError("insufficient variance in x for linear fit")
    slope = cov / var_x
    intercept = y_mean - (slope * x_mean)
    ss_tot = sum((y - y_mean) ** 2 for y in y_vals)
    ss_res = sum((y - (intercept + (slope * x))) ** 2 for x, y in zip(x_vals, y_vals))
    r2 = 1.0 if ss_tot <= 0 else max(0.0, 1.0 - (ss_res / ss_tot))
    return intercept, slope, r2


def _resolve_csv_path(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return Path.cwd() / path


def _aggregate_series(values: pd.Series, aggregate_stat: str) -> float:
    if aggregate_stat == "median":
        return float(values.median())
    if aggregate_stat == "mean":
        return float(values.mean())
    raise ValueError(f"unsupported aggregate_stat: {aggregate_stat}")


def _normalize_pinned_value(value: object) -> bool | None:
    if pd.isna(value):
        return None
    if isinstance(value, bool):
        return value
    token = str(value).strip().lower()
    if token in {"true", "1", "yes", "y", "pinned", "pin"}:
        return True
    if token in {"false", "0", "no", "n", "pageable", "paged"}:
        return False
    if token in {"na", "n/a", "none", "null", "unknown", "", "nan"}:
        return None
    raise ValueError(f"invalid pinned value: {value}")


def _validate_measured_frame(
    *,
    df: pd.DataFrame,
    expected_path: str,
    system_id: str,
    pinned_column: str,
) -> pd.DataFrame:
    if expected_path == "host_touch":
        required_cols = ["system_id", "path", "payload_bytes", "repetition", "time_s"]
    else:
        required_cols = [
            "system_id",
            "path",
            "payload_bytes",
            "concurrency",
            "repetition",
            "time_s",
            pinned_column,
        ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"measured CSV for {expected_path} missing required columns: {missing_cols}")

    normalized = df.copy()
    normalized["system_id"] = normalized["system_id"].astype(str)
    normalized["path"] = normalized["path"].astype(str)
    if not normalized.empty and set(normalized["path"].unique()) != {expected_path}:
        raise ValueError(
            f"measured CSV path mismatch for {expected_path}; found values: {sorted(set(normalized['path'].unique()))}"
        )
    if not normalized.empty and set(normalized["system_id"].unique()) != {system_id}:
        raise ValueError(
            f"measured CSV system_id mismatch for {expected_path}; expected {system_id}"
        )

    if "concurrency" not in normalized.columns:
        normalized["concurrency"] = 1

    for col in ["payload_bytes", "concurrency", "repetition", "time_s"]:
        normalized[col] = pd.to_numeric(normalized[col], errors="coerce")
    if normalized[["payload_bytes", "concurrency", "repetition", "time_s"]].isna().any().any():
        raise ValueError(f"measured CSV for {expected_path} contains non-numeric values")

    normalized["payload_bytes"] = normalized["payload_bytes"].astype(int)
    normalized["concurrency"] = normalized["concurrency"].astype(int)
    normalized["repetition"] = normalized["repetition"].astype(int)
    normalized["time_s"] = normalized["time_s"].astype(float)

    if (normalized["payload_bytes"] <= 0).any():
        raise ValueError(f"measured CSV for {expected_path} has non-positive payload_bytes")
    if (normalized["concurrency"] <= 0).any():
        raise ValueError(f"measured CSV for {expected_path} has non-positive concurrency")
    if (normalized["repetition"] < 0).any():
        raise ValueError(f"measured CSV for {expected_path} has negative repetition")
    if (normalized["time_s"] <= 0).any():
        raise ValueError(f"measured CSV for {expected_path} has non-positive time_s")

    optional_columns = [
        "tool",
        "pinned",
        "numa_policy",
        "dma_engine",
        "percentile_source",
        "timestamp",
        "notes",
    ]
    for column in optional_columns:
        if column not in normalized.columns:
            normalized[column] = pd.NA

    if expected_path != "host_touch":
        try:
            normalized["pinned"] = normalized[pinned_column].map(_normalize_pinned_value)
        except ValueError as exc:
            raise ValueError(f"measured CSV for {expected_path} has invalid pinned values: {exc}") from exc

    if "measured_s" in normalized.columns:
        normalized = normalized.drop(columns=["measured_s"])
    normalized = normalized.rename(columns={"time_s": "measured_s"})
    return normalized


def _enforce_memory_mode_policy(
    *,
    measured_df: pd.DataFrame,
    required_paths: Sequence[str],
    memory_mode_policy: Mapping[str, object],
) -> None:
    required_paths_must_be_pinned = bool(memory_mode_policy["required_paths_must_be_pinned"])
    allow_mixed_memory_mode = bool(memory_mode_policy["allow_mixed_memory_mode"])

    for path_name in required_paths:
        if path_name == "host_touch":
            continue
        subset = measured_df[measured_df["path"] == path_name].copy()
        if subset.empty:
            continue
        if "pinned" not in subset.columns:
            raise ValueError(f"required calibration path {path_name} missing pinned column")

        if required_paths_must_be_pinned:
            not_pinned = subset[subset["pinned"] != True]
            if not not_pinned.empty:
                raise ValueError(
                    f"required calibration path {path_name} contains non-pinned rows; "
                    "set memory_mode_policy.required_paths_must_be_pinned=false to allow this"
                )

        if not allow_mixed_memory_mode:
            pinned_tokens = {
                "unknown" if pd.isna(v) else str(bool(v)).lower()
                for v in subset["pinned"].tolist()
            }
            if len(pinned_tokens) > 1:
                raise ValueError(
                    f"required calibration path {path_name} mixes pinned/pageable states: {sorted(pinned_tokens)}; "
                    "set memory_mode_policy.allow_mixed_memory_mode=true to allow this"
                )


def _build_coverage_summary(
    *,
    measured_df: pd.DataFrame,
    required_paths: Sequence[str],
    optional_paths: Sequence[str],
    payloads: Sequence[int],
    conc_levels: Sequence[int],
    required_points_min_samples: int,
    coverage_policy: Mapping[str, object],
) -> Dict[str, object]:
    expected_pairs = [(int(p), int(c)) for p in payloads for c in conc_levels]
    warn_on_low_samples = bool(coverage_policy["warn_on_low_samples"])
    fail_on_missing_required_point = bool(coverage_policy["fail_on_missing_required_point"])

    missing_required: List[str] = []
    missing_optional: List[str] = []
    low_sample_points: List[str] = []

    def _count_for(path_name: str, payload: int, concurrency: int) -> int:
        subset = measured_df[
            (measured_df["path"] == path_name)
            & (measured_df["payload_bytes"] == payload)
            & (measured_df["concurrency"] == concurrency)
        ]
        return int(len(subset))

    required_points_total = len(required_paths) * len(expected_pairs)
    required_points_observed = 0

    for path_name in required_paths:
        for payload, conc in expected_pairs:
            count = _count_for(path_name, payload, conc)
            point_id = f"{path_name}(payload={payload},concurrency={conc})"
            if count == 0:
                missing_required.append(point_id)
                continue
            required_points_observed += 1
            if count < required_points_min_samples:
                low_sample_points.append(f"{point_id}:samples={count}")

    for path_name in optional_paths:
        for payload, conc in expected_pairs:
            count = _count_for(path_name, payload, conc)
            point_id = f"{path_name}(payload={payload},concurrency={conc})"
            if count == 0:
                missing_optional.append(point_id)
                continue
            if count < required_points_min_samples:
                low_sample_points.append(f"{point_id}:samples={count}")

    if missing_required and fail_on_missing_required_point:
        raise ValueError(
            "measured CSV missing required payload/concurrency points: "
            + ", ".join(sorted(missing_required))
        )

    coverage_warnings: List[str] = []
    if missing_required and not fail_on_missing_required_point:
        coverage_warnings.append(
            "missing required points tolerated by policy: " + ", ".join(sorted(missing_required))
        )
    if missing_optional:
        coverage_warnings.append("missing optional points: " + ", ".join(sorted(missing_optional)))
    if warn_on_low_samples and low_sample_points:
        coverage_warnings.append(
            "points below required_points_min_samples: " + ", ".join(sorted(low_sample_points))
        )

    return {
        "required_points_total": required_points_total,
        "required_points_observed": required_points_observed,
        "required_points_missing_count": len(missing_required),
        "optional_points_missing_count": len(missing_optional),
        "low_sample_points_count": len(low_sample_points),
        "required_points_min_samples": int(required_points_min_samples),
        "missing_required_points": sorted(missing_required),
        "missing_optional_points": sorted(missing_optional),
        "low_sample_points": sorted(low_sample_points),
        "coverage_warnings": coverage_warnings,
    }


def _build_measurement_semantics_summary(measured_df: pd.DataFrame) -> Dict[str, object]:
    keys = ["pinned", "tool", "numa_policy", "dma_engine"]
    per_path: Dict[str, Dict[str, List[str]]] = {}

    for path_name in sorted(measured_df["path"].dropna().unique()):
        subset = measured_df[measured_df["path"] == path_name]
        entry: Dict[str, List[str]] = {}
        for key in keys:
            if key in subset.columns:
                values = sorted({str(v) for v in subset[key].dropna().tolist()})
            else:
                values = []
            entry[key] = values
        per_path[str(path_name)] = entry

    global_sets: Dict[str, List[str]] = {}
    for key in keys:
        if key in measured_df.columns:
            global_sets[key] = sorted({str(v) for v in measured_df[key].dropna().tolist()})
        else:
            global_sets[key] = []

    return {
        "global": global_sets,
        "per_path": per_path,
    }


def _load_measured_rows(
    *,
    system_id: str,
    payloads: Sequence[int],
    conc_levels: Sequence[int],
    measured_inputs: Mapping[str, str],
    required_paths: Sequence[str],
    optional_paths: Sequence[str],
    required_points_min_samples: int,
    coverage_policy: Mapping[str, object],
    memory_mode_policy: Mapping[str, object],
) -> Tuple[pd.DataFrame, Dict[str, str], List[str], Dict[str, object], Dict[str, object]]:
    loaded_paths: Dict[str, str] = {}
    missing_paths: List[str] = []
    frames: List[pd.DataFrame] = []
    pinned_column = str(memory_mode_policy["pinned_column"])

    for path_name, csv_path_raw in measured_inputs.items():
        csv_path = _resolve_csv_path(csv_path_raw)
        if not csv_path.exists():
            if path_name in required_paths:
                raise FileNotFoundError(f"required measured CSV not found for {path_name}: {csv_path}")
            missing_paths.append(path_name)
            continue

        frame = pd.read_csv(csv_path)
        validated = _validate_measured_frame(
            df=frame,
            expected_path=path_name,
            system_id=system_id,
            pinned_column=pinned_column,
        )
        if validated.empty:
            if path_name in required_paths:
                raise ValueError(f"required measured CSV has no rows for {path_name}: {csv_path}")
            missing_paths.append(path_name)
            continue
        validated["source_file"] = str(csv_path)
        frames.append(validated)
        loaded_paths[path_name] = str(csv_path)

    for required_path in required_paths:
        if required_path not in loaded_paths:
            raise ValueError(f"missing required measured calibration path: {required_path}")

    if not frames:
        raise ValueError("no measured calibration rows loaded")

    measured_df = pd.concat(frames, ignore_index=True)

    _enforce_memory_mode_policy(
        measured_df=measured_df,
        required_paths=required_paths,
        memory_mode_policy=memory_mode_policy,
    )

    coverage_summary = _build_coverage_summary(
        measured_df=measured_df,
        required_paths=required_paths,
        optional_paths=optional_paths,
        payloads=payloads,
        conc_levels=conc_levels,
        required_points_min_samples=required_points_min_samples,
        coverage_policy=coverage_policy,
    )
    semantics_summary = _build_measurement_semantics_summary(measured_df)
    return measured_df, loaded_paths, missing_paths, coverage_summary, semantics_summary


def _build_aggregated_rows(
    *,
    measured_df: pd.DataFrame,
    aggregate_stat: str,
    config: Dict[str, object],
) -> pd.DataFrame:
    grouped = (
        measured_df.groupby(["path", "payload_bytes", "concurrency"])["measured_s"]
        .agg(
            sample_count="size",
            mean_s="mean",
            p50_s=lambda s: float(s.quantile(0.50)),
            p95_s=lambda s: float(s.quantile(0.95)),
            p99_s=lambda s: float(s.quantile(0.99)),
        )
        .reset_index()
    )
    grouped["measured_s"] = grouped.apply(
        lambda row: float(row["p50_s"]) if aggregate_stat == "median" else float(row["mean_s"]),
        axis=1,
    )
    grouped = grouped.sort_values(["path", "payload_bytes", "concurrency"]).reset_index(drop=True)
    grouped["simulated_s"] = grouped.apply(
        lambda row: _sim_time_for_path(
            path=str(row["path"]),
            payload_bytes=int(row["payload_bytes"]),
            concurrency=int(row["concurrency"]),
            config=config,
        ),
        axis=1,
    )
    grouped["error_s"] = grouped["measured_s"] - grouped["simulated_s"]
    grouped["error_pct"] = grouped.apply(
        lambda row: ((float(row["measured_s"]) / float(row["simulated_s"])) - 1.0) * 100.0
        if float(row["simulated_s"]) > 0
        else 0.0,
        axis=1,
    )
    return grouped


def _fit_path(
    *,
    path_name: str,
    agg_df: pd.DataFrame,
    fit_reference_concurrency: int,
) -> Dict[str, float | int | str]:
    fit_subset = agg_df[
        (agg_df["path"] == path_name) & (agg_df["concurrency"] == fit_reference_concurrency)
    ].copy()
    if fit_subset.empty:
        raise ValueError(
            f"no measured aggregate rows for {path_name} at fit_reference_concurrency={fit_reference_concurrency}"
        )

    fit_subset = fit_subset.sort_values("payload_bytes")
    x_vals = [float(v) for v in fit_subset["payload_bytes"].tolist()]
    y_vals = [float(v) for v in fit_subset["measured_s"].tolist()]
    intercept, slope, r2 = _linear_fit(x_vals=x_vals, y_vals=y_vals)
    slope = max(float(slope), 1e-15)
    bw = 1.0 / slope
    latency = max(0.0, float(intercept))
    preds = [latency + (x / bw) for x in x_vals]
    mape = (
        sum(abs((y - p) / max(y, 1e-12)) for y, p in zip(y_vals, preds)) / len(y_vals) * 100.0
        if y_vals
        else 0.0
    )
    return {
        "bandwidth_Bps": float(bw),
        "latency_s": float(latency),
        "r2": float(r2),
        "mape_percent": float(mape),
        "n_points": len(x_vals),
        "fit_concurrency": int(fit_reference_concurrency),
        "calibration_status": "measured",
    }


def _fit_host_touch_from_measured(
    *,
    agg_df: pd.DataFrame,
    fit_reference_concurrency: int,
    fallback_stage_defaults: Mapping[str, object],
) -> Tuple[Dict[str, float | int | str], List[str]]:
    warnings: List[str] = []
    fit_subset = agg_df[
        (agg_df["path"] == "host_touch") & (agg_df["concurrency"] == fit_reference_concurrency)
    ].copy()
    fit_subset = fit_subset.sort_values("payload_bytes")

    if len(fit_subset) < 2:
        warnings.append("host_touch_measured_rows_insufficient_points_using_stage_defaults")
        return (
            {
                "host_touch_Bps": float(fallback_stage_defaults["host_touch_Bps"]),
                "host_touch_fixed_s": float(fallback_stage_defaults["host_touch_fixed_s"]),
                "r2": 0.0,
                "mape_percent": 0.0,
                "n_points": int(len(fit_subset)),
                "fit_concurrency": int(fit_reference_concurrency),
                "status": "fallback_defaults",
            },
            warnings,
        )

    x_vals = [float(v) for v in fit_subset["payload_bytes"].tolist()]
    y_vals = [float(v) for v in fit_subset["measured_s"].tolist()]
    intercept, slope, r2 = _linear_fit(x_vals=x_vals, y_vals=y_vals)
    slope = max(float(slope), 1e-15)
    bw = 1.0 / slope
    fixed = max(0.0, float(intercept))
    preds = [fixed + (x / bw) for x in x_vals]
    mape = (
        sum(abs((y - p) / max(y, 1e-12)) for y, p in zip(y_vals, preds)) / len(y_vals) * 100.0
        if y_vals
        else 0.0
    )
    return (
        {
            "host_touch_Bps": float(bw),
            "host_touch_fixed_s": float(fixed),
            "r2": float(r2),
            "mape_percent": float(mape),
            "n_points": len(x_vals),
            "fit_concurrency": int(fit_reference_concurrency),
            "status": "measured_stream",
        },
        warnings,
    )


def _fit_host_touch_from_bounce(
    *,
    payloads: Sequence[int],
    fit_reference_concurrency: int,
    agg_df: pd.DataFrame,
    path_fits: Mapping[str, Mapping[str, float | int | str]],
    fallback_stage_defaults: Mapping[str, object],
    negative_residual_policy: Mapping[str, object],
) -> Tuple[Dict[str, float | int | str], List[str], Dict[str, object]]:
    warnings: List[str] = []
    mode = str(negative_residual_policy["mode"])
    epsilon_s = float(negative_residual_policy["epsilon_s"])

    bounce_subset = agg_df[
        (agg_df["path"] == "bounce") & (agg_df["concurrency"] == fit_reference_concurrency)
    ].copy()
    bounce_map = {
        int(row.payload_bytes): float(row.measured_s)
        for row in bounce_subset[["payload_bytes", "measured_s"]].itertuples(index=False)
    }

    h2d_latency = float(path_fits["host_h2d"]["latency_s"])
    h2d_bw = float(path_fits["host_h2d"]["bandwidth_Bps"])
    d2h_latency = float(path_fits["host_d2h"]["latency_s"])
    d2h_bw = float(path_fits["host_d2h"]["bandwidth_Bps"])

    x_vals: List[float] = []
    touch_vals: List[float] = []
    negative_points: List[str] = []

    for payload in payloads:
        payload_i = int(payload)
        if payload_i not in bounce_map:
            continue
        payload_f = float(payload_i)
        bounce_t = bounce_map[payload_i]
        estimated_touch = bounce_t - (
            d2h_latency + (payload_f / d2h_bw) + h2d_latency + (payload_f / h2d_bw)
        )
        if estimated_touch < 0:
            negative_points.append(f"bounce(payload={payload_i},concurrency={fit_reference_concurrency})")
            if mode == "drop":
                continue
            if mode == "clamp_to_zero":
                estimated_touch = 0.0
            elif mode == "clamp_to_epsilon":
                estimated_touch = epsilon_s
        x_vals.append(payload_f)
        touch_vals.append(float(estimated_touch))

    negative_summary: Dict[str, object] = {
        "policy_mode": mode,
        "epsilon_s": epsilon_s,
        "negative_points_count": len(negative_points),
        "affected_paths": ["bounce"] if negative_points else [],
        "affected_points": negative_points,
        "action_applied": mode,
    }

    if len(x_vals) < 2:
        warnings.append("host_touch_fit_insufficient_points_using_stage_defaults")
        return (
            {
                "host_touch_Bps": float(fallback_stage_defaults["host_touch_Bps"]),
                "host_touch_fixed_s": float(fallback_stage_defaults["host_touch_fixed_s"]),
                "r2": 0.0,
                "mape_percent": 0.0,
                "n_points": len(x_vals),
                "fit_concurrency": int(fit_reference_concurrency),
                "status": "fallback_defaults",
            },
            warnings,
            negative_summary,
        )

    intercept, slope, r2 = _linear_fit(x_vals=x_vals, y_vals=touch_vals)
    slope = max(float(slope), 1e-15)
    bw = 1.0 / slope
    fixed = max(0.0, float(intercept))
    preds = [fixed + (x / bw) for x in x_vals]
    mape = (
        sum(abs((y - p) / max(y, 1e-12)) for y, p in zip(touch_vals, preds)) / len(touch_vals) * 100.0
        if touch_vals
        else 0.0
    )
    return (
        {
            "host_touch_Bps": float(bw),
            "host_touch_fixed_s": float(fixed),
            "r2": float(r2),
            "mape_percent": float(mape),
            "n_points": len(x_vals),
            "fit_concurrency": int(fit_reference_concurrency),
            "status": "derived_from_bounce",
        },
        warnings,
        negative_summary,
    )


def _compute_host_touch_sanity(
    *,
    host_touch_source: str,
    host_touch_fit: Mapping[str, float | int | str],
    host_touch_sanity_cfg: Mapping[str, object],
) -> Dict[str, object]:
    enabled = bool(host_touch_sanity_cfg["enabled"])
    ratio_min = float(host_touch_sanity_cfg["ratio_min"])
    ratio_max = float(host_touch_sanity_cfg["ratio_max"])
    expected_bw = host_touch_sanity_cfg.get("expected_bandwidth_Bps")
    warn_only_if_missing_reference = bool(host_touch_sanity_cfg["warn_only_if_missing_reference"])

    result: Dict[str, object] = {
        "enabled": enabled,
        "source": host_touch_source,
        "ratio_min": ratio_min,
        "ratio_max": ratio_max,
        "stream_methodology_note": "STREAM guidance: arrays >= 4x LLC sum (or >=1M elements).",
        "reference_source": "",
        "reference_bandwidth_Bps": None,
        "derived_bandwidth_Bps": float(host_touch_fit["host_touch_Bps"]),
        "ratio": None,
        "status": "disabled" if not enabled else "pass",
        "note": "",
    }
    if not enabled:
        return result

    if host_touch_source == "measured_stream":
        result["reference_source"] = "measured_stream"
        result["reference_bandwidth_Bps"] = float(host_touch_fit["host_touch_Bps"])
        result["ratio"] = 1.0
        result["status"] = "pass"
        result["note"] = "Host-touch fit used direct measured input."
        return result

    if expected_bw is None:
        result["reference_source"] = "none"
        result["status"] = "warn_no_reference" if warn_only_if_missing_reference else "fail_no_reference"
        result["note"] = "No measured host_touch input or expected_bandwidth_Bps reference provided."
        return result

    ref_bw = float(expected_bw)
    derived_bw = float(host_touch_fit["host_touch_Bps"])
    ratio = derived_bw / max(ref_bw, 1e-12)
    result["reference_source"] = "expected_bandwidth_Bps"
    result["reference_bandwidth_Bps"] = ref_bw
    result["ratio"] = ratio
    if ratio_min <= ratio <= ratio_max:
        result["status"] = "pass"
        result["note"] = "Derived host-touch bandwidth is within configured sanity range."
    else:
        result["status"] = "warn_out_of_range"
        result["note"] = "Derived host-touch bandwidth is outside configured sanity range."
    return result


def _pcie_one_way_ceiling_Bps(pcie_gen: int, lane_width: int) -> float:
    per_lane_gbps = {
        3: 0.985,
        4: 1.969,
        5: 3.938,
        6: 7.563,
    }
    if pcie_gen not in per_lane_gbps:
        raise ValueError(f"unsupported pcie_gen for ceiling_check: {pcie_gen}")
    if lane_width <= 0:
        raise ValueError("ceiling_check lane_width must be > 0")
    return per_lane_gbps[pcie_gen] * 1e9 * float(lane_width)


def _compute_ceiling_check(
    *,
    enabled: bool,
    path_fits: Mapping[str, Mapping[str, float | int | str]],
    ceiling_cfg: Mapping[str, object],
) -> Dict[str, object]:
    if not enabled:
        return {
            "enabled": False,
            "ceiling_check_pass": True,
            "ceiling_violation_paths": [],
            "ceiling_violation_notes": "disabled",
            "sanity_note": "One-way sanity check disabled.",
        }

    pcie_gen = int(ceiling_cfg["pcie_gen"])
    lane_width = int(ceiling_cfg["lane_width"])
    util = float(ceiling_cfg["max_one_way_utilization_fraction"])
    if util <= 0.0:
        raise ValueError("ceiling_check.max_one_way_utilization_fraction must be > 0")
    one_way_theoretical = _pcie_one_way_ceiling_Bps(pcie_gen=pcie_gen, lane_width=lane_width)
    threshold = one_way_theoretical * util

    violations: List[str] = []
    for path_name in ["host_h2d", "host_d2h"]:
        path_fit = path_fits.get(path_name, {})
        bw_raw = path_fit.get("bandwidth_Bps", 0.0)
        bw = float(bw_raw) if bw_raw is not None else 0.0
        if math.isfinite(bw) and bw > threshold:
            violations.append(path_name)

    pass_check = len(violations) == 0
    return {
        "enabled": True,
        "pcie_gen": pcie_gen,
        "lane_width": lane_width,
        "max_one_way_utilization_fraction": util,
        "one_way_theoretical_Bps": one_way_theoretical,
        "one_way_threshold_Bps": threshold,
        "ceiling_check_pass": pass_check,
        "ceiling_violation_paths": violations,
        "ceiling_violation_notes": (
            "" if pass_check else "Measured/fitted one-way bandwidth exceeded configured PCIe sanity threshold"
        ),
        "sanity_note": (
            "One-way sanity check derived from x16 bidirectional vendor table approximation / 2, "
            "scaled by utilization fraction."
        ),
    }


def _resolve_direct_status(
    *,
    direct_measured: bool,
    direct_policy: Mapping[str, object],
) -> str:
    if direct_measured:
        return "measured"
    if bool(direct_policy["allow_crosscheck_only"]):
        return "crosscheck_only"
    if bool(direct_policy["allow_cited_sweep_only"]):
        return "cited_sweep_only"
    raise ValueError(
        "direct path is unmeasured and both crosscheck_only and cited_sweep_only fallbacks are disabled"
    )


def run_calibration(config: Dict[str, object], out_dir: Path) -> Dict[str, object]:
    validation = ensure_validation_config(config)
    cal_cfg = ensure_calibration_config(validation)
    if not bool(cal_cfg.get("enabled", False)):
        return {"enabled": False, "reason": "validation.calibration.enabled=false"}

    payloads = [int(v) for v in cal_cfg["payload_bytes"]]
    conc_levels = [int(v) for v in cal_cfg["concurrency_levels"]]
    system_id = str(validation["system_id"])
    required_paths = [str(v) for v in cal_cfg["required_paths"]]
    optional_paths = [str(v) for v in cal_cfg["optional_paths"]]
    fit_reference_concurrency = int(cal_cfg["fit_reference_concurrency"])
    aggregate_stat = str(cal_cfg["aggregate_stat"])

    measured_df, loaded_paths, missing_paths, coverage_summary, semantics_summary = _load_measured_rows(
        system_id=system_id,
        payloads=payloads,
        conc_levels=conc_levels,
        measured_inputs=cal_cfg["measured_inputs"],
        required_paths=required_paths,
        optional_paths=optional_paths,
        required_points_min_samples=int(cal_cfg["required_points_min_samples"]),
        coverage_policy=cal_cfg["coverage_policy"],
        memory_mode_policy=cal_cfg["memory_mode_policy"],
    )

    measured_df = measured_df.copy()
    measured_df["simulated_s"] = measured_df.apply(
        lambda row: _sim_time_for_path(
            path=str(row["path"]),
            payload_bytes=int(row["payload_bytes"]),
            concurrency=int(row["concurrency"]),
            config=config,
        ),
        axis=1,
    )
    measured_df["error_s"] = measured_df["measured_s"] - measured_df["simulated_s"]
    measured_df["error_pct"] = measured_df.apply(
        lambda row: ((float(row["measured_s"]) / float(row["simulated_s"])) - 1.0) * 100.0
        if float(row["simulated_s"]) > 0
        else 0.0,
        axis=1,
    )

    agg_df = _build_aggregated_rows(
        measured_df=measured_df,
        aggregate_stat=aggregate_stat,
        config=config,
    )

    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / "microbench_raw.csv"
    agg_path = out_dir / "microbench_agg.csv"
    measured_df.to_csv(raw_path, index=False)
    agg_df.to_csv(agg_path, index=False)

    fit: Dict[str, object] = {
        "system_id": system_id,
        "input_mode": str(cal_cfg["input_mode"]),
        "fit_model": str(cal_cfg["fit_model"]),
        "aggregate_stat": aggregate_stat,
        "fit_reference_concurrency": fit_reference_concurrency,
        "calibration_status": {},
        "direct_status": "",
        "paths": {},
        "paths_loaded": loaded_paths,
        "paths_missing": sorted(missing_paths),
        "fit_warnings": [],
        "coverage_summary": coverage_summary,
        "measurement_semantics_summary": semantics_summary,
        "negative_residual_summary": {},
        "direct_status_note": "",
        "overlay": {},
    }

    path_fits: Dict[str, Dict[str, float | int | str]] = {}
    configured_inputs = set(cal_cfg["measured_inputs"].keys())
    transfer_paths = sorted(
        {
            path_name
            for path_name in (required_paths + optional_paths + ["direct"])
            if path_name in TRANSFER_CALIBRATION_PATHS
        }
    )

    for path_name in transfer_paths:
        has_measured = path_name in configured_inputs and path_name not in missing_paths
        if path_name == "direct" and not has_measured:
            direct_status = _resolve_direct_status(
                direct_measured=False,
                direct_policy=cal_cfg["direct_provenance_policy"],
            )
            fit["calibration_status"][path_name] = direct_status
            path_fits[path_name] = {
                "bandwidth_Bps": float("nan"),
                "latency_s": float("nan"),
                "r2": float("nan"),
                "mape_percent": float("nan"),
                "n_points": 0,
                "fit_concurrency": fit_reference_concurrency,
                "calibration_status": direct_status,
            }
            fit["direct_status"] = direct_status
            continue

        if not has_measured:
            if path_name in required_paths:
                raise ValueError(f"missing required measured calibration path: {path_name}")
            fit["fit_warnings"].append(f"optional path {path_name} missing; skipped")
            continue

        path_fit = _fit_path(
            path_name=path_name,
            agg_df=agg_df,
            fit_reference_concurrency=fit_reference_concurrency,
        )
        fit["calibration_status"][path_name] = "measured"
        path_fits[path_name] = path_fit
        if path_name == "direct":
            fit["direct_status"] = "measured"

    if "direct" not in fit["calibration_status"]:
        direct_measured = "direct" in path_fits and fit["calibration_status"].get("direct") == "measured"
        fit["direct_status"] = _resolve_direct_status(
            direct_measured=direct_measured,
            direct_policy=cal_cfg["direct_provenance_policy"],
        )
        fit["calibration_status"]["direct"] = fit["direct_status"]
        if not direct_measured:
            path_fits["direct"] = {
                "bandwidth_Bps": float("nan"),
                "latency_s": float("nan"),
                "r2": float("nan"),
                "mape_percent": float("nan"),
                "n_points": 0,
                "fit_concurrency": fit_reference_concurrency,
                "calibration_status": fit["direct_status"],
            }

    for required_path in ["host_h2d", "host_d2h", "bounce"]:
        if required_path not in path_fits:
            raise ValueError(f"required path fit missing after ingestion: {required_path}")

    host_touch_source = "derived_from_bounce"
    negative_summary: Dict[str, object] = {
        "policy_mode": str(cal_cfg["negative_residual_policy"]["mode"]),
        "epsilon_s": float(cal_cfg["negative_residual_policy"]["epsilon_s"]),
        "negative_points_count": 0,
        "affected_paths": [],
        "affected_points": [],
        "action_applied": str(cal_cfg["negative_residual_policy"]["mode"]),
    }
    if "host_touch" in configured_inputs and "host_touch" not in missing_paths:
        host_touch_fit, touch_warnings = _fit_host_touch_from_measured(
            agg_df=agg_df,
            fit_reference_concurrency=fit_reference_concurrency,
            fallback_stage_defaults=config["stage_defaults"],
        )
        host_touch_source = "measured_stream"
    else:
        host_touch_fit, touch_warnings, negative_summary = _fit_host_touch_from_bounce(
            payloads=payloads,
            fit_reference_concurrency=fit_reference_concurrency,
            agg_df=agg_df,
            path_fits=path_fits,
            fallback_stage_defaults=config["stage_defaults"],
            negative_residual_policy=cal_cfg["negative_residual_policy"],
        )
    fit["host_touch_source"] = host_touch_source
    fit["host_touch_fit"] = host_touch_fit
    fit["fit_warnings"].extend(touch_warnings)
    fit["negative_residual_summary"] = negative_summary

    host_touch_sanity = _compute_host_touch_sanity(
        host_touch_source=host_touch_source,
        host_touch_fit=host_touch_fit,
        host_touch_sanity_cfg=cal_cfg["host_touch_sanity"],
    )
    fit["host_touch_sanity"] = host_touch_sanity

    ceiling_cfg = cal_cfg["ceiling_check"]
    ceiling_result = _compute_ceiling_check(
        enabled=bool(ceiling_cfg["enabled"]),
        path_fits=path_fits,
        ceiling_cfg=ceiling_cfg,
    )
    fit["ceiling_check"] = ceiling_result
    if bool(ceiling_cfg.get("fail_on_violation", False)) and not bool(ceiling_result["ceiling_check_pass"]):
        violations = ", ".join(ceiling_result["ceiling_violation_paths"])
        raise ValueError(f"ceiling check violation for paths: {violations}")

    fit["direct_provenance_policy"] = dict(cal_cfg["direct_provenance_policy"])
    if fit["direct_status"] == "measured":
        fit["direct_status_note"] = "Direct path measured and calibrated from measured CSV."
    elif fit["direct_status"] == "crosscheck_only":
        fit["direct_status_note"] = (
            "Direct path unmeasured; validated via processor-share cross-check and not calibrated."
        )
    elif fit["direct_status"] == "cited_sweep_only":
        fit["direct_status_note"] = (
            "Direct path unmeasured; treated as cited+swept envelope (Melody latency/BW range)."
        )
    fit["direct_cited_envelope"] = {
        "latency_ns_range": list(cal_cfg["direct_provenance_policy"]["cited_latency_ns_range"]),
        "bandwidth_GBps_range": list(cal_cfg["direct_provenance_policy"]["cited_bandwidth_GBps_range"]),
        "switch_latency_ns": float(cal_cfg["direct_provenance_policy"]["cited_switch_latency_ns"]),
        "switch_hop_latency_ns_sweep": list(
            cal_cfg["direct_provenance_policy"]["cited_switch_hop_latency_ns_sweep"]
        ),
        "switch_bottleneck_factor_sweep": list(
            cal_cfg["direct_provenance_policy"]["cited_switch_bottleneck_factor_sweep"]
        ),
    }

    link_profile = config["link_profile"]
    direct_link_id = str(link_profile["cxl_direct_link"])

    overlay: Dict[str, object] = {
        "link_constant_overrides": {
            str(link_profile["host_h2d_link"]): {
                "bandwidth_Bps": float(path_fits["host_h2d"]["bandwidth_Bps"]),
                "latency_s": float(path_fits["host_h2d"]["latency_s"]),
            },
            str(link_profile["host_d2h_link"]): {
                "bandwidth_Bps": float(path_fits["host_d2h"]["bandwidth_Bps"]),
                "latency_s": float(path_fits["host_d2h"]["latency_s"]),
            },
        },
        "stage_defaults": {
            "host_touch_Bps": float(host_touch_fit["host_touch_Bps"]),
            "host_touch_fixed_s": float(host_touch_fit["host_touch_fixed_s"]),
        },
    }

    if fit["direct_status"] == "measured":
        overlay["link_constant_overrides"][direct_link_id] = {
            "bandwidth_Bps": float(path_fits["direct"]["bandwidth_Bps"]),
            "latency_s": float(path_fits["direct"]["latency_s"]),
        }
        overlay["cxl_direct_concurrency"] = {
            "dma_issue_fixed_s": float(
                max(
                    0.0,
                    float(path_fits["direct"]["latency_s"]) - float(sources.LINKS[direct_link_id]["latency_s"]),
                )
            )
        }

    fit["overlay"] = overlay
    fit["paths"] = path_fits

    fit_path = out_dir / "microbench_fit.yaml"
    save_yaml(fit_path, fit)
    overlay_path = out_dir / "microbench_overlay.yaml"
    save_yaml(overlay_path, overlay)

    return {
        "system_id": system_id,
        "raw_csv": str(raw_path),
        "agg_csv": str(agg_path),
        "fit_yaml": str(fit_path),
        "overlay_yaml": str(overlay_path),
        "calibration_status": fit["calibration_status"],
        "direct_status": fit["direct_status"],
    }


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config = load_yaml(Path(args.config))
    out_dir = Path(args.out)
    summary = run_calibration(config=config, out_dir=out_dir)
    print(f"Wrote {summary.get('raw_csv', '')}")
    print(f"Wrote {summary.get('agg_csv', '')}")
    print(f"Wrote {summary.get('fit_yaml', '')}")
    print(f"Wrote {summary.get('overlay_yaml', '')}")


if __name__ == "__main__":
    main()
