"""Run microbenchmark calibration fitting from measured CSV inputs."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Tuple

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import sources
from tools.validation.common import (
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


def _validate_measured_frame(
    *,
    df: pd.DataFrame,
    expected_path: str,
    system_id: str,
    payloads: Sequence[int],
    conc_levels: Sequence[int],
    required: bool,
) -> pd.DataFrame:
    required_cols = ["system_id", "path", "payload_bytes", "concurrency", "repetition", "time_s"]
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

    expected_pairs = {(int(p), int(c)) for p in payloads for c in conc_levels}
    observed_pairs = {
        (int(row.payload_bytes), int(row.concurrency))
        for row in normalized[["payload_bytes", "concurrency"]].itertuples(index=False)
    }
    missing_pairs = sorted(expected_pairs - observed_pairs)
    if required and missing_pairs:
        raise ValueError(
            f"measured CSV for {expected_path} missing payload/concurrency coverage: {missing_pairs}"
        )

    if "measured_s" in normalized.columns:
        normalized = normalized.drop(columns=["measured_s"])
    normalized = normalized.rename(columns={"time_s": "measured_s"})
    return normalized


def _load_measured_rows(
    *,
    system_id: str,
    payloads: Sequence[int],
    conc_levels: Sequence[int],
    measured_inputs: Mapping[str, str],
    required_paths: Sequence[str],
) -> Tuple[pd.DataFrame, Dict[str, str], List[str]]:
    loaded_paths: Dict[str, str] = {}
    missing_paths: List[str] = []
    frames: List[pd.DataFrame] = []

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
            payloads=payloads,
            conc_levels=conc_levels,
            required=(path_name in required_paths),
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
    return pd.concat(frames, ignore_index=True), loaded_paths, missing_paths


def _fit_path(
    *,
    path_name: str,
    measured_df: pd.DataFrame,
    fit_reference_concurrency: int,
    aggregate_stat: str,
) -> Tuple[Dict[str, float | int | str], pd.DataFrame]:
    fit_subset = measured_df[
        (measured_df["path"] == path_name) & (measured_df["concurrency"] == fit_reference_concurrency)
    ].copy()
    if fit_subset.empty:
        raise ValueError(
            f"no measured rows for {path_name} at fit_reference_concurrency={fit_reference_concurrency}"
        )

    grouped = (
        fit_subset.groupby("payload_bytes", as_index=False)["measured_s"]
        .agg(lambda values: _aggregate_series(values, aggregate_stat))
        .sort_values("payload_bytes")
    )
    x_vals = [float(v) for v in grouped["payload_bytes"].tolist()]
    y_vals = [float(v) for v in grouped["measured_s"].tolist()]
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
    return (
        {
            "bandwidth_Bps": float(bw),
            "latency_s": float(latency),
            "r2": float(r2),
            "mape_percent": float(mape),
            "n_points": len(x_vals),
            "fit_concurrency": int(fit_reference_concurrency),
            "calibration_status": "measured",
        },
        grouped,
    )


def _fit_host_touch(
    *,
    payloads: Sequence[int],
    fit_reference_concurrency: int,
    aggregate_stat: str,
    measured_df: pd.DataFrame,
    path_fits: Mapping[str, Mapping[str, float | int | str]],
    fallback_stage_defaults: Mapping[str, object],
) -> Tuple[Dict[str, float | int], List[str]]:
    warnings: List[str] = []
    fit_subset = measured_df[
        (measured_df["path"] == "bounce") & (measured_df["concurrency"] == fit_reference_concurrency)
    ].copy()
    grouped = (
        fit_subset.groupby("payload_bytes", as_index=False)["measured_s"]
        .agg(lambda values: _aggregate_series(values, aggregate_stat))
        .sort_values("payload_bytes")
    )

    x_vals: List[float] = []
    touch_vals: List[float] = []
    h2d_latency = float(path_fits["host_h2d"]["latency_s"])
    h2d_bw = float(path_fits["host_h2d"]["bandwidth_Bps"])
    d2h_latency = float(path_fits["host_d2h"]["latency_s"])
    d2h_bw = float(path_fits["host_d2h"]["bandwidth_Bps"])

    grouped_map = {
        int(row.payload_bytes): float(row.measured_s)
        for row in grouped[["payload_bytes", "measured_s"]].itertuples(index=False)
    }

    for payload in payloads:
        if int(payload) not in grouped_map:
            continue
        payload_f = float(payload)
        bounce_t = grouped_map[int(payload)]
        estimated_touch = bounce_t - (
            d2h_latency + (payload_f / d2h_bw) + h2d_latency + (payload_f / h2d_bw)
        )
        if estimated_touch < 0:
            warnings.append(
                f"host_touch_estimate_negative_at_payload_{payload}; clamped to zero"
            )
            estimated_touch = 0.0
        x_vals.append(payload_f)
        touch_vals.append(float(estimated_touch))

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
            "status": "measured_decomposition",
        },
        warnings,
    )


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
        }

    pcie_gen = int(ceiling_cfg["pcie_gen"])
    lane_width = int(ceiling_cfg["lane_width"])
    util = float(ceiling_cfg["max_one_way_utilization_fraction"])
    if util <= 0.0:
        raise ValueError("ceiling_check.max_one_way_utilization_fraction must be > 0")
    threshold = _pcie_one_way_ceiling_Bps(pcie_gen=pcie_gen, lane_width=lane_width) * util

    violations: List[str] = []
    for path_name in ["host_h2d", "host_d2h"]:
        path_fit = path_fits.get(path_name, {})
        bw = float(path_fit.get("bandwidth_Bps", 0.0))
        if bw > threshold:
            violations.append(path_name)

    pass_check = len(violations) == 0
    return {
        "enabled": True,
        "pcie_gen": pcie_gen,
        "lane_width": lane_width,
        "max_one_way_utilization_fraction": util,
        "one_way_theoretical_Bps": _pcie_one_way_ceiling_Bps(pcie_gen=pcie_gen, lane_width=lane_width),
        "one_way_threshold_Bps": threshold,
        "ceiling_check_pass": pass_check,
        "ceiling_violation_paths": violations,
        "ceiling_violation_notes": (
            "" if pass_check else "Measured/fitted one-way bandwidth exceeded configured PCIe ceiling threshold"
        ),
    }


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

    measured_df, loaded_paths, missing_paths = _load_measured_rows(
        system_id=system_id,
        payloads=payloads,
        conc_levels=conc_levels,
        measured_inputs=cal_cfg["measured_inputs"],
        required_paths=required_paths,
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

    out_dir.mkdir(parents=True, exist_ok=True)
    raw_path = out_dir / "microbench_raw.csv"
    measured_df.to_csv(raw_path, index=False)

    fit: Dict[str, object] = {
        "system_id": system_id,
        "input_mode": str(cal_cfg["input_mode"]),
        "fit_model": str(cal_cfg["fit_model"]),
        "aggregate_stat": aggregate_stat,
        "fit_reference_concurrency": fit_reference_concurrency,
        "calibration_status": {},
        "paths": {},
        "paths_loaded": loaded_paths,
        "paths_missing": sorted(missing_paths),
        "fit_warnings": [],
        "overlay": {},
    }

    path_fits: Dict[str, Dict[str, float | int | str]] = {}
    configured_inputs = set(cal_cfg["measured_inputs"].keys())
    for path_name in required_paths + optional_paths:
        if path_name in optional_paths and (
            path_name not in configured_inputs or path_name in missing_paths
        ):
            fit["calibration_status"][path_name] = "fallback_crosscheck"
            path_fits[path_name] = {
                "bandwidth_Bps": float("nan"),
                "latency_s": float("nan"),
                "r2": float("nan"),
                "mape_percent": float("nan"),
                "n_points": 0,
                "fit_concurrency": fit_reference_concurrency,
                "calibration_status": "fallback_crosscheck",
            }
            continue

        path_fit, _ = _fit_path(
            path_name=path_name,
            measured_df=measured_df,
            fit_reference_concurrency=fit_reference_concurrency,
            aggregate_stat=aggregate_stat,
        )
        fit["calibration_status"][path_name] = "measured"
        path_fits[path_name] = path_fit

    fit["paths"] = path_fits

    host_touch_fit, touch_warnings = _fit_host_touch(
        payloads=payloads,
        fit_reference_concurrency=fit_reference_concurrency,
        aggregate_stat=aggregate_stat,
        measured_df=measured_df,
        path_fits=path_fits,
        fallback_stage_defaults=config["stage_defaults"],
    )
    fit["host_touch_fit"] = host_touch_fit
    fit["fit_warnings"].extend(touch_warnings)

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

    if fit["calibration_status"].get("direct") == "measured":
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

    fit_path = out_dir / "microbench_fit.yaml"
    save_yaml(fit_path, fit)
    overlay_path = out_dir / "microbench_overlay.yaml"
    save_yaml(overlay_path, overlay)

    return {
        "system_id": system_id,
        "raw_csv": str(raw_path),
        "fit_yaml": str(fit_path),
        "overlay_yaml": str(overlay_path),
        "calibration_status": fit["calibration_status"],
    }


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config = load_yaml(Path(args.config))
    out_dir = Path(args.out)
    summary = run_calibration(config=config, out_dir=out_dir)
    print(f"Wrote {summary.get('raw_csv', '')}")
    print(f"Wrote {summary.get('fit_yaml', '')}")
    print(f"Wrote {summary.get('overlay_yaml', '')}")


if __name__ == "__main__":
    main()
