"""Generate synthetic microbenchmark calibration artifacts for the simulator."""

from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import pandas as pd

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import sources
from tools.validation.common import ensure_validation_config, load_yaml, save_yaml


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


def _deterministic_measured_time(
    *,
    simulated_s: float,
    path: str,
    payload_bytes: int,
    concurrency: int,
    repetition: int,
) -> float:
    path_bias = {
        "host_h2d": 0.015,
        "host_d2h": 0.025,
        "bounce": 0.030,
        "direct": 0.012,
    }[path]
    signal = math.sin(
        float(payload_bytes) / 10_000_000.0
        + (0.37 * float(concurrency))
        + (0.91 * float(repetition))
        + (0.23 * float(len(path)))
    )
    jitter = 0.006 * signal
    measured = simulated_s * (1.0 + path_bias + jitter)
    return max(measured, simulated_s * 0.5)


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


def run_calibration(config: Dict[str, object], out_dir: Path) -> Dict[str, object]:
    validation = ensure_validation_config(config)
    cal_cfg = validation["calibration"]
    if not bool(cal_cfg.get("enabled", False)):
        return {"enabled": False, "reason": "validation.calibration.enabled=false"}

    payloads = [int(v) for v in cal_cfg["payload_bytes"]]
    conc_levels = [int(v) for v in cal_cfg["concurrency_levels"]]
    repetitions = int(cal_cfg["repetitions"])
    paths = [str(v) for v in cal_cfg["paths"]]
    system_id = str(validation["system_id"])

    rows: List[Dict[str, object]] = []
    for path in paths:
        for payload in payloads:
            for conc in conc_levels:
                for rep in range(repetitions):
                    sim_s = _sim_time_for_path(
                        path=path,
                        payload_bytes=payload,
                        concurrency=conc,
                        config=config,
                    )
                    measured_s = _deterministic_measured_time(
                        simulated_s=sim_s,
                        path=path,
                        payload_bytes=payload,
                        concurrency=conc,
                        repetition=rep,
                    )
                    rows.append(
                        {
                            "system_id": system_id,
                            "path": path,
                            "payload_bytes": payload,
                            "concurrency": conc,
                            "repetition": rep,
                            "simulated_s": sim_s,
                            "measured_s": measured_s,
                            "error_s": measured_s - sim_s,
                            "error_pct": ((measured_s / sim_s) - 1.0) * 100.0 if sim_s > 0 else 0.0,
                        }
                    )

    out_dir.mkdir(parents=True, exist_ok=True)
    raw_df = pd.DataFrame(rows)
    raw_path = out_dir / "microbench_raw.csv"
    raw_df.to_csv(raw_path, index=False)

    fit: Dict[str, object] = {"system_id": system_id, "paths": {}, "overlay": {}}
    for path in paths:
        subset = raw_df[(raw_df["path"] == path) & (raw_df["concurrency"] == 1)]
        grouped = subset.groupby("payload_bytes", as_index=False)["measured_s"].median()
        x_vals = [float(v) for v in grouped["payload_bytes"].tolist()]
        y_vals = [float(v) for v in grouped["measured_s"].tolist()]
        intercept, slope, r2 = _linear_fit(x_vals=x_vals, y_vals=y_vals)
        slope = max(slope, 1e-15)
        bw = 1.0 / slope
        latency = max(0.0, intercept)
        preds = [latency + (x / bw) for x in x_vals]
        mape = (
            sum(abs((y - p) / max(y, 1e-12)) for y, p in zip(y_vals, preds)) / len(y_vals) * 100.0
            if y_vals
            else 0.0
        )
        fit["paths"][path] = {
            "bandwidth_Bps": float(bw),
            "latency_s": float(latency),
            "r2": float(r2),
            "mape_percent": float(mape),
            "n_points": len(x_vals),
        }

    link_profile = config["link_profile"]
    direct_link_id = str(link_profile["cxl_direct_link"])
    fit["overlay"] = {
        "link_constant_overrides": {
            str(link_profile["host_h2d_link"]): {
                "bandwidth_Bps": float(fit["paths"]["host_h2d"]["bandwidth_Bps"]),
                "latency_s": float(fit["paths"]["host_h2d"]["latency_s"]),
            },
            str(link_profile["host_d2h_link"]): {
                "bandwidth_Bps": float(fit["paths"]["host_d2h"]["bandwidth_Bps"]),
                "latency_s": float(fit["paths"]["host_d2h"]["latency_s"]),
            },
            direct_link_id: {
                "bandwidth_Bps": float(fit["paths"]["direct"]["bandwidth_Bps"]),
                "latency_s": float(fit["paths"]["direct"]["latency_s"]),
            },
        },
        "cxl_direct_concurrency": {
            "dma_issue_fixed_s": float(
                max(
                    0.0,
                    float(fit["paths"]["direct"]["latency_s"])
                    - float(sources.LINKS[direct_link_id]["latency_s"]),
                )
            )
        },
    }

    fit_path = out_dir / "microbench_fit.yaml"
    save_yaml(fit_path, fit)
    overlay_path = out_dir / "microbench_overlay.yaml"
    save_yaml(overlay_path, fit["overlay"])

    return {
        "system_id": system_id,
        "raw_csv": str(raw_path),
        "fit_yaml": str(fit_path),
        "overlay_yaml": str(overlay_path),
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
