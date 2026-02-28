"""Sensitivity sweeps and ablation exports for validation appendices."""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path
from typing import Dict, List, Mapping, Sequence, Tuple

import pandas as pd
import yaml

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import sources
from simulator import generate_runs_from_config
from tools.validation.common import deep_merge, ensure_validation_config, load_yaml


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run sensitivity sweeps and ablations.")
    parser.add_argument("--config", default="configs/runs.yaml", help="Path to run config YAML.")
    parser.add_argument("--out", default="artifacts/validation", help="Validation output directory.")
    parser.add_argument(
        "--ablations-config",
        default="paper/configs/ablations.yaml",
        help="Path to ablations config file.",
    )
    return parser.parse_args(argv)


def _build_links_catalog(
    *,
    base_links_catalog: Mapping[str, Mapping[str, object]] | None,
    overrides: Mapping[str, Mapping[str, float]] | None,
) -> Dict[str, Dict[str, object]]:
    source_catalog = sources.LINKS if base_links_catalog is None else base_links_catalog
    links_catalog: Dict[str, Dict[str, object]] = {
        str(link_id): dict(link_cfg) for link_id, link_cfg in source_catalog.items()
    }
    if not overrides:
        return links_catalog
    for link_id, patch in overrides.items():
        if link_id not in links_catalog:
            raise ValueError(f"unknown link id for override: {link_id}")
        updated = dict(links_catalog[link_id])
        updated.update(dict(patch))
        links_catalog[link_id] = updated
    return links_catalog


def _compute_ratios(metrics_rows: List[Dict[str, object]]) -> pd.DataFrame:
    df = pd.DataFrame(metrics_rows)
    df["stage_size_multiplier"] = pd.to_numeric(df["stage_size_multiplier"], errors="coerce")
    keep_cols = ["dataset_profile", "workload_family", "workload_variant", "stage_size_multiplier", "scenario"]
    pivot = df.pivot_table(
        index=keep_cols[:-1],
        columns="scenario",
        values=["makespan_s", "total_energy_J", "dominant_lb_component"],
        aggfunc="first",
    )
    rows: List[Dict[str, object]] = []
    for idx, row in pivot.iterrows():
        profile, family, variant, mult = idx
        bounce_m = row.get(("makespan_s", sources.SCENARIO_PIM_HOST_BOUNCE))
        direct_m = row.get(("makespan_s", sources.SCENARIO_PIM_FLOWCXL_DIRECT))
        cpu_m = row.get(("makespan_s", sources.SCENARIO_CPU_ONLY))
        bounce_e = row.get(("total_energy_J", sources.SCENARIO_PIM_HOST_BOUNCE))
        direct_e = row.get(("total_energy_J", sources.SCENARIO_PIM_FLOWCXL_DIRECT))
        cpu_e = row.get(("total_energy_J", sources.SCENARIO_CPU_ONLY))
        rows.append(
            {
                "workload_profile": profile,
                "workload_family": family,
                "workload_variant": variant,
                "multiplier": float(mult),
                "bounce_over_direct_makespan": float(bounce_m) / float(direct_m)
                if pd.notna(bounce_m) and pd.notna(direct_m) and float(direct_m) > 0
                else float("nan"),
                "bounce_over_direct_energy": float(bounce_e) / float(direct_e)
                if pd.notna(bounce_e) and pd.notna(direct_e) and float(direct_e) > 0
                else float("nan"),
                "cpu_over_direct_makespan": float(cpu_m) / float(direct_m)
                if pd.notna(cpu_m) and pd.notna(direct_m) and float(direct_m) > 0
                else float("nan"),
                "cpu_over_direct_energy": float(cpu_e) / float(direct_e)
                if pd.notna(cpu_e) and pd.notna(direct_e) and float(direct_e) > 0
                else float("nan"),
                "dominant_lb_bounce": row.get(("dominant_lb_component", sources.SCENARIO_PIM_HOST_BOUNCE), ""),
                "dominant_lb_direct": row.get(("dominant_lb_component", sources.SCENARIO_PIM_FLOWCXL_DIRECT), ""),
            }
        )
    return pd.DataFrame(rows)


def _apply_tpch_memory_preset(config: Dict[str, object], preset: str) -> Dict[str, object]:
    cfg = copy.deepcopy(config)
    tpch_stages = cfg["memory_system_by_template"]["tpch_3op"]["cpu_baseline_system"]["stages"]
    if preset == "baseline":
        return cfg
    if preset == "pessimistic":
        scales = {"row_hit_rate": 0.75, "mlp": 0.7, "avg_miss_latency_ns": 1.25}
    elif preset == "optimistic":
        scales = {"row_hit_rate": 1.1, "mlp": 1.25, "avg_miss_latency_ns": 0.8}
    else:
        raise ValueError(f"unknown tpch memory preset {preset}")
    for stage_name, stage_cfg in tpch_stages.items():
        row_hit = min(0.999, max(0.01, float(stage_cfg["row_hit_rate"]) * scales["row_hit_rate"]))
        mlp = max(1.0, float(stage_cfg["mlp"]) * scales["mlp"])
        miss_lat = max(1.0, float(stage_cfg["avg_miss_latency_ns"]) * scales["avg_miss_latency_ns"])
        stage_cfg["row_hit_rate"] = row_hit
        stage_cfg["mlp"] = mlp
        stage_cfg["avg_miss_latency_ns"] = miss_lat
    return cfg


def _apply_global_pim_speedup(config: Dict[str, object], factor: float) -> Dict[str, object]:
    cfg = copy.deepcopy(config)
    maps = cfg["pim_speedup_vs_cpu_by_stage_by_template"]
    for template in maps:
        for stage in maps[template]:
            maps[template][stage] = float(maps[template][stage]) * float(factor)
    return cfg


def _run_case(
    *,
    base_config: Dict[str, object],
    base_links_catalog: Mapping[str, Mapping[str, object]] | None = None,
    patch: Mapping[str, object] | None = None,
    link_overrides: Mapping[str, Mapping[str, float]] | None = None,
) -> pd.DataFrame:
    cfg = copy.deepcopy(base_config)
    cfg["workload_variants"] = [{"name": "base", "overrides": {}}]
    cfg["trace_max_tiles"] = 0
    if patch:
        cfg = deep_merge(cfg, patch)
    links_catalog = _build_links_catalog(
        base_links_catalog=base_links_catalog,
        overrides=link_overrides,
    )
    metrics, _ = generate_runs_from_config(cfg, links_catalog=links_catalog)
    return _compute_ratios(metrics)


def _apply_glue_fixed_cost(config: Dict[str, object], fixed_s: float) -> Dict[str, object]:
    cfg = copy.deepcopy(config)
    models = cfg.get("tiling_model_by_template", {})
    for model in models.values():
        if not isinstance(model, Mapping):
            continue
        boundary_mappings = model.get("boundary_mappings", {})
        if not isinstance(boundary_mappings, Mapping):
            continue
        for mapping in boundary_mappings.values():
            if not isinstance(mapping, Mapping):
                continue
            if str(mapping.get("mapping_type", "IDENTITY")) != "IDENTITY":
                mapping["glue_fixed_s"] = float(fixed_s)
    return cfg


def _apply_glue_roofline_factor(config: Dict[str, object], factor: float) -> Dict[str, object]:
    cfg = copy.deepcopy(config)
    base_bw = float(cfg["stage_defaults"]["host_touch_Bps"])
    models = cfg.get("tiling_model_by_template", {})
    for model in models.values():
        if not isinstance(model, Mapping):
            continue
        boundary_mappings = model.get("boundary_mappings", {})
        if not isinstance(boundary_mappings, Mapping):
            continue
        for mapping in boundary_mappings.values():
            if not isinstance(mapping, Mapping):
                continue
            if str(mapping.get("mapping_type", "IDENTITY")) == "IDENTITY":
                continue
            if "glue_compute_Bps" in mapping:
                mapping["glue_compute_Bps"] = max(1.0, base_bw * float(factor))
            if "glue_mem_Bps" in mapping:
                mapping["glue_mem_Bps"] = max(1.0, base_bw * float(factor))
    return cfg


def _apply_pim_mode_effects_scale(config: Dict[str, object], factor: float) -> Dict[str, object]:
    cfg = copy.deepcopy(config)
    effects = cfg.get("pim_mode_effects", {})
    for mode_name, mode_cfg in effects.items():
        if str(mode_name) in {"NONE"}:
            continue
        if not isinstance(mode_cfg, Mapping):
            continue
        if "compute_multiplier" in mode_cfg:
            mode_cfg["compute_multiplier"] = max(0.01, float(mode_cfg["compute_multiplier"]) * float(factor))
        if "mem_multiplier" in mode_cfg:
            mode_cfg["mem_multiplier"] = max(0.01, float(mode_cfg["mem_multiplier"]) * float(factor))
        if "command_overhead_s" in mode_cfg:
            mode_cfg["command_overhead_s"] = max(0.0, float(mode_cfg["command_overhead_s"]) * float(factor))
    return cfg


def run_sensitivity(
    config: Dict[str, object],
    links_catalog: Mapping[str, Mapping[str, object]] | None,
    out_dir: Path,
    ablations_config_path: Path,
) -> Dict[str, object]:
    validation = ensure_validation_config(config)
    sens_cfg = validation["sensitivity"]
    energy_cfg = validation["energy"]
    if not bool(sens_cfg.get("enabled", False)):
        return {"enabled": False, "reason": "validation.sensitivity.enabled=false"}
    system_id = str(validation["system_id"])

    records: List[Dict[str, object]] = []

    enabled_families = {
        str(v) for v in sens_cfg.get("families", ["cxl_link", "pim_speedup", "tpch_memory"])
    }
    baseline_df = _run_case(base_config=config, base_links_catalog=links_catalog)
    for _, row in baseline_df.iterrows():
        record = dict(row)
        record.update({"system_id": system_id, "sweep_family": "baseline", "sweep_case": "baseline"})
        records.append(record)

    if "cxl_link" in enabled_families:
        cxl_bw_values = [32e9, 64e9, 96e9]
        cxl_lat_values = [250e-9, 350e-9, 500e-9]
        for bw in cxl_bw_values:
            for lat in cxl_lat_values:
                case = f"bw_{int(bw)}__lat_{lat:.2e}"
                link_overrides = {sources.LINK_CXL_SWITCH: {"bandwidth_Bps": bw, "latency_s": lat}}
                patch = {"link_profile": {"cxl_direct_link": sources.LINK_CXL_SWITCH}}
                case_df = _run_case(
                    base_config=config,
                    base_links_catalog=links_catalog,
                    patch=patch,
                    link_overrides=link_overrides,
                )
                for _, row in case_df.iterrows():
                    rec = dict(row)
                    rec.update({"system_id": system_id, "sweep_family": "cxl_link", "sweep_case": case})
                    records.append(rec)

    if "pim_speedup" in enabled_families:
        for factor in [0.75, 1.0, 1.25, 1.5]:
            case_df = _run_case(
                base_config=_apply_global_pim_speedup(config, factor),
                base_links_catalog=links_catalog,
            )
            for _, row in case_df.iterrows():
                rec = dict(row)
                rec.update(
                    {
                        "system_id": system_id,
                        "sweep_family": "pim_speedup",
                        "sweep_case": f"factor_{factor:.2f}",
                    }
                )
                records.append(rec)

    if "tpch_memory" in enabled_families:
        for preset in ["pessimistic", "baseline", "optimistic"]:
            case_df = _run_case(
                base_config=_apply_tpch_memory_preset(config, preset),
                base_links_catalog=links_catalog,
            )
            for _, row in case_df.iterrows():
                rec = dict(row)
                rec.update({"system_id": system_id, "sweep_family": "tpch_memory", "sweep_case": preset})
                records.append(rec)

    if "energy" in enabled_families:
        for scale in [float(v) for v in energy_cfg.get("power_scale_factors", [1.0])]:
            patch = {
                "transfer_power_W": {
                    key: float(val) * scale
                    for key, val in config["transfer_power_W"].items()
                },
                "stage_defaults": {
                    "cpu_unit_power_W": float(config["stage_defaults"]["cpu_unit_power_W"]) * scale,
                    "pim_unit_power_W": float(config["stage_defaults"]["pim_unit_power_W"]) * scale,
                },
            }
            case_df = _run_case(base_config=config, base_links_catalog=links_catalog, patch=patch)
            for _, row in case_df.iterrows():
                rec = dict(row)
                rec.update({"system_id": system_id, "sweep_family": "energy", "sweep_case": f"scale_{scale:.2f}"})
                records.append(rec)

    if "glue_fixed_cost" in enabled_families:
        for fixed_s in [0.05e-6, 0.1e-6, 0.5e-6, 1e-6, 5e-6]:
            case_df = _run_case(
                base_config=_apply_glue_fixed_cost(config, fixed_s),
                base_links_catalog=links_catalog,
            )
            for _, row in case_df.iterrows():
                rec = dict(row)
                rec.update(
                    {
                        "system_id": system_id,
                        "sweep_family": "glue_fixed_cost",
                        "sweep_case": f"fixed_s_{fixed_s:.2e}",
                    }
                )
                records.append(rec)

    if "glue_roofline_factor" in enabled_families:
        for factor in [0.2, 0.5, 1.0, 1.5, 2.0]:
            case_df = _run_case(
                base_config=_apply_glue_roofline_factor(config, factor),
                base_links_catalog=links_catalog,
            )
            for _, row in case_df.iterrows():
                rec = dict(row)
                rec.update(
                    {
                        "system_id": system_id,
                        "sweep_family": "glue_roofline_factor",
                        "sweep_case": f"factor_{factor:.2f}",
                    }
                )
                records.append(rec)

    if "pim_mode_effects_scale" in enabled_families:
        for factor in [0.6, 0.8, 1.0, 1.2, 1.4]:
            case_df = _run_case(
                base_config=_apply_pim_mode_effects_scale(config, factor),
                base_links_catalog=links_catalog,
            )
            for _, row in case_df.iterrows():
                rec = dict(row)
                rec.update(
                    {
                        "system_id": system_id,
                        "sweep_family": "pim_mode_effects_scale",
                        "sweep_case": f"factor_{factor:.2f}",
                    }
                )
                records.append(rec)

    out_dir.mkdir(parents=True, exist_ok=True)
    sensitivity_df = pd.DataFrame(records)
    sensitivity_path = out_dir / "sensitivity_results.csv"
    sensitivity_df.to_csv(sensitivity_path, index=False)

    target_profile = sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE
    target = sensitivity_df[
        (sensitivity_df["workload_profile"] == target_profile)
        & (sensitivity_df["workload_variant"] == "base")
        & (sensitivity_df["multiplier"] == 1.0)
    ].copy()
    baseline_row = target[(target["sweep_family"] == "baseline") & (target["sweep_case"] == "baseline")].iloc[0]
    base_m = float(baseline_row["bounce_over_direct_makespan"])
    base_e = float(baseline_row["bounce_over_direct_energy"])
    target["delta_makespan"] = target["bounce_over_direct_makespan"].astype(float) - base_m
    target["delta_energy"] = target["bounce_over_direct_energy"].astype(float) - base_e
    target["effect_score"] = target[["delta_makespan", "delta_energy"]].abs().max(axis=1)
    target["knob"] = target["sweep_family"].astype(str) + "::" + target["sweep_case"].astype(str)
    tornado_df = (
        target[target["sweep_family"] != "baseline"]
        .sort_values("effect_score", ascending=False)
        .head(8)[["knob", "sweep_family", "sweep_case", "effect_score", "delta_makespan", "delta_energy"]]
    )
    tornado_path = out_dir / "tornado_top8.csv"
    tornado_df.to_csv(tornado_path, index=False)

    ablations_path = out_dir / "ablations.csv"
    _run_ablations(
        base_config=config,
        links_catalog=links_catalog,
        ablations_config_path=ablations_config_path,
        out_path=ablations_path,
        system_id=system_id,
    )

    return {
        "system_id": system_id,
        "sensitivity_csv": str(sensitivity_path),
        "tornado_csv": str(tornado_path),
        "ablations_csv": str(ablations_path),
    }


def _run_ablations(
    *,
    base_config: Dict[str, object],
    links_catalog: Mapping[str, Mapping[str, object]] | None,
    ablations_config_path: Path,
    out_path: Path,
    system_id: str,
) -> None:
    if not ablations_config_path.exists():
        raise FileNotFoundError(f"ablations config not found: {ablations_config_path}")
    with ablations_config_path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict) or "ablations" not in payload:
        raise ValueError("ablations config must include top-level 'ablations' list")

    rows: List[Dict[str, object]] = []
    for entry in payload["ablations"]:
        name = str(entry["name"])
        patch = entry.get("overrides", {})
        cfg = deep_merge(base_config, patch) if patch else copy.deepcopy(base_config)
        cfg["trace_max_tiles"] = 0
        metrics, _ = generate_runs_from_config(cfg, links_catalog=links_catalog)
        ratio_df = _compute_ratios(metrics)
        one_x = ratio_df[ratio_df["multiplier"] == 1.0]
        for _, row in one_x.iterrows():
            rec = dict(row)
            rec.update({"system_id": system_id, "ablation": name})
            rows.append(rec)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(out_path, index=False)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config = load_yaml(Path(args.config))
    summary = run_sensitivity(
        config=config,
        links_catalog=None,
        out_dir=Path(args.out),
        ablations_config_path=Path(args.ablations_config),
    )
    print(f"Wrote {summary.get('sensitivity_csv', '')}")
    print(f"Wrote {summary.get('tornado_csv', '')}")
    print(f"Wrote {summary.get('ablations_csv', '')}")


if __name__ == "__main__":
    main()
