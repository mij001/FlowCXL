"""Reporting for tiled stage-capacity FlowCXL experiments."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import matplotlib.pyplot as plt
import pandas as pd

import sources

SCENARIO_ORDER = [
    sources.SCENARIO_CPU_ONLY,
    sources.SCENARIO_PIM_HOST_BOUNCE,
    sources.SCENARIO_PIM_FLOWCXL_DIRECT,
]
PIM_ONLY_SCENARIO_ORDER = [
    sources.SCENARIO_PIM_HOST_BOUNCE,
    sources.SCENARIO_PIM_FLOWCXL_DIRECT,
]

SCENARIO_LABELS = {
    sources.SCENARIO_CPU_ONLY: "CPU only",
    sources.SCENARIO_PIM_HOST_BOUNCE: "PIM host bounce",
    sources.SCENARIO_PIM_FLOWCXL_DIRECT: "PIM FlowCXL direct",
}


def _build_markdown_table(table_df: pd.DataFrame) -> str:
    header = "| " + " | ".join(table_df.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(table_df.columns)) + " |"
    rows: List[str] = []
    for _, row in table_df.iterrows():
        rows.append("| " + " | ".join(str(row[col]) for col in table_df.columns) + " |")
    return "\n".join([header, separator] + rows)


def _format_multiplier(value: float) -> str:
    return f"{float(value):g}x"


def _sanitize_token(value: object) -> str:
    token = str(value)
    for ch in [" ", "/", "\\", ":", "|", "*", "?", "\"", "<", ">"]:
        token = token.replace(ch, "_")
    token = token.replace(".", "p")
    return token


def _ensure_workload_columns(metrics_df: pd.DataFrame) -> pd.DataFrame:
    out = metrics_df.copy()
    if "workload_profile" not in out.columns:
        out["workload_profile"] = out.get("dataset_profile", "")
    if "workload_variant" not in out.columns:
        out["workload_variant"] = "base"
    if "deepvariant_mode" not in out.columns:
        out["deepvariant_mode"] = "new"
    if "workload_family" not in out.columns:
        out["workload_family"] = out.get("pipeline_template", "").map(
            lambda x: "deepvariant" if x == sources.PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE else "tpch"
        )
    if "baseline_id" not in out.columns:
        out["baseline_id"] = (
            out["workload_family"].astype(str)
            + "|"
            + out["workload_profile"].astype(str)
            + "|"
            + out["workload_variant"].astype(str)
            + "|"
            + out["deepvariant_mode"].astype(str)
            + "|m"
            + out["stage_size_multiplier"].astype(str)
        )
    return out


def _plot_grouped_metric(
    subset: pd.DataFrame,
    metric_col: str,
    ylabel: str,
    title: str,
    output_path: Path,
    scenario_order: List[str] | None = None,
) -> None:
    if subset.empty:
        return
    selected_scenarios = list(scenario_order or SCENARIO_ORDER)
    use = subset.copy()
    use["stage_size_multiplier"] = pd.to_numeric(use["stage_size_multiplier"], errors="coerce")
    use = use.dropna(subset=["stage_size_multiplier"])
    use = use.sort_values("stage_size_multiplier")
    pivot = use.pivot_table(
        index="stage_size_multiplier",
        columns="scenario",
        values=metric_col,
        aggfunc="first",
    )
    pivot = pivot.reindex(columns=selected_scenarios)
    pivot = pivot.rename(columns=SCENARIO_LABELS)

    ax = pivot.plot(kind="bar", figsize=(10, 5), rot=0)
    ax.set_xlabel("Stage data size multiplier")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticklabels([_format_multiplier(value) for value in pivot.index])
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Scenario")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _build_ratio_wide(subset: pd.DataFrame, value_col: str) -> pd.DataFrame:
    use = subset.copy()
    use["stage_size_multiplier"] = pd.to_numeric(use["stage_size_multiplier"], errors="coerce")
    use = use.dropna(subset=["stage_size_multiplier"])
    pivot = use.pivot_table(
        index="stage_size_multiplier",
        columns="scenario",
        values=value_col,
        aggfunc="first",
    )
    required = [sources.SCENARIO_PIM_HOST_BOUNCE, sources.SCENARIO_PIM_FLOWCXL_DIRECT]
    if any(col not in pivot.columns for col in required):
        return pd.DataFrame()

    bounce = pivot[sources.SCENARIO_PIM_HOST_BOUNCE]
    direct = pivot[sources.SCENARIO_PIM_FLOWCXL_DIRECT]
    result = pd.DataFrame(index=pivot.index)
    result["direct_over_bounce"] = direct / bounce

    if sources.SCENARIO_CPU_ONLY in pivot.columns:
        result["cpu_over_bounce"] = pivot[sources.SCENARIO_CPU_ONLY] / bounce
    else:
        result["cpu_over_bounce"] = float("nan")

    result["bounce_over_bounce"] = 1.0
    return result.sort_index()


def _plot_direct_over_bounce_ratio(
    ratio_df: pd.DataFrame,
    ylabel: str,
    title: str,
    output_path: Path,
) -> None:
    if ratio_df.empty:
        return
    series = ratio_df[["direct_over_bounce"]].rename(columns={"direct_over_bounce": "Direct/Bounce"})
    ax = series.plot(kind="bar", figsize=(10, 5), rot=0)
    ax.axhline(1.0, color="black", linewidth=1.0, linestyle="--")
    ax.set_xlabel("Stage data size multiplier")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticklabels([_format_multiplier(value) for value in series.index])
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_norm_to_bounce_ratio(
    ratio_df: pd.DataFrame,
    ylabel: str,
    title: str,
    output_path: Path,
) -> None:
    if ratio_df.empty:
        return
    series = ratio_df[["cpu_over_bounce", "bounce_over_bounce", "direct_over_bounce"]].rename(
        columns={
            "cpu_over_bounce": "CPU/Bounce",
            "bounce_over_bounce": "Bounce/Bounce",
            "direct_over_bounce": "Direct/Bounce",
        }
    )
    ax = series.plot(kind="bar", figsize=(10, 5), rot=0)
    ax.axhline(1.0, color="black", linewidth=1.0, linestyle="--")
    ax.set_xlabel("Stage data size multiplier")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticklabels([_format_multiplier(value) for value in series.index])
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Ratio")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _format_metric_fields(table_df: pd.DataFrame) -> pd.DataFrame:
    out = table_df.copy()
    numeric_cols = [
        "makespan_s",
        "total_energy_J",
        "host_touch_energy_J",
        "cpu_materialize_energy_J",
        "total_compute_time_component_s",
        "total_cpu_mem_time_component_s",
        "total_cpu_mem_latency_bound_time_component_s",
        "total_cpu_mem_peak_bound_time_component_s",
        "total_cpu_mem_service_time_component_s",
        "total_cpu_mem_queue_delay_component_s",
        "total_pim_mem_time_component_s",
        "total_pim_mem_service_time_component_s",
        "total_pim_mem_queue_delay_component_s",
        "total_cpu_materialize_time_component_s",
        "total_retain_handoff_time_component_s",
        "total_cxl_dma_issue_time_component_s",
        "lb_compute_stage_max_s",
        "lb_host_h2d_ingress_s",
        "lb_host_h2d_stage_s",
        "lb_host_d2h_s",
        "lb_host_link_s",
        "lb_host_touch_s",
        "lb_cxl_direct_s",
        "total_bytes_pim_retained",
        "total_retain_fallback_bytes",
        "cxl_direct_stream_slots",
        "cxl_active_direct_endpoints",
        "cxl_effective_striping_factor",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = out[col].map(lambda value: f"{float(value):.6f}")
    return out


def _default_view(metrics_df: pd.DataFrame) -> pd.DataFrame:
    view = metrics_df.copy()
    if "workload_variant" in view.columns:
        view = view[view["workload_variant"] == "base"]
    if "deepvariant_mode" in view.columns:
        view = view[view["deepvariant_mode"] == "new"]
    return view


def _directional_narrative(metrics_df: pd.DataFrame) -> str:
    lines: List[str] = []
    group_cols = ["workload_profile", "workload_variant", "deepvariant_mode"]
    for _, group_row in metrics_df[group_cols].drop_duplicates().sort_values(group_cols).iterrows():
        mask = (
            (metrics_df["workload_profile"] == group_row["workload_profile"])
            & (metrics_df["workload_variant"] == group_row["workload_variant"])
            & (metrics_df["deepvariant_mode"] == group_row["deepvariant_mode"])
        )
        subset = metrics_df[mask]
        ratios: List[float] = []
        for multiplier in sorted(subset["stage_size_multiplier"].unique()):
            bounce = subset[
                (subset["scenario"] == sources.SCENARIO_PIM_HOST_BOUNCE)
                & (subset["stage_size_multiplier"] == multiplier)
            ]
            direct = subset[
                (subset["scenario"] == sources.SCENARIO_PIM_FLOWCXL_DIRECT)
                & (subset["stage_size_multiplier"] == multiplier)
            ]
            if bounce.empty or direct.empty:
                continue
            ratios.append(float(direct.iloc[0]["makespan_s"]) / float(bounce.iloc[0]["makespan_s"]))
        if not ratios:
            continue
        directional_ok = all(value <= 1.0 + 1e-12 for value in ratios)
        lines.append(
            f"- {group_row['workload_profile']} ({group_row['workload_variant']}, {group_row['deepvariant_mode']}): "
            f"direct<=bounce `{str(directional_ok).lower()}`, direct/bounce range `{min(ratios):.6f}` to `{max(ratios):.6f}`."
        )
    return "\n".join(lines) if lines else "- No groups with both bounce and direct scenarios."


def _tpch_target_narrative(metrics_df: pd.DataFrame) -> str:
    subset = _default_view(metrics_df)
    subset = subset[
        (subset["workload_profile"] == sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE)
        & (subset["stage_size_multiplier"] == 1.0)
    ]
    if subset.empty:
        return "- TPC-H high profile 1x target unavailable."

    bounce = subset[subset["scenario"] == sources.SCENARIO_PIM_HOST_BOUNCE]
    direct = subset[subset["scenario"] == sources.SCENARIO_PIM_FLOWCXL_DIRECT]
    if bounce.empty or direct.empty:
        return "- TPC-H high profile 1x target unavailable (missing bounce/direct rows)."

    ratio = float(bounce.iloc[0]["makespan_s"]) / float(direct.iloc[0]["makespan_s"])
    gain_pct = (ratio - 1.0) * 100.0
    status = "PASS" if ratio >= 2.0 else "FAIL"
    return (
        f"- `{sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE}` at `1x`: "
        f"bounce/direct ratio `{ratio:.6f}` ({gain_pct:.3f}% gain) -> `{status}` (target `>=2.0`)."
    )


def _tpch_cpu_direct_regime_narrative(metrics_df: pd.DataFrame) -> str:
    subset = _default_view(metrics_df)
    subset = subset[
        (subset["workload_profile"] == sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE)
        & (subset["stage_size_multiplier"] == 1.0)
    ]
    if subset.empty:
        return "- High-intermediate CPU/direct regime check unavailable."

    cpu = subset[subset["scenario"] == sources.SCENARIO_CPU_ONLY]
    direct = subset[subset["scenario"] == sources.SCENARIO_PIM_FLOWCXL_DIRECT]
    bounce = subset[subset["scenario"] == sources.SCENARIO_PIM_HOST_BOUNCE]
    if cpu.empty or direct.empty or bounce.empty:
        return "- High-intermediate CPU/direct regime check unavailable (missing scenario rows)."

    cpu_direct_ratio = float(cpu.iloc[0]["makespan_s"]) / float(direct.iloc[0]["makespan_s"])
    cpu_direct_status = "PASS" if cpu_direct_ratio >= 1.2 else "FAIL"
    bounce_dominant = str(bounce.iloc[0]["dominant_lb_component"])
    movement_bound_status = "PASS" if bounce_dominant in {"host_link", "host_touch"} else "FAIL"
    return (
        f"- `{sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE}` at `1x`: "
        f"cpu/direct ratio `{cpu_direct_ratio:.6f}` -> `{cpu_direct_status}` (target `>=1.2`); "
        f"bounce dominant `{bounce_dominant}` -> `{movement_bound_status}` (must be `host_link` or `host_touch`)."
    )


def _relative_results_narrative(metrics_df: pd.DataFrame) -> str:
    records: List[Dict[str, object]] = []
    group_cols = ["workload_profile", "workload_variant", "deepvariant_mode"]
    for _, group_row in metrics_df[group_cols].drop_duplicates().sort_values(group_cols).iterrows():
        mask = (
            (metrics_df["workload_profile"] == group_row["workload_profile"])
            & (metrics_df["workload_variant"] == group_row["workload_variant"])
            & (metrics_df["deepvariant_mode"] == group_row["deepvariant_mode"])
        )
        ratio_df = _build_ratio_wide(metrics_df[mask], "makespan_s")
        for multiplier, row in ratio_df.iterrows():
            records.append(
                {
                    "workload_profile": group_row["workload_profile"],
                    "workload_variant": group_row["workload_variant"],
                    "deepvariant_mode": group_row["deepvariant_mode"],
                    "stage_size_multiplier": float(multiplier),
                    "direct_over_bounce": float(row["direct_over_bounce"]),
                }
            )
    if not records:
        return "- Relative ratio table unavailable (missing bounce/direct pairs)."

    rel_df = pd.DataFrame(records)
    best = rel_df.loc[rel_df["direct_over_bounce"].idxmin()]
    worst = rel_df.loc[rel_df["direct_over_bounce"].idxmax()]
    return (
        "- Best direct_over_bounce makespan: "
        f"`{best['direct_over_bounce']:.6f}` at `{best['workload_profile']}`/`{best['workload_variant']}`/`{best['deepvariant_mode']}`/`{_format_multiplier(best['stage_size_multiplier'])}`.\n"
        "- Worst direct_over_bounce makespan: "
        f"`{worst['direct_over_bounce']:.6f}` at `{worst['workload_profile']}`/`{worst['workload_variant']}`/`{worst['deepvariant_mode']}`/`{_format_multiplier(worst['stage_size_multiplier'])}`."
    )


def _deepvariant_legacy_delta_table(metrics_df: pd.DataFrame) -> pd.DataFrame:
    subset = metrics_df[
        (metrics_df["workload_family"] == "deepvariant")
        & (metrics_df["stage_size_multiplier"] == 1.0)
        & (metrics_df["deepvariant_mode"].isin(["new", "legacy"]))
    ].copy()
    if subset.empty:
        return pd.DataFrame(columns=["note"])

    rows: List[Dict[str, object]] = []
    for keys, group in subset.groupby(["workload_profile", "workload_variant", "scenario"], dropna=False):
        new_rows = group[group["deepvariant_mode"] == "new"]
        legacy_rows = group[group["deepvariant_mode"] == "legacy"]
        if new_rows.empty or legacy_rows.empty:
            continue
        new_row = new_rows.iloc[0]
        legacy_row = legacy_rows.iloc[0]
        rows.append(
            {
                "workload_profile": keys[0],
                "workload_variant": keys[1],
                "scenario": SCENARIO_LABELS.get(keys[2], str(keys[2])),
                "makespan_new_s": float(new_row["makespan_s"]),
                "makespan_legacy_s": float(legacy_row["makespan_s"]),
                "legacy_over_new_makespan": float(legacy_row["makespan_s"]) / float(new_row["makespan_s"]),
                "energy_new_J": float(new_row["total_energy_J"]),
                "energy_legacy_J": float(legacy_row["total_energy_J"]),
                "legacy_over_new_energy": float(legacy_row["total_energy_J"]) / float(new_row["total_energy_J"]),
            }
        )

    if not rows:
        return pd.DataFrame(columns=["note"])

    out = pd.DataFrame(rows)
    return _format_metric_fields(out)


def main() -> None:
    metrics_path = Path("artifacts/metrics.csv")
    if not metrics_path.exists():
        raise FileNotFoundError("artifacts/metrics.csv not found. Run `python run.py` first.")

    report_dir = Path("artifacts/report")
    report_dir.mkdir(parents=True, exist_ok=True)
    for stale_plot in report_dir.glob("plot_*.png"):
        stale_plot.unlink()

    metrics_df = pd.read_csv(metrics_path)
    metrics_df = _ensure_workload_columns(metrics_df)

    group_cols = ["workload_family", "workload_profile", "workload_variant", "deepvariant_mode"]
    group_rows = metrics_df[group_cols].drop_duplicates().sort_values(group_cols)

    plot_lines: List[str] = []
    diagnostic_tables: List[str] = []

    for _, group in group_rows.iterrows():
        mask = (
            (metrics_df["workload_family"] == group["workload_family"])
            & (metrics_df["workload_profile"] == group["workload_profile"])
            & (metrics_df["workload_variant"] == group["workload_variant"])
            & (metrics_df["deepvariant_mode"] == group["deepvariant_mode"])
        )
        subset = metrics_df[mask].copy()

        profile_token = _sanitize_token(group["workload_profile"])
        variant_token = _sanitize_token(group["workload_variant"])
        mode_token = _sanitize_token(group["deepvariant_mode"])
        group_token = f"{profile_token}_{variant_token}_{mode_token}"

        makespan_plot = f"plot_makespan_grouped_{group_token}.png"
        makespan_pim_only_plot = f"plot_makespan_grouped_pim_only_{group_token}.png"
        energy_plot = f"plot_energy_grouped_{group_token}.png"
        energy_pim_only_plot = f"plot_energy_grouped_pim_only_{group_token}.png"

        _plot_grouped_metric(
            subset=subset,
            metric_col="makespan_s",
            ylabel="Makespan (s)",
            title=(
                "Makespan by Stage Size and Scenario "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / makespan_plot,
        )
        _plot_grouped_metric(
            subset=subset,
            metric_col="makespan_s",
            ylabel="Makespan (s)",
            title=(
                "Makespan by Stage Size (PIM only) "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / makespan_pim_only_plot,
            scenario_order=PIM_ONLY_SCENARIO_ORDER,
        )
        _plot_grouped_metric(
            subset=subset,
            metric_col="total_energy_J",
            ylabel="Total Energy (J)",
            title=(
                "Total Energy by Stage Size and Scenario "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / energy_plot,
        )
        _plot_grouped_metric(
            subset=subset,
            metric_col="total_energy_J",
            ylabel="Total Energy (J)",
            title=(
                "Total Energy by Stage Size (PIM only) "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / energy_pim_only_plot,
            scenario_order=PIM_ONLY_SCENARIO_ORDER,
        )

        ratio_make = _build_ratio_wide(subset, "makespan_s")
        ratio_energy = _build_ratio_wide(subset, "total_energy_J")

        rel_make_plot = (
            f"plot_ratio_direct_over_bounce_makespan_{profile_token}_{variant_token}_{mode_token}.png"
        )
        rel_energy_plot = (
            f"plot_ratio_direct_over_bounce_energy_{profile_token}_{variant_token}_{mode_token}.png"
        )
        rel_norm_make_plot = (
            f"plot_ratio_norm_to_bounce_makespan_{profile_token}_{variant_token}_{mode_token}.png"
        )
        rel_norm_energy_plot = (
            f"plot_ratio_norm_to_bounce_energy_{profile_token}_{variant_token}_{mode_token}.png"
        )

        _plot_direct_over_bounce_ratio(
            ratio_df=ratio_make,
            ylabel="Direct/Bounce Makespan Ratio",
            title=(
                "Direct/Bounce Makespan Ratio "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / rel_make_plot,
        )
        _plot_direct_over_bounce_ratio(
            ratio_df=ratio_energy,
            ylabel="Direct/Bounce Energy Ratio",
            title=(
                "Direct/Bounce Energy Ratio "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / rel_energy_plot,
        )
        _plot_norm_to_bounce_ratio(
            ratio_df=ratio_make,
            ylabel="Scenario/Bounce Makespan Ratio",
            title=(
                "Scenario Normalized to Bounce (Makespan) "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / rel_norm_make_plot,
        )
        _plot_norm_to_bounce_ratio(
            ratio_df=ratio_energy,
            ylabel="Scenario/Bounce Energy Ratio",
            title=(
                "Scenario Normalized to Bounce (Energy) "
                f"({group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']})"
            ),
            output_path=report_dir / rel_norm_energy_plot,
        )

        for item in [
            makespan_plot,
            energy_plot,
            makespan_pim_only_plot,
            energy_pim_only_plot,
            rel_make_plot,
            rel_energy_plot,
            rel_norm_make_plot,
            rel_norm_energy_plot,
        ]:
            plot_lines.append(f"- {item}")

        diag_cols = [
            "workload_family",
            "workload_profile",
            "workload_variant",
            "deepvariant_mode",
            "stage_size_multiplier",
            "scenario",
            "makespan_s",
            "total_energy_J",
            "dominant_lb_component",
            "lb_compute_stage_max_s",
            "lb_host_h2d_ingress_s",
            "lb_host_h2d_stage_s",
            "lb_host_d2h_s",
            "lb_host_link_s",
            "lb_host_touch_s",
            "lb_cxl_direct_s",
            "total_bytes_pim_retained",
            "total_retain_fallback_bytes",
            "cxl_effective_striping_factor",
            "total_cxl_dma_issue_time_component_s",
        ]
        diag_df = subset[diag_cols].copy()
        diag_df["scenario"] = diag_df["scenario"].map(SCENARIO_LABELS)
        diag_df["stage_size_multiplier"] = diag_df["stage_size_multiplier"].map(_format_multiplier)
        diag_df = _format_metric_fields(diag_df)
        diagnostic_tables.append(
            "### "
            f"{group['workload_profile']} | {group['workload_variant']} | {group['deepvariant_mode']}"
            "\n\n"
            f"{_build_markdown_table(diag_df)}"
        )

    summary_cols = [
        "workload_family",
        "workload_profile",
        "workload_variant",
        "deepvariant_mode",
        "stage_size_multiplier",
        "scenario",
        "makespan_s",
        "total_energy_J",
        "dominant_lb_component",
    ]
    summary_df = metrics_df[summary_cols].copy()
    summary_df["scenario"] = summary_df["scenario"].map(SCENARIO_LABELS)
    summary_df["stage_size_multiplier"] = summary_df["stage_size_multiplier"].map(_format_multiplier)
    summary_df = _format_metric_fields(summary_df)

    legacy_delta_df = _deepvariant_legacy_delta_table(metrics_df)
    if "note" in legacy_delta_df.columns:
        legacy_delta_text = "- No DeepVariant new/legacy pairs at 1x in this run matrix."
    else:
        legacy_delta_text = _build_markdown_table(legacy_delta_df)

    report_text = (
        "# FlowCXL Tiled Stage-Capacity Report\n\n"
        "## Single Claim\n"
        "Template-aware stage modeling with true host bounce and direct CXL movement shows where "
        "intermediate-staging penalties dominate multi-stage pipelines.\n\n"
        "## Directional Check\n"
        f"{_directional_narrative(metrics_df)}\n\n"
        "## Relative Results\n"
        f"{_relative_results_narrative(metrics_df)}\n\n"
        "## TPC-H Target Check\n"
        f"{_tpch_target_narrative(metrics_df)}\n\n"
        "## High-Intermediate Regime Check\n"
        f"{_tpch_cpu_direct_regime_narrative(metrics_df)}\n\n"
        "## DeepVariant New vs Legacy (1x)\n"
        f"{legacy_delta_text}\n\n"
        "## Plot Artifacts\n"
        f"{chr(10).join(plot_lines)}\n\n"
        "## Results Table\n"
        f"{_build_markdown_table(summary_df)}\n\n"
        "## Bottleneck Diagnostics\n"
        f"{chr(10).join(diagnostic_tables)}\n\n"
        "## Citations\n"
        + "\n".join(
            f"- `{name}`: {entry['url']}\n  Quote: \"{entry['quote']}\"\n  Used as: {entry['how_used']}"
            for name, entry in sources.CITED_VALUES.items()
        )
        + "\n"
    )

    report_path = report_dir / "report.md"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
