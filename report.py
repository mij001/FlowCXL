"""Reporting for tiled stage-capacity FlowCXL experiments."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List, Sequence, Tuple

import matplotlib

matplotlib.use("Agg", force=True)
import matplotlib.pyplot as plt
import pandas as pd
import yaml

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

MAIN_VARIANTS = ("base", "ingressless")
APPENDIX_VARIANTS = ("retention_colocated", "switch_striping")


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate FlowCXL report artifacts.")
    parser.add_argument("--config", default="configs/runs.yaml", help="Path to run config YAML.")
    parser.add_argument("--artifacts-dir", default="artifacts", help="Artifacts directory.")
    parser.add_argument(
        "--metrics-file",
        default=None,
        help="Optional explicit metrics.csv path. Defaults to <artifacts-dir>/metrics.csv",
    )
    return parser.parse_args(argv)


def _build_markdown_table(table_df: pd.DataFrame) -> str:
    if table_df.empty:
        return "- No rows available."
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
    for ch in [" ", "/", "\\", ":", "|", "*", "?", '"', "<", ">"]:
        token = token.replace(ch, "_")
    token = token.replace(".", "p")
    return token


def _ensure_workload_columns(metrics_df: pd.DataFrame) -> pd.DataFrame:
    out = metrics_df.copy()
    if "workload_profile" not in out.columns:
        out["workload_profile"] = out.get("dataset_profile", "")
    if "workload_variant" not in out.columns:
        out["workload_variant"] = "base"
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


def _format_metric_fields(table_df: pd.DataFrame) -> pd.DataFrame:
    out = table_df.copy()
    numeric_cols = [
        "makespan_s",
        "total_energy_J",
        "direct_over_bounce_1x",
        "total_bytes_pim_retained",
        "total_retain_fallback_bytes",
        "cxl_effective_striping_factor",
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = out[col].map(lambda value: f"{float(value):.6f}")
    return out


def _summary_at_1x(subset: pd.DataFrame) -> Tuple[Dict[str, object], str]:
    use = subset.copy()
    use["stage_size_multiplier"] = pd.to_numeric(use["stage_size_multiplier"], errors="coerce")
    if use.empty:
        return {}, "No data available."

    one_x = use[use["stage_size_multiplier"] == 1.0]
    if one_x.empty:
        first_multiplier = sorted(use["stage_size_multiplier"].dropna().unique())[0]
        one_x = use[use["stage_size_multiplier"] == first_multiplier]

    makespan_by_scenario = {
        row["scenario"]: float(row["makespan_s"]) for _, row in one_x.iterrows()
    }
    if not makespan_by_scenario:
        return {}, "No scenario rows available."

    best_scenario = min(makespan_by_scenario, key=makespan_by_scenario.get)
    worst_scenario = max(makespan_by_scenario, key=makespan_by_scenario.get)

    bounce = one_x[one_x["scenario"] == sources.SCENARIO_PIM_HOST_BOUNCE]
    direct = one_x[one_x["scenario"] == sources.SCENARIO_PIM_FLOWCXL_DIRECT]
    direct_over_bounce = float("nan")
    dominant_bounce = ""
    dominant_direct = ""
    if not bounce.empty and not direct.empty:
        direct_over_bounce = float(direct.iloc[0]["makespan_s"]) / float(bounce.iloc[0]["makespan_s"])
        dominant_bounce = str(bounce.iloc[0]["dominant_lb_component"])
        dominant_direct = str(direct.iloc[0]["dominant_lb_component"])

    summary = {
        "workload_family": str(one_x.iloc[0]["workload_family"]),
        "workload_profile": str(one_x.iloc[0]["workload_profile"]),
        "workload_variant": str(one_x.iloc[0]["workload_variant"]),
        "best_scenario_1x": SCENARIO_LABELS.get(best_scenario, best_scenario),
        "worst_scenario_1x": SCENARIO_LABELS.get(worst_scenario, worst_scenario),
        "direct_over_bounce_1x": direct_over_bounce,
        "dominant_lb_bounce_1x": dominant_bounce,
        "dominant_lb_direct_1x": dominant_direct,
    }

    if pd.isna(direct_over_bounce):
        interpretation = "Direct/bounce comparison unavailable at 1x."
    else:
        if direct_over_bounce < 1.0:
            rel = (1.0 - direct_over_bounce) * 100.0
            interpretation = (
                f"At 1x, direct is faster than bounce by {rel:.2f}% (direct/bounce={direct_over_bounce:.6f}). "
                f"Bounce is dominated by `{dominant_bounce}`, direct by `{dominant_direct}`."
            )
        elif direct_over_bounce > 1.0:
            rel = (direct_over_bounce - 1.0) * 100.0
            interpretation = (
                f"At 1x, direct is slower than bounce by {rel:.2f}% (direct/bounce={direct_over_bounce:.6f}). "
                f"Bounce is dominated by `{dominant_bounce}`, direct by `{dominant_direct}`."
            )
        else:
            interpretation = (
                f"At 1x, direct and bounce are tied (direct/bounce={direct_over_bounce:.6f}). "
                f"Bounce is dominated by `{dominant_bounce}`, direct by `{dominant_direct}`."
            )
    return summary, interpretation


def _plot_validation_measured_vs_sim(
    raw_df: pd.DataFrame,
    report_dir: Path,
) -> List[str]:
    artifacts: List[str] = []
    if raw_df.empty:
        return artifacts
    for path_name in sorted(raw_df["path"].dropna().unique()):
        subset = raw_df[raw_df["path"] == path_name].copy()
        if subset.empty:
            continue
        grouped = (
            subset.groupby("payload_bytes", as_index=False)[["measured_s", "simulated_s"]]
            .median()
            .sort_values("payload_bytes")
        )
        fig, ax = plt.subplots(figsize=(7, 4))
        ax.plot(grouped["payload_bytes"], grouped["measured_s"], marker="o", label="Measured (median)")
        ax.plot(grouped["payload_bytes"], grouped["simulated_s"], marker="x", label="Simulated")
        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("Payload (bytes)")
        ax.set_ylabel("Time (s)")
        ax.set_title(f"Measured vs Simulated: {path_name}")
        ax.grid(alpha=0.3)
        ax.legend()
        filename = f"plot_validation_measured_vs_sim_{_sanitize_token(path_name)}.png"
        plt.tight_layout()
        plt.savefig(report_dir / filename, dpi=160)
        plt.close(fig)
        artifacts.append(filename)
    return artifacts


def _plot_sweep_bands(
    sensitivity_df: pd.DataFrame,
    report_dir: Path,
) -> List[str]:
    artifacts: List[str] = []
    if sensitivity_df.empty:
        return artifacts

    target = sensitivity_df[
        (sensitivity_df["workload_profile"] == sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE)
        & (sensitivity_df["workload_variant"] == "base")
        & (sensitivity_df["sweep_family"] != "baseline")
    ].copy()
    if target.empty:
        return artifacts

    for family in sorted(target["sweep_family"].dropna().unique()):
        fam = target[target["sweep_family"] == family].copy()
        if fam.empty:
            continue
        grouped = (
            fam.groupby("multiplier", as_index=False)["bounce_over_direct_makespan"]
            .agg(["min", "median", "max"])
            .reset_index()
            .sort_values("multiplier")
        )
        if grouped.empty:
            continue
        fig, ax = plt.subplots(figsize=(7, 4))
        x_vals = grouped["multiplier"].astype(float)
        ax.plot(x_vals, grouped["median"], marker="o", label="Median")
        ax.fill_between(x_vals, grouped["min"], grouped["max"], alpha=0.25, label="Min/Max band")
        ax.set_xticks(x_vals)
        ax.set_xticklabels([_format_multiplier(v) for v in x_vals])
        ax.set_xlabel("Stage data size multiplier")
        ax.set_ylabel("Bounce/Direct Makespan Ratio")
        ax.set_title(f"Sensitivity Band: {family}")
        ax.grid(alpha=0.3)
        ax.legend()
        filename = f"plot_validation_sweep_band_{_sanitize_token(family)}.png"
        plt.tight_layout()
        plt.savefig(report_dir / filename, dpi=160)
        plt.close(fig)
        artifacts.append(filename)
    return artifacts


def _profile_variant_section(
    *,
    subset: pd.DataFrame,
    report_dir: Path,
) -> Tuple[List[str], Dict[str, object], str]:
    profile = str(subset.iloc[0]["workload_profile"])
    variant = str(subset.iloc[0]["workload_variant"])
    profile_token = _sanitize_token(profile)
    variant_token = _sanitize_token(variant)

    makespan_plot = f"plot_makespan_grouped_{profile_token}_{variant_token}.png"
    energy_plot = f"plot_energy_grouped_{profile_token}_{variant_token}.png"
    makespan_pim_plot = f"plot_makespan_grouped_pim_only_{profile_token}_{variant_token}.png"
    energy_pim_plot = f"plot_energy_grouped_pim_only_{profile_token}_{variant_token}.png"

    _plot_grouped_metric(
        subset=subset,
        metric_col="makespan_s",
        ylabel="Makespan (s)",
        title=f"Makespan by Stage Size and Scenario ({profile} | {variant})",
        output_path=report_dir / makespan_plot,
    )
    _plot_grouped_metric(
        subset=subset,
        metric_col="total_energy_J",
        ylabel="Total Energy (J)",
        title=f"Total Energy by Stage Size and Scenario ({profile} | {variant})",
        output_path=report_dir / energy_plot,
    )
    _plot_grouped_metric(
        subset=subset,
        metric_col="makespan_s",
        ylabel="Makespan (s)",
        title=f"Makespan by Stage Size (PIM only) ({profile} | {variant})",
        output_path=report_dir / makespan_pim_plot,
        scenario_order=PIM_ONLY_SCENARIO_ORDER,
    )
    _plot_grouped_metric(
        subset=subset,
        metric_col="total_energy_J",
        ylabel="Total Energy (J)",
        title=f"Total Energy by Stage Size (PIM only) ({profile} | {variant})",
        output_path=report_dir / energy_pim_plot,
        scenario_order=PIM_ONLY_SCENARIO_ORDER,
    )

    summary, interpretation = _summary_at_1x(subset)
    return [makespan_plot, energy_plot, makespan_pim_plot, energy_pim_plot], summary, interpretation


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"config file not found: {config_path}")
    with config_path.open("r", encoding="utf-8") as handle:
        yaml.safe_load(handle)

    artifacts_dir = Path(args.artifacts_dir)
    metrics_path = Path(args.metrics_file) if args.metrics_file else artifacts_dir / "metrics.csv"
    if not metrics_path.exists():
        raise FileNotFoundError(f"metrics file not found: {metrics_path}. Run `python run.py` first.")

    report_dir = artifacts_dir / "report"
    report_dir.mkdir(parents=True, exist_ok=True)
    for stale_plot in report_dir.glob("plot_*.png"):
        stale_plot.unlink()

    metrics_df = pd.read_csv(metrics_path)
    metrics_df = _ensure_workload_columns(metrics_df)

    group_cols = ["workload_family", "workload_profile", "workload_variant"]
    groups = metrics_df[group_cols].drop_duplicates().sort_values(group_cols)

    main_sections: List[str] = []
    appendix_sections: List[str] = []
    all_plots: List[str] = []
    main_summary_rows: List[Dict[str, object]] = []
    appendix_summary_rows: List[Dict[str, object]] = []

    for _, group in groups.iterrows():
        profile = str(group["workload_profile"])
        variant = str(group["workload_variant"])
        mask = (
            (metrics_df["workload_family"] == group["workload_family"])
            & (metrics_df["workload_profile"] == profile)
            & (metrics_df["workload_variant"] == variant)
        )
        subset = metrics_df[mask].copy()

        plots, summary_row, interpretation = _profile_variant_section(
            subset=subset,
            report_dir=report_dir,
        )
        all_plots.extend(plots)

        section = (
            f"### {profile} | {variant}\n\n"
            f"{interpretation}\n\n"
            + "\n".join(f"- {plot}" for plot in plots)
        )

        if variant in MAIN_VARIANTS:
            main_sections.append(section)
            if summary_row:
                main_summary_rows.append(summary_row)
        elif variant in APPENDIX_VARIANTS:
            appendix_sections.append(section)
            if summary_row:
                appendix_summary_rows.append(summary_row)

    main_summary_df = _format_metric_fields(pd.DataFrame(main_summary_rows))
    appendix_summary_df = _format_metric_fields(pd.DataFrame(appendix_summary_rows))

    appendix_diag = metrics_df[
        metrics_df["workload_variant"].isin(APPENDIX_VARIANTS)
        & (pd.to_numeric(metrics_df["stage_size_multiplier"], errors="coerce") == 1.0)
    ][
        [
            "workload_family",
            "workload_profile",
            "workload_variant",
            "scenario",
            "total_bytes_pim_retained",
            "total_retain_fallback_bytes",
            "cxl_effective_striping_factor",
        ]
    ].copy()
    appendix_diag["scenario"] = appendix_diag["scenario"].map(SCENARIO_LABELS)
    appendix_diag = _format_metric_fields(appendix_diag)

    validation_dir = artifacts_dir / "validation"
    validation_sections: List[str] = []
    validation_plots: List[str] = []
    if validation_dir.exists():
        raw_path = validation_dir / "microbench_raw.csv"
        fit_path = validation_dir / "microbench_fit.yaml"
        cross_path = validation_dir / "cxl_ps_crosscheck.csv"
        sensitivity_path = validation_dir / "sensitivity_results.csv"
        tornado_path = validation_dir / "tornado_top8.csv"
        ablations_path = validation_dir / "ablations.csv"

        if raw_path.exists():
            raw_df = pd.read_csv(raw_path)
            validation_plots.extend(_plot_validation_measured_vs_sim(raw_df, report_dir))
            validation_sections.append(
                "### Microbenchmark Calibration\n\n"
                f"- Source: `{raw_path}`\n"
                + "\n".join(f"- {p}" for p in validation_plots if "measured_vs_sim" in p)
            )
        if fit_path.exists():
            with fit_path.open("r", encoding="utf-8") as handle:
                fit_payload = yaml.safe_load(handle) or {}
            fit_rows: List[Dict[str, object]] = []
            for path_name, values in (fit_payload.get("paths", {}) or {}).items():
                fit_rows.append(
                    {
                        "path": path_name,
                        "bandwidth_Bps": values.get("bandwidth_Bps", ""),
                        "latency_s": values.get("latency_s", ""),
                        "mape_percent": values.get("mape_percent", ""),
                        "r2": values.get("r2", ""),
                    }
                )
            fit_df = pd.DataFrame(fit_rows)
            validation_sections.append(
                "### Calibration Fit Summary\n\n"
                f"{_build_markdown_table(_format_metric_fields(fit_df))}"
            )
        if cross_path.exists():
            cross_df = pd.read_csv(cross_path)
            show_cols = [
                "pattern",
                "payload_bytes",
                "concurrency",
                "mape_percent",
                "max_abs_error_s",
                "passes_tolerance",
            ]
            cross_show = cross_df[show_cols] if not cross_df.empty else cross_df
            validation_sections.append(
                "### Processor-Share Cross-Check\n\n"
                f"- Source: `{cross_path}`\n\n"
                f"{_build_markdown_table(_format_metric_fields(cross_show.head(16)))}"
            )
        if sensitivity_path.exists():
            sensitivity_df = pd.read_csv(sensitivity_path)
            validation_plots.extend(_plot_sweep_bands(sensitivity_df, report_dir))
            sensitivity_head = sensitivity_df.head(20)
            validation_sections.append(
                "### Sensitivity Sweeps\n\n"
                f"- Source: `{sensitivity_path}`\n"
                + "\n".join(f"- {p}" for p in validation_plots if "sweep_band" in p)
                + "\n\n"
                + _build_markdown_table(_format_metric_fields(sensitivity_head))
            )
        if tornado_path.exists():
            tornado_df = pd.read_csv(tornado_path)
            validation_sections.append(
                "### Tornado Top 8\n\n"
                f"- Source: `{tornado_path}`\n\n"
                f"{_build_markdown_table(_format_metric_fields(tornado_df))}"
            )
        if ablations_path.exists():
            ablations_df = pd.read_csv(ablations_path)
            ablation_show = ablations_df[
                [
                    "ablation",
                    "workload_profile",
                    "multiplier",
                    "bounce_over_direct_makespan",
                    "bounce_over_direct_energy",
                    "cpu_over_direct_makespan",
                ]
            ].head(32)
            validation_sections.append(
                "### Ablation Summary\n\n"
                f"- Source: `{ablations_path}`\n\n"
                f"{_build_markdown_table(_format_metric_fields(ablation_show))}"
            )

    provenance_rows = []
    for key, value in sorted(sources.PARAMETER_PROVENANCE.items()):
        provenance_rows.append(
            {
                "parameter": key,
                "class": value.get("class", ""),
                "source": value.get("source", ""),
                "config_key": value.get("config_key", ""),
                "note": value.get("note", ""),
            }
        )
    provenance_df = pd.DataFrame(provenance_rows)

    report_chunks = [
        "# FlowCXL Tiled Stage-Capacity Report\n\n",
        "## Main Results\n",
        "Main body includes only `base` and `ingressless` variants for each profile.\n\n",
        f"{_build_markdown_table(main_summary_df)}\n\n",
        f"{chr(10).join(main_sections) if main_sections else '- No main sections generated.'}\n\n",
        "## Appendix: Additional Variants\n",
        "Appendix includes `retention_colocated` and `switch_striping`.\n\n",
        f"{_build_markdown_table(appendix_summary_df)}\n\n",
        f"{chr(10).join(appendix_sections) if appendix_sections else '- No appendix sections generated.'}\n\n",
        "## Appendix Diagnostics (1x)\n",
        f"{_build_markdown_table(appendix_diag)}\n\n",
        "## Validation Appendix\n",
        f"{chr(10).join(validation_sections)}\n\n" if validation_sections else "- No validation artifacts found.\n\n",
        "## Parameter Provenance\n",
        f"{_build_markdown_table(provenance_df)}\n\n",
        "## Plot Artifacts\n",
        f"{chr(10).join(f'- {plot}' for plot in (all_plots + validation_plots))}\n\n",
        "## Citations\n",
        "\n".join(
            f"- `{name}`: {entry['url']}\n  Quote: \"{entry['quote']}\"\n  Used as: {entry['how_used']}"
            for name, entry in sources.CITED_VALUES.items()
        ),
        "\n",
    ]
    report_text = "".join(report_chunks)

    report_path = report_dir / "report.md"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
