"""Reporting for tiled stage-capacity FlowCXL experiments."""

from __future__ import annotations

from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import pandas as pd

import sources

SCENARIO_ORDER = [
    sources.SCENARIO_CPU_ONLY,
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


def _plot_grouped_metric(
    metrics_df: pd.DataFrame,
    dataset_profile: str,
    metric_col: str,
    ylabel: str,
    title: str,
    output_path: Path,
) -> None:
    subset = metrics_df[metrics_df["dataset_profile"] == dataset_profile].copy()
    if subset.empty:
        return

    subset["stage_size_multiplier"] = pd.to_numeric(subset["stage_size_multiplier"], errors="coerce")
    subset = subset.dropna(subset=["stage_size_multiplier"])
    subset = subset.sort_values("stage_size_multiplier")

    pivot = subset.pivot_table(
        index="stage_size_multiplier",
        columns="scenario",
        values=metric_col,
        aggfunc="first",
    )
    pivot = pivot.reindex(columns=SCENARIO_ORDER)
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
    for col in [
        "makespan_s",
        "total_energy_J",
        "host_touch_energy_J",
        "total_compute_time_component_s",
        "total_cpu_mem_time_component_s",
        "total_pim_mem_time_component_s",
        "lb_compute_stage_max_s",
        "lb_host_h2d_ingress_s",
        "lb_host_h2d_stage_s",
        "lb_host_d2h_s",
        "lb_host_link_s",
        "lb_host_touch_s",
        "lb_cxl_direct_s",
    ]:
        if col in out:
            out[col] = out[col].map(lambda value: f"{float(value):.6f}")
    return out


def _dataset_diagnostic_table(metrics_df: pd.DataFrame, dataset_profile: str) -> pd.DataFrame:
    subset = metrics_df[metrics_df["dataset_profile"] == dataset_profile].copy()
    subset = subset.sort_values(["stage_size_multiplier", "scenario"])
    subset["scenario"] = subset["scenario"].map(SCENARIO_LABELS)
    subset["stage_size_multiplier"] = subset["stage_size_multiplier"].map(_format_multiplier)
    cols = [
        "dataset_profile",
        "pipeline_template",
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
    ]
    subset = subset[cols]
    return _format_metric_fields(subset)


def _memory_ceiling_diagnostic_table(metrics_df: pd.DataFrame) -> pd.DataFrame:
    subset = metrics_df[metrics_df["stage_size_multiplier"] == 1.0].copy()
    subset = subset.sort_values(["dataset_profile", "scenario"])
    subset["scenario"] = subset["scenario"].map(SCENARIO_LABELS)
    cols = [
        "dataset_profile",
        "scenario",
        "memory_ceiling_enabled",
        "total_compute_time_component_s",
        "total_cpu_mem_time_component_s",
        "total_pim_mem_time_component_s",
    ]
    subset = subset[cols]
    return _format_metric_fields(subset)


def _directional_narrative(metrics_df: pd.DataFrame) -> str:
    lines: List[str] = []
    profiles = sorted(metrics_df["dataset_profile"].dropna().unique())
    for dataset_profile in profiles:
        subset = metrics_df[metrics_df["dataset_profile"] == dataset_profile]
        template = str(subset.iloc[0]["pipeline_template"]) if not subset.empty else "unknown"
        ratios: List[tuple[float, float, str, str]] = []
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
            b = bounce.iloc[0]
            d = direct.iloc[0]
            ratio = float(b["makespan_s"]) / float(d["makespan_s"])
            ratios.append((float(multiplier), ratio, str(b["dominant_lb_component"]), str(d["dominant_lb_component"])))

        if not ratios:
            lines.append(f"- {dataset_profile}: insufficient bounce/direct rows.")
            continue

        directional_ok = all(ratio >= 1.0 - 1e-12 for _, ratio, _, _ in ratios)
        strict_points = sum(1 for _, ratio, _, _ in ratios if ratio > 1.0 + 1e-9)
        min_ratio = min(ratio for _, ratio, _, _ in ratios)
        max_ratio = max(ratio for _, ratio, _, _ in ratios)
        ratio_1x_info = next((info for info in ratios if abs(info[0] - 1.0) < 1e-12), None)

        if ratio_1x_info is None:
            ratio_1x_text = "n/a"
            dom_text = "n/a"
        else:
            ratio_1x_text = f"{ratio_1x_info[1]:.6f}"
            dom_text = f"bounce `{ratio_1x_info[2]}`, direct `{ratio_1x_info[3]}`"

        lines.append(
            f"- {dataset_profile} (`{template}`): directional `{str(directional_ok).lower()}`, "
            f"strictly-better points `{strict_points}`, "
            f"1x bounce/direct ratio `{ratio_1x_text}`, "
            f"ratio range `{min_ratio:.6f}` to `{max_ratio:.6f}`, 1x dominants {dom_text}."
        )

    lines.append(
        "- Directional condition checks `direct <= bounce`; ratio range captures sensitivity across stage-size multipliers."
    )
    return "\n".join(lines)


def _tpch_target_narrative(metrics_df: pd.DataFrame) -> str:
    subset = metrics_df[
        (metrics_df["dataset_profile"] == sources.PROFILE_TPCH_SF100_HIGH_INTERMEDIATE)
        & (metrics_df["stage_size_multiplier"] == 1.0)
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


def main() -> None:
    metrics_path = Path("artifacts/metrics.csv")
    if not metrics_path.exists():
        raise FileNotFoundError("artifacts/metrics.csv not found. Run `python run.py` first.")

    report_dir = Path("artifacts/report")
    report_dir.mkdir(parents=True, exist_ok=True)

    metrics_df = pd.read_csv(metrics_path)
    profiles = list(metrics_df["dataset_profile"].dropna().unique())

    for dataset_profile in profiles:
        _plot_grouped_metric(
            metrics_df=metrics_df,
            dataset_profile=dataset_profile,
            metric_col="makespan_s",
            ylabel="Makespan (s)",
            title=f"Makespan by Stage Size and Scenario ({dataset_profile})",
            output_path=report_dir / f"plot_makespan_grouped_{dataset_profile}.png",
        )
        _plot_grouped_metric(
            metrics_df=metrics_df,
            dataset_profile=dataset_profile,
            metric_col="total_energy_J",
            ylabel="Total Energy (J)",
            title=f"Total Energy by Stage Size and Scenario ({dataset_profile})",
            output_path=report_dir / f"plot_energy_grouped_{dataset_profile}.png",
        )

    summary_df = metrics_df[
        [
            "dataset_profile",
            "pipeline_template",
            "stage_size_multiplier",
            "scenario",
            "makespan_s",
            "total_energy_J",
            "host_touch_energy_J",
            "total_bytes_host_touch",
            "dominant_lb_component",
        ]
    ].copy()
    summary_df["scenario"] = summary_df["scenario"].map(SCENARIO_LABELS)
    summary_df["stage_size_multiplier"] = summary_df["stage_size_multiplier"].map(_format_multiplier)
    summary_df = _format_metric_fields(summary_df)

    diagnostic_tables = []
    for dataset_profile in profiles:
        diag_df = _dataset_diagnostic_table(metrics_df=metrics_df, dataset_profile=dataset_profile)
        diagnostic_tables.append(f"### {dataset_profile}\n\n{_build_markdown_table(diag_df)}")
    memory_diag_table = _memory_ceiling_diagnostic_table(metrics_df=metrics_df)

    plot_lines: List[str] = []
    for dataset_profile in profiles:
        plot_lines.append(f"- plot_makespan_grouped_{dataset_profile}.png")
        plot_lines.append(f"- plot_energy_grouped_{dataset_profile}.png")

    report_text = (
        "# FlowCXL Tiled Stage-Capacity Report\n\n"
        "## Single Claim\n"
        "Template-aware stage modeling with true host bounce and direct CXL movement shows where "
        "intermediate-staging penalties dominate multi-stage pipelines.\n\n"
        "## Modeled\n"
        "- Dual templates: DeepVariant (`deepvariant_3stage`) and OLAP (`tpch_3op`)\n"
        "- Stage-limited compute capacity with per-template stage-device maps\n"
        "- Tile-by-tile pipelined execution with bounded in-flight admission\n"
        "- True host bounce for inter-PIM transfer: D2H -> HOST_TOUCH -> H2D(stage)\n"
        "- Split host H2D resources: ingress vs inter-stage staging\n"
        "- Absolute makespan (seconds) and total energy (joules)\n"
        "- Lower-bound bottleneck diagnostics by resource family\n\n"
        "## Directional Check\n"
        f"{_directional_narrative(metrics_df)}\n\n"
        "## TPC-H Target Check\n"
        "- In `tpch_3op`, large S1->S2 and S2->S3 intermediates make host-bounce pay double link traversal + touch, "
        "while FlowCXL direct pays a single inter-device transfer.\n"
        f"{_tpch_target_narrative(metrics_df)}\n\n"
        "## CPU Memory Ceiling Diagnostics (1x)\n"
        f"{_build_markdown_table(memory_diag_table)}\n\n"
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
