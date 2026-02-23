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


def _deepvariant_directional_narrative(metrics_df: pd.DataFrame) -> str:
    lines: List[str] = []
    for dataset_profile in sorted(metrics_df["dataset_profile"].dropna().unique()):
        subset = metrics_df[metrics_df["dataset_profile"] == dataset_profile]
        ratios: List[tuple[float, float]] = []
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
            ratio = float(bounce.iloc[0]["makespan_s"]) / float(direct.iloc[0]["makespan_s"])
            ratios.append((float(multiplier), ratio))

        if not ratios:
            lines.append(f"- {dataset_profile}: insufficient bounce/direct rows.")
            continue

        directional_ok = all(ratio >= 1.0 - 1e-12 for _, ratio in ratios)
        strict_points = sum(1 for _, ratio in ratios if ratio > 1.0 + 1e-9)
        min_ratio = min(ratio for _, ratio in ratios)
        max_ratio = max(ratio for _, ratio in ratios)
        sensitivity_delta = max_ratio - min_ratio
        ratio_1x = next((ratio for mult, ratio in ratios if abs(mult - 1.0) < 1e-12), None)

        if ratio_1x is None:
            ratio_1x_text = "n/a"
        else:
            ratio_1x_text = f"{ratio_1x:.6f}"

        lines.append(
            f"- {dataset_profile}: directional `{str(directional_ok).lower()}`, "
            f"strictly-better points `{strict_points}`, "
            f"1x bounce/direct ratio `{ratio_1x_text}`, "
            f"sensitivity delta (max-min ratio) `{sensitivity_delta:.6f}`."
        )

    lines.append(
        "- Directional condition checks `direct <= bounce`; sensitivity delta reports how ratio changes "
        "across stage-size multipliers."
    )
    lines.append(
        "- Streaming admission (`max_inflight_tiles`) and split H2D pools separate ingress pressure "
        "from inter-stage staging, while only PIM->PIM transitions differ between bounce and direct."
    )
    return "\n".join(lines)


def main() -> None:
    metrics_path = Path("artifacts/metrics.csv")
    if not metrics_path.exists():
        raise FileNotFoundError("artifacts/metrics.csv not found. Run `python run.py` first.")

    report_dir = Path("artifacts/report")
    report_dir.mkdir(parents=True, exist_ok=True)

    metrics_df = pd.read_csv(metrics_path)

    for dataset_profile in metrics_df["dataset_profile"].dropna().unique():
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
    for dataset_profile in metrics_df["dataset_profile"].dropna().unique():
        diag_df = _dataset_diagnostic_table(metrics_df=metrics_df, dataset_profile=dataset_profile)
        diagnostic_tables.append(f"### {dataset_profile}\n\n{_build_markdown_table(diag_df)}")

    report_text = (
        "# DeepVariant Tiled Stage-Capacity Report\n\n"
        "## Single Claim\n"
        "With bounded streaming admission and split host H2D topology, FlowCXL direct transfer isolates "
        "DeepVariant inter-stage staging costs from ingress contention and exposes overlap-dependent gains.\n\n"
        "## Modeled\n"
        "- Fixed DeepVariant three-stage pipeline: make_examples, call_variants, postprocess_variants\n"
        "- Stage-limited compute capacity with scenario stage-device mapping (CPU or PIM)\n"
        "- Tile-by-tile pipelined execution with bounded in-flight admission\n"
        "- True host bounce for intermediates: D2H -> HOST_TOUCH -> H2D(stage)\n"
        "- Split host H2D resources: ingress vs inter-stage staging\n"
        "- Absolute makespan (seconds) and total energy (joules)\n"
        "- Lower-bound bottleneck diagnostics by resource family\n\n"
        "## Directional Check\n"
        f"{_deepvariant_directional_narrative(metrics_df)}\n\n"
        "## Plot Artifacts\n"
        "- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X.png\n"
        "- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X.png\n"
        "- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X.png\n"
        "- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X.png\n\n"
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
