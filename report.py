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
        ["dataset_profile", "stage_size_multiplier", "scenario", "makespan_s", "total_energy_J"]
    ].copy()
    summary_df["scenario"] = summary_df["scenario"].map(SCENARIO_LABELS)
    summary_df["stage_size_multiplier"] = summary_df["stage_size_multiplier"].map(_format_multiplier)
    summary_df["makespan_s"] = summary_df["makespan_s"].map(lambda value: f"{float(value):.6f}")
    summary_df["total_energy_J"] = summary_df["total_energy_J"].map(lambda value: f"{float(value):.6f}")

    report_text = (
        "# FlowCXL Tiled Stage-Capacity Report\n\n"
        "## Single Claim\n"
        "When each stage has fixed compute units and large boundaries must be tiled, direct PIM-to-PIM transfers "
        "reduce host-bounce overhead and improve end-to-end makespan/energy relative to host-bounced PIM execution.\n\n"
        "## Modeled\n"
        "- Stage-limited compute capacity (CPU or PIM units)\n"
        "- Tile-by-tile pipelined execution with resource contention\n"
        "- Host bounce path vs direct CXL stage-to-stage path\n"
        "- Absolute makespan (seconds) and total energy (joules)\n\n"
        "## Plot Artifacts\n"
        "- plot_makespan_grouped_PROFILE_ONT_100Gbases.png\n"
        "- plot_makespan_grouped_PROFILE_ILLUMINA_NA12878.png\n"
        "- plot_energy_grouped_PROFILE_ONT_100Gbases.png\n"
        "- plot_energy_grouped_PROFILE_ILLUMINA_NA12878.png\n\n"
        "## Results Table\n"
        f"{_build_markdown_table(summary_df)}\n\n"
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
