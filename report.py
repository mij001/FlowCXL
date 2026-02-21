"""Reporting for contention-aware transfer experiments."""

from __future__ import annotations

from pathlib import Path
from typing import List

import matplotlib.pyplot as plt
import pandas as pd

import sources

SCENARIO_LABELS = {
    sources.SCENARIO_PIM_NO_CXL_BOUNCE: "PCIe Bounce",
    sources.SCENARIO_PIM_CXL_BOUNCE: "CXL Bounce",
    sources.SCENARIO_PIM_CXL_CHAIN: "CXL Chain",
}


def _build_markdown_table(table_df: pd.DataFrame) -> str:
    header = "| " + " | ".join(table_df.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(table_df.columns)) + " |"
    rows: List[str] = []
    for _, row in table_df.iterrows():
        rows.append("| " + " | ".join(str(row[col]) for col in table_df.columns) + " |")
    return "\n".join([header, separator] + rows)


def _shared_mode_series(series: pd.Series) -> pd.Series:
    return series.map(lambda value: "shared" if str(value).lower() == "true" else "duplex")


def _run_label(df: pd.DataFrame) -> pd.Series:
    return (
        df["dataset_profile"]
        + " | "
        + df["link_type"]
        + " | "
        + _shared_mode_series(df["shared_link"])
        + " | k="
        + df["num_chunks"].astype(str)
    )


def _plot_makespan(metrics_df: pd.DataFrame, output_path: Path) -> None:
    plot_df = metrics_df.copy()
    plot_df["scenario"] = plot_df["scenario"].map(SCENARIO_LABELS)
    plot_df["run_label"] = _run_label(plot_df)
    pivot = plot_df.pivot_table(index="run_label", columns="scenario", values="makespan_s", aggfunc="first")

    ax = pivot.plot(kind="bar", figsize=(14, 6), rot=30)
    ax.set_xlabel("Dataset | Link | Mode | num_chunks")
    ax.set_ylabel("Makespan (s)")
    ax.set_title("Makespan by Scenario")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Scenario")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_total_bytes(metrics_df: pd.DataFrame, output_path: Path) -> None:
    plot_df = metrics_df.copy()
    plot_df["scenario"] = plot_df["scenario"].map(SCENARIO_LABELS)
    plot_df["run_label"] = _run_label(plot_df)
    pivot = plot_df.pivot_table(index="run_label", columns="scenario", values="total_bytes_moved", aggfunc="first")

    ax = pivot.plot(kind="bar", figsize=(14, 6), rot=30)
    ax.set_xlabel("Dataset | Link | Mode | num_chunks")
    ax.set_ylabel("Total Bytes Moved")
    ax.set_title("Host-Link Bytes by Scenario")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Scenario")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_speedup(metrics_df: pd.DataFrame, output_path: Path) -> None:
    speed_df = metrics_df[
        (metrics_df["scenario"] == sources.SCENARIO_PIM_CXL_BOUNCE)
        & (metrics_df["link_type"].isin([sources.LINK_CXL_LOCAL, sources.LINK_CXL_REMOTE]))
    ].copy()
    speed_df["x"] = (
        speed_df["dataset_profile"]
        + " | "
        + _shared_mode_series(speed_df["shared_link"])
        + " | k="
        + speed_df["num_chunks"].astype(str)
    )
    speed_df["speedup_vs_chain"] = pd.to_numeric(speed_df["speedup_vs_chain"], errors="coerce")

    pivot = speed_df.pivot_table(index="x", columns="link_type", values="speedup_vs_chain", aggfunc="first")
    ax = pivot.plot(kind="bar", figsize=(12, 5), rot=25)
    ax.set_xlabel("Dataset | Mode | num_chunks")
    ax.set_ylabel("Speedup (CXL bounce / CXL chain)")
    ax.set_title("CXL Speedup")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Link")
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_queue_blocking_total(metrics_df: pd.DataFrame, output_path: Path) -> None:
    plot_df = metrics_df.copy()
    plot_df["run_label"] = _run_label(plot_df) + " | " + plot_df["scenario"]
    ax = plot_df.set_index("run_label")["queue_total_blocking_s"].plot(kind="bar", figsize=(15, 6), rot=30)
    ax.set_xlabel("Run")
    ax.set_ylabel("Blocking Queue Time (s)")
    ax.set_title("Total Blocking Queue Time per Run")
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_queue_stacked_attributed(metrics_df: pd.DataFrame, output_path: Path) -> None:
    queue_cols = [f"queue_{name}_s" for name in sources.RESOURCE_NAMES]
    plot_df = metrics_df.copy()
    plot_df["run_label"] = _run_label(plot_df) + " | " + plot_df["scenario"]

    queue_data = plot_df.set_index("run_label")[queue_cols]
    ax = queue_data.plot(kind="bar", stacked=True, figsize=(15, 6), rot=30)
    ax.set_xlabel("Run")
    ax.set_ylabel("Attributed Queue Time (s)")
    ax.set_title("Attributed Queue Time by Resource")
    ax.grid(axis="y", alpha=0.25)
    ax.legend(title="Resource", ncol=3)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_utilization_heatmap(metrics_df: pd.DataFrame, output_path: Path) -> None:
    util_cols = [f"util_{name}" for name in sources.RESOURCE_NAMES]
    heat_df = metrics_df.copy()
    heat_df["run_label"] = _run_label(heat_df) + " | " + heat_df["scenario"]

    matrix = heat_df[util_cols].to_numpy()

    fig, ax = plt.subplots(figsize=(13, 8))
    im = ax.imshow(matrix, aspect="auto", cmap="viridis")

    ax.set_xticks(range(len(util_cols)))
    ax.set_xticklabels(util_cols, rotation=30, ha="right")
    ax.set_yticks(range(len(heat_df)))
    ax.set_yticklabels(heat_df["run_label"])
    ax.set_title("Resource Utilization Heatmap")

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Utilization")

    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _ont_contention_explanation(metrics_df: pd.DataFrame) -> str:
    rows = metrics_df[
        (metrics_df["dataset_profile"] == sources.PROFILE_ONT_100Gbases)
        & (metrics_df["link_type"] == sources.LINK_CXL_LOCAL)
        & (metrics_df["scenario"] == sources.SCENARIO_PIM_CXL_BOUNCE)
    ]

    def _speed(mode: bool, k: int) -> float:
        row = rows[(rows["shared_link"] == mode) & (rows["num_chunks"] == k)]
        if row.empty:
            return float("nan")
        return float(row.iloc[0]["speedup_vs_chain"])

    duplex_k1 = _speed(False, 1)
    duplex_k8 = _speed(False, 8)
    shared_k1 = _speed(True, 1)
    shared_k8 = _speed(True, 8)

    return (
        "For ONT on CXL_LOCAL, both bounce and chain still carry the dominant X0 boundary "
        f"({sources.ONT_X0} bytes) over H2D. Under duplex contention, speedup shrinks "
        f"(k=1 {duplex_k1:.3f} -> k=8 {duplex_k8:.3f}) because eliminated transfers are not the dominant bottleneck. "
        f"Under shared-link mode, both scenarios serialize on one link and the ratio stays near constant "
        f"(k=1 {shared_k1:.3f}, k=8 {shared_k8:.3f})."
    )


def main() -> None:
    metrics_path = Path("artifacts/metrics.csv")
    if not metrics_path.exists():
        raise FileNotFoundError("artifacts/metrics.csv not found. Run `python run.py` first.")

    report_dir = Path("artifacts/report")
    report_dir.mkdir(parents=True, exist_ok=True)

    metrics_df = pd.read_csv(metrics_path)

    _plot_makespan(metrics_df=metrics_df, output_path=report_dir / "plot_makespan_by_scenario.png")
    _plot_total_bytes(metrics_df=metrics_df, output_path=report_dir / "plot_total_bytes_by_scenario.png")
    _plot_speedup(metrics_df=metrics_df, output_path=report_dir / "plot_speedup_cxl_bounce_vs_chain.png")
    _plot_queue_blocking_total(metrics_df=metrics_df, output_path=report_dir / "plot_queue_total_blocking.png")
    _plot_queue_stacked_attributed(
        metrics_df=metrics_df,
        output_path=report_dir / "plot_queue_time_by_resource_attributed.png",
    )
    _plot_utilization_heatmap(metrics_df=metrics_df, output_path=report_dir / "plot_resource_utilization_heatmap.png")

    summary_cols = [
        "dataset_profile",
        "link_type",
        "shared_link",
        "num_chunks",
        "scenario",
        "makespan_s",
        "total_bytes_moved",
        "queue_total_blocking_s",
        "queue_total_attributed_s",
        "speedup_vs_chain",
    ]
    summary_df = metrics_df[summary_cols].copy()
    summary_df["makespan_s"] = summary_df["makespan_s"].map(lambda value: f"{float(value):.6f}")
    summary_df["queue_total_blocking_s"] = summary_df["queue_total_blocking_s"].map(
        lambda value: f"{float(value):.6f}"
    )
    summary_df["queue_total_attributed_s"] = summary_df["queue_total_attributed_s"].map(
        lambda value: f"{float(value):.6f}"
    )
    summary_df["speedup_vs_chain"] = summary_df["speedup_vs_chain"].map(
        lambda value: "" if pd.isna(value) else f"{float(value):.6f}"
    )

    report_text = (
        "# Flow-CXL Contention-Aware Transfer Report\n\n"
        "## Single Claim\n"
        "Flow-CXL chaining reduces host-bounce staging transfers across multi-stage pipelines. "
        "Gain under contention depends on whether eliminated transfers overlap with the dominant bottleneck.\n\n"
        "## Modeled\n"
        "- Transfer fixed costs and bandwidth\n"
        "- Deterministic queueing from shared resources\n"
        "- Multi-chunk contention at num_chunks in {1, 8}\n"
        "- Duplex vs shared-link resource modes\n\n"
        "## Queue Accounting\n"
        "- `queue_total_blocking_s`: sum of per-operation blocking waits (one value per operation).\n"
        "- `queue_total_attributed_s`: sum of per-resource attributed waits. Blocking wait is attributed only to bottleneck resource(s); tie waits are split.\n\n"
        "## ONT k=8 Interpretation\n"
        f"{_ont_contention_explanation(metrics_df)}\n\n"
        "## Results Table\n"
        f"{_build_markdown_table(summary_df)}\n\n"
        "## Plot Artifacts\n"
        "- plot_makespan_by_scenario.png\n"
        "- plot_total_bytes_by_scenario.png\n"
        "- plot_speedup_cxl_bounce_vs_chain.png\n"
        "- plot_queue_total_blocking.png\n"
        "- plot_queue_time_by_resource_attributed.png\n"
        "- plot_resource_utilization_heatmap.png\n\n"
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
