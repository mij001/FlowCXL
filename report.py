"""Reporting: plots + markdown summary from artifacts/metrics.csv."""

from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

import sources

SCENARIO_LABELS = {
    sources.SCENARIO_BOUNCE: "Conventional Host Bounce",
    sources.SCENARIO_CHAIN: "Flow-CXL Chain",
}


def _build_markdown_table(table_df: pd.DataFrame) -> str:
    header = "| " + " | ".join(table_df.columns) + " |"
    separator = "| " + " | ".join(["---"] * len(table_df.columns)) + " |"
    rows = []
    for _, row in table_df.iterrows():
        rows.append("| " + " | ".join(str(row[col]) for col in table_df.columns) + " |")
    return "\n".join([header, separator] + rows)


def _plot_metric_by_scenario(metrics_df: pd.DataFrame, metric: str, ylabel: str, output_path: Path) -> None:
    plot_df = metrics_df.copy()
    plot_df["scenario"] = plot_df["scenario"].map(SCENARIO_LABELS)
    plot_df["payload_link"] = plot_df["payload_name"] + " | " + plot_df["link_type"]

    pivot = plot_df.pivot_table(index="payload_link", columns="scenario", values=metric, aggfunc="first")

    ax = pivot.plot(kind="bar", figsize=(12, 5), rot=20)
    ax.set_xlabel("Payload | Link Type")
    ax.set_ylabel(ylabel)
    ax.legend(title="Scenario")
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout()
    plt.savefig(output_path, dpi=160)
    plt.close()


def _plot_speedup(metrics_df: pd.DataFrame, output_path: Path) -> None:
    speed_df = metrics_df[
        (metrics_df["scenario"] == sources.SCENARIO_BOUNCE)
        & (metrics_df["link_type"].isin(["CXL Local", "CXL Remote"]))
    ].copy()

    speed_df["speedup_vs_chain"] = pd.to_numeric(speed_df["speedup_vs_chain"], errors="coerce")
    pivot = speed_df.pivot_table(index="payload_name", columns="link_type", values="speedup_vs_chain", aggfunc="first")

    ax = pivot.plot(kind="bar", figsize=(8, 5), rot=0)
    ax.set_xlabel("Payload")
    ax.set_ylabel("Speedup (bounce / chain)")
    ax.legend(title="Link Type")
    ax.grid(axis="y", alpha=0.25)
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

    _plot_metric_by_scenario(
        metrics_df=metrics_df,
        metric="total_transfer_time_s",
        ylabel="Total Transfer Time (s)",
        output_path=report_dir / "plot_total_transfer_time_s.png",
    )
    _plot_metric_by_scenario(
        metrics_df=metrics_df,
        metric="total_bytes_moved",
        ylabel="Total Bytes Moved",
        output_path=report_dir / "plot_total_bytes_moved.png",
    )
    _plot_speedup(
        metrics_df=metrics_df,
        output_path=report_dir / "plot_speedup_bounce_vs_chain.png",
    )

    table_df = metrics_df.copy()
    table_df["total_transfer_time_s"] = table_df["total_transfer_time_s"].map(lambda v: f"{v:.6f}")
    table_df["speedup_vs_chain"] = table_df["speedup_vs_chain"].map(
        lambda v: "" if pd.isna(v) else f"{float(v):.6f}"
    )
    table_md = _build_markdown_table(table_df)

    citation_lines = []
    for citation_id, citation in sources.CITATIONS.items():
        citation_lines.append(
            f"- `{citation_id}`: {citation['url']}\n"
            f"  Quote: \"{citation['quote']}\""
        )

    report_text = (
        "# Flow-CXL Transfer Model Report\n\n"
        "## Single Claim Under Test\n"
        "Conventional host-bounce staging moves intermediate data over the host link each stage, while Flow-CXL chaining avoids intermediate host bounce and keeps only initial load + final output on the host link.\n\n"
        "## Fixed Configurations Run\n"
        "- Pipeline stages: 4\n"
        "- Payloads: FASTQ_100GB and RAW_1TB\n"
        "- Link/scenario matrix: PCIe bounce; CXL Local bounce+chain; CXL Remote bounce+chain\n"
        "- Queueing: 0 (serial transfers)\n\n"
        "## Results Table\n"
        f"{table_md}\n\n"
        "## Generated Plots\n"
        "- plot_total_transfer_time_s.png\n"
        "- plot_total_bytes_moved.png\n"
        "- plot_speedup_bounce_vs_chain.png\n\n"
        "## Citations\n"
        + "\n".join(citation_lines)
        + "\n"
    )

    report_path = report_dir / "report.md"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"Wrote {report_path}")


if __name__ == "__main__":
    main()
