"""Runner for contention-aware Flow-CXL transfer experiments."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd
import yaml

import sources
from simulator import generate_runs_from_config

BASE_METRICS_COLUMNS = [
    "run_id",
    "dataset_profile",
    "link_type",
    "shared_link",
    "num_chunks",
    "scenario",
    "num_stages",
    "transfers_count",
    "makespan_s",
    "bytes_pcie_h2d",
    "bytes_pcie_d2h",
    "bytes_cxl_h2d",
    "bytes_cxl_d2h",
    "total_bytes_moved",
    "queue_total_blocking_s",
    "queue_total_attributed_s",
]


def metrics_columns() -> List[str]:
    cols = list(BASE_METRICS_COLUMNS)
    for resource_name in sources.RESOURCE_NAMES:
        cols.append(f"queue_{resource_name}_s")
    for resource_name in sources.RESOURCE_NAMES:
        cols.append(f"util_{resource_name}")
    cols.append("speedup_vs_chain")
    return cols


def load_config(config_path: Path) -> Dict[str, object]:
    with config_path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def main() -> None:
    config_path = Path("configs/runs.yaml")
    if not config_path.exists():
        raise FileNotFoundError("configs/runs.yaml not found.")

    config = load_config(config_path)
    metrics, traces = generate_runs_from_config(config)

    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    metrics_df = pd.DataFrame(metrics)
    ordered_columns = metrics_columns()
    metrics_df = metrics_df.reindex(columns=ordered_columns)
    metrics_path = artifacts_dir / "metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)

    traces_df = pd.DataFrame(traces)
    traces_csv_path = artifacts_dir / "traces.csv"
    traces_df.to_csv(traces_csv_path, index=False)

    traces_yaml_path = artifacts_dir / "traces.yaml"
    with traces_yaml_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"events": traces}, handle, sort_keys=False)

    print(f"Wrote {metrics_path} ({len(metrics_df)} rows)")
    print(f"Wrote {traces_csv_path} ({len(traces_df)} rows)")
    print(f"Wrote {traces_yaml_path} ({len(traces)} events)")


if __name__ == "__main__":
    main()
