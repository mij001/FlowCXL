"""Runner for tiled stage-capacity FlowCXL experiments."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd
import yaml

from simulator import generate_runs_from_config

BASE_METRICS_COLUMNS = [
    "run_id",
    "dataset_profile",
    "stage_size_multiplier",
    "scenario",
    "num_stages",
    "num_tiles",
    "makespan_s",
    "total_energy_J",
    "compute_energy_J",
    "transfer_energy_J",
    "total_bytes_host_link",
    "total_bytes_cxl_direct",
    "total_bytes_moved",
]
TRACE_YAML_MAX_EVENTS = 2000


def metrics_columns() -> List[str]:
    return list(BASE_METRICS_COLUMNS)


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
    metrics_df = metrics_df.reindex(columns=metrics_columns())
    metrics_path = artifacts_dir / "metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)

    traces_df = pd.DataFrame(traces)
    traces_csv_path = artifacts_dir / "traces.csv"
    traces_df.to_csv(traces_csv_path, index=False)

    traces_yaml_path = artifacts_dir / "traces.yaml"
    yaml_events = traces[:TRACE_YAML_MAX_EVENTS]
    with traces_yaml_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(
            {
                "total_events": len(traces),
                "yaml_events": len(yaml_events),
                "truncated": len(traces) > len(yaml_events),
                "events": yaml_events,
            },
            handle,
            sort_keys=False,
        )

    print(f"Wrote {metrics_path} ({len(metrics_df)} rows)")
    print(f"Wrote {traces_csv_path} ({len(traces_df)} rows)")
    print(
        f"Wrote {traces_yaml_path} ({len(yaml_events)} events in YAML, total events {len(traces)})"
    )


if __name__ == "__main__":
    main()
