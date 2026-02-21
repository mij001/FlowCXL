"""Runner for fixed Flow-CXL transfer-model experiments."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import yaml

from simulator import generate_fixed_runs

METRICS_COLUMNS = [
    "run_id",
    "payload_name",
    "payload_bytes",
    "link_type",
    "scenario",
    "transfers_count",
    "total_bytes_moved",
    "total_transfer_time_s",
    "speedup_vs_chain",
]


def main() -> None:
    metrics, traces = generate_fixed_runs()

    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    metrics_df = pd.DataFrame(metrics, columns=METRICS_COLUMNS)
    metrics_path = artifacts_dir / "metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)

    traces_path = artifacts_dir / "traces.yaml"
    with traces_path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump({"transfers": traces}, handle, sort_keys=False)

    print(f"Wrote {metrics_path} ({len(metrics_df)} rows)")
    print(f"Wrote {traces_path} ({len(traces)} transfers)")


if __name__ == "__main__":
    main()
