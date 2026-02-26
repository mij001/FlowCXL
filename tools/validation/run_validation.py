"""Run the full validation pipeline (calibration, cross-check, sweeps, ablations)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.validation.calibrate_microbench import run_calibration
from tools.validation.common import load_yaml, save_yaml
from tools.validation.crosscheck_ps import run_crosscheck
from tools.validation.sensitivity import run_sensitivity


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run FlowCXL validation artifact pipeline.")
    parser.add_argument("--config", default="configs/runs.yaml", help="Path to run config YAML.")
    parser.add_argument("--artifacts-dir", default="artifacts", help="Artifacts directory.")
    parser.add_argument(
        "--ablations-config",
        default="paper/configs/ablations.yaml",
        help="Path to ablations config.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config = load_yaml(Path(args.config))
    validation_dir = Path(args.artifacts_dir) / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)

    calibration_summary = run_calibration(config=config, out_dir=validation_dir)
    cross_summary = run_crosscheck(config=config, out_dir=validation_dir)
    sensitivity_summary = run_sensitivity(
        config=config,
        out_dir=validation_dir,
        ablations_config_path=Path(args.ablations_config),
    )

    summary = {
        "config": args.config,
        "artifacts_dir": str(Path(args.artifacts_dir)),
        "calibration": calibration_summary,
        "crosscheck": cross_summary,
        "sensitivity": sensitivity_summary,
    }
    summary_path = validation_dir / "validation_summary.yaml"
    save_yaml(summary_path, summary)

    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
