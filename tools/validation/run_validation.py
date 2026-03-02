"""Run the full validation pipeline (calibration, cross-check, sweeps, ablations)."""

from __future__ import annotations

import argparse
import copy
import sys
from pathlib import Path
from typing import Sequence

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.validation.calibrate_microbench import run_calibration
from tools.validation.common import deep_merge, load_yaml, save_yaml
from tools.validation.crosscheck_ps import run_crosscheck
from tools.validation.sensitivity import run_sensitivity
import sources


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


def _apply_validation_overlay(
    *,
    config: dict,
    overlay: dict,
) -> tuple[dict, dict[str, dict[str, object]]]:
    resolved_links: dict[str, dict[str, object]] = {
        str(link_id): dict(link_cfg) for link_id, link_cfg in sources.LINKS.items()
    }
    effective_overlay = copy.deepcopy(overlay)
    link_overrides = effective_overlay.pop("link_constant_overrides", {})
    if link_overrides:
        if not isinstance(link_overrides, dict):
            raise ValueError("validation overlay link_constant_overrides must be a mapping")
        for link_id, link_patch in link_overrides.items():
            if link_id not in resolved_links:
                raise ValueError(f"validation overlay references unknown link id: {link_id}")
            if not isinstance(link_patch, dict):
                raise ValueError(f"validation overlay for {link_id} must be a mapping")
            merged = dict(resolved_links[link_id])
            merged.update(link_patch)
            resolved_links[link_id] = merged
    return deep_merge(config, effective_overlay), resolved_links


def main(argv: Sequence[str] | None = None) -> None:
    args = _parse_args(argv)
    config = load_yaml(Path(args.config))
    validation_dir = Path(args.artifacts_dir) / "validation"
    validation_dir.mkdir(parents=True, exist_ok=True)

    cross_summary = run_crosscheck(config=config, out_dir=validation_dir)
    calibration_summary = run_calibration(
        config=config,
        out_dir=validation_dir,
        crosscheck_summary=cross_summary,
    )
    sensitivity_cfg = config
    sensitivity_links_catalog = {str(link_id): dict(link_cfg) for link_id, link_cfg in sources.LINKS.items()}
    overlay_yaml = calibration_summary.get("overlay_yaml")
    if isinstance(overlay_yaml, str) and overlay_yaml:
        overlay_path = Path(overlay_yaml)
        if overlay_path.exists():
            overlay_payload = load_yaml(overlay_path)
            sensitivity_cfg, sensitivity_links_catalog = _apply_validation_overlay(
                config=config,
                overlay=overlay_payload,
            )
    sensitivity_summary = run_sensitivity(
        config=sensitivity_cfg,
        links_catalog=sensitivity_links_catalog,
        out_dir=validation_dir,
        ablations_config_path=Path(args.ablations_config),
    )

    summary = {
        "config": args.config,
        "artifacts_dir": str(Path(args.artifacts_dir)),
        "calibration": calibration_summary,
        "crosscheck": cross_summary,
        "sensitivity": sensitivity_summary,
        "direct_status": calibration_summary.get("direct_status", ""),
        "calibration_available": bool(calibration_summary.get("fit_yaml")),
        "crosscheck_available": bool(cross_summary.get("crosscheck_csv")),
        "crosscheck_pass": cross_summary.get("crosscheck_pass", False),
        "crosscheck_mape_percent_mean": cross_summary.get("crosscheck_mape_percent_mean", ""),
        "crosscheck_mape_percent_max": cross_summary.get("crosscheck_mape_percent_max", ""),
        "crosscheck_n_points": cross_summary.get("n_points", 0),
        "direct_cited_envelope": {},
    }
    fit_yaml = calibration_summary.get("fit_yaml")
    if isinstance(fit_yaml, str) and fit_yaml:
        fit_path = Path(fit_yaml)
        if fit_path.exists():
            fit_payload = load_yaml(fit_path)
            summary["direct_cited_envelope"] = fit_payload.get("direct_cited_envelope", {})
    summary_path = validation_dir / "validation_summary.yaml"
    save_yaml(summary_path, summary)

    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
