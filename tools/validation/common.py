"""Shared helpers for validation tooling."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Dict, Mapping, MutableMapping

import yaml

VALID_CALIBRATION_PATHS = ("host_h2d", "host_d2h", "bounce", "direct")
DEFAULT_REQUIRED_CALIBRATION_PATHS = ("host_h2d", "host_d2h", "bounce")
DEFAULT_OPTIONAL_CALIBRATION_PATHS = ("direct",)


def load_yaml(path: Path) -> Dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"YAML root must be a mapping: {path}")
    return payload


def save_yaml(path: Path, payload: Mapping[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(dict(payload), handle, sort_keys=False)


def deep_merge(base: Mapping[str, object], patch: Mapping[str, object]) -> Dict[str, object]:
    merged = copy.deepcopy(dict(base))
    for key, value in patch.items():
        if key in merged and isinstance(merged[key], MutableMapping) and isinstance(value, Mapping):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def ensure_validation_config(config: Mapping[str, object]) -> Dict[str, object]:
    raw = config.get("validation")
    if not isinstance(raw, Mapping):
        raise KeyError("missing required config block: validation")
    validation = dict(raw)
    if not validation.get("system_id"):
        raise KeyError("validation.system_id is required")
    for key in ["calibration", "crosscheck", "sensitivity", "energy"]:
        if key not in validation or not isinstance(validation[key], Mapping):
            raise KeyError(f"validation.{key} must be a mapping")
    return validation


def ensure_calibration_config(validation: Mapping[str, object]) -> Dict[str, object]:
    raw = validation.get("calibration")
    if not isinstance(raw, Mapping):
        raise KeyError("validation.calibration must be a mapping")
    cal = dict(raw)

    enabled = bool(cal.get("enabled", False))
    cal["enabled"] = enabled
    if not enabled:
        return cal

    input_mode = str(cal.get("input_mode", "measured_csv"))
    if input_mode != "measured_csv":
        raise ValueError("validation.calibration.input_mode must be measured_csv")
    cal["input_mode"] = input_mode

    payload_bytes = cal.get("payload_bytes")
    concurrency_levels = cal.get("concurrency_levels")
    if not isinstance(payload_bytes, list) or not payload_bytes:
        raise KeyError("validation.calibration.payload_bytes must be a non-empty list")
    if not isinstance(concurrency_levels, list) or not concurrency_levels:
        raise KeyError("validation.calibration.concurrency_levels must be a non-empty list")
    cal["payload_bytes"] = [int(v) for v in payload_bytes]
    cal["concurrency_levels"] = [int(v) for v in concurrency_levels]

    repetitions = int(cal.get("repetitions", 1))
    if repetitions <= 0:
        raise ValueError("validation.calibration.repetitions must be > 0")
    cal["repetitions"] = repetitions

    fit_model = str(cal.get("fit_model", "latency_plus_bytes_over_bw"))
    if fit_model != "latency_plus_bytes_over_bw":
        raise ValueError("validation.calibration.fit_model must be latency_plus_bytes_over_bw")
    cal["fit_model"] = fit_model

    fit_reference_concurrency = int(cal.get("fit_reference_concurrency", 1))
    if fit_reference_concurrency <= 0:
        raise ValueError("validation.calibration.fit_reference_concurrency must be > 0")
    cal["fit_reference_concurrency"] = fit_reference_concurrency

    aggregate_stat = str(cal.get("aggregate_stat", "median"))
    if aggregate_stat not in {"median", "mean"}:
        raise ValueError("validation.calibration.aggregate_stat must be median or mean")
    cal["aggregate_stat"] = aggregate_stat

    measured_inputs_raw = cal.get("measured_inputs")
    if measured_inputs_raw is None:
        paths_raw = cal.get("paths")
        if not isinstance(paths_raw, list) or not paths_raw:
            raise KeyError(
                "validation.calibration requires measured_inputs map (or legacy paths list)"
            )
        input_dir = str(cal.get("input_dir", "artifacts/validation_inputs"))
        measured_inputs_raw = {str(path): str(Path(input_dir) / f"{path}.csv") for path in paths_raw}

    if not isinstance(measured_inputs_raw, Mapping) or not measured_inputs_raw:
        raise KeyError("validation.calibration.measured_inputs must be a non-empty map")
    measured_inputs: Dict[str, str] = {}
    for path_name, path_value in measured_inputs_raw.items():
        name = str(path_name)
        if name not in VALID_CALIBRATION_PATHS:
            raise ValueError(f"unknown calibration path in measured_inputs: {name}")
        csv_path = str(path_value).strip()
        if not csv_path:
            raise ValueError(f"empty CSV path for calibration path {name}")
        measured_inputs[name] = csv_path
    cal["measured_inputs"] = measured_inputs

    required_paths_raw = cal.get("required_paths", list(DEFAULT_REQUIRED_CALIBRATION_PATHS))
    optional_paths_raw = cal.get("optional_paths", list(DEFAULT_OPTIONAL_CALIBRATION_PATHS))
    if not isinstance(required_paths_raw, list) or not isinstance(optional_paths_raw, list):
        raise ValueError("validation.calibration required_paths/optional_paths must be lists")
    required_paths = [str(v) for v in required_paths_raw]
    optional_paths = [str(v) for v in optional_paths_raw]

    for path_name in required_paths + optional_paths:
        if path_name not in VALID_CALIBRATION_PATHS:
            raise ValueError(f"unknown calibration path in required/optional lists: {path_name}")
    overlap = set(required_paths) & set(optional_paths)
    if overlap:
        raise ValueError(f"calibration paths cannot be both required and optional: {sorted(overlap)}")

    for path_name in required_paths:
        if path_name not in measured_inputs:
            raise KeyError(f"required calibration path missing from measured_inputs: {path_name}")

    cal["required_paths"] = required_paths
    cal["optional_paths"] = optional_paths

    ceiling_raw = cal.get("ceiling_check", {})
    if not isinstance(ceiling_raw, Mapping):
        raise ValueError("validation.calibration.ceiling_check must be a mapping")
    ceiling = {
        "enabled": bool(ceiling_raw.get("enabled", False)),
        "pcie_gen": int(ceiling_raw.get("pcie_gen", 4)),
        "lane_width": int(ceiling_raw.get("lane_width", 16)),
        "max_one_way_utilization_fraction": float(ceiling_raw.get("max_one_way_utilization_fraction", 0.95)),
        "fail_on_violation": bool(ceiling_raw.get("fail_on_violation", False)),
    }
    if ceiling["lane_width"] <= 0:
        raise ValueError("validation.calibration.ceiling_check.lane_width must be > 0")
    if ceiling["max_one_way_utilization_fraction"] <= 0.0:
        raise ValueError(
            "validation.calibration.ceiling_check.max_one_way_utilization_fraction must be > 0"
        )
    cal["ceiling_check"] = ceiling

    return cal
