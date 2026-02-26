"""Shared helpers for validation tooling."""

from __future__ import annotations

import copy
from pathlib import Path
from typing import Dict, Mapping, MutableMapping

import yaml


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
