#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml

OVERLAY="artifacts/validation/microbench_overlay.yaml"
if [[ -f "${OVERLAY}" ]]; then
  python run.py --config configs/runs.yaml --artifacts-dir artifacts --validation-overlay "${OVERLAY}"
else
  python run.py --config configs/runs.yaml --artifacts-dir artifacts
fi

python report.py --config configs/runs.yaml --artifacts-dir artifacts
