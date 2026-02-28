# Artifact Execution Guide

This file maps each reported figure/table to an executable command and expected output artifacts.

## Environment

Install pinned dependencies:

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.lock.txt
```

## Core Workflow

1. Validation pipeline:

```bash
python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml
```

Expected outputs:
- `artifacts/validation/microbench_raw.csv`
- `artifacts/validation/microbench_agg.csv`
- `artifacts/validation/microbench_fit.yaml`
- `artifacts/validation/microbench_overlay.yaml`
- `artifacts/validation/cxl_ps_crosscheck.csv`
- `artifacts/validation/sensitivity_results.csv`
- `artifacts/validation/tornado_top8.csv`
- `artifacts/validation/ablations.csv`
- `artifacts/validation/validation_summary.yaml`

2. Simulation + report with overlay:

```bash
python run.py --config configs/runs.yaml --artifacts-dir artifacts --validation-overlay artifacts/validation/microbench_overlay.yaml
python report.py --config configs/runs.yaml --artifacts-dir artifacts --metrics-file artifacts/metrics.csv
```

Expected outputs:
- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- `artifacts/report/report.md`
- grouped absolute plots in `artifacts/report/plot_*.png`

## Figure/Table Mapping

| Figure/Table | Command | Expected files |
| --- | --- | --- |
| Main grouped makespan/energy plots | `python report.py --config configs/runs.yaml --artifacts-dir artifacts --metrics-file artifacts/metrics.csv` | `artifacts/report/plot_makespan_grouped_<profile>_<variant>.png`, `artifacts/report/plot_energy_grouped_<profile>_<variant>.png`, `artifacts/report/plot_makespan_grouped_pim_only_<profile>_<variant>.png`, `artifacts/report/plot_energy_grouped_pim_only_<profile>_<variant>.png` |
| Validation measured-vs-sim curves | `python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml` then `python report.py ...` | `artifacts/report/plot_validation_measured_vs_sim_<path>.png` |
| Sensitivity bands / tornado | `python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml` then `python report.py ...` | `artifacts/validation/sensitivity_results.csv`, `artifacts/validation/tornado_top8.csv`, `artifacts/report/plot_validation_sweep_band_<family>.png` |
| Ablation table | `python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml` | `artifacts/validation/ablations.csv` |
| Provenance table | `python report.py --config configs/runs.yaml --artifacts-dir artifacts --metrics-file artifacts/metrics.csv` | `artifacts/report/report.md` (Parameter Provenance section) |

## One-command reproduction

```bash
bash paper/run_all.sh
```

or

```bash
make -C paper all
```

## Result tolerance statement

- Timing/ratio values are deterministic for fixed inputs but may differ at floating-point micro-level across Python/Numpy/Pandas builds.
- Validation checks use tolerance-based comparisons where appropriate.
- Story-gate thresholds are optional regression gates (`FLOWCXL_ENABLE_STORY_GATES=1`) and are not universal physical invariants.
