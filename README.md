# Flow-CXL Transfer-Only Staging Model

Minimal Python repo for quantifying host-link staging-tax elimination between:

- `conventional_host_bounce`
- `flowcxl_chain`

Scope is intentionally strict: transfer/staging time only (no compute model).

## Repository Layout

- `sources.py`: fixed parameters and citations
- `simulator.py`: transfer model and fixed 10-run generation
- `run.py`: executes runs and writes artifacts
- `report.py`: reads metrics, creates plots and markdown report
- `docs/equations.md`: equations and scenario definitions
- `docs/sources.md`: source list and fixed numeric values
- `tests/test_simulator.py`: two required checks

## Requirements

- Python 3.10+
- `pyyaml`
- `pandas`
- `matplotlib`

Install:

```bash
pip install -r requirements.txt
```

## Run

```bash
python run.py
python report.py
```

Generated outputs:

- `artifacts/metrics.csv`
- `artifacts/traces.yaml`
- `artifacts/report/plot_total_transfer_time_s.png`
- `artifacts/report/plot_total_bytes_moved.png`
- `artifacts/report/plot_speedup_bounce_vs_chain.png`
- `artifacts/report/report.md`

## Tests

```bash
python -m unittest discover -s tests -v
```
