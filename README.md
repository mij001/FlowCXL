# FlowCXL Tiled Stage-Capacity Pipeline Model

Small Python repo for comparing end-to-end pipeline behavior across:

- `cpu_only`
- `pim_host_bounce`
- `pim_flowcxl_direct`

The model enforces fixed compute units per stage, tiles large boundaries, and schedules tile work through contested compute/transfer resources.

## What is modeled

- Stage-limited compute pools (CPU or PIM)
- Tile-by-tile pipeline overlap
- Inter-stage transfer behavior:
  - true host bounce (`D2H -> HOST_TOUCH -> H2D`)
  - direct CXL (`PIM -> PIM`)
- Absolute makespan (seconds)
- Absolute total energy (joules)
- Bottleneck lower-bound diagnostics (`compute`, `host_link`, `host_touch`, `cxl_direct`)

## What is not modeled

- Cache effects and memory hierarchy internals
- Device-specific microarchitecture details beyond configurable rates/power

## Repository layout

- `sources.py`: cited constants and dataset boundaries
- `simulator.py`: tiled pipeline scheduler + energy model
- `configs/runs.yaml`: run matrix and model parameters
- `run.py`: executes runs and writes artifacts
- `report.py`: grouped bar plots and markdown report
- `docs/equations.md`: equations and scheduling rules
- `docs/modeling.md`: modeling choices and assumptions
- `tests/test_simulator.py`: correctness checks

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

Artifacts:

- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- `artifacts/report/plot_makespan_grouped_PROFILE_ONT_100Gbases.png`
- `artifacts/report/plot_makespan_grouped_PROFILE_ILLUMINA_NA12878.png`
- `artifacts/report/plot_energy_grouped_PROFILE_ONT_100Gbases.png`
- `artifacts/report/plot_energy_grouped_PROFILE_ILLUMINA_NA12878.png`
- `artifacts/report/report.md`

Note: `trace_max_tiles` in `configs/runs.yaml` limits trace artifact size only. Metrics still use all simulated tiles.

FlowCXL gains depend on the dominant bottleneck:

- if `compute_stage_max` or shared ingress/egress dominates, bounce-removal gains can be small,
- if bounce-specific `host_touch`/host-link costs dominate, FlowCXL direct gains become large.

## Tests

```bash
python -m unittest discover -s tests -v
```
