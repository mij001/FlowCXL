# FlowCXL Tiled Stage-Capacity Pipeline Model

Python simulator for multi-stage pipeline comparison across:

- `cpu_only`
- `pim_host_bounce`
- `pim_flowcxl_direct`

It models fixed compute capacity per stage, tile-level pipelining, bounded streaming admission, host-bounce penalties, and direct CXL movement.

## Supported templates

- `deepvariant_3stage`
  - `make_examples -> call_variants -> postprocess_variants`
- `tpch_3op`
  - `scan_filter_project -> join -> groupby_agg`

Template selection is profile-driven from `sources.py` (`pipeline_template` field per profile).

## Current default runs

`configs/runs.yaml` defaults to two TPC-H-like profiles:

- `PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE`
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE`

The high-intermediate profile is configured to expose host-bounce penalties and target `bounce/direct >= 2.0` at `1x`.

## What is modeled

- Stage-limited compute pools (CPU/PIM)
- Per-template scenario stage-device maps
- Tile overlap with bounded in-flight window (`max_inflight_tiles`)
- Transition-aware transfer graph:
  - `cpu->cpu`: no transfer
  - `cpu->pim`: `host_h2d_stage`
  - `pim->cpu`: `host_d2h`
  - `pim->pim`:
    - bounce: `host_d2h -> HOST_TOUCH -> host_h2d_stage`
    - direct: `cxl_direct`
- Split host H2D topology: ingress vs stage channels
- Makespan and energy accounting
- Lower-bound bottleneck diagnostics (`lb_*`, `dominant_lb_component`)

## Repo layout

- `sources.py`: constants, profile definitions, derived boundaries, citations
- `simulator.py`: scheduler, transfer graph, energy model
- `configs/runs.yaml`: run matrix + template-specific knobs
- `run.py`: execute runs and write artifacts
- `report.py`: grouped bars and markdown report
- `tests/test_simulator.py`: model checks
- `docs/`: equations, modeling notes, sources

## Run

```bash
python run.py
python report.py
```

Artifacts:

- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- `artifacts/report/plot_makespan_grouped_*.png`
- `artifacts/report/plot_energy_grouped_*.png`
- `artifacts/report/report.md`

## Tests

```bash
python -m unittest discover -s tests -v
```
