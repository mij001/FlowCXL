# DeepVariant FlowCXL Tiled Pipeline Model

Small Python repo for comparing DeepVariant-style end-to-end inference behavior across:

- `cpu_only`
- `pim_host_bounce`
- `pim_flowcxl_direct`

The model enforces fixed compute units per stage, tiles large boundaries, and schedules tile work through contested compute/transfer resources.
Pipeline stages are fixed to:

- `make_examples`
- `call_variants`
- `postprocess_variants`

## What is modeled

- Stage-limited compute pools with per-scenario stage-device mapping
  - `cpu_only`: `cpu/cpu/cpu`
  - `pim_host_bounce`: `pim/pim/cpu`
  - `pim_flowcxl_direct`: `pim/pim/cpu`
- Tile-by-tile pipeline overlap with bounded in-flight admission (`max_inflight_tiles`)
- Transition-aware transfer behavior:
  - `cpu->cpu`: no transfer
  - `cpu->pim`: `H2D(stage)`
  - `pim->cpu`: `D2H`
  - `pim->pim`: bounce (`D2H -> HOST_TOUCH -> H2D`) or direct CXL
- Split host H2D topology:
  - ingress H2D pool
  - inter-stage staging H2D pool
- DeepVariant boundary derivation from coverage/candidate density/tensor shape:
  - `num_examples = covered_bases * candidate_density * (coverage / ref_coverage)`
  - stage boundaries derived as `[aligned_input, example_tensors, call_outputs, postprocess_outputs]`
- Absolute makespan (seconds)
- Absolute total energy (joules)
- Bottleneck lower-bound diagnostics (`compute`, `host_link`, `host_touch`, `cxl_direct`)

## What is not modeled

- Cache effects and memory hierarchy internals
- Device-specific microarchitecture details beyond configurable rates/power

## Repository layout

- `sources.py`: cited constants, DeepVariant profile defaults, derived boundaries
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
- `artifacts/report/plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X.png`
- `artifacts/report/plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X.png`
- `artifacts/report/plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X.png`
- `artifacts/report/plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X.png`
- `artifacts/report/report.md`

Note: `trace_max_tiles` in `configs/runs.yaml` limits trace artifact size only. Metrics still use all simulated tiles.

FlowCXL gains depend on the dominant bottleneck:

- if `compute_stage_max` or shared ingress/egress dominates, bounce-removal gains can be small,
- if bounce-specific `host_touch`/host-link costs dominate, FlowCXL direct gains become large.

Default DeepVariant overlap-focused knobs in `configs/runs.yaml`:

- `pim_units = 32`
- `max_inflight_tiles = 128`
- `scenario_stage_device_map` with `pim/pim/cpu` mapping for PIM scenarios
- `pim_speedup_vs_cpu_by_stage` for calibrated per-stage PIM rates

## Tests

```bash
python -m unittest discover -s tests -v
```
