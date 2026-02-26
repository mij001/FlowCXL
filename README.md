# FlowCXL Tiled Stage-Capacity Pipeline Model

Python simulator for multi-stage pipeline comparison across:

- `cpu_only`
- `pim_host_bounce`
- `pim_flowcxl_direct`

It models fixed compute capacity per stage, tile-level pipelining, bounded streaming admission, host-bounce penalties, and direct CXL movement.

## Supported templates

- `deepvariant_3stage`
  - `make_examples -> call_variants -> postprocess_variants`
  - internally executed as 5 kernels:
    - `make_examples_frontend`
    - `make_examples_tensorize`
    - `call_variants_infer`
    - `call_variants_post`
    - `postprocess_variants`
- `tpch_3op`
  - `scan_filter_project -> join -> groupby_agg`

Template selection is profile-driven from `sources.py` (`pipeline_template` field per profile).

## Current default runs

`configs/runs.yaml` defaults to TPCH + DeepVariant profiles:

- `PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE`
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE`
- `PROFILE_DV_ILLUMINA_WGS_30X`
- `PROFILE_DV_ILLUMINA_WES_100X`

All default profiles run across:

- variants: `base`, `ingressless`, `retention_colocated`, `switch_striping`
- multipliers: `0.5x, 1x, 2x, 4x`
- scenarios: `cpu_only`, `pim_host_bounce`, `pim_flowcxl_direct`

The high-intermediate TPCH profile is configured to expose host-bounce penalties and target `bounce/direct >= 2.0` at `1x`.

## What is modeled

- Stage-limited compute pools (CPU/PIM)
- First-class template-scoped memory systems via `memory_system_by_template`:
  - `cpu_baseline_system` and `pim_system` are configured separately
  - DeepVariant defaults to `enabled: false` (compute-only behavior preserved)
  - TPC-H defaults to `enabled: true`
- Memory service model (when enabled):
  - `T_stage = max(T_compute, T_mem)`
  - service BW from access pattern + miss behavior + peak BW
  - queueing multiplier from utilization (`queue_alpha`, `rho_cap`)
  - bytes-touched factors model scan/join/group-by read/write pressure
  - CPU penalty multiplier remains supported for compatibility as part of stage service config
- Per-template scenario stage-device maps
- DeepVariant public-vs-execution modeling:
  - public metrics remain 3-stage (`num_stages=3`)
  - execution uses 5 kernels (`num_kernels=5`) to model internal PIM->PIM boundaries
- Tile overlap with bounded in-flight window (`max_inflight_tiles`)
- Transition-aware transfer graph:
  - `cpu->cpu`: no transfer
  - `cpu->pim`: `host_h2d_stage`
  - `pim->cpu`: `host_d2h`
  - `pim->pim`:
    - bounce: `host_d2h -> HOST_TOUCH -> host_h2d_stage`
    - direct: `cxl_direct`
- Split host H2D topology: ingress vs stage channels
- Directional host links for host staging (`host_h2d_link`, `host_d2h_link`)
  - Legacy `host_link` is still accepted and mapped to both directions
- Symmetric processor-sharing CXL direct model for overlapping transfers
- CPU materialization policy is baseline-engine gated:
  - `baseline_engine=vectorized_pipeline` (default): no forced barriers
  - `baseline_engine=blocking_volcano`: configured breaker boundaries (default `[1,2]`)
- Makespan and energy accounting
- Lower-bound bottleneck diagnostics (`lb_*`, `dominant_lb_component`)
- Memory-ceiling diagnostics:
  - `memory_ceiling_enabled`
  - `cpu_baseline_engine`
  - `total_compute_time_component_s`
  - `total_cpu_mem_time_component_s`
  - `total_cpu_mem_latency_bound_time_component_s`
  - `total_cpu_mem_peak_bound_time_component_s`
  - `total_cpu_mem_service_time_component_s`
  - `total_cpu_mem_queue_delay_component_s`
  - `total_pim_mem_time_component_s`
  - `total_pim_mem_service_time_component_s`
  - `total_pim_mem_queue_delay_component_s`
  - `total_cpu_materialize_time_component_s`

## Repo layout

- `sources.py`: constants, profile definitions, derived boundaries, citations
- `simulator.py`: scheduler, transfer graph, energy model
- `configs/runs.yaml`: run matrix + template-specific knobs
- `run.py`: execute runs and write artifacts
- `report.py`: grouped bars and markdown report
- `tests/test_simulator.py`: model checks
- `docs/`: equations, modeling notes, sources

Model contract details:

- `docs/model_contract.md`

## Run

```bash
python run.py --config configs/runs.yaml --artifacts-dir artifacts
python report.py --config configs/runs.yaml --artifacts-dir artifacts
```

Validation pipeline:

```bash
python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts
```

Claim-to-evidence mapping for paper artifacts:

- `paper/CLAIMS.md`

Artifacts:

- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- `artifacts/report/plot_makespan_grouped_*.png`
- `artifacts/report/plot_energy_grouped_*.png`
- `artifacts/report/plot_makespan_grouped_pim_only_*.png`
- `artifacts/report/plot_energy_grouped_pim_only_*.png`
- `artifacts/report/report.md`

Report structure:

- main body: `base` + `ingressless` per profile
- appendix: `retention_colocated` + `switch_striping`

## Tests

```bash
python -m unittest discover -s tests -v
```
