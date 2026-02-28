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

## Optional Retiling + Glue/Barrier Model

The simulator now supports an optional boundary retiling layer (`tiling_model_by_template`) with:

- tile-domain remapping (`IDENTITY`, `GROUP_K_TO_1`, `SPLIT_1_TO_M`, `REPARTITION_HASH`)
- boundary glue costs (`GLUE_COPY`, `GLUE_REDUCE`, `GLUE_SHUFFLE`)
- barrier wait accounting before downstream stage release
- transition-keyed mappings (`<src_stage>-><dst_stage>`) with required stable `mapping_id`
- default glue contention on consumer compute resources (`glue_resource_mode: shared_consumer_compute`)

Example:

```yaml
tiling_model_by_template:
  tpch_3op:
    enabled: true
    glue_resource_mode: shared_consumer_compute
    boundary_mappings:
      "scan_filter_project->join":
        mapping_id: tpch_join_shuffle_v1
        mapping_type: REPARTITION_HASH
        partitions: pim_units
      "join->groupby_agg":
        mapping_id: tpch_groupby_reduce_v1
        mapping_type: GROUP_K_TO_1
        group_k: 4
```

Defaults keep `enabled: false` for both templates, preserving the previous linear tile-chain behavior.

## Optional PIM Mode Effects

`pim_mode_by_stage_by_template` and `pim_mode_effects` let each PIM stage run in one of
`NONE`, `BANK`, `BANK_GROUP`, `BUFFER` modes with deterministic compute/memory multipliers and
optional per-kernel command overhead.

## Validation Suite (Microbench + Cross-check + Sensitivity)

### 1) What This Suite Validates

- `tools/validation/calibrate_microbench.py`: measured CSV ingest, calibration fit, host-touch derivation/provenance, and overlay generation.
- `tools/validation/crosscheck_ps.py`: direct-link scheduler cross-check against an independent fluid processor-sharing reference model.
- `tools/validation/sensitivity.py`: sweep families (`cxl_link`, `pim_speedup`, `tpch_memory`, `energy`), top-8 tornado export, and ablation export.
- `tools/validation/run_validation.py`: orchestrates calibration + cross-check + sensitivity and writes `validation_summary.yaml`.

### 2) End-to-End Workflow

Canonical path A: full measured

1. Provide measured CSVs for `host_h2d`, `host_d2h`, `bounce`, and `direct`.
2. Run validation pipeline.
3. `microbench_fit.yaml` reports `direct_status=measured`.
4. `microbench_overlay.yaml` includes host links + direct link overrides.

Canonical path B: missing direct, cross-check enabled

1. Provide measured CSVs for required host paths; omit `direct`.
2. Keep `direct_provenance_policy.allow_crosscheck_only=true`.
3. Run validation pipeline.
4. `microbench_fit.yaml` reports `direct_status=crosscheck_only` (validated, not calibrated).
5. Direct override is not emitted; PS cross-check artifact is the validation evidence.

Canonical path C: missing direct, cited+sweep posture

1. Provide measured CSVs for required host paths; omit `direct`.
2. Set `direct_provenance_policy.allow_crosscheck_only=false` and `allow_cited_sweep_only=true`.
3. Run validation pipeline.
4. `microbench_fit.yaml` reports `direct_status=cited_sweep_only`.
5. Direct override is not emitted; direct path is treated as cited+sweep envelope only.

### 3) CLI Commands

Calibrate microbench inputs:

```bash
python tools/validation/calibrate_microbench.py --config configs/runs.yaml --out artifacts/validation
```

Run direct scheduler cross-check:

```bash
python tools/validation/crosscheck_ps.py --config configs/runs.yaml --out artifacts/validation
```

Run sensitivity + ablations:

```bash
python tools/validation/sensitivity.py --config configs/runs.yaml --out artifacts/validation --ablations-config paper/configs/ablations.yaml
```

Run full validation orchestration:

```bash
python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml
```

Run workload simulation with generated overlay:

```bash
python run.py --config configs/runs.yaml --artifacts-dir artifacts --validation-overlay artifacts/validation/microbench_overlay.yaml
python report.py --config configs/runs.yaml --artifacts-dir artifacts --metrics-file artifacts/metrics.csv
```

### 4) Validation Config Schema (validation.\*)

```yaml
validation:
  system_id: system_x_2026q1
  calibration:
    enabled: true
    input_mode: measured_csv
    measured_inputs:
      host_h2d: tools/validation/sample_inputs/system_x_2026q1/host_h2d.csv
      host_d2h: tools/validation/sample_inputs/system_x_2026q1/host_d2h.csv
      bounce: tools/validation/sample_inputs/system_x_2026q1/bounce.csv
      direct: tools/validation/sample_inputs/system_x_2026q1/direct.csv
      host_touch: tools/validation/sample_inputs/system_x_2026q1/host_touch.csv
    required_paths: [host_h2d, host_d2h, bounce]
    optional_paths: [direct]
    required_points_min_samples: 5
    coverage_policy:
      warn_on_low_samples: true
      fail_on_missing_required_point: true
    memory_mode_policy:
      required_paths_must_be_pinned: true
      allow_mixed_memory_mode: false
      pinned_column: pinned
    host_touch_sanity:
      enabled: true
      expected_bandwidth_Bps: null
      ratio_min: 0.2
      ratio_max: 2.0
      warn_only_if_missing_reference: true
    negative_residual_policy:
      mode: clamp_to_zero
      epsilon_s: 1.0e-9
    direct_provenance_policy:
      allow_crosscheck_only: true
      allow_cited_sweep_only: true
      cited_latency_ns_range: [214, 394]
      cited_bandwidth_GBps_range: [18, 52]
    payload_bytes: [4194304, 33554432, 268435456]
    concurrency_levels: [1, 2, 4, 8]
    repetitions: 10
    fit_model: latency_plus_bytes_over_bw
    fit_reference_concurrency: 1
    aggregate_stat: median
    ceiling_check:
      enabled: true
      pcie_gen: 4
      lane_width: 16
      max_one_way_utilization_fraction: 0.95
      fail_on_violation: false
  crosscheck:
    enabled: true
    reference_model: processor_share
    tolerance_mape_percent: 5.0
  sensitivity:
    enabled: true
    families: [cxl_link, pim_speedup, tpch_memory]
  energy:
    mode: relative_sweep
    power_scale_factors: [0.7, 1.0, 1.3]
```

### 5) Measured CSV Contracts

Transfer-path CSVs (`host_h2d`, `host_d2h`, `bounce`, optional `direct`):

| Column              | Required | Type            | Notes                                              |
| ------------------- | -------- | --------------- | -------------------------------------------------- |
| `system_id`         | yes      | string          | Must match `validation.system_id`                  |
| `path`              | yes      | enum string     | one of transfer paths                              |
| `payload_bytes`     | yes      | int > 0         | per transaction payload                            |
| `concurrency`       | yes      | int > 0         | stream count for this sample                       |
| `repetition`        | yes      | int >= 0        | sample id only                                     |
| `time_s`            | yes      | float > 0       | measured duration                                  |
| `pinned`            | yes      | bool-like token | required by default policy for required host paths |
| `tool`              | optional | string          | e.g. `pcie_bench`, `bandwidthTest`, `custom`       |
| `numa_policy`       | optional | string          | measurement context                                |
| `dma_engine`        | optional | string          | measurement context                                |
| `percentile_source` | optional | string          | provenance                                         |
| `timestamp`         | optional | string          | provenance                                         |
| `notes`             | optional | string          | provenance                                         |

Optional host-touch CSV (`host_touch`):

| Column                                                                         | Required | Type      | Notes                             |
| ------------------------------------------------------------------------------ | -------- | --------- | --------------------------------- |
| `system_id`                                                                    | yes      | string    | Must match `validation.system_id` |
| `path`                                                                         | yes      | string    | must be `host_touch`              |
| `payload_bytes`                                                                | yes      | int > 0   | payload size                      |
| `repetition`                                                                   | yes      | int >= 0  | sample id only                    |
| `time_s`                                                                       | yes      | float > 0 | measured host-touch time          |
| `concurrency`                                                                  | optional | int > 0   | defaults to `1` if omitted        |
| `tool`, `numa_policy`, `dma_engine`, `percentile_source`, `timestamp`, `notes` | optional | string    | context fields                    |

Default sample inputs (for local exercisability):

- `tools/validation/sample_inputs/system_x_2026q1/host_h2d.csv`
- `tools/validation/sample_inputs/system_x_2026q1/host_d2h.csv`
- `tools/validation/sample_inputs/system_x_2026q1/bounce.csv`
- `tools/validation/sample_inputs/system_x_2026q1/direct.csv`
- `tools/validation/sample_inputs/system_x_2026q1/host_touch.csv`

### 6) Calibration Semantics

- Aggregate-first comparison: measured rows are aggregated by `(path,payload_bytes,concurrency)` with `aggregate_stat`.
- `repetition` is sample-id only and is not used as a join key for prediction.
- Fit model per path:
  - `T = latency_s + bytes / bandwidth_Bps`
  - fitted at `fit_reference_concurrency`.
- Host-touch provenance:
  - `measured_stream`: fitted directly from `host_touch` measured CSV.
  - `derived_from_bounce`: estimated via `bounce - d2h_fit - h2d_fit`.
- Negative residual handling policy is explicit and audited:
  - `drop`
  - `clamp_to_zero`
  - `clamp_to_epsilon`
- PCIe one-way ceiling is a sanity check only (not a full throughput oracle).

### 7) Direct Path Provenance States

- `measured`: direct path has measured CSV and fitted parameters.
- `crosscheck_only`: direct path not measured; validated against PS cross-check results. This is validated, not calibrated.
- `cited_sweep_only`: direct path not measured and treated as cited+sweep envelope only.

Only `direct_status=measured` can write direct link overrides into `microbench_overlay.yaml`.

### 8) Coverage and Data-Quality Policy

- Required measured paths: `host_h2d`, `host_d2h`, `bounce`.
- Optional measured paths: `direct`, `host_touch`.
- Required host paths must be pinned by default (`required_paths_must_be_pinned=true`).
- Mixed pinned/pageable rows for required paths are rejected by default (`allow_mixed_memory_mode=false`).
- Coverage requires at least `required_points_min_samples` per required `(path,payload,concurrency)` point.
- Missing required points fail by default (`fail_on_missing_required_point=true`).
- Low-sample points are reported via `coverage_warnings`.

### 9) Output Artifacts and How to Read Them

| Artifact                                       | Producer                  | Purpose                                                    | Key fields                                                                                     | Downstream usage                       |
| ---------------------------------------------- | ------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | -------------------------------------- |
| `artifacts/validation/microbench_raw.csv`      | `calibrate_microbench.py` | normalized measured rows + per-row predictions             | `measured_s`, `simulated_s`, `error_s`, context columns                                        | audit raw ingestion                    |
| `artifacts/validation/microbench_agg.csv`      | `calibrate_microbench.py` | aggregate-level measured vs simulated comparison           | `path,payload_bytes,concurrency,sample_count,measured_s,simulated_s`                           | report measured-vs-sim plots           |
| `artifacts/validation/microbench_fit.yaml`     | `calibrate_microbench.py` | fit parameters, statuses, coverage/semantics/sanity audits | `paths`, `direct_status`, `host_touch_source`, `coverage_summary`, `negative_residual_summary` | report tables + audit record           |
| `artifacts/validation/microbench_overlay.yaml` | `calibrate_microbench.py` | run-time overlay derived from measured host calibration    | `link_constant_overrides`, `stage_defaults`                                                    | input to `run.py --validation-overlay` |
| `artifacts/validation/cxl_ps_crosscheck.csv`   | `crosscheck_ps.py`        | direct scheduler cross-check evidence                      | `mape_percent`, `passes_tolerance`                                                             | validates `crosscheck_only` posture    |
| `artifacts/validation/sensitivity_results.csv` | `sensitivity.py`          | sweep outcomes                                             | ratio columns + `sweep_family/sweep_case`                                                      | validation appendix                    |
| `artifacts/validation/tornado_top8.csv`        | `sensitivity.py`          | top contributors near target point                         | `effect_score`, deltas                                                                         | robustness summary                     |
| `artifacts/validation/ablations.csv`           | `sensitivity.py`          | ablation outcomes at `1x`                                  | `ablation`, ratio columns                                                                      | ablation appendix                      |
| `artifacts/validation/validation_summary.yaml` | `run_validation.py`       | orchestration summary pointer file                         | calibration/crosscheck/sensitivity outputs                                                     | quick run integrity check              |

### 10) Applying Calibration Overlay to Simulation

Generate overlay:

```bash
python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts --ablations-config paper/configs/ablations.yaml
```

Apply overlay:

```bash
python run.py --config configs/runs.yaml --artifacts-dir artifacts --validation-overlay artifacts/validation/microbench_overlay.yaml
python report.py --config configs/runs.yaml --artifacts-dir artifacts --metrics-file artifacts/metrics.csv
```

Reproducibility note: overlay link overrides are applied via an injected link catalog for a run; base `sources.LINKS` is not globally mutated.

### 11) Report Integration (Validation Appendix)

`report.py` consumes validation artifacts and appends:

- measured-vs-sim plots (from aggregate data)
- measurement semantics table (`pinned`, `tool`, `numa_policy`, `dma_engine`)
- coverage quality table
- host-touch provenance/sanity section
- residual policy audit section
- one-way PCIe sanity summary
- direct-status narrative (`measured`, `crosscheck_only`, `cited_sweep_only`)

### 12) Troubleshooting and Common Failures

| Symptom / Error                                    | Likely cause                                       | Fix                                                                      |
| -------------------------------------------------- | -------------------------------------------------- | ------------------------------------------------------------------------ |
| `required measured CSV not found`                  | path missing in `measured_inputs`                  | fix path or file placement                                               |
| `missing required columns`                         | CSV schema mismatch                                | add required columns for that path type                                  |
| `contains non-pinned rows`                         | required host path has pageable/unknown rows       | use pinned rows or relax `required_paths_must_be_pinned`                 |
| `mixes pinned/pageable states`                     | mixed memory mode in required path                 | harmonize rows or set `allow_mixed_memory_mode=true`                     |
| `missing required payload/concurrency points`      | coverage gaps                                      | add missing `(payload,concurrency)` samples                              |
| `points below required_points_min_samples` warning | too few reps per point                             | increase sample count or lower threshold                                 |
| unsupported enum / invalid policy config           | typo or invalid config value                       | align with allowed values in schema                                      |
| strict PCIe ceiling failure                        | measured/fitted one-way throughput above threshold | verify measurement setup or lower strictness (`fail_on_violation=false`) |

### 13) Reproducibility Checklist

- Set `validation.system_id` for the target machine.
- Ensure measured CSVs exist and satisfy schema/policy.
- Run full validation pipeline.
- Verify these artifacts exist:
  - `microbench_raw.csv`
  - `microbench_agg.csv`
  - `microbench_fit.yaml`
  - `microbench_overlay.yaml`
  - `cxl_ps_crosscheck.csv`
  - `sensitivity_results.csv`
  - `tornado_top8.csv`
  - `ablations.csv`
  - `validation_summary.yaml`
- Run simulation with `--validation-overlay`.
- Regenerate report and inspect the validation appendix.
- Cross-check claims against `paper/CLAIMS.md`.

## Tests

```bash
python -m unittest discover -s tests -v
```
