# Modeling Notes

## Scope

- Model includes stage-limited compute, transfer costs, contention, and energy.
- Supports two templates selected per profile:
  - `deepvariant_3stage`
  - `tpch_3op`
- Large boundaries are tiled and processed chunk-by-chunk.
- Tile admission is bounded by `max_inflight_tiles`.
- Overlap is enabled across tiles and stages.

## Template behavior

### DeepVariant (`deepvariant_3stage`)

- Stages: `make_examples -> call_variants -> postprocess_variants`
- Boundaries are derived from coverage, candidate density, and tensor shape.
- CPU stage rates are calibrated from profile runtime shares.
- Memory ceiling is disabled by default to preserve calibrated stage-time behavior.

### TPC-H (`tpch_3op`)

- Stages: `scan_filter_project -> join -> groupby_agg`
- Boundaries are derived from rows/selectivity/fanout/reduction parameters.
- CPU stage rates come from template stage-rate defaults.
- Memory ceiling is enabled by default.
- Stage duration uses `max(compute_component, memory_component)`.
- Bytes touched are derived per stage via input/output/amplification factors.
- CPU and PIM memory ceilings are per-stage budgets shared across stage units.
- CPU stages use access-pattern DRAM-service descriptors:
  - `access_pattern`, `row_hit_rate`, `mlp`, `avg_miss_latency_ns`
  - sequential scan uses peak streaming cap
  - hash/group-by patterns can become latency-limited (`mlp * cacheline / latency / miss_fraction`)
- Legacy CPU random-access penalties remain as multiplicative compatibility factors after access-pattern service selection.
- CPU-only pipeline breakers can inject explicit `MATERIALIZE` operations (default boundaries: `S1->S2`, `S2->S3`).

## Stage-device mapping

Scenario maps are template-specific:

- DeepVariant defaults:
  - `cpu_only`: `cpu/cpu/cpu`
  - `pim_host_bounce`: `pim/pim/cpu`
  - `pim_flowcxl_direct`: `pim/pim/cpu`
- TPC-H defaults:
  - `cpu_only`: `cpu/cpu/cpu`
  - `pim_host_bounce`: `pim/pim/pim`
  - `pim_flowcxl_direct`: `pim/pim/pim`

`pim/pim/pim` in TPC-H creates two inter-PIM boundaries (`S1->S2`, `S2->S3`), where FlowCXL direct can significantly reduce host-bounce overhead.

## Transfer paths

Per adjacent stage transition:

- `cpu->cpu`: no transfer
- `cpu->pim`: `host_h2d_stage`
- `pim->cpu`: `host_d2h`
- `pim->pim`:
  - bounce: `host_d2h -> HOST_TOUCH -> host_h2d_stage`
  - direct: `cxl_direct`
- `cpu_only` (`tpch_3op`) breaker boundaries:
  - `MATERIALIZE` on CPU materialization pool

Ingress/egress rules:

- `host_h2d_ingress` only if stage 1 is PIM
- final `host_d2h` only if final stage is PIM
- Host staging uses directional links:
  - `host_h2d_link` for H2D paths
  - `host_d2h_link` for D2H paths

## Why FlowCXL gain can exceed 2x in TPC-H high profile

When scan selectivity and join fanout produce very large intermediates:

- bounce repeatedly pays `D2H + host_touch + H2D` on each inter-PIM boundary,
- direct pays one CXL inter-device transfer,
- host staging channels become shared bottlenecks.

This tends to push bounce toward `host_link`/`host_touch` LB dominance, while direct shifts bottlenecks toward `cxl_direct` or compute.

## Channels and contention

- Host H2D split pools:
  - ingress (`host_h2d_ingress_channels`)
  - stage staging (`host_h2d_stage_channels`)
- Other shared pools:
  - `host_d2h_channels`
  - `host_touch_channels`
  - `cxl_direct_channels`
  - `cpu_materialize_channels`

## Metrics

Per run:

- `makespan_s`, `total_energy_J`
- compute/transfer split including `host_touch_energy_J`
- transfer bytes (`host_link`, `cxl_direct`, `host_touch`, path-specific bytes)
- LB diagnostics (`lb_*`) and `dominant_lb_component`
- `pipeline_template`
- Memory-ceiling diagnostics:
  - `memory_ceiling_enabled`
  - `total_compute_time_component_s`
  - `total_cpu_mem_time_component_s`
  - `total_cpu_mem_latency_bound_time_component_s`
  - `total_cpu_mem_peak_bound_time_component_s`
  - `total_pim_mem_time_component_s`
- CPU materialization diagnostics:
  - `total_cpu_materialize_bytes`
  - `total_cpu_materialize_time_component_s`
  - `cpu_materialize_energy_J`
