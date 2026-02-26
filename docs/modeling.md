# Modeling Notes

## Scope

- Tile-level pipeline simulator with bounded streaming admission.
- Scenario vocabulary is fixed:
  - `cpu_only`
  - `pim_host_bounce`
  - `pim_flowcxl_direct`
- Transfer DAG, retention, and CXL topology/concurrency are template-agnostic.

## Public vs Execution Stages

### DeepVariant (`deepvariant_3stage`)

Public stages (reported in metrics as `num_stages=3`):

- `make_examples`
- `call_variants`
- `postprocess_variants`

Execution kernels (scheduled as `num_kernels=5`):

- `make_examples_frontend`
- `make_examples_tensorize`
- `call_variants_infer`
- `call_variants_post`
- `postprocess_variants`

Default DeepVariant stage-device maps are 5 entries:

- `cpu_only`: `[cpu, cpu, cpu, cpu, cpu]`
- `pim_host_bounce`: `[cpu, pim, pim, pim, cpu]`
- `pim_flowcxl_direct`: `[cpu, pim, pim, pim, cpu]`

This creates two PIM->PIM boundaries (`tensorize->infer`, `infer->post`) where direct vs bounce differs.

### TPC-H (`tpch_3op`)

Execution stages and public stages are both 3:

- `scan_filter_project`
- `join`
- `groupby_agg`

Default maps:

- `cpu_only`: `[cpu, cpu, cpu]`
- `pim_host_bounce`: `[pim, pim, pim]`
- `pim_flowcxl_direct`: `[pim, pim, pim]`

## Transfer Semantics

Stage transition routing:

- `cpu->cpu`: no transfer
- `cpu->pim`: `host_h2d_stage`
- `pim->cpu`: `host_d2h`
- `pim->pim`:
  - bounce: `host_d2h -> HOST_TOUCH -> host_h2d_stage`
  - direct: CXL direct path with processor-sharing bandwidth model
  - retain: only when same endpoint + scenario/policy/capacity allow

Ingress/egress:

- `host_h2d_ingress` only when stage-1 device is PIM and ingress is not skipped.
- final `host_d2h` only when final stage device is PIM.

Ingressless:

- For configured scenarios, the first host->PIM transfer per tile is skipped.
- This can be `host_h2d_ingress` (PIM stage-1) or first `host_h2d_stage` (CPU frontend -> first PIM stage).
- Interpretation: stage-1 input is resident/pinned on the destination PIM endpoint before timed execution.
- It does not alter subsequent inter-stage transfer semantics or add extra direct bandwidth.

## Memory Systems

`memory_system_by_template` is the only accepted memory-service schema.

- `cpu_baseline_system`: baseline engine + per-stage memory service and queueing.
- `pim_system`: per-stage memory service and queueing.
- Template-level `enabled` switch gates `T_stage=max(T_compute,T_mem)` behavior.

CPU materialization barriers are engine-gated:

- `vectorized_pipeline`: typically no forced boundaries.
- `blocking_volcano`: configured breaker boundaries.

## CXL Direct Fairness

Direct transfers use symmetric processor sharing:

- total direct BW is shared equally among active direct streams.
- overlapping transfers slow each other symmetrically.
- slot cap is enforced from `channels * virtual_channels_per_channel`.

Topology striping is physically capped:

- `striping_factor = min(max_stripes, num_physical_links, active_direct_endpoints)`
- applied only for links enabled in `cxl_topology.applies_to_links`.

## Diagnostics

Metrics include:

- public/execution counts: `num_stages`, `num_kernels`
- transfer/retention/topology diagnostics
- memory service and queue-delay components
- lower-bound families and `dominant_lb_component`
