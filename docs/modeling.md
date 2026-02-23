# Modeling Notes

## Scope

- Model includes stage-limited compute, transfer costs, contention, and energy.
- Pipeline template is fixed to DeepVariant inference stages:
  - `make_examples`
  - `call_variants`
  - `postprocess_variants`
- Large stage boundaries are tiled and processed chunk-by-chunk.
- Pipeline overlap is enabled across tiles and stages.
- Tile admission is bounded by `max_inflight_tiles` to avoid all-at-once ingress flooding.

## Stage Compute Capacity

Each stage has a fixed number of compute units with a fixed per-unit throughput and power:

- CPU baseline uses `cpu_units`, `cpu_unit_compute_Bps`, `cpu_unit_power_W`.
- PIM scenarios use `pim_units`, `pim_unit_compute_Bps`, `pim_unit_power_W`.

CPU/PIM per-stage rates are calibrated from profile timing shares at `1x`, then scaled by input bytes.
Optional `stage_overrides` let any stage deviate from calibrated defaults.

## DeepVariant Boundary Derivation

Profile boundaries are not hand-entered sizes. They are derived from:

- covered bases and coverage (`aligned` input bytes)
- candidate density (`num_examples`)
- tensor shape (`example_info`) and element width (`make_examples` output bytes)
- per-example call/postprocess output widths

Boundaries are emitted as:

- `X0`: aligned input bytes
- `X1`: example tensor bytes
- `X2`: call output bytes
- `X3`: postprocess output bytes

## Transfer Paths

Stage-device mapping is explicit per scenario:

- `cpu_only`: `cpu/cpu/cpu`
- `pim_host_bounce`: `pim/pim/cpu`
- `pim_flowcxl_direct`: `pim/pim/cpu`

Transfer ops are generated from adjacent stage-device transitions:

- `cpu->cpu`: no transfer
- `cpu->pim`: host `H2D(stage)`
- `pim->cpu`: host `D2H`
- `pim->pim`:
  - bounce: `D2H -> HOST_TOUCH -> H2D(stage)`
  - direct: `CXL direct`

Under the default map, only the `make_examples -> call_variants` (`pim->pim`) transition differs between bounce and direct. This isolates FlowCXL benefit to inter-PIM movement.

## Links and Channels

- Host transfers use `link_profile.host_link` parameters from `sources.LINKS`.
- Direct transfers use `link_profile.cxl_direct_link`.
- Channel counts are limited (`host_h2d_ingress_channels`, `host_h2d_stage_channels`, `host_d2h_channels`, `cxl_direct_channels`, `host_touch_channels`), which introduces queueing at transfer resources.
- Host ingress and inter-stage staging use separate H2D pools to decouple their contention effects.
- Host-touch uses shared host resource contention and a configurable per-touch model (`host_touch_fixed_s + bytes / host_touch_Bps`).

## Stage-size Sweep

Default x-axis categories for grouped bars are:

- `0.5x`
- `1x`
- `2x`
- `4x`

All boundaries in a dataset profile are scaled together to preserve relative pipeline shape.
Default overlap-focused settings use `pim_units=32` and `max_inflight_tiles=128`.

## Trace Sampling

`trace_max_tiles` controls how many tile IDs are written to `traces.csv`/`traces.yaml`.
Run metrics always use all tiles; only trace artifact size is bounded.

## Metrics

Per run:

- Absolute makespan (`makespan_s`)
- Absolute total energy (`total_energy_J`)
- Compute and transfer energy split (including `host_touch_energy_J`)
- Host-link bytes, direct-CXL bytes, host-touch bytes, and total bytes moved
- Lower-bound bottleneck attribution (`lb_*` fields + `dominant_lb_component`)
- `pipeline_template` metadata (`deepvariant_3stage`)
