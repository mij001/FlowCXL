# Modeling Notes

## Scope

- Model includes stage-limited compute, transfer costs, contention, and energy.
- Large stage boundaries are tiled and processed chunk-by-chunk.
- Pipeline overlap is enabled across tiles and stages.

## Stage Compute Capacity

Each stage has a fixed number of compute units with a fixed per-unit throughput and power:

- CPU baseline uses `cpu_units`, `cpu_unit_compute_Bps`, `cpu_unit_power_W`.
- PIM scenarios use `pim_units`, `pim_unit_compute_Bps`, `pim_unit_power_W`.

Optional `stage_overrides` let any stage deviate from defaults.

## Transfer Paths

- `cpu_only`: no inter-stage transfer modeling.
- `pim_host_bounce`: data between PIM stages goes through host memory (`D2H` then `H2D`).
- `pim_flowcxl_direct`: data between PIM stages uses direct CXL device-to-device transfer.

Both PIM scenarios include host ingress (to stage 1) and host egress (from final stage).

## Links and Channels

- Host transfers use `link_profile.host_link` parameters from `sources.LINKS`.
- Direct transfers use `link_profile.cxl_direct_link`.
- Channel counts are limited (`host_h2d_channels`, `host_d2h_channels`, `cxl_direct_channels`), which introduces queueing at transfer resources.

## Stage-size Sweep

Default x-axis categories for grouped bars are:

- `0.5x`
- `1x`
- `2x`
- `4x`

All boundaries in a dataset profile are scaled together to preserve relative pipeline shape.

## Trace Sampling

`trace_max_tiles` controls how many tile IDs are written to `traces.csv`/`traces.yaml`.
Run metrics always use all tiles; only trace artifact size is bounded.

## Metrics

Per run:

- Absolute makespan (`makespan_s`)
- Absolute total energy (`total_energy_J`)
- Compute and transfer energy split
- Host-link bytes, direct-CXL bytes, and total bytes moved
