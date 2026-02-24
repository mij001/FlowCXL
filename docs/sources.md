# Sources and Cited Constants

All cited constants and references are recorded in `sources.py`.

## Transfer/link constants

- PCIe Gen4 x16 host bandwidth and fixed latency components
- Measured UPMEM-order host H2D and D2H bandwidth points (directional defaults)
- CXL local/remote representative latency and bandwidth points

These constants are used directly in transfer duration equations.

## DeepVariant anchors

- Stage names:
  - `make_examples`, `call_variants`, `postprocess_variants`
- Example tensor shape anchor (`example_info` style): `[100, 147, 10]`
- Runtime-share context used for CPU calibration defaults

## TPC-H / OLAP anchors

- TPC-H benchmark context for scan/join/aggregation pipeline modeling
- Host-bounce vs direct-copy context from direct-data-path literature/blogs
- PIM-operator context references for scan, join, and analytics aggregation
- OLAP memory-pressure context used to justify stage memory-ceiling modeling

## Configurable modeling assumptions (not fixed hardware truth)

These are explicit knobs in `sources.py`/`configs/runs.yaml`:

- TPC-H profile parameters:
  - selectivity, join fanout, aggregation reduction ratio
  - row-byte widths per stage boundary
- Stage compute rates/speedups:
  - `cpu_stage_unit_compute_Bps_by_template`
  - `pim_speedup_vs_cpu_by_stage_by_template`
- Stage memory-ceiling knobs:
  - `enable_memory_ceiling_by_template`
  - `dram_service_defaults`
  - `cpu_mem_Bps_by_stage_by_template`
  - `pim_mem_Bps_by_stage_by_template`
  - `bytes_touched_factors_by_stage_by_template`
  - `cpu_random_access_penalty_by_stage_by_template`
  - `cpu_access_pattern_by_stage_by_template`
- CPU pipeline-break materialization knobs:
  - `cpu_materialization_by_template`
  - `resource_capacity.cpu_materialize_channels`
  - `transfer_power_W.cpu_materialize_channel`
- Scenario stage maps:
  - `scenario_stage_device_map_by_template`
- Transfer-channel counts and per-channel power
- Host-touch throughput/fixed overhead
- Directional host-link selection (`host_h2d_link`, `host_d2h_link`)
- Tile size and in-flight window

These knobs are intended for controlled sensitivity studies, not as universal measured constants.
