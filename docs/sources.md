# Sources and Cited Constants

All cited constants and references are recorded in `sources.py`.

## Transfer/link constants

- PCIe Gen4 x16 host bandwidth and fixed latency components
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

## Configurable modeling assumptions (not fixed hardware truth)

These are explicit knobs in `sources.py`/`configs/runs.yaml`:

- TPC-H profile parameters:
  - selectivity, join fanout, aggregation reduction ratio
  - row-byte widths per stage boundary
- Stage compute rates/speedups:
  - `cpu_stage_unit_compute_Bps_by_template`
  - `pim_speedup_vs_cpu_by_stage_by_template`
- Scenario stage maps:
  - `scenario_stage_device_map_by_template`
- Transfer-channel counts and per-channel power
- Host-touch throughput/fixed overhead
- Tile size and in-flight window

These knobs are intended for controlled sensitivity studies, not as universal measured constants.
