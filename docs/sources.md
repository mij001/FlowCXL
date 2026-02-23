# Sources and Cited Constants

All cited numeric constants live in `sources.py` with URL, quote, and how-used metadata.

## Host link (PCIe defaults)

- `PCIE4_X16_BW_Bps = 32e9`
  - URL: https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf
  - Quote: "Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32"
- `PCIE_WIRE_LATENCY_s = 900e-9`
  - URL: https://web.stanford.edu/class/cs244/papers/neugebauer-sigcomm18.pdf
  - Quote: "PCIe contributing around 900 ns."
- `PCIE_ENQUEUE_OVERHEAD_s = 1.2e-6`
- `PCIE_DRIVER_OVERHEAD_s = 7.0e-6`
  - URL: https://mrmgroup.cs.princeton.edu/papers/dlustigHPCA13.pdf
  - Quote: "1.2 us to enqueue" and "7 us to process"

## CXL direct points

- `CXL_LOCAL_LAT_s = 214e-9`, `CXL_LOCAL_BW_Bps = 52e9`
- `CXL_REMOTE_LAT_s = 621e-9`, `CXL_REMOTE_BW_Bps = 13e9`
  - URL: https://huaicheng.github.io/p/asplos25-melody.pdf
  - URL: https://huaicheng.github.io/s/asplos25-melody-slides.pdf

## DeepVariant pipeline anchors

- DeepVariant stage vocabulary:
  - `make_examples`, `call_variants`, `postprocess_variants`
  - URL: https://github.com/google/deepvariant
- Example tensor shape anchor (`example_info`):
  - representative shape: `[100, 147, 10]`
  - URL: https://github.com/google/deepvariant/releases
- Runtime breakdown context used for CPU calibration seeds:
  - URL: https://developer.nvidia.com/blog/accelerating-deepvariant/
- Hardware acceleration context for DeepVariant deployments:
  - URL: https://developer.nvidia.com/blog/accelerate-genomic-analysis-for-any-sequencer-with-parabricks-v4-2/

## Configurable baseline defaults (not cited hardware claims)

The following are model defaults in `configs/runs.yaml` and can be changed:

- DeepVariant profile parameters:
  - coverage/candidate density
  - aligned bytes per covered base
  - per-example call/postprocess output bytes
  - CPU stage-share timing splits
- Stage compute-unit counts/rates/power for CPU and PIM
- PIM speedup factors by stage (`pim_speedup_vs_cpu_by_stage`)
- Scenario stage-device maps (`scenario_stage_device_map`)
- Transfer-channel counts (including split host H2D ingress vs stage channels)
- Transfer power per channel (including split host H2D ingress vs stage power)
- Host-touch throughput and fixed overhead used by true-bounce modeling
- Tile size
- Max in-flight tile admission window

These defaults are simulation knobs, not measured constants from literature.
