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

## Dataset boundaries

- ONT boundaries: `[2TB, 112GB, 112GB*0.0529, 147GB, 128MB]`
  - Sources: Nanopore deck, TargetCall, GIAB/NA12878 size table
- Illumina boundaries: `[28GB, 147GB, 128MB]`
  - Sources: NA12878 workflow doc + GIAB/NA12878 size table

## Configurable baseline defaults (not cited hardware claims)

The following are model defaults in `configs/runs.yaml` and can be changed:

- Stage compute-unit counts/rates/power for CPU and PIM
- Transfer-channel counts (including split host H2D ingress vs stage channels)
- Transfer power per channel (including split host H2D ingress vs stage power)
- Host-touch throughput and fixed overhead used by true-bounce modeling
- Tile size
- Max in-flight tile admission window

These defaults are simulation knobs, not measured constants from literature.
