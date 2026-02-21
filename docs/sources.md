# Sources and Cited Constants

All numeric constants live in `sources.py` with URL, quote, and usage.

## PCIe

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

## CXL

- `CXL_LOCAL_LAT_s = 214e-9`, `CXL_LOCAL_BW_Bps = 52e9`
- `CXL_REMOTE_LAT_s = 621e-9`, `CXL_REMOTE_BW_Bps = 13e9`
  - URL: https://huaicheng.github.io/p/asplos25-melody.pdf
  - URL: https://huaicheng.github.io/s/asplos25-melody-slides.pdf

## Dataset boundaries

- ONT profile boundaries: `[2TB, 112GB, 112GB*0.0529, 147GB, 128MB]`
  - Sources: Nanopore deck, TargetCall, GIAB/NA12878 size table
  - Note: representative boundary profile assembled from multiple cited sources.
- Illumina profile boundaries: `[28GB, 147GB, 128MB]`
  - Sources: NA12878 workflow doc + GIAB/NA12878 size table
