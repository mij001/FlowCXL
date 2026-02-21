# Sources and Fixed Numeric Values

This project uses only the fixed values below.

## PCIe Gen4 x16

- `PCIE4_X16_BW_Bps = 32e9`
- `PCIE_L_s = 0`
- URL: https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf
- Quote: "4.0 ... Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32" and "PCIe uses dual simplex ... simultaneous two-way communication."

## CXL Local

- `CXL_LOCAL_L_s = 214e-9`
- `CXL_LOCAL_BW_Bps = 52e9`
- URL: https://huaicheng.github.io/p/asplos25-melody.pdf
- Quote: "CXL devices' average latency and bandwidth are 214-394ns and 18-52GB/s (Table 1, column 'Local')."

## CXL Remote / Extended

- `CXL_REMOTE_L_s = 621e-9`
- `CXL_REMOTE_BW_Bps = 13e9`
- URL: https://huaicheng.github.io/p/asplos25-melody.pdf
- Quote: "Table 1 remote entries show higher latency and reduced bandwidth (remote up to ~621ns; bandwidth ~13-14GB/s)."
- Supporting URL: https://huaicheng.github.io/s/asplos25-melody-slides.pdf
- Supporting quote: "latencies ... locally-attached ... 200-400ns ... switch(es) ... approximately 600ns."

## Flow-CXL proposal (provided by user)

- `NUM_STAGES = 4`
- `PAYLOAD_FASTQ_BYTES = 100e9` (100 GB)
- `PAYLOAD_RAW_BYTES = 1e12` (1 TB)
- Source: Flow-CXL proposal PDF provided by user
- Quote: "1 TB Raw Data ... 100 GB FASTQ is moved between the host and the PIM units ... Flow-CXL ... reduce ... to essentially just the initial load ... eliminating ... Host Bounce."
- Quote: "four-stage genomic processing pipeline"
