# Flow-CXL Transfer Model Report

## Single Claim Under Test
Conventional host-bounce staging moves intermediate data over the host link each stage, while Flow-CXL chaining avoids intermediate host bounce and keeps only initial load + final output on the host link.

## Fixed Configurations Run
- Pipeline stages: 4
- Payloads: FASTQ_100GB and RAW_1TB
- Link/scenario matrix: PCIe bounce; CXL Local bounce+chain; CXL Remote bounce+chain
- Queueing: 0 (serial transfers)

## Results Table
| run_id | payload_name | payload_bytes | link_type | scenario | transfers_count | total_bytes_moved | total_transfer_time_s | speedup_vs_chain |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| run_01_FASTQ_100GB_PCIe_Gen4_x16_conventional_host_bounce | FASTQ_100GB | 100000000000 | PCIe Gen4 x16 | conventional_host_bounce | 8 | 800000000000 | 25.000000 |  |
| run_02_FASTQ_100GB_CXL_Local_conventional_host_bounce | FASTQ_100GB | 100000000000 | CXL Local | conventional_host_bounce | 8 | 800000000000 | 15.384617 | 4.000000 |
| run_03_FASTQ_100GB_CXL_Local_flowcxl_chain | FASTQ_100GB | 100000000000 | CXL Local | flowcxl_chain | 2 | 200000000000 | 3.846154 |  |
| run_04_FASTQ_100GB_CXL_Remote_conventional_host_bounce | FASTQ_100GB | 100000000000 | CXL Remote | conventional_host_bounce | 8 | 800000000000 | 61.538467 | 4.000000 |
| run_05_FASTQ_100GB_CXL_Remote_flowcxl_chain | FASTQ_100GB | 100000000000 | CXL Remote | flowcxl_chain | 2 | 200000000000 | 15.384617 |  |
| run_06_RAW_1TB_PCIe_Gen4_x16_conventional_host_bounce | RAW_1TB | 1000000000000 | PCIe Gen4 x16 | conventional_host_bounce | 8 | 8000000000000 | 250.000000 |  |
| run_07_RAW_1TB_CXL_Local_conventional_host_bounce | RAW_1TB | 1000000000000 | CXL Local | conventional_host_bounce | 8 | 8000000000000 | 153.846156 | 4.000000 |
| run_08_RAW_1TB_CXL_Local_flowcxl_chain | RAW_1TB | 1000000000000 | CXL Local | flowcxl_chain | 2 | 2000000000000 | 38.461539 |  |
| run_09_RAW_1TB_CXL_Remote_conventional_host_bounce | RAW_1TB | 1000000000000 | CXL Remote | conventional_host_bounce | 8 | 8000000000000 | 615.384620 | 4.000000 |
| run_10_RAW_1TB_CXL_Remote_flowcxl_chain | RAW_1TB | 1000000000000 | CXL Remote | flowcxl_chain | 2 | 2000000000000 | 153.846155 |  |

## Generated Plots
- plot_total_transfer_time_s.png
- plot_total_bytes_moved.png
- plot_speedup_bounce_vs_chain.png

## Citations
- `microchip_pcie_gen4_x16`: https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf
  Quote: "4.0 ... Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32 and PCIe uses dual simplex ... simultaneous two-way communication."
- `melody_local`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "CXL devices' average latency and bandwidth are 214-394ns and 18-52GB/s (Table 1, column 'Local')."
- `melody_remote`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "Table 1 remote entries show higher latency and reduced bandwidth (remote up to ~621ns; bandwidth ~13-14GB/s)."
- `melody_slides`: https://huaicheng.github.io/s/asplos25-melody-slides.pdf
  Quote: "latencies ... locally-attached ... 200-400ns ... switch(es) ... approximately 600ns."
- `flowcxl_user_pdf`: Source: Flow-CXL proposal PDF provided by user
  Quote: "1 TB Raw Data ... 100 GB FASTQ is moved between the host and the PIM units ... Flow-CXL ... reduce ... to essentially just the initial load ... eliminating ... Host Bounce. Also: four-stage genomic processing pipeline."
