# Flow-CXL Contention-Aware Transfer Report

## Single Claim
Flow-CXL chaining reduces host-bounce staging transfers across multi-stage pipelines. Gain under contention depends on whether eliminated transfers overlap with the dominant bottleneck.

## Modeled
- Transfer fixed costs and bandwidth
- Deterministic queueing from shared resources
- Multi-chunk contention at num_chunks in {1, 8}
- Duplex vs shared-link resource modes

## Queue Accounting
- `queue_total_blocking_s`: sum of per-operation blocking waits (one value per operation).
- `queue_total_attributed_s`: sum of per-resource attributed waits. Blocking wait is attributed only to bottleneck resource(s); tie waits are split.

## ONT k=8 Interpretation
For ONT on CXL_LOCAL, both bounce and chain still carry the dominant X0 boundary (2000000000000 bytes) over H2D. Under duplex contention, speedup shrinks (k=1 1.265 -> k=8 1.139) because eliminated transfers are not the dominant bottleneck. Under shared-link mode, both scenarios serialize on one link and the ratio stays near constant (k=1 1.265, k=8 1.265).

## Results Table
| dataset_profile | link_type | shared_link | num_chunks | scenario | makespan_s | total_bytes_moved | queue_total_blocking_s | queue_total_attributed_s | speedup_vs_chain |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ONT_100Gbases | PCIe Gen4 x16 | False | 1 | pim_no_cxl_bounce | 79.061873 | 2529977600000 | 0.000000 | 0.000000 |  |
| PROFILE_ONT_100Gbases | CXL_LOCAL | False | 1 | pim_cxl_bounce | 48.653417 | 2529977600000 | 0.000000 | 0.000000 | 1.264908 |
| PROFILE_ONT_100Gbases | CXL_LOCAL | True | 1 | pim_cxl_bounce | 48.653417 | 2529977600000 | 0.000000 | 0.000000 | 1.264908 |
| PROFILE_ONT_100Gbases | CXL_REMOTE | False | 1 | pim_cxl_bounce | 194.613667 | 2529977600000 | 0.000000 | 0.000000 | 1.264908 |
| PROFILE_ONT_100Gbases | CXL_REMOTE | True | 1 | pim_cxl_bounce | 194.613667 | 2529977600000 | 0.000000 | 0.000000 | 1.264908 |
| PROFILE_ONT_100Gbases | CXL_LOCAL | False | 1 | pim_cxl_chain | 38.464000 | 2000128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ONT_100Gbases | CXL_LOCAL | True | 1 | pim_cxl_chain | 38.464000 | 2000128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ONT_100Gbases | CXL_REMOTE | False | 1 | pim_cxl_chain | 153.856001 | 2000128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ONT_100Gbases | CXL_REMOTE | True | 1 | pim_cxl_chain | 153.856001 | 2000128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ONT_100Gbases | PCIe Gen4 x16 | False | 8 | pim_no_cxl_bounce | 569.533146 | 20239820800000 | 3891.698061 | 3891.698061 |  |
| PROFILE_ONT_100Gbases | CXL_LOCAL | False | 8 | pim_cxl_bounce | 350.481790 | 20239820800000 | 2394.890220 | 2394.890220 | 1.139057 |
| PROFILE_ONT_100Gbases | CXL_LOCAL | True | 8 | pim_cxl_bounce | 389.227337 | 20239820800000 | 2724.522428 | 2724.522428 | 1.264908 |
| PROFILE_ONT_100Gbases | CXL_REMOTE | False | 8 | pim_cxl_bounce | 1401.927155 | 20239820800000 | 9579.560842 | 9579.560842 | 1.139057 |
| PROFILE_ONT_100Gbases | CXL_REMOTE | True | 8 | pim_cxl_bounce | 1556.909332 | 20239820800000 | 10898.089615 | 10898.089615 | 1.264908 |
| PROFILE_ONT_100Gbases | CXL_LOCAL | False | 8 | pim_cxl_chain | 307.694771 | 16001024000000 | 1076.923083 | 1076.923083 |  |
| PROFILE_ONT_100Gbases | CXL_LOCAL | True | 8 | pim_cxl_chain | 307.712003 | 16001024000000 | 2153.915095 | 2153.915095 |  |
| PROFILE_ONT_100Gbases | CXL_REMOTE | False | 8 | pim_cxl_chain | 1230.779083 | 16001024000000 | 4307.692325 | 4307.692325 |  |
| PROFILE_ONT_100Gbases | CXL_REMOTE | True | 8 | pim_cxl_chain | 1230.848010 | 16001024000000 | 8615.660360 | 8615.660360 |  |
| PROFILE_ILLUMINA_NA12878 | PCIe Gen4 x16 | False | 1 | pim_no_cxl_bounce | 10.066536 | 322128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | False | 1 | pim_cxl_bounce | 6.194770 | 322128000000 | 0.000000 | 0.000000 | 11.452211 |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | True | 1 | pim_cxl_bounce | 6.194770 | 322128000000 | 0.000000 | 0.000000 | 11.452211 |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | False | 1 | pim_cxl_bounce | 24.779079 | 322128000000 | 0.000000 | 0.000000 | 11.452213 |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | True | 1 | pim_cxl_bounce | 24.779079 | 322128000000 | 0.000000 | 0.000000 | 11.452213 |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | False | 1 | pim_cxl_chain | 0.540924 | 28128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | True | 1 | pim_cxl_chain | 0.540924 | 28128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | False | 1 | pim_cxl_chain | 2.163694 | 28128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | True | 1 | pim_cxl_chain | 2.163694 | 28128000000 | 0.000000 | 0.000000 |  |
| PROFILE_ILLUMINA_NA12878 | PCIe Gen4 x16 | False | 8 | pim_no_cxl_bounce | 43.754155 | 2577024000000 | 228.216942 | 228.216942 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | False | 8 | pim_cxl_bounce | 26.925542 | 2577024000000 | 140.440785 | 140.440785 | 6.247000 |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | True | 8 | pim_cxl_bounce | 49.558161 | 2577024000000 | 346.838196 | 346.838196 | 11.452211 |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | False | 8 | pim_cxl_bounce | 107.702164 | 2577024000000 | 561.763124 | 561.763124 | 6.247000 |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | True | 8 | pim_cxl_bounce | 198.232635 | 2577024000000 | 1387.352737 | 1387.352737 | 11.452213 |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | False | 8 | pim_cxl_chain | 4.310156 | 225024000000 | 15.076929 | 15.076929 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_LOCAL | True | 8 | pim_cxl_chain | 4.327388 | 225024000000 | 30.222787 | 30.222787 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | False | 8 | pim_cxl_chain | 17.240621 | 225024000000 | 60.307710 | 60.307710 |  |
| PROFILE_ILLUMINA_NA12878 | CXL_REMOTE | True | 8 | pim_cxl_chain | 17.309548 | 225024000000 | 120.891129 | 120.891129 |  |

## Plot Artifacts
- plot_makespan_by_scenario.png
- plot_total_bytes_by_scenario.png
- plot_speedup_cxl_bounce_vs_chain.png
- plot_queue_total_blocking.png
- plot_queue_time_by_resource_attributed.png
- plot_resource_utilization_heatmap.png

## Citations
- `PCIE4_X16_BW_Bps`: https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf
  Quote: "Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32"
  Used as: PCIe one-way bandwidth for B/BW.
- `PCIE_WIRE_LATENCY_s`: https://web.stanford.edu/class/cs244/papers/neugebauer-sigcomm18.pdf
  Quote: "PCIe contributing around 900 ns."
  Used as: Wire component in PCIe fixed transfer cost.
- `PCIE_ENQUEUE_OVERHEAD_s`: https://mrmgroup.cs.princeton.edu/papers/dlustigHPCA13.pdf
  Quote: "1.2 us to enqueue"
  Used as: Software enqueue component in PCIe fixed cost.
- `PCIE_DRIVER_OVERHEAD_s`: https://mrmgroup.cs.princeton.edu/papers/dlustigHPCA13.pdf
  Quote: "7 us to process"
  Used as: Driver processing component in PCIe fixed cost.
- `CXL_LOCAL_LAT_s`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "average latency and bandwidth are 214-394ns and 18-52GB/s"
  Used as: Representative best-local latency point for CXL_LOCAL.
- `CXL_LOCAL_BW_Bps`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "average latency and bandwidth are 214-394ns and 18-52GB/s"
  Used as: Representative best-local bandwidth point for CXL_LOCAL.
- `CXL_REMOTE_LAT_s`: https://huaicheng.github.io/s/asplos25-melody-slides.pdf
  Quote: "locally-attached ... 200-400ns ... switch(es) ... approximately 600ns"
  Used as: Representative remote-ish latency point for CXL_REMOTE.
- `CXL_REMOTE_BW_Bps`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "remote entries show higher latency and reduced bandwidth (~13-14GB/s)"
  Used as: Representative remote bandwidth point for CXL_REMOTE.
- `ONT_FAST5_X0`: https://media.tghn.org/medialibrary/2020/09/Introduction_to_Nanopore_Data_analysis_-_Alp_Aydin.pdf
  Quote: "fast5 = 2 terrabytes"
  Used as: ONT boundary X0.
- `ONT_FASTQ_GZ_X1`: https://media.tghn.org/medialibrary/2020/09/Introduction_to_Nanopore_Data_analysis_-_Alp_Aydin.pdf
  Quote: "fastq gzipped = 112 gigabytes"
  Used as: ONT boundary X1.
- `TARGETCALL_KEEP_FRACTION`: https://www.frontiersin.org/journals/genetics/articles/10.3389/fgene.2024.1429306/full
  Quote: "filters out 94.71% of the off-target reads"
  Used as: Derived keep fraction for ONT boundary X2.
- `GIAB_BAM_X`: https://digital.library.adelaide.edu.au/dspace/bitstream/2440/136736/1/Lan2022_PhD.pdf
  Quote: ".bam ... 147 GB"
  Used as: Boundary proxy for aligned BAM size.
- `GIAB_VCF_X`: https://digital.library.adelaide.edu.au/dspace/bitstream/2440/136736/1/Lan2022_PhD.pdf
  Quote: ".vcf.gz ... 128 MB"
  Used as: Boundary proxy for final VCF size.
- `ILLUMINA_X0`: https://oak.chosun.ac.kr/bitstream/2020.oak/18470/2/Constructing%20an%20ethnic-specific%20variant%20calling%20workflow%20based%20on%20a%20systematic%20comparison%20of%20multipl.pdf
  Quote: "Since the NA12878 data was 28GB"
  Used as: Illumina boundary X0.
