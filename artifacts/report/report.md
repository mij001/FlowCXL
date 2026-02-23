# FlowCXL Tiled Stage-Capacity Report

## Single Claim
With bounded streaming admission and split host H2D topology, FlowCXL direct transfer isolates inter-stage staging costs from ingress contention and exposes overlap-dependent gains.

## Modeled
- Stage-limited compute capacity (CPU or PIM units)
- Tile-by-tile pipelined execution with bounded in-flight admission
- True host bounce for intermediates: D2H -> HOST_TOUCH -> H2D(stage)
- Split host H2D resources: ingress vs inter-stage staging
- Absolute makespan (seconds) and total energy (joules)
- Lower-bound bottleneck diagnostics by resource family

## ONT Gain Check
- 0.5x: bounce/direct ratio `1.276091` (27.609% gain), bounce dominant `host_touch`, direct dominant `compute_stage_max`.
- 1x: bounce/direct ratio `1.277401` (27.740% gain), bounce dominant `host_touch`, direct dominant `compute_stage_max`.
- 2x: bounce/direct ratio `1.277006` (27.701% gain), bounce dominant `host_touch`, direct dominant `compute_stage_max`.
- 4x: bounce/direct ratio `1.276832` (27.683% gain), bounce dominant `host_touch`, direct dominant `compute_stage_max`.
- ONT 1x target (`>=5%`): ratio `1.277401` (27.740% gain) -> `PASS`.
- Streaming admission (`max_inflight_tiles`) and split H2D pools separate ingress pressure from inter-stage staging, making overlap/transfer effects easier to attribute.

## Plot Artifacts
- plot_makespan_grouped_PROFILE_ONT_100Gbases.png
- plot_makespan_grouped_PROFILE_ILLUMINA_NA12878.png
- plot_energy_grouped_PROFILE_ONT_100Gbases.png
- plot_energy_grouped_PROFILE_ILLUMINA_NA12878.png

## Results Table
| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | host_touch_energy_J | total_bytes_host_touch | dominant_lb_component |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ONT_100Gbases | 0.5x | CPU only | 39.295646 | 21233.670000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 0.5x | PIM host bounce | 5.369075 | 2088.131016 | 159.625560 | 132462400000 | host_touch |
| PROFILE_ONT_100Gbases | 0.5x | PIM FlowCXL direct | 4.207438 | 1799.654599 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 1x | CPU only | 78.221870 | 42467.340000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 1x | PIM host bounce | 10.689927 | 4176.260396 | 319.250940 | 264924800000 | host_touch |
| PROFILE_ONT_100Gbases | 1x | PIM FlowCXL direct | 8.368499 | 3599.308824 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 2x | CPU only | 156.399296 | 84934.680000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 2x | PIM host bounce | 21.331625 | 8352.520792 | 638.501880 | 529849600000 | host_touch |
| PROFILE_ONT_100Gbases | 2x | PIM FlowCXL direct | 16.704408 | 7198.617647 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 4x | CPU only | 312.764642 | 169869.360000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 4x | PIM host bounce | 42.615017 | 16705.039948 | 1277.003580 | 1059699200000 | host_touch |
| PROFILE_ONT_100Gbases | 4x | PIM FlowCXL direct | 33.375581 | 14397.234921 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 0.5x | CPU only | 3.081661 | 1640.625000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM host bounce | 3.001533 | 276.580912 | 88.216440 | 73500000000 | host_touch |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM FlowCXL direct | 1.457721 | 119.006059 | 0.000000 | 0 | cxl_direct |
| PROFILE_ILLUMINA_NA12878 | 1x | CPU only | 6.099453 | 3281.250000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM host bounce | 5.942081 | 553.161824 | 176.432880 | 147000000000 | host_touch |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM FlowCXL direct | 2.871242 | 238.012118 | 0.000000 | 0 | cxl_direct |
| PROFILE_ILLUMINA_NA12878 | 2x | CPU only | 11.799726 | 6562.500000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM host bounce | 11.823177 | 1106.323648 | 352.865760 | 294000000000 | host_touch |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM FlowCXL direct | 5.698282 | 476.024235 | 0.000000 | 0 | cxl_direct |
| PROFILE_ILLUMINA_NA12878 | 4x | CPU only | 23.210863 | 13125.000000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM host bounce | 23.585395 | 2212.646508 | 705.731460 | 588000000000 | host_touch |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM FlowCXL direct | 11.352383 | 952.048103 | 0.000000 | 0 | cxl_direct |

## Bottleneck Diagnostics
### PROFILE_ONT_100Gbases

| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ONT_100Gbases | 0.5x | CPU only | 39.295646 | 21233.670000 | compute_stage_max | 39.062500 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 0.5x | PIM FlowCXL direct | 4.207438 | 1799.654599 | compute_stage_max | 4.166667 | 1.955244 | 0.000000 | 0.035907 | 1.955244 | 0.000000 | 2.549746 |
| PROFILE_ONT_100Gbases | 0.5x | PIM host bounce | 5.369075 | 2088.131016 | host_touch | 4.166667 | 1.955244 | 4.241170 | 4.277076 | 4.277076 | 5.320852 | 0.000000 |
| PROFILE_ONT_100Gbases | 1x | CPU only | 78.221870 | 42467.340000 | compute_stage_max | 78.125000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 1x | PIM FlowCXL direct | 8.368499 | 3599.308824 | compute_stage_max | 8.333333 | 3.910488 | 0.000000 | 0.071804 | 3.910488 | 0.000000 | 5.099491 |
| PROFILE_ONT_100Gbases | 1x | PIM host bounce | 10.689927 | 4176.260396 | host_touch | 8.333333 | 3.910488 | 8.482312 | 8.554116 | 8.554116 | 10.641698 | 0.000000 |
| PROFILE_ONT_100Gbases | 2x | CPU only | 156.399296 | 84934.680000 | compute_stage_max | 156.250000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 2x | PIM FlowCXL direct | 16.704408 | 7198.617647 | compute_stage_max | 16.666667 | 7.820976 | 0.000000 | 0.143608 | 7.820976 | 0.000000 | 10.198982 |
| PROFILE_ONT_100Gbases | 2x | PIM host bounce | 21.331625 | 8352.520792 | host_touch | 16.666667 | 7.820976 | 16.964625 | 17.108233 | 17.108233 | 21.283396 | 0.000000 |
| PROFILE_ONT_100Gbases | 4x | CPU only | 312.764642 | 169869.360000 | compute_stage_max | 312.500000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 4x | PIM FlowCXL direct | 33.375581 | 14397.234921 | compute_stage_max | 33.333333 | 15.641950 | 0.000000 | 0.287207 | 15.641950 | 0.000000 | 20.397964 |
| PROFILE_ONT_100Gbases | 4x | PIM host bounce | 42.615017 | 16705.039948 | host_touch | 33.333333 | 15.641950 | 33.929222 | 34.216429 | 34.216429 | 42.566786 | 0.000000 |
### PROFILE_ILLUMINA_NA12878

| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ILLUMINA_NA12878 | 0.5x | CPU only | 3.081661 | 1640.625000 | compute_stage_max | 2.871094 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM FlowCXL direct | 1.457721 | 119.006059 | cxl_direct | 0.306250 | 0.027500 | 0.000000 | 0.004493 | 0.027500 | 0.000000 | 1.413520 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM host bounce | 3.001533 | 276.580912 | host_touch | 0.306250 | 0.027500 | 2.299368 | 2.303862 | 2.303862 | 2.940548 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 1x | CPU only | 6.099453 | 3281.250000 | compute_stage_max | 5.742188 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM FlowCXL direct | 2.871242 | 238.012118 | cxl_direct | 0.612500 | 0.054999 | 0.000000 | 0.008987 | 0.054999 | 0.000000 | 2.827040 |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM host bounce | 5.942081 | 553.161824 | host_touch | 0.612500 | 0.054999 | 4.598737 | 4.607724 | 4.607724 | 5.881096 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 2x | CPU only | 11.799726 | 6562.500000 | compute_stage_max | 11.484375 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM FlowCXL direct | 5.698282 | 476.024235 | cxl_direct | 1.225000 | 0.109998 | 0.000000 | 0.017974 | 0.109998 | 0.000000 | 5.654081 |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM host bounce | 11.823177 | 1106.323648 | host_touch | 1.225000 | 0.109998 | 9.197474 | 9.215447 | 9.215447 | 11.762192 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 4x | CPU only | 23.210863 | 13125.000000 | compute_stage_max | 22.968750 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM FlowCXL direct | 11.352383 | 952.048103 | cxl_direct | 2.450000 | 0.219996 | 0.000000 | 0.035938 | 0.219996 | 0.000000 | 11.308161 |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM host bounce | 23.585395 | 2212.646508 | host_touch | 2.450000 | 0.219996 | 18.394938 | 18.430876 | 18.430876 | 23.524382 | 0.000000 |

## Citations
- `PCIE4_X16_BW_Bps`: https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf
  Quote: "Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32"
  Used as: Host-link one-way bandwidth for B/BW.
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
  Used as: Representative local latency point for CXL direct transfers.
- `CXL_LOCAL_BW_Bps`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "average latency and bandwidth are 214-394ns and 18-52GB/s"
  Used as: Representative local bandwidth point for CXL direct transfers.
- `CXL_REMOTE_LAT_s`: https://huaicheng.github.io/s/asplos25-melody-slides.pdf
  Quote: "locally-attached ... 200-400ns ... switch(es) ... approximately 600ns"
  Used as: Representative remote-ish latency point for CXL direct transfers.
- `CXL_REMOTE_BW_Bps`: https://huaicheng.github.io/p/asplos25-melody.pdf
  Quote: "remote entries show higher latency and reduced bandwidth (~13-14GB/s)"
  Used as: Representative remote bandwidth point for CXL direct transfers.
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
