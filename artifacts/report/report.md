# FlowCXL Tiled Stage-Capacity Report

## Single Claim
When each stage has fixed compute units and large boundaries must be tiled, direct PIM-to-PIM transfers reduce host-bounce overhead and improve end-to-end makespan/energy relative to host-bounced PIM execution.

## Modeled
- Stage-limited compute capacity (CPU or PIM units)
- Tile-by-tile pipelined execution with resource contention
- True host bounce for intermediates: D2H -> HOST_TOUCH -> H2D
- Absolute makespan (seconds) and total energy (joules)
- Lower-bound bottleneck diagnostics by resource family

## ONT Bottleneck Interpretation
- 0.5x: bounce dominant `compute_stage_max`, direct dominant `compute_stage_max`, bounce/direct makespan ratio `1.000071` (0.007% gain).
- 1x: bounce dominant `compute_stage_max`, direct dominant `compute_stage_max`, bounce/direct makespan ratio `1.000030` (0.003% gain).
- 2x: bounce dominant `compute_stage_max`, direct dominant `compute_stage_max`, bounce/direct makespan ratio `1.000018` (0.002% gain).
- 4x: bounce dominant `compute_stage_max`, direct dominant `compute_stage_max`, bounce/direct makespan ratio `1.000008` (0.001% gain).
- Interpretation: ONT remains largely compute/ingress-bound in FlowCXL-direct runs, so inter-stage bounce removal helps only when host-touch/link terms are competitive.

## Plot Artifacts
- plot_makespan_grouped_PROFILE_ONT_100Gbases.png
- plot_makespan_grouped_PROFILE_ILLUMINA_NA12878.png
- plot_energy_grouped_PROFILE_ONT_100Gbases.png
- plot_energy_grouped_PROFILE_ILLUMINA_NA12878.png

## Results Table
| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | host_touch_energy_J | total_bytes_host_touch | dominant_lb_component |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ONT_100Gbases | 0.5x | CPU only | 39.295646 | 21233.670000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 0.5x | PIM host bounce | 50.095706 | 4353.055816 | 159.62556000000055 | 132462400000 | compute_stage_max |
| PROFILE_ONT_100Gbases | 0.5x | PIM FlowCXL direct | 50.092129 | 4064.579399 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 1x | CPU only | 78.221870 | 42467.340000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 1x | PIM host bounce | 100.110234 | 8706.109996 | 319.25093999998904 | 264924800000 | compute_stage_max |
| PROFILE_ONT_100Gbases | 1x | PIM FlowCXL direct | 100.107213 | 8129.158424 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 2x | CPU only | 156.399296 | 84934.680000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 2x | PIM host bounce | 200.095719 | 17412.219992 | 638.5018800001401 | 529849600000 | compute_stage_max |
| PROFILE_ONT_100Gbases | 2x | PIM FlowCXL direct | 200.092142 | 16258.316847 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 4x | CPU only | 312.764642 | 169869.360000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ONT_100Gbases | 4x | PIM host bounce | 400.110237 | 34824.438348 | 1277.0035799993934 | 1059699200000 | compute_stage_max |
| PROFILE_ONT_100Gbases | 4x | PIM FlowCXL direct | 400.107217 | 32516.633321 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 0.5x | CPU only | 3.081661 | 1640.625000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM host bounce | 4.212268 | 451.580912 | 88.21644000000018 | 73500000000 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM FlowCXL direct | 3.787852 | 294.006059 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 1x | CPU only | 6.099453 | 3281.250000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM host bounce | 8.317220 | 903.161824 | 176.43288000000135 | 147000000000 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM FlowCXL direct | 7.446345 | 588.012118 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 2x | CPU only | 11.799726 | 6562.500000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM host bounce | 16.527125 | 1806.323648 | 352.86576000000366 | 294000000000 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM FlowCXL direct | 14.763331 | 1176.024235 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 4x | CPU only | 23.210863 | 13125.000000 | 0.0 | 0 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM host bounce | 32.992143 | 3612.646508 | 705.73145999998 | 588000000000 | compute_stage_max |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM FlowCXL direct | 29.471617 | 2352.048103 | 0.0 | 0 | compute_stage_max |

## Bottleneck Diagnostics
### PROFILE_ONT_100Gbases

| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ONT_100Gbases | 0.5x | CPU only | 39.295646 | 21233.670000 | compute_stage_max | 39.062500 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 0.5x | PIM FlowCXL direct | 50.092129 | 4064.579399 | compute_stage_max | 50.000000 | 31.283907 | 0.000000 | 2.549746 |
| PROFILE_ONT_100Gbases | 0.5x | PIM host bounce | 50.095706 | 4353.055816 | compute_stage_max | 50.000000 | 35.525076 | 5.320852 | 0.000000 |
| PROFILE_ONT_100Gbases | 1x | CPU only | 78.221870 | 42467.340000 | compute_stage_max | 78.125000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 1x | PIM FlowCXL direct | 100.107213 | 8129.158424 | compute_stage_max | 100.000000 | 62.567804 | 0.000000 | 5.099491 |
| PROFILE_ONT_100Gbases | 1x | PIM host bounce | 100.110234 | 8706.109996 | compute_stage_max | 100.000000 | 71.050116 | 10.641698 | 0.000000 |
| PROFILE_ONT_100Gbases | 2x | CPU only | 156.399296 | 84934.680000 | compute_stage_max | 156.250000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 2x | PIM FlowCXL direct | 200.092142 | 16258.316847 | compute_stage_max | 200.000000 | 125.135608 | 0.000000 | 10.198982 |
| PROFILE_ONT_100Gbases | 2x | PIM host bounce | 200.095719 | 17412.219992 | compute_stage_max | 200.000000 | 142.100233 | 21.283396 | 0.000000 |
| PROFILE_ONT_100Gbases | 4x | CPU only | 312.764642 | 169869.360000 | compute_stage_max | 312.500000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ONT_100Gbases | 4x | PIM FlowCXL direct | 400.107217 | 32516.633321 | compute_stage_max | 400.000000 | 250.271207 | 0.000000 | 20.397964 |
| PROFILE_ONT_100Gbases | 4x | PIM host bounce | 400.110237 | 34824.438348 | compute_stage_max | 400.000000 | 284.200429 | 42.566786 | 0.000000 |
### PROFILE_ILLUMINA_NA12878

| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_ILLUMINA_NA12878 | 0.5x | CPU only | 3.081661 | 1640.625000 | compute_stage_max | 2.871094 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM FlowCXL direct | 3.787852 | 294.006059 | compute_stage_max | 3.675000 | 0.439993 | 0.000000 | 1.413520 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM host bounce | 4.212268 | 451.580912 | compute_stage_max | 3.675000 | 2.739362 | 2.940548 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 1x | CPU only | 6.099453 | 3281.250000 | compute_stage_max | 5.742188 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM FlowCXL direct | 7.446345 | 588.012118 | compute_stage_max | 7.350000 | 0.879987 | 0.000000 | 2.827040 |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM host bounce | 8.317220 | 903.161824 | compute_stage_max | 7.350000 | 5.478724 | 5.881096 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 2x | CPU only | 11.799726 | 6562.500000 | compute_stage_max | 11.484375 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM FlowCXL direct | 14.763331 | 1176.024235 | compute_stage_max | 14.700000 | 1.759974 | 0.000000 | 5.654081 |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM host bounce | 16.527125 | 1806.323648 | compute_stage_max | 14.700000 | 10.957447 | 11.762192 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 4x | CPU only | 23.210863 | 13125.000000 | compute_stage_max | 22.968750 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM FlowCXL direct | 29.471617 | 2352.048103 | compute_stage_max | 29.400000 | 3.519938 | 0.000000 | 11.308161 |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM host bounce | 32.992143 | 3612.646508 | compute_stage_max | 29.400000 | 21.914876 | 23.524382 | 0.000000 |

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
