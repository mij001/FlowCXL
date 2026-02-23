# FlowCXL Tiled Stage-Capacity Report

## Single Claim
When each stage has fixed compute units and large boundaries must be tiled, direct PIM-to-PIM transfers reduce host-bounce overhead and improve end-to-end makespan/energy relative to host-bounced PIM execution.

## Modeled
- Stage-limited compute capacity (CPU or PIM units)
- Tile-by-tile pipelined execution with resource contention
- Host bounce path vs direct CXL stage-to-stage path
- Absolute makespan (seconds) and total energy (joules)

## Plot Artifacts
- plot_makespan_grouped_PROFILE_ONT_100Gbases.png
- plot_makespan_grouped_PROFILE_ILLUMINA_NA12878.png
- plot_energy_grouped_PROFILE_ONT_100Gbases.png
- plot_energy_grouped_PROFILE_ILLUMINA_NA12878.png

## Results Table
| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J |
| --- | --- | --- | --- | --- |
| PROFILE_ONT_100Gbases | 0.5x | CPU only | 39.295646 | 21233.670000 |
| PROFILE_ONT_100Gbases | 0.5x | PIM host bounce | 50.093722 | 4193.430256 |
| PROFILE_ONT_100Gbases | 0.5x | PIM FlowCXL direct | 50.092129 | 4064.579399 |
| PROFILE_ONT_100Gbases | 1x | CPU only | 78.221870 | 42467.340000 |
| PROFILE_ONT_100Gbases | 1x | PIM host bounce | 100.108805 | 8386.859056 |
| PROFILE_ONT_100Gbases | 1x | PIM FlowCXL direct | 100.107213 | 8129.158424 |
| PROFILE_ONT_100Gbases | 2x | CPU only | 156.399296 | 84934.680000 |
| PROFILE_ONT_100Gbases | 2x | PIM host bounce | 200.093734 | 16773.718112 |
| PROFILE_ONT_100Gbases | 2x | PIM FlowCXL direct | 200.092142 | 16258.316847 |
| PROFILE_ONT_100Gbases | 4x | CPU only | 312.764642 | 169869.360000 |
| PROFILE_ONT_100Gbases | 4x | PIM host bounce | 400.108809 | 33547.434768 |
| PROFILE_ONT_100Gbases | 4x | PIM FlowCXL direct | 400.107217 | 32516.633321 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | CPU only | 3.081661 | 1640.625000 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM host bounce | 4.212268 | 363.364472 |
| PROFILE_ILLUMINA_NA12878 | 0.5x | PIM FlowCXL direct | 3.787852 | 294.006059 |
| PROFILE_ILLUMINA_NA12878 | 1x | CPU only | 6.099453 | 3281.250000 |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM host bounce | 8.317220 | 726.728944 |
| PROFILE_ILLUMINA_NA12878 | 1x | PIM FlowCXL direct | 7.446345 | 588.012118 |
| PROFILE_ILLUMINA_NA12878 | 2x | CPU only | 11.799726 | 6562.500000 |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM host bounce | 16.527125 | 1453.457888 |
| PROFILE_ILLUMINA_NA12878 | 2x | PIM FlowCXL direct | 14.763331 | 1176.024235 |
| PROFILE_ILLUMINA_NA12878 | 4x | CPU only | 23.210863 | 13125.000000 |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM host bounce | 32.992143 | 2906.915048 |
| PROFILE_ILLUMINA_NA12878 | 4x | PIM FlowCXL direct | 29.471617 | 2352.048103 |

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
