# DeepVariant Tiled Stage-Capacity Report

## Single Claim
With bounded streaming admission and split host H2D topology, FlowCXL direct transfer isolates DeepVariant inter-stage staging costs from ingress contention and exposes overlap-dependent gains.

## Modeled
- Fixed DeepVariant three-stage pipeline: make_examples, call_variants, postprocess_variants
- Stage-limited compute capacity with scenario stage-device mapping (CPU or PIM)
- Tile-by-tile pipelined execution with bounded in-flight admission
- True host bounce for intermediates: D2H -> HOST_TOUCH -> H2D(stage)
- Split host H2D resources: ingress vs inter-stage staging
- Absolute makespan (seconds) and total energy (joules)
- Lower-bound bottleneck diagnostics by resource family

## Directional Check
- PROFILE_DV_ILLUMINA_WES_100X: directional `true`, strictly-better points `4`, 1x bounce/direct ratio `1.000438`, sensitivity delta (max-min ratio) `0.000868`.
- PROFILE_DV_ILLUMINA_WGS_30X: directional `true`, strictly-better points `4`, 1x bounce/direct ratio `1.000003`, sensitivity delta (max-min ratio) `0.000003`.
- Directional condition checks `direct <= bounce`; sensitivity delta reports how ratio changes across stage-size multipliers.
- Streaming admission (`max_inflight_tiles`) and split H2D pools separate ingress pressure from inter-stage staging, while only PIM->PIM transitions differ between bounce and direct.

## Plot Artifacts
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X.png

## Results Table
| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | host_touch_energy_J | total_bytes_host_touch | dominant_lb_component |
| --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_DV_ILLUMINA_WGS_30X | 0.5x | CPU only | 12086.046220 | 8830080.000000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 0.5x | PIM host bounce | 6070.040280 | 2785792.013274 | 546.941880 | 455700000000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 0.5x | PIM FlowCXL direct | 6070.012337 | 2784815.049521 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 1x | CPU only | 23825.407034 | 17660160.000001 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 1x | PIM host bounce | 11939.742161 | 5571584.026548 | 1093.883760 | 911400000000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 1x | PIM FlowCXL direct | 11939.703066 | 5569630.099041 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 2x | CPU only | 47311.094182 | 35320319.999999 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 2x | PIM host bounce | 23682.621837 | 11143168.052308 | 2187.767460 | 1822800000000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 2x | PIM FlowCXL direct | 23682.566007 | 11139260.197715 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 4x | CPU only | 94282.393300 | 70640640.000002 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 4x | PIM host bounce | 47168.337568 | 22286336.103827 | 4375.534860 | 3645600000000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WGS_30X | 4x | PIM FlowCXL direct | 47168.248273 | 22278520.395061 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 0.5x | CPU only | 254.942883 | 133906.608000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 0.5x | PIM host bounce | 171.175578 | 50713.865686 | 29.405491 | 24499975500 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 0.5x | PIM FlowCXL direct | 171.003999 | 50661.340507 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 1x | CPU only | 494.494146 | 267813.216000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 1x | PIM host bounce | 330.829156 | 101427.730584 | 58.810921 | 48999951000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 1x | PIM FlowCXL direct | 330.684455 | 101322.680647 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 2x | CPU only | 970.809905 | 535626.432000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 2x | PIM host bounce | 648.276823 | 202855.461168 | 117.621842 | 97999902000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 2x | PIM FlowCXL direct | 648.182188 | 202645.361293 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 4x | CPU only | 1846.578806 | 1071252.864000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 4x | PIM host bounce | 1232.263684 | 405710.921548 | 235.243625 | 195999804000 | compute_stage_max |
| PROFILE_DV_ILLUMINA_WES_100X | 4x | PIM FlowCXL direct | 1232.096503 | 405290.722219 | 0.000000 | 0 | compute_stage_max |

## Bottleneck Diagnostics
### PROFILE_DV_ILLUMINA_WGS_30X

| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_DV_ILLUMINA_WGS_30X | 0.5x | CPU only | 12086.046220 | 8830080.000000 | compute_stage_max | 11753.204400 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 0.5x | PIM FlowCXL direct | 6070.012337 | 2784815.049521 | compute_stage_max | 5876.602200 | 1.759202 | 0.000000 | 0.016614 | 1.759202 | 0.000000 | 8.763825 |
| PROFILE_DV_ILLUMINA_WGS_30X | 0.5x | PIM host bounce | 6070.040280 | 2785792.013274 | compute_stage_max | 5876.602200 | 1.759202 | 14.256077 | 14.272691 | 14.272691 | 18.231396 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 1x | CPU only | 23825.407034 | 17660160.000001 | compute_stage_max | 23506.408800 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 1x | PIM FlowCXL direct | 11939.703066 | 5569630.099041 | compute_stage_max | 11753.204400 | 3.518404 | 0.000000 | 0.033229 | 3.518404 | 0.000000 | 17.527650 |
| PROFILE_DV_ILLUMINA_WGS_30X | 1x | PIM host bounce | 11939.742161 | 5571584.026548 | compute_stage_max | 11753.204400 | 3.518404 | 28.512154 | 28.545382 | 28.545382 | 36.462792 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 2x | CPU only | 47311.094182 | 35320319.999999 | compute_stage_max | 47012.817600 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 2x | PIM FlowCXL direct | 23682.566007 | 11139260.197715 | compute_stage_max | 23506.408800 | 7.036798 | 0.000000 | 0.066448 | 7.036798 | 0.000000 | 35.055299 |
| PROFILE_DV_ILLUMINA_WGS_30X | 2x | PIM host bounce | 23682.621837 | 11143168.052308 | compute_stage_max | 23506.408800 | 7.036798 | 57.024298 | 57.090746 | 57.090746 | 72.925582 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 4x | CPU only | 94282.393300 | 70640640.000002 | compute_stage_max | 94025.635200 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WGS_30X | 4x | PIM FlowCXL direct | 47168.248273 | 22278520.395061 | compute_stage_max | 47012.817600 | 14.073587 | 0.000000 | 0.132887 | 14.073587 | 0.000000 | 70.110599 |
| PROFILE_DV_ILLUMINA_WGS_30X | 4x | PIM host bounce | 47168.337568 | 22286336.103827 | compute_stage_max | 47012.817600 | 14.073587 | 114.048587 | 114.181474 | 114.181474 | 145.851162 | 0.000000 |
### PROFILE_DV_ILLUMINA_WES_100X

| dataset_profile | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_DV_ILLUMINA_WES_100X | 0.5x | CPU only | 254.942883 | 133906.608000 | compute_stage_max | 226.994400 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 0.5x | PIM FlowCXL direct | 171.003999 | 50661.340507 | compute_stage_max | 151.329600 | 0.094587 | 0.000000 | 0.000900 | 0.094587 | 0.000000 | 0.471173 |
| PROFILE_DV_ILLUMINA_WES_100X | 0.5x | PIM host bounce | 171.175578 | 50713.865686 | compute_stage_max | 151.329600 | 0.094587 | 0.766461 | 0.767361 | 0.767361 | 0.980183 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 1x | CPU only | 494.494146 | 267813.216000 | compute_stage_max | 453.988800 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 1x | PIM FlowCXL direct | 330.684455 | 101322.680647 | compute_stage_max | 302.659200 | 0.189165 | 0.000000 | 0.001790 | 0.189165 | 0.000000 | 0.942346 |
| PROFILE_DV_ILLUMINA_WES_100X | 1x | PIM host bounce | 330.829156 | 101427.730584 | compute_stage_max | 302.659200 | 0.189165 | 1.532914 | 1.534704 | 1.534704 | 1.960364 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 2x | CPU only | 970.809905 | 535626.432000 | compute_stage_max | 907.977600 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 2x | PIM FlowCXL direct | 648.182188 | 202645.361293 | compute_stage_max | 605.318400 | 0.378331 | 0.000000 | 0.003581 | 0.378331 | 0.000000 | 1.884692 |
| PROFILE_DV_ILLUMINA_WES_100X | 2x | PIM host bounce | 648.276823 | 202855.461168 | compute_stage_max | 605.318400 | 0.378331 | 3.065828 | 3.069408 | 3.069408 | 3.920728 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 4x | CPU only | 1846.578806 | 1071252.864000 | compute_stage_max | 1815.955200 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_DV_ILLUMINA_WES_100X | 4x | PIM FlowCXL direct | 1232.096503 | 405290.722219 | compute_stage_max | 1210.636800 | 0.756652 | 0.000000 | 0.007152 | 0.756652 | 0.000000 | 3.769383 |
| PROFILE_DV_ILLUMINA_WES_100X | 4x | PIM host bounce | 1232.263684 | 405710.921548 | compute_stage_max | 1210.636800 | 0.756652 | 6.131646 | 6.138798 | 6.138798 | 7.841454 | 0.000000 |

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
- `DEEPVARIANT_STAGE_NAMES`: https://github.com/google/deepvariant
  Quote: "make_examples, call_variants, postprocess_variants"
  Used as: Defines fixed 3-stage DeepVariant pipeline in simulator.
- `DEEPVARIANT_EXAMPLE_SHAPE`: https://github.com/google/deepvariant/releases
  Quote: "example shape [100, 147, 10]"
  Used as: Tensor materialization size seed for stage-1 output bytes.
- `DEEPVARIANT_TIMING_BREAKDOWN_CONTEXT`: https://developer.nvidia.com/blog/accelerating-deepvariant/
  Quote: "make_examples and call_variants dominate runtime depending on hardware path."
  Used as: Calibration context for stage runtime shares at 1x.
- `PARABRICKS_DV_CONTEXT`: https://developer.nvidia.com/blog/accelerate-genomic-analysis-for-any-sequencer-with-parabricks-v4-2/
  Quote: "accelerates DeepVariant and end-to-end variant calling runtime."
  Used as: Context that hardware acceleration materially shifts call_variants throughput.
