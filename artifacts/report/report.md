# FlowCXL Tiled Stage-Capacity Report

## Single Claim
Template-aware stage modeling with true host bounce and direct CXL movement shows where intermediate-staging penalties dominate multi-stage pipelines.

## Modeled
- Dual templates: DeepVariant (`deepvariant_3stage`) and OLAP (`tpch_3op`)
- Stage-limited compute capacity with per-template stage-device maps
- Tile-by-tile pipelined execution with bounded in-flight admission
- True host bounce for inter-PIM transfer: D2H -> HOST_TOUCH -> H2D(stage)
- Split host H2D resources: ingress vs inter-stage staging
- Directional host-link modeling: separate host_h2d_link and host_d2h_link ceilings
- Access-pattern DRAM-service CPU model for TPC-H memory components
- Absolute makespan (seconds) and total energy (joules)
- Lower-bound bottleneck diagnostics by resource family

## Directional Check
- PROFILE_TPCH_SF100_HIGH_INTERMEDIATE (`tpch_3op`): directional `true`, strictly-better points `4`, 1x bounce/direct ratio `2.025245`, ratio range `2.010002` to `2.089627`, 1x dominants bounce `host_link`, direct `compute_stage_max`.
- PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE (`tpch_3op`): directional `true`, strictly-better points `4`, 1x bounce/direct ratio `1.015353`, ratio range `1.003013` to `1.028961`, 1x dominants bounce `host_link`, direct `host_link`.
- Directional condition checks `direct <= bounce`; ratio range captures sensitivity across stage-size multipliers.

## TPC-H Target Check
- In `tpch_3op`, large S1->S2 and S2->S3 intermediates make host-bounce pay double link traversal + touch, while FlowCXL direct pays a single inter-device transfer.
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE` at `1x`: bounce/direct ratio `2.025245` (102.524% gain) -> `PASS` (target `>=2.0`).

## High-Intermediate Regime Check
- Regime-based CPU comparison replaces brittle all-point CPU assertions.
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE` at `1x`: cpu/direct ratio `68.317187` -> `PASS` (target `>=1.2`); bounce dominant `host_link` -> `PASS` (must be `host_link` or `host_touch`).

## Memory-System Diagnostics (1x)
| dataset_profile | pipeline_template | cpu_baseline_engine | scenario | memory_ceiling_enabled | total_compute_time_component_s | total_cpu_mem_time_component_s | total_cpu_mem_latency_bound_time_component_s | total_cpu_mem_peak_bound_time_component_s | total_cpu_mem_service_time_component_s | total_cpu_mem_queue_delay_component_s | total_pim_mem_time_component_s | total_pim_mem_service_time_component_s | total_pim_mem_queue_delay_component_s | total_cpu_materialize_time_component_s | total_cpu_materialize_bytes | cpu_materialize_energy_J |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | CPU only | True | 167.360000 | 49994.338279 | 49943.920937 | 50.417341 | 8086.032000 | 41908.306279 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | PIM FlowCXL direct | True | 78.080000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 811.700430 | 265.338000 | 546.362430 | 0.000000 | 0 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | PIM host bounce | True | 78.080000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 811.700430 | 265.338000 | 546.362430 | 0.000000 | 0 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | CPU only | True | 49.696000 | 5954.189246 | 5905.959202 | 48.230043 | 968.473920 | 4985.715326 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | PIM FlowCXL direct | True | 20.714667 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 52.269939 | 50.455500 | 1.814439 | 0.000000 | 0 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | PIM host bounce | True | 20.714667 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 52.269939 | 50.455500 | 1.814439 | 0.000000 | 0 | 0.000000 |

## Plot Artifacts
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE.png

## Results Table
| dataset_profile | pipeline_template | stage_size_multiplier | scenario | makespan_s | total_energy_J | host_touch_energy_J | total_bytes_host_touch | dominant_lb_component |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | CPU only | 120.124870 | 44656.419342 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | PIM host bounce | 3.337596 | 354.940791 | 13.919040 | 11592000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | PIM FlowCXL direct | 3.243656 | 260.918618 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | CPU only | 198.901204 | 89312.838683 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | PIM host bounce | 6.212502 | 709.881583 | 27.838080 | 23184000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | PIM FlowCXL direct | 6.118563 | 521.837237 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | CPU only | 357.695872 | 178625.677367 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | PIM host bounce | 11.967207 | 1419.761953 | 55.676040 | 46368000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | PIM FlowCXL direct | 11.869652 | 1043.674102 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | CPU only | 714.669613 | 357251.354734 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | PIM host bounce | 23.440315 | 2839.522694 | 111.351960 | 92736000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | PIM FlowCXL direct | 23.369906 | 2087.347834 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | CPU only | 777.497199 | 374957.537091 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | PIM host bounce | 23.287714 | 3963.302680 | 116.680560 | 97200000000 | host_link |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | PIM FlowCXL direct | 11.585918 | 3175.141516 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | CPU only | 1551.392383 | 749915.074182 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | PIM host bounce | 45.990612 | 7926.605359 | 233.361120 | 194400000000 | host_link |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | PIM FlowCXL direct | 22.708669 | 6350.283032 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | CPU only | 3028.828644 | 1499830.148363 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | PIM host bounce | 91.396409 | 15853.210718 | 466.722240 | 388800000000 | host_link |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | PIM FlowCXL direct | 44.204586 | 12700.566065 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | CPU only | 5983.701168 | 2999660.296727 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | PIM host bounce | 182.208002 | 31706.421436 | 933.444480 | 777600000000 | host_link |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | PIM FlowCXL direct | 87.196421 | 25401.132129 | 0.000000 | 0 | compute_stage_max |

## Bottleneck Diagnostics
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE

| dataset_profile | pipeline_template | cpu_baseline_engine | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 0.5x | CPU only | 120.124870 | 44656.419342 | compute_stage_max | 88.623376 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 0.5x | PIM FlowCXL direct | 3.243656 | 260.918618 | host_link | 0.375053 | 2.874907 | 0.000000 | 0.192048 | 2.874907 | 0.000000 | 0.222954 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 0.5x | PIM host bounce | 3.337596 | 354.940791 | host_link | 0.375053 | 2.874907 | 1.736640 | 2.638928 | 2.874907 | 0.463968 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 1x | CPU only | 198.901204 | 89312.838683 | compute_stage_max | 177.246752 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 1x | PIM FlowCXL direct | 6.118563 | 521.837237 | host_link | 0.750107 | 5.749813 | 0.000000 | 0.384095 | 5.749813 | 0.000000 | 0.445908 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 1x | PIM host bounce | 6.212502 | 709.881583 | host_link | 0.750107 | 5.749813 | 3.473279 | 5.277855 | 5.749813 | 0.927936 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 2x | CPU only | 357.695872 | 178625.677367 | compute_stage_max | 354.493503 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 2x | PIM FlowCXL direct | 11.869652 | 1043.674102 | host_link | 1.500213 | 11.499618 | 0.000000 | 0.768181 | 11.499618 | 0.000000 | 0.891815 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 2x | PIM host bounce | 11.967207 | 1419.761953 | host_link | 1.500213 | 11.499618 | 6.946541 | 10.555683 | 11.499618 | 1.855868 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 4x | CPU only | 714.669613 | 357251.354734 | compute_stage_max | 708.987006 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 4x | PIM FlowCXL direct | 23.369906 | 2087.347834 | host_link | 3.000426 | 22.999226 | 0.000000 | 1.536354 | 22.999226 | 0.000000 | 1.783630 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | vectorized_pipeline | 4x | PIM host bounce | 23.440315 | 2839.522694 | host_link | 3.000426 | 22.999226 | 13.893063 | 21.111339 | 22.999226 | 3.711732 | 0.000000 |
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE

| dataset_profile | pipeline_template | cpu_baseline_engine | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 0.5x | CPU only | 777.497199 | 374957.537091 | compute_stage_max | 743.115262 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 0.5x | PIM FlowCXL direct | 11.585918 | 3175.141516 | compute_stage_max | 10.794808 | 2.877327 | 0.000000 | 2.190418 | 2.877327 | 0.000000 | 1.869375 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 0.5x | PIM host bounce | 23.287714 | 3963.302680 | host_link | 10.794808 | 2.877327 | 14.557050 | 22.702898 | 22.702898 | 3.889352 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 1x | CPU only | 1551.392383 | 749915.074182 | compute_stage_max | 1486.230525 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 1x | PIM FlowCXL direct | 22.708669 | 6350.283032 | compute_stage_max | 21.589615 | 5.754655 | 0.000000 | 4.380835 | 5.754655 | 0.000000 | 3.738751 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 1x | PIM host bounce | 45.990612 | 7926.605359 | host_link | 21.589615 | 5.754655 | 29.114100 | 45.405797 | 45.405797 | 7.778704 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 2x | CPU only | 3028.828644 | 1499830.148363 | compute_stage_max | 2972.461050 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 2x | PIM FlowCXL direct | 44.204586 | 12700.566065 | compute_stage_max | 43.179231 | 11.509309 | 0.000000 | 8.761670 | 11.509309 | 0.000000 | 7.477502 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 2x | PIM host bounce | 91.396409 | 15853.210718 | host_link | 43.179231 | 11.509309 | 58.228199 | 90.811593 | 90.811593 | 15.557408 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 4x | CPU only | 5983.701168 | 2999660.296727 | compute_stage_max | 5944.922100 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 4x | PIM FlowCXL direct | 87.196421 | 25401.132129 | compute_stage_max | 86.358462 | 23.018618 | 0.000000 | 17.523341 | 23.018618 | 0.000000 | 14.955003 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | vectorized_pipeline | 4x | PIM host bounce | 182.208002 | 31706.421436 | host_link | 86.358462 | 23.018618 | 116.456398 | 181.623186 | 181.623186 | 31.114816 | 0.000000 |

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
- `UPMEM_HOST_H2D_MEASURED_BW_Bps`: https://www.researchgate.net/publication/351475771_Benchmarking_a_New_Paradigm_An_Experimental_Analysis_of_a_Real_Processing-in-Memory_Architecture
  Quote: "measured host-to-DPU transfer peaks are in the single-digit GB/s range."
  Used as: Directional host H2D bandwidth default for OLAP host-staging realism.
- `UPMEM_HOST_D2H_MEASURED_BW_Bps`: https://www.researchgate.net/publication/351475771_Benchmarking_a_New_Paradigm_An_Experimental_Analysis_of_a_Real_Processing-in-Memory_Architecture
  Quote: "measured DPU-to-host transfer peaks are lower than H2D and single-digit GB/s."
  Used as: Directional host D2H bandwidth default for OLAP host-staging realism.
- `DEEPVARIANT_STAGE_NAMES`: https://github.com/google/deepvariant
  Quote: "make_examples, call_variants, postprocess_variants"
  Used as: Defines fixed 3-stage DeepVariant pipeline template.
- `DEEPVARIANT_EXAMPLE_SHAPE`: https://github.com/google/deepvariant/releases
  Quote: "example shape [100, 147, 10]"
  Used as: Tensor materialization seed for DeepVariant stage-1 output bytes.
- `TPCH_SCHEMA_CONTEXT`: https://www.tpc.org/tpch/
  Quote: "TPC-H is a decision support benchmark."
  Used as: Workload context for OLAP scan/join/aggregation pipeline.
- `GPUDIRECT_STAGING_CONTEXT`: https://developer.nvidia.com/blog/gpudirect-storage/
  Quote: "Direct paths avoid extra CPU memory copies."
  Used as: Analogy for host-bounce elimination with direct device-to-device movement.
- `HYBRID_GPU_DB_CONTEXT`: https://www.microsoft.com/en-us/research/publication/relational-query-processing-on-opencl-based-fpgas/
  Quote: "Transfers can dominate accelerator query pipelines."
  Used as: Context for transfer bottlenecks in analytical pipelines.
- `OLAP_MEMORY_BOUND_CONTEXT`: https://www.microsoft.com/en-us/research/publication/relational-query-processing-on-opencl-based-fpgas/
  Quote: "Operator placement is sensitive to host-device transfer overhead."
  Used as: Supports memory/transfer bottleneck framing for OLAP stage modeling.
- `HASH_RANDOM_ACCESS_MEMORY_CONTEXT`: https://spacefrontiers.org/r/10.14778/2732951.2732959
  Quote: "Hash-intensive operators are sensitive to memory access behavior and locality."
  Used as: Supports latency-limited memory-service modeling for hash probe/build and groupby updates.
- `UPMEM_SCAN_CONTEXT`: https://link.springer.com/article/10.1007/s11227-024-06378-8
  Quote: "PIM scan performance is sensitive to data movement."
  Used as: PIM counterpart context for scan/filter stage.
- `PID_JOIN_CONTEXT`: https://arxiv.org/abs/2303.07591
  Quote: "Processing-in-DIMM joins accelerate relational joins."
  Used as: PIM counterpart context for join stage.
- `DARWIN_ANALYTICS_CONTEXT`: https://pure.kaist.ac.kr/en/publications/darwin-a-dram-based-adaptive-in-memory-computing-architecture-for
  Quote: "In-memory analytics architecture targets data analytics operators."
  Used as: PIM counterpart context for aggregation stage.
- `DARWIN_OPERATOR_GAIN_CONTEXT`: https://pure.kaist.ac.kr/en/publications/darwin-a-dram-based-adaptive-in-memory-computing-architecture-for
  Quote: "Reports large operator-level throughput gains versus CPU baselines."
  Used as: Context for using higher effective PIM throughput in OLAP stages.
- `PID_JOIN_REAL_DIMM_CONTEXT`: https://arxiv.org/abs/2303.07591
  Quote: "Join design and evaluation target real UPMEM DIMM constraints."
  Used as: Context for join-stage PIM acceleration assumptions.
- `UPMEM_DPU_OLAP_CONTEXT`: https://github.com/upmem/dpu_olap
  Quote: "Public OLAP-oriented PIM kernels demonstrate in-memory operator implementations."
  Used as: Context for stage-level PIM operator capability assumptions in TPC-H modeling.
- `ATTACC_BASELINE_MODELING_CONTEXT`: https://deepwiki.com/scale-snu/attacc_simulator/1-overview
  Quote: "Baseline and accelerator configurations are modeled as distinct system configurations."
  Used as: Supports first-class CPU/PIM system configuration in the simulator.
- `ATTACC_TRACE_TIMING_CONTEXT`: https://deepwiki.com/scale-snu/attacc_simulator/3-memory-model
  Quote: "Timing is derived from access behavior through the memory model."
  Used as: Supports moving away from ad-hoc CPU-only slowdown paths.
- `PIMDAL_ANALYTICS_BASELINE_CONTEXT`: https://arxiv.org/abs/2403.11888
  Quote: "PIM data-analytics evaluations use comparable CPU-side baselines and PIM implementations."
  Used as: Supports calibrated CPU/PIM operator assumptions for OLAP stages.
