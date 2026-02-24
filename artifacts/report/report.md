# FlowCXL Tiled Stage-Capacity Report

## Single Claim
Template-aware stage modeling with true host bounce and direct CXL movement shows where intermediate-staging penalties dominate multi-stage pipelines.

## Modeled
- Dual templates: DeepVariant (`deepvariant_3stage`) and OLAP (`tpch_3op`)
- Stage-limited compute capacity with per-template stage-device maps
- Tile-by-tile pipelined execution with bounded in-flight admission
- True host bounce for inter-PIM transfer: D2H -> HOST_TOUCH -> H2D(stage)
- Split host H2D resources: ingress vs inter-stage staging
- Absolute makespan (seconds) and total energy (joules)
- Lower-bound bottleneck diagnostics by resource family

## Directional Check
- PROFILE_TPCH_SF100_HIGH_INTERMEDIATE (`tpch_3op`): directional `true`, strictly-better points `4`, 1x bounce/direct ratio `2.038663`, ratio range `1.988558` to `2.069109`, 1x dominants bounce `host_touch`, direct `cxl_direct`.
- PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE (`tpch_3op`): directional `true`, strictly-better points `4`, 1x bounce/direct ratio `1.018702`, ratio range `1.002911` to `1.026181`, 1x dominants bounce `host_link`, direct `host_link`.
- Directional condition checks `direct <= bounce`; ratio range captures sensitivity across stage-size multipliers.

## TPC-H Target Check
- In `tpch_3op`, large S1->S2 and S2->S3 intermediates make host-bounce pay double link traversal + touch, while FlowCXL direct pays a single inter-device transfer.
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE` at `1x`: bounce/direct ratio `2.038663` (103.866% gain) -> `PASS` (target `>=2.0`).

## CPU Memory Ceiling Diagnostics (1x)
| dataset_profile | scenario | memory_ceiling_enabled | total_compute_time_component_s | total_cpu_mem_time_component_s | total_pim_mem_time_component_s |
| --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | CPU only | True | 167.360000 | 186.393600 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | PIM FlowCXL direct | True | 78.080000 | 0.000000 | 93.196800 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | PIM host bounce | True | 78.080000 | 0.000000 | 93.196800 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | CPU only | True | 49.696000 | 62.638080 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | PIM FlowCXL direct | True | 20.714667 | 0.000000 | 31.319040 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | PIM host bounce | True | 20.714667 | 0.000000 | 31.319040 |

## Plot Artifacts
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE.png

## Results Table
| dataset_profile | pipeline_template | stage_size_multiplier | scenario | makespan_s | total_energy_J | host_touch_energy_J | total_bytes_host_touch | dominant_lb_component |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | CPU only | 1.140693 | 520.588800 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | PIM host bounce | 0.867295 | 171.201864 | 13.919040 | 11592000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | PIM FlowCXL direct | 0.845168 | 146.307670 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | CPU only | 1.799360 | 1041.177600 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | PIM host bounce | 1.472863 | 342.403728 | 27.838080 | 23184000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | PIM FlowCXL direct | 1.445823 | 292.615341 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | CPU only | 3.127553 | 2082.355200 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | PIM host bounce | 2.667308 | 684.806244 | 55.676040 | 46368000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | PIM FlowCXL direct | 2.647976 | 585.230310 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | CPU only | 6.112529 | 4164.710400 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | PIM host bounce | 5.065711 | 1369.611276 | 111.351960 | 92736000000 | host_link |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | PIM FlowCXL direct | 5.051007 | 1170.460250 | 0.000000 | 0 | host_link |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | CPU only | 2.164260 | 1900.800000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | PIM host bounce | 4.070659 | 732.229656 | 116.680560 | 97200000000 | host_touch |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | PIM FlowCXL direct | 2.047040 | 523.713039 | 0.000000 | 0 | cxl_direct |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | CPU only | 4.132544 | 3801.600000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | PIM host bounce | 7.984250 | 1464.459312 | 233.361120 | 194400000000 | host_touch |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | PIM FlowCXL direct | 3.916416 | 1047.426078 | 0.000000 | 0 | cxl_direct |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | CPU only | 7.890178 | 7603.200000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | PIM host bounce | 15.738715 | 2928.918624 | 466.722240 | 388800000000 | host_touch |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | PIM FlowCXL direct | 7.655167 | 2094.852156 | 0.000000 | 0 | cxl_direct |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | CPU only | 15.405444 | 15206.400000 | 0.000000 | 0 | compute_stage_max |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | PIM host bounce | 31.311136 | 5857.837248 | 933.444480 | 777600000000 | host_touch |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | PIM FlowCXL direct | 15.132668 | 4189.704311 | 0.000000 | 0 | cxl_direct |

## Bottleneck Diagnostics
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE

| dataset_profile | pipeline_template | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | CPU only | 1.140693 | 520.588800 | compute_stage_max | 0.741000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | PIM FlowCXL direct | 0.845168 | 146.307670 | host_link | 0.370500 | 0.600655 | 0.000000 | 0.029005 | 0.600655 | 0.000000 | 0.222954 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 0.5x | PIM host bounce | 0.867295 | 171.201864 | host_link | 0.370500 | 0.600655 | 0.363560 | 0.392566 | 0.600655 | 0.463968 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | CPU only | 1.799360 | 1041.177600 | compute_stage_max | 1.482000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | PIM FlowCXL direct | 1.445823 | 292.615341 | host_link | 0.741000 | 1.201310 | 0.000000 | 0.058010 | 1.201310 | 0.000000 | 0.445908 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 1x | PIM host bounce | 1.472863 | 342.403728 | host_link | 0.741000 | 1.201310 | 0.727121 | 0.785131 | 1.201310 | 0.927936 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | CPU only | 3.127553 | 2082.355200 | compute_stage_max | 2.964000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | PIM FlowCXL direct | 2.647976 | 585.230310 | host_link | 1.482000 | 2.402612 | 0.000000 | 0.116012 | 2.402612 | 0.000000 | 0.891815 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 2x | PIM host bounce | 2.667308 | 684.806244 | host_link | 1.482000 | 2.402612 | 1.454223 | 1.570235 | 2.402612 | 1.855868 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | CPU only | 6.112529 | 4164.710400 | compute_stage_max | 5.928000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | PIM FlowCXL direct | 5.051007 | 1170.460250 | host_link | 2.964000 | 4.805214 | 0.000000 | 0.232014 | 4.805214 | 0.000000 | 1.783630 |
| PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | tpch_3op | 4x | PIM host bounce | 5.065711 | 1369.611276 | host_link | 2.964000 | 4.805214 | 2.908429 | 3.140443 | 4.805214 | 3.711732 | 0.000000 |
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE

| dataset_profile | pipeline_template | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | CPU only | 2.164260 | 1900.800000 | compute_stage_max | 1.890000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | PIM FlowCXL direct | 2.047040 | 523.713039 | cxl_direct | 0.945000 | 0.603076 | 0.000000 | 0.327076 | 0.603076 | 0.000000 | 1.869375 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 0.5x | PIM host bounce | 4.070659 | 732.229656 | host_touch | 0.945000 | 0.603076 | 3.043652 | 3.370727 | 3.370727 | 3.889352 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | CPU only | 4.132544 | 3801.600000 | compute_stage_max | 3.780000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | PIM FlowCXL direct | 3.916416 | 1047.426078 | cxl_direct | 1.890000 | 1.206152 | 0.000000 | 0.654152 | 1.206152 | 0.000000 | 3.738751 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 1x | PIM host bounce | 7.984250 | 1464.459312 | host_touch | 1.890000 | 1.206152 | 6.087303 | 6.741455 | 6.741455 | 7.778704 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | CPU only | 7.890178 | 7603.200000 | compute_stage_max | 7.560000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | PIM FlowCXL direct | 7.655167 | 2094.852156 | cxl_direct | 3.780000 | 2.412303 | 0.000000 | 1.308303 | 2.412303 | 0.000000 | 7.477502 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 2x | PIM host bounce | 15.738715 | 2928.918624 | host_touch | 3.780000 | 2.412303 | 12.174606 | 13.482910 | 13.482910 | 15.557408 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | CPU only | 15.405444 | 15206.400000 | compute_stage_max | 15.120000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | PIM FlowCXL direct | 15.132668 | 4189.704311 | cxl_direct | 7.560000 | 4.824606 | 0.000000 | 2.616606 | 4.824606 | 0.000000 | 14.955003 |
| PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | tpch_3op | 4x | PIM host bounce | 31.311136 | 5857.837248 | host_touch | 7.560000 | 4.824606 | 24.349213 | 26.965819 | 26.965819 | 31.114816 | 0.000000 |

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
