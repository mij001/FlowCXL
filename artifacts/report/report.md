# FlowCXL Tiled Stage-Capacity Report

## Main Results
Main body includes only `base` and `ingressless` variants for each profile.

| workload_family | workload_profile | workload_variant | best_scenario_1x | worst_scenario_1x | direct_over_bounce_1x | dominant_lb_bounce_1x | dominant_lb_direct_1x |
| --- | --- | --- | --- | --- | --- | --- | --- |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | base | PIM FlowCXL direct | CPU only | 0.996331 | compute_stage_max | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | ingressless | PIM FlowCXL direct | CPU only | 0.996289 | compute_stage_max | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | PIM FlowCXL direct | CPU only | 0.999979 | compute_stage_max | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | ingressless | PIM FlowCXL direct | CPU only | 0.999978 | compute_stage_max | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | PIM FlowCXL direct | CPU only | 0.493767 | host_link | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | ingressless | PIM FlowCXL direct | CPU only | 0.493846 | host_link | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | PIM FlowCXL direct | CPU only | 0.984879 | host_link | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | ingressless | PIM FlowCXL direct | CPU only | 0.196598 | host_link | compute_stage_max |

### PROFILE_DV_ILLUMINA_WES_100X | base

At 1x, direct is faster than bounce by 0.37% (direct/bounce=0.996331). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_base.png
### PROFILE_DV_ILLUMINA_WES_100X | ingressless

At 1x, direct is faster than bounce by 0.37% (direct/bounce=0.996289). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
### PROFILE_DV_ILLUMINA_WGS_30X | base

At 1x, direct is faster than bounce by 0.00% (direct/bounce=0.999979). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base.png
### PROFILE_DV_ILLUMINA_WGS_30X | ingressless

At 1x, direct is faster than bounce by 0.00% (direct/bounce=0.999978). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base

At 1x, direct is faster than bounce by 50.62% (direct/bounce=0.493767). Bounce is dominated by `host_link`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | ingressless

At 1x, direct is faster than bounce by 50.62% (direct/bounce=0.493846). Bounce is dominated by `host_link`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base

At 1x, direct is faster than bounce by 1.51% (direct/bounce=0.984879). Bounce is dominated by `host_link`, direct by `host_link`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | ingressless

At 1x, direct is faster than bounce by 80.34% (direct/bounce=0.196598). Bounce is dominated by `host_link`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png

## Appendix: Additional Variants
Appendix includes `retention_colocated` and `switch_striping`.

| workload_family | workload_profile | workload_variant | best_scenario_1x | worst_scenario_1x | direct_over_bounce_1x | dominant_lb_bounce_1x | dominant_lb_direct_1x |
| --- | --- | --- | --- | --- | --- | --- | --- |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | retention_colocated | PIM host bounce | CPU only | 1.000000 | compute_stage_max | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | switch_striping | PIM FlowCXL direct | CPU only | 0.996264 | compute_stage_max | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | retention_colocated | PIM host bounce | CPU only | 1.000000 | compute_stage_max | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | switch_striping | PIM FlowCXL direct | CPU only | 0.999978 | compute_stage_max | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | retention_colocated | PIM host bounce | CPU only | 1.000000 | compute_stage_max | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | switch_striping | PIM FlowCXL direct | CPU only | 0.493680 | host_link | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | retention_colocated | PIM host bounce | CPU only | 1.000000 | host_link | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | switch_striping | PIM FlowCXL direct | CPU only | 0.984516 | host_link | host_link |

### PROFILE_DV_ILLUMINA_WES_100X | retention_colocated

At 1x, direct and bounce are tied (direct/bounce=1.000000). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
### PROFILE_DV_ILLUMINA_WES_100X | switch_striping

At 1x, direct is faster than bounce by 0.37% (direct/bounce=0.996264). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
### PROFILE_DV_ILLUMINA_WGS_30X | retention_colocated

At 1x, direct and bounce are tied (direct/bounce=1.000000). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
### PROFILE_DV_ILLUMINA_WGS_30X | switch_striping

At 1x, direct is faster than bounce by 0.00% (direct/bounce=0.999978). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | retention_colocated

At 1x, direct and bounce are tied (direct/bounce=1.000000). Bounce is dominated by `compute_stage_max`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | switch_striping

At 1x, direct is faster than bounce by 50.63% (direct/bounce=0.493680). Bounce is dominated by `host_link`, direct by `compute_stage_max`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | retention_colocated

At 1x, direct and bounce are tied (direct/bounce=1.000000). Bounce is dominated by `host_link`, direct by `host_link`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | switch_striping

At 1x, direct is faster than bounce by 1.55% (direct/bounce=0.984516). Bounce is dominated by `host_link`, direct by `host_link`.

- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png

## Appendix Diagnostics (1x)
| workload_family | workload_profile | workload_variant | scenario | total_bytes_pim_retained | total_retain_fallback_bytes | cxl_effective_striping_factor |
| --- | --- | --- | --- | --- | --- | --- |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | retention_colocated | CPU only | 0.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | retention_colocated | PIM host bounce | 23184000000.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | retention_colocated | PIM FlowCXL direct | 23184000000.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | retention_colocated | CPU only | 0.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | retention_colocated | PIM host bounce | 194400000000.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | retention_colocated | PIM FlowCXL direct | 194400000000.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | retention_colocated | CPU only | 0.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | retention_colocated | PIM host bounce | 911400000000.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | retention_colocated | PIM FlowCXL direct | 911400000000.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | retention_colocated | CPU only | 0.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | retention_colocated | PIM host bounce | 48999951000.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | retention_colocated | PIM FlowCXL direct | 48999951000.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | switch_striping | CPU only | 0.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | switch_striping | PIM host bounce | 0.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | switch_striping | PIM FlowCXL direct | 0.000000 | 0.000000 | 3.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | switch_striping | CPU only | 0.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | switch_striping | PIM host bounce | 0.000000 | 0.000000 | 1.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | switch_striping | PIM FlowCXL direct | 0.000000 | 0.000000 | 3.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | switch_striping | CPU only | 0.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | switch_striping | PIM host bounce | 0.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | switch_striping | PIM FlowCXL direct | 0.000000 | 0.000000 | 2.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | switch_striping | CPU only | 0.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | switch_striping | PIM host bounce | 0.000000 | 0.000000 | 1.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WES_100X | switch_striping | PIM FlowCXL direct | 0.000000 | 0.000000 | 2.000000 |

## Plot Artifacts
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_base.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_ingressless.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_retention_colocated.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WES_100X_switch_striping.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_ingressless.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_retention_colocated.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_switch_striping.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_ingressless.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_retention_colocated.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_switch_striping.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_ingressless.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_retention_colocated.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_switch_striping.png

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
- `CXL_SWITCH_LAT_s`: https://www.computeexpresslink.org/
  Quote: "CXL switched fabrics introduce additional hop latency versus local attachment."
  Used as: Configurable switch-link direct-path latency assumption.
- `CXL_SWITCH_BW_Bps`: https://www.computeexpresslink.org/
  Quote: "CXL fabrics can aggregate bandwidth across switched links and ports."
  Used as: Configurable switch-link direct-path bandwidth assumption with striping support.
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
