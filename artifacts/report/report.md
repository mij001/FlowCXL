# FlowCXL Tiled Stage-Capacity Report

## Single Claim
Template-aware stage modeling with true host bounce and direct CXL movement shows where intermediate-staging penalties dominate multi-stage pipelines.

## Directional Check
- PROFILE_DV_ILLUMINA_WGS_30X (base, legacy): direct<=bounce `true`, direct/bounce range `0.999975` to `0.999985`.
- PROFILE_DV_ILLUMINA_WGS_30X (base, new): direct<=bounce `true`, direct/bounce range `0.999975` to `0.999985`.
- PROFILE_TPCH_SF100_HIGH_INTERMEDIATE (base, new): direct<=bounce `true`, direct/bounce range `0.478554` to `0.497512`.
- PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE (base, new): direct<=bounce `true`, direct/bounce range `0.971854` to `0.996996`.

## Relative Results
- Best direct_over_bounce makespan: `0.478554` at `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE`/`base`/`new`/`4x`.
- Worst direct_over_bounce makespan: `0.999985` at `PROFILE_DV_ILLUMINA_WGS_30X`/`base`/`legacy`/`4x`.

## TPC-H Target Check
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE` at `1x`: bounce/direct ratio `2.025245` (102.524% gain) -> `PASS` (target `>=2.0`).

## High-Intermediate Regime Check
- `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE` at `1x`: cpu/direct ratio `68.317186` -> `PASS` (target `>=1.2`); bounce dominant `host_link` -> `PASS` (must be `host_link` or `host_touch`).

## DeepVariant New vs Legacy (1x)
| workload_profile | workload_variant | scenario | makespan_new_s | makespan_legacy_s | legacy_over_new_makespan | energy_new_J | energy_legacy_J | legacy_over_new_energy |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| PROFILE_DV_ILLUMINA_WGS_30X | base | CPU only | 23825.40703433086 | 23825.40703433086 | 1.0 | 17660160.000000954 | 17660160.000000954 | 1.0 |
| PROFILE_DV_ILLUMINA_WGS_30X | base | PIM FlowCXL direct | 11939.716587031717 | 11939.716587031717 | 1.0 | 5569844.060694607 | 5569844.060694607 | 1.0 |
| PROFILE_DV_ILLUMINA_WGS_30X | base | PIM host bounce | 11939.963752436714 | 11939.963752436714 | 1.0 | 5577283.737843946 | 5577283.737843946 | 1.0 |

## Plot Artifacts
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_ratio_direct_over_bounce_makespan_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_ratio_direct_over_bounce_energy_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_ratio_norm_to_bounce_makespan_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_ratio_norm_to_bounce_energy_PROFILE_DV_ILLUMINA_WGS_30X_base_legacy.png
- plot_makespan_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_energy_grouped_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_makespan_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_energy_grouped_pim_only_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_ratio_direct_over_bounce_makespan_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_ratio_direct_over_bounce_energy_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_ratio_norm_to_bounce_makespan_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_ratio_norm_to_bounce_energy_PROFILE_DV_ILLUMINA_WGS_30X_base_new.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_energy_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_ratio_direct_over_bounce_makespan_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_ratio_direct_over_bounce_energy_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_ratio_norm_to_bounce_makespan_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_ratio_norm_to_bounce_energy_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base_new.png
- plot_makespan_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_energy_grouped_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_makespan_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_energy_grouped_pim_only_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_ratio_direct_over_bounce_makespan_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_ratio_direct_over_bounce_energy_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_ratio_norm_to_bounce_makespan_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png
- plot_ratio_norm_to_bounce_energy_PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE_base_new.png

## Results Table
| workload_family | workload_profile | workload_variant | deepvariant_mode | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 0.5x | CPU only | 120.124870 | 44656.419342 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 0.5x | PIM host bounce | 3.337596 | 354.940791 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 0.5x | PIM FlowCXL direct | 3.243657 | 258.243287 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 1x | CPU only | 198.901204 | 89312.838683 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 1x | PIM host bounce | 6.212502 | 709.881583 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 1x | PIM FlowCXL direct | 6.118563 | 516.486574 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 2x | CPU only | 357.695872 | 178625.677367 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 2x | PIM host bounce | 11.967207 | 1419.761953 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 2x | PIM FlowCXL direct | 11.869652 | 1032.972780 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 4x | CPU only | 714.669613 | 357251.354734 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 4x | PIM host bounce | 23.440315 | 2839.522694 | host_link |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 4x | PIM FlowCXL direct | 23.369907 | 2065.945193 | host_link |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 0.5x | CPU only | 777.497199 | 374957.537091 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 0.5x | PIM host bounce | 23.287714 | 3963.302680 | host_link |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 0.5x | PIM FlowCXL direct | 11.585919 | 3152.802460 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 1x | CPU only | 1551.392383 | 749915.074182 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 1x | PIM host bounce | 45.990612 | 7926.605359 | host_link |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 1x | PIM FlowCXL direct | 22.708669 | 6305.650637 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 2x | CPU only | 3028.828644 | 1499830.148363 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 2x | PIM host bounce | 91.396409 | 15853.210718 | host_link |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 2x | PIM FlowCXL direct | 44.204587 | 12611.354365 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 4x | CPU only | 5983.701168 | 2999660.296727 | compute_stage_max |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 4x | PIM host bounce | 182.208002 | 31706.421436 | host_link |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 4x | PIM FlowCXL direct | 87.196421 | 25222.751498 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 0.5x | CPU only | 12086.046220 | 8830080.000000 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 0.5x | PIM host bounce | 6070.170089 | 2788641.868922 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 0.5x | PIM FlowCXL direct | 6070.021162 | 2784923.010947 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 1x | CPU only | 23825.407034 | 17660160.000001 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 1x | PIM host bounce | 11939.963752 | 5577283.737844 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 1x | PIM FlowCXL direct | 11939.716587 | 5569844.060695 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 2x | CPU only | 47311.094182 | 35320319.999999 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 2x | PIM host bounce | 23682.981155 | 11154567.474900 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 2x | PIM FlowCXL direct | 23682.583994 | 11139686.144610 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 4x | CPU only | 94282.393300 | 70640640.000002 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 4x | PIM host bounce | 47168.972321 | 22309134.949010 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 4x | PIM FlowCXL direct | 47168.265795 | 22279370.559787 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 0.5x | CPU only | 12086.046220 | 8830080.000000 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 0.5x | PIM host bounce | 6070.170089 | 2788641.868922 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 0.5x | PIM FlowCXL direct | 6070.021162 | 2784923.010947 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 1x | CPU only | 23825.407034 | 17660160.000001 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 1x | PIM host bounce | 11939.963752 | 5577283.737844 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 1x | PIM FlowCXL direct | 11939.716587 | 5569844.060695 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 2x | CPU only | 47311.094182 | 35320319.999999 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 2x | PIM host bounce | 23682.981155 | 11154567.474900 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 2x | PIM FlowCXL direct | 23682.583994 | 11139686.144610 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 4x | CPU only | 94282.393300 | 70640640.000002 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 4x | PIM host bounce | 47168.972321 | 22309134.949010 | compute_stage_max |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 4x | PIM FlowCXL direct | 47168.265795 | 22279370.559787 | compute_stage_max |

## Bottleneck Diagnostics
### PROFILE_DV_ILLUMINA_WGS_30X | base | legacy

| workload_family | workload_profile | workload_variant | deepvariant_mode | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s | total_bytes_pim_retained | total_retain_fallback_bytes | cxl_effective_striping_factor | total_cxl_dma_issue_time_component_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 0.5x | CPU only | 12086.046220 | 8830080.000000 | compute_stage_max | 11753.204400 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 0.5x | PIM host bounce | 6070.170089 | 2788641.868922 | compute_stage_max | 5876.602200 | 8.368745 | 68.234015 | 96.177992 | 96.177992 | 18.231396 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 0.5x | PIM FlowCXL direct | 6070.021162 | 2784923.010947 | compute_stage_max | 5876.602200 | 8.368745 | 0.000000 | 0.023300 | 8.368745 | 0.000000 | 7.241128 | 0.000000 | 0.000000 | 1.000000 | 0.000340 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 1x | CPU only | 23825.407034 | 17660160.000001 | compute_stage_max | 23506.408800 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 1x | PIM host bounce | 11939.963752 | 5577283.737844 | compute_stage_max | 11753.204400 | 16.737490 | 136.468029 | 192.355984 | 192.355984 | 36.462792 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 1x | PIM FlowCXL direct | 11939.716587 | 5569844.060695 | compute_stage_max | 11753.204400 | 16.737490 | 0.000000 | 0.046600 | 16.737490 | 0.000000 | 14.359681 | 0.000000 | 0.000000 | 1.000000 | 0.000679 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 2x | CPU only | 47311.094182 | 35320319.999999 | compute_stage_max | 47012.817600 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 2x | PIM host bounce | 23682.981155 | 11154567.474900 | compute_stage_max | 23506.408800 | 33.474972 | 272.936050 | 384.711951 | 384.711951 | 72.925582 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 2x | PIM FlowCXL direct | 23682.583994 | 11139686.144610 | compute_stage_max | 23506.408800 | 33.474972 | 0.000000 | 0.093191 | 33.474972 | 0.000000 | 28.595835 | 0.000000 | 0.000000 | 1.000000 | 0.001358 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 4x | CPU only | 94282.393300 | 70640640.000002 | compute_stage_max | 94025.635200 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 4x | PIM host bounce | 47168.972321 | 22309134.949010 | compute_stage_max | 47012.817600 | 66.949934 | 545.872090 | 769.423883 | 769.423883 | 145.851162 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | legacy | 4x | PIM FlowCXL direct | 47168.265795 | 22279370.559787 | compute_stage_max | 47012.817600 | 66.949934 | 0.000000 | 0.186372 | 66.949934 | 0.000000 | 57.083604 | 0.000000 | 0.000000 | 1.000000 | 0.002716 |
### PROFILE_DV_ILLUMINA_WGS_30X | base | new

| workload_family | workload_profile | workload_variant | deepvariant_mode | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s | total_bytes_pim_retained | total_retain_fallback_bytes | cxl_effective_striping_factor | total_cxl_dma_issue_time_component_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 0.5x | CPU only | 12086.046220 | 8830080.000000 | compute_stage_max | 11753.204400 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 0.5x | PIM host bounce | 6070.170089 | 2788641.868922 | compute_stage_max | 5876.602200 | 8.368745 | 68.234015 | 96.177992 | 96.177992 | 18.231396 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 0.5x | PIM FlowCXL direct | 6070.021162 | 2784923.010947 | compute_stage_max | 5876.602200 | 8.368745 | 0.000000 | 0.023300 | 8.368745 | 0.000000 | 7.241128 | 0.000000 | 0.000000 | 1.000000 | 0.000340 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 1x | CPU only | 23825.407034 | 17660160.000001 | compute_stage_max | 23506.408800 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 1x | PIM host bounce | 11939.963752 | 5577283.737844 | compute_stage_max | 11753.204400 | 16.737490 | 136.468029 | 192.355984 | 192.355984 | 36.462792 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 1x | PIM FlowCXL direct | 11939.716587 | 5569844.060695 | compute_stage_max | 11753.204400 | 16.737490 | 0.000000 | 0.046600 | 16.737490 | 0.000000 | 14.359681 | 0.000000 | 0.000000 | 1.000000 | 0.000679 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 2x | CPU only | 47311.094182 | 35320319.999999 | compute_stage_max | 47012.817600 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 2x | PIM host bounce | 23682.981155 | 11154567.474900 | compute_stage_max | 23506.408800 | 33.474972 | 272.936050 | 384.711951 | 384.711951 | 72.925582 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 2x | PIM FlowCXL direct | 23682.583994 | 11139686.144610 | compute_stage_max | 23506.408800 | 33.474972 | 0.000000 | 0.093191 | 33.474972 | 0.000000 | 28.595835 | 0.000000 | 0.000000 | 1.000000 | 0.001358 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 4x | CPU only | 94282.393300 | 70640640.000002 | compute_stage_max | 94025.635200 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 4x | PIM host bounce | 47168.972321 | 22309134.949010 | compute_stage_max | 47012.817600 | 66.949934 | 545.872090 | 769.423883 | 769.423883 | 145.851162 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| deepvariant | PROFILE_DV_ILLUMINA_WGS_30X | base | new | 4x | PIM FlowCXL direct | 47168.265795 | 22279370.559787 | compute_stage_max | 47012.817600 | 66.949934 | 0.000000 | 0.186372 | 66.949934 | 0.000000 | 57.083604 | 0.000000 | 0.000000 | 1.000000 | 0.002716 |
### PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new

| workload_family | workload_profile | workload_variant | deepvariant_mode | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s | total_bytes_pim_retained | total_retain_fallback_bytes | cxl_effective_striping_factor | total_cxl_dma_issue_time_component_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 0.5x | CPU only | 777.497199 | 374957.537091 | compute_stage_max | 743.115262 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 0.5x | PIM host bounce | 23.287714 | 3963.302680 | host_link | 10.794808 | 2.877327 | 14.557050 | 22.702898 | 22.702898 | 3.889352 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 0.5x | PIM FlowCXL direct | 11.585919 | 3152.802460 | compute_stage_max | 10.794808 | 2.877327 | 0.000000 | 2.190418 | 2.877327 | 0.000000 | 0.473184 | 0.000000 | 0.000000 | 1.000000 | 0.000135 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 1x | CPU only | 1551.392383 | 749915.074182 | compute_stage_max | 1486.230525 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 1x | PIM host bounce | 45.990612 | 7926.605359 | host_link | 21.589615 | 5.754655 | 29.114100 | 45.405797 | 45.405797 | 7.778704 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 1x | PIM FlowCXL direct | 22.708669 | 6305.650637 | compute_stage_max | 21.589615 | 5.754655 | 0.000000 | 4.380835 | 5.754655 | 0.000000 | 0.949226 | 0.000000 | 0.000000 | 1.000000 | 0.000270 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 2x | CPU only | 3028.828644 | 1499830.148363 | compute_stage_max | 2972.461050 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 2x | PIM host bounce | 91.396409 | 15853.210718 | host_link | 43.179231 | 11.509309 | 58.228199 | 90.811593 | 90.811593 | 15.557408 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 2x | PIM FlowCXL direct | 44.204587 | 12611.354365 | compute_stage_max | 43.179231 | 11.509309 | 0.000000 | 8.761670 | 11.509309 | 0.000000 | 1.901771 | 0.000000 | 0.000000 | 1.000000 | 0.000541 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 4x | CPU only | 5983.701168 | 2999660.296727 | compute_stage_max | 5944.922100 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 4x | PIM host bounce | 182.208002 | 31706.421436 | host_link | 86.358462 | 23.018618 | 116.456398 | 181.623186 | 181.623186 | 31.114816 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_HIGH_INTERMEDIATE | base | new | 4x | PIM FlowCXL direct | 87.196421 | 25222.751498 | compute_stage_max | 86.358462 | 23.018618 | 0.000000 | 17.523341 | 23.018618 | 0.000000 | 3.806214 | 0.000000 | 0.000000 | 1.000000 | 0.001082 |
### PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new

| workload_family | workload_profile | workload_variant | deepvariant_mode | stage_size_multiplier | scenario | makespan_s | total_energy_J | dominant_lb_component | lb_compute_stage_max_s | lb_host_h2d_ingress_s | lb_host_h2d_stage_s | lb_host_d2h_s | lb_host_link_s | lb_host_touch_s | lb_cxl_direct_s | total_bytes_pim_retained | total_retain_fallback_bytes | cxl_effective_striping_factor | total_cxl_dma_issue_time_component_s |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 0.5x | CPU only | 120.124870 | 44656.419342 | compute_stage_max | 88.623376 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 0.5x | PIM host bounce | 3.337596 | 354.940791 | host_link | 0.375053 | 2.874907 | 1.736640 | 2.638928 | 2.874907 | 0.463968 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 0.5x | PIM FlowCXL direct | 3.243657 | 258.243287 | host_link | 0.375053 | 2.874907 | 0.000000 | 0.192048 | 2.874907 | 0.000000 | 0.055746 | 0.000000 | 0.000000 | 1.000000 | 0.000029 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 1x | CPU only | 198.901204 | 89312.838683 | compute_stage_max | 177.246752 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 1x | PIM host bounce | 6.212502 | 709.881583 | host_link | 0.750107 | 5.749813 | 3.473279 | 5.277855 | 5.749813 | 0.927936 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 1x | PIM FlowCXL direct | 6.118563 | 516.486574 | host_link | 0.750107 | 5.749813 | 0.000000 | 0.384095 | 5.749813 | 0.000000 | 0.111491 | 0.000000 | 0.000000 | 1.000000 | 0.000058 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 2x | CPU only | 357.695872 | 178625.677367 | compute_stage_max | 354.493503 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 2x | PIM host bounce | 11.967207 | 1419.761953 | host_link | 1.500213 | 11.499618 | 6.946541 | 10.555683 | 11.499618 | 1.855868 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 2x | PIM FlowCXL direct | 11.869652 | 1032.972780 | host_link | 1.500213 | 11.499618 | 0.000000 | 0.768181 | 11.499618 | 0.000000 | 0.222982 | 0.000000 | 0.000000 | 1.000000 | 0.000115 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 4x | CPU only | 714.669613 | 357251.354734 | compute_stage_max | 708.987006 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 4x | PIM host bounce | 23.440315 | 2839.522694 | host_link | 3.000426 | 22.999226 | 13.893063 | 21.111339 | 22.999226 | 3.711732 | 0.000000 | 0.000000 | 0.000000 | 1.000000 | 0.000000 |
| tpch | PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE | base | new | 4x | PIM FlowCXL direct | 23.369907 | 2065.945193 | host_link | 3.000426 | 22.999226 | 0.000000 | 1.536354 | 22.999226 | 0.000000 | 0.445965 | 0.000000 | 0.000000 | 1.000000 | 0.000229 |

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
