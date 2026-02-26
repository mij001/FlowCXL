# Claims-To-Evidence Map

This file maps each headline claim to reproducible artifacts, configs, provenance class, and robustness scope.

## Claim 1: FlowCXL direct is not slower than host bounce on the default matrix
- Claim statement:
  - For default TPCH + DeepVariant profiles, `pim_flowcxl_direct <= pim_host_bounce` across the configured multiplier sweep.
- Supporting artifacts:
  - `artifacts/report/plot_makespan_grouped_*_base.png`
  - `artifacts/metrics.csv` (scenario rows for `pim_host_bounce` and `pim_flowcxl_direct`)
  - `tests/test_story_gates.py` (optional narrative gate)
- Canonical config:
  - `paper/configs/fig_main.yaml` (base config path: `configs/runs.yaml`)
- Reproduce:
  - `python run.py --config configs/runs.yaml --artifacts-dir artifacts`
  - `python report.py --config configs/runs.yaml --artifacts-dir artifacts`
- Parameter provenance:
  - measured_cited: `LINK_UPMEM_HOST_H2D_MEASURED.bandwidth_Bps`, `LINK_UPMEM_HOST_D2H_MEASURED.bandwidth_Bps`
  - derived_workload: `tpch_3op.boundaries_bytes`, `deepvariant_3stage.boundaries_bytes`
  - assumed_sweepable: `cxl_direct_concurrency`, `cxl_topology`, `memory_system_by_template`, `pim_retention`
- Sensitivity statement:
  - Checked in `artifacts/validation/sensitivity_results.csv` under families `cxl_link`, `pim_speedup`, `tpch_memory`.
- Residual caveats:
  - Direct-link service uses a fluid processor-sharing approximation (not packet-level simulation).

## Claim 2: High-intermediate TPCH exhibits strong bounce penalty
- Claim statement:
  - At TPCH high profile and `1x`, bounce/direct ratio is expected to be high (narrative gate uses `>= 2.0`).
- Supporting artifacts:
  - `artifacts/metrics.csv` filtered to `PROFILE_TPCH_SF100_HIGH_INTERMEDIATE`, `workload_variant=base`, `stage_size_multiplier=1.0`
  - `artifacts/report/plot_makespan_grouped_PROFILE_TPCH_SF100_HIGH_INTERMEDIATE_base.png`
- Canonical config:
  - `paper/configs/fig_main.yaml`
- Reproduce:
  - `FLOWCXL_ENABLE_STORY_GATES=1 python -m unittest tests/test_story_gates.py -v`
- Parameter provenance:
  - measured_cited: directional host-link points
  - derived_workload: TPCH boundary derivation
  - assumed_sweepable: host touch, queueing, retention/topology knobs
- Sensitivity statement:
  - Robustness ranked in `artifacts/validation/tornado_top8.csv` (target point: high TPCH at `1x`).
- Residual caveats:
  - Threshold is a narrative regression gate, not a universal physical invariant.

## Claim 3: Retention and striping variants exercise their intended mechanisms
- Claim statement:
  - `retention_colocated` yields retained bytes (`total_bytes_pim_retained > 0`), and `switch_striping` yields striping factor > 1 in direct runs.
- Supporting artifacts:
  - `artifacts/metrics.csv` columns: `total_bytes_pim_retained`, `cxl_effective_striping_factor`, `cxl_active_direct_endpoints`
  - Appendix plots:
    - `artifacts/report/plot_makespan_grouped_*_retention_colocated.png`
    - `artifacts/report/plot_makespan_grouped_*_switch_striping.png`
- Canonical config:
  - `paper/configs/fig_main.yaml`
- Reproduce:
  - `python -m unittest discover -s tests -v`
- Parameter provenance:
  - assumed_sweepable: `pim_retention`, `cxl_topology`, `cxl_direct_concurrency`
- Sensitivity statement:
  - Effects remain observable across link/speedup/memory sweeps in `sensitivity_results.csv`.
- Residual caveats:
  - Retention capacity check is boundary-level guard (no fine-grained runtime occupancy model).

## Claim 4: Calibration overlays are system-scoped and reproducible
- Claim statement:
  - Calibration outputs are tied to `validation.system_id`, and overlay application is run-scoped without global link mutation.
- Supporting artifacts:
  - `artifacts/validation/microbench_raw.csv`
  - `artifacts/validation/microbench_fit.yaml`
  - `artifacts/validation/microbench_overlay.yaml`
  - `artifacts/config_validation_overlay.yaml`
  - `tests/test_validation_pipeline.py::test_validation_overlay_does_not_mutate_sources_links`
- Canonical config:
  - `paper/configs/fig_validation.yaml`
- Reproduce:
  - `python tools/validation/run_validation.py --config configs/runs.yaml --artifacts-dir artifacts`
  - `python run.py --config configs/runs.yaml --artifacts-dir artifacts --validation-overlay artifacts/validation/microbench_overlay.yaml`
- Parameter provenance:
  - measured_cited: directional host-link anchors
  - assumed_sweepable: CXL switch/issue/topology knobs
- Sensitivity statement:
  - Calibration/cross-check residuals are tracked and surfaced in validation appendix sections.
- Residual caveats:
  - Synthetic calibration suite is deterministic and system-tagged; unavailable physical paths use model cross-check evidence.

## Claim 5: Direct scheduler behavior matches independent PS reference within configured tolerance
- Claim statement:
  - CXL scheduler completion behavior cross-checks against an independent processor-sharing solver with bounded MAPE.
- Supporting artifacts:
  - `artifacts/validation/cxl_ps_crosscheck.csv`
  - `tests/test_simulator.py` processor-sharing invariants
- Canonical config:
  - `paper/configs/fig_validation.yaml`
- Reproduce:
  - `python tools/validation/crosscheck_ps.py --config configs/runs.yaml --out artifacts/validation`
- Parameter provenance:
  - assumed_sweepable: scheduler concurrency and issue-overhead controls
- Sensitivity statement:
  - Cross-check is exercised across configured payloads and concurrency levels.
- Residual caveats:
  - PS is a fluid approximation: fairness and instantaneous rate reallocation are assumed.
