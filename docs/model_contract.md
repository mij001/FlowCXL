# Model Contract

## Inputs

Required high-level config keys are validated in `simulator._validate_config` and validation tooling.

Key requirements:

- `memory_system_by_template` is mandatory.
- legacy memory keys are hard-invalid.
- profile definitions must provide template + stage/boundary consistency.
- variant overrides are merged recursively and validated post-merge.

Validation calibration contract:

- `validation.calibration.input_mode` must be `measured_csv`.
- `validation.calibration.measured_inputs` maps each path to a CSV file.
- required measured paths: `host_h2d`, `host_d2h`, `bounce`.
- optional measured path: `direct`.
- when optional `direct` is missing, calibration status is `fallback_crosscheck` and direct-path validity is taken from PS cross-check outputs.

Canonical measured CSV schema per path:

- required columns: `system_id`, `path`, `payload_bytes`, `concurrency`, `repetition`, `time_s`
- optional columns: `tool`, `pinned`, `percentile_source`, `timestamp`, `notes`

## Outputs

Primary outputs:

- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- grouped absolute plot artifacts and `artifacts/report/report.md`

Validation outputs:

- `artifacts/validation/microbench_raw.csv`
- `artifacts/validation/microbench_fit.yaml`
- `artifacts/validation/microbench_overlay.yaml`
- `artifacts/validation/cxl_ps_crosscheck.csv`

Metric semantics:

- `num_stages`: public stage count (DeepVariant=3, TPC-H=3)
- `num_kernels`: execution stage count (DeepVariant=5, TPC-H=3)
- `lb_*`: pool lower-bound diagnostics (`busy_time/capacity`)

Trace semantics:

- one row per executed operation event
- direct CXL events reflect processor-sharing completion timing
- stage endpoint and handoff-mode fields describe inter-stage movement path

## Determinism And Numerical Notes

- Run matrix expansion and IDs are deterministic for a fixed config.
- Floating-point values may differ at sub-ulp scales; tests should use tolerances for timing equality.
- Validation overlays are run-scoped: link constant overrides are applied to an injected link catalog and do not mutate `sources.LINKS`.

## Processor-Sharing Approximation

Direct CXL scheduling uses a fluid processor-sharing approximation:

- fair sharing among active flows,
- instantaneous rate reallocation at arrivals/completions,
- single-bottleneck link abstraction.

This is not packet-level simulation and does not model burstiness/HOL details.

## Processor-Sharing Performance Note

The scheduler reissues completion candidates for all active direct transfers at each admit/complete event (using stale-token invalidation for superseded events).

- Practical cost scales with active direct streams.
- Keep direct concurrency in a moderate range for fast simulation turnaround.
- The model is intended for architectural trend analysis, not high-fanout packet-level stress simulation.

## Ingressless Semantics

`ingressless` skips only the first host->PIM transfer per tile.

- TPCH (PIM stage-1): skips `host_h2d_ingress`.
- DeepVariant (CPU frontend then PIM): skips first `host_h2d_stage`.

Physical interpretation is resident/pinned stage-1 input placement, not free additional link bandwidth.

## Calibration Fitting Model

Per path fit uses:

- `T = latency_s + bytes / bandwidth_Bps`
- aggregate statistic (`median` or `mean`) over measured rows
- fitting at configured reference concurrency (`fit_reference_concurrency`)

Bounce decomposition derives host-touch fit:

- `T_touch_est = T_bounce_measured - T_h2d_fit - T_d2h_fit`
- `T_touch_est = host_touch_fixed_s + bytes / host_touch_Bps`

PCIe sanity guard can flag suspicious one-way throughput using configured generation/lane width and utilization cap.

## Non-goals

The model does not implement:

- full coherence protocol timing
- page migration policy microdynamics
- cycle-accurate DRAM/bus/controller behavior
- exact software stack overhead decomposition beyond configured fixed terms
