# Model Contract

## Inputs

Required high-level config keys are validated in `simulator._validate_config`.

Key requirements:

- `memory_system_by_template` is mandatory.
- legacy memory keys are hard-invalid.
- profile definitions must provide template + stage/boundary consistency.
- variant overrides are merged recursively and validated post-merge.

## Outputs

Primary outputs:

- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- grouped absolute plot artifacts and `artifacts/report/report.md`

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

## Non-goals

The model does not implement:

- full coherence protocol timing
- page migration policy microdynamics
- cycle-accurate DRAM/bus/controller behavior
- exact software stack overhead decomposition beyond configured fixed terms
