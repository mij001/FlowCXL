# Sources And Provenance

All references and constants are defined in `sources.py`:

- `CITED_VALUES`
- `CITATIONS`
- `PARAMETER_PROVENANCE`

## Provenance Classes

Use these classes consistently:

- `measured_cited`: value point anchored to published measurement context.
- `derived_workload`: computed from workload/profile equations.
- `assumed_sweepable`: modeling knob for design-space sweeps.

## Important Clarifications

- `CXL_SWITCH_LAT_s` and `CXL_SWITCH_BW_Bps` are **assumed_sweepable** topology points, not directly measured constants from the cited CXL homepage.
- `UPMEM_HOST_H2D_MEASURED_BW_Bps` and `UPMEM_HOST_D2H_MEASURED_BW_Bps` are selected directional points within cited single-digit GB/s context.
- DeepVariant internal 5-kernel split factors/byte factors are explicit modeling assumptions layered on cited public stage definitions.
- `tiling_model_by_template`, `pim_mode_by_stage_by_template`, and `pim_mode_effects` are explicit **assumed_sweepable** controls for optional regroup/glue/barrier and mode-dependent PIM behavior.
- Validation artifacts are system-tagged (`validation.system_id`) and should be interpreted as system-specific calibration, not universal constants.
- Validation overlay link constants are applied per run through an injected catalog; base `sources.LINKS` remains unchanged across runs/tests.

## How To Audit A Parameter

1. Locate config key/value in `configs/runs.yaml`.
2. Check `sources.PARAMETER_PROVENANCE[config_key-or-logical-id]`.
3. Follow `source` into `CITED_VALUES` or workload derivation fields.
4. Confirm classification (`measured_cited`, `derived_workload`, `assumed_sweepable`) matches report narrative.
