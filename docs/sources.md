# Sources And Provenance

All references and constants are defined in `sources.py`:

- `CITED_VALUES`
- `CITATIONS`
- `PARAMETER_PROVENANCE`

## Provenance Classes

Use these classes consistently:

- `measured`: value fitted from system-scoped measured CSV inputs.
- `spec_or_literature_backed`: value anchored to specifications or published measurements.
- `derived_workload`: computed from workload/profile equations.
- `assumed_sweepable`: modeling knob for design-space sweeps.

## Important Clarifications

- Validation calibration is system-specific and keyed by `validation.system_id`.
- `validation.calibration.measured_inputs` is the authoritative source for host-path calibration.
- `host_h2d`, `host_d2h`, and `bounce` are required measured paths.
- `direct` uses explicit provenance status: `measured`, `crosscheck_only`, or `cited_sweep_only`.
- `CXL_SWITCH_LAT_s` and `CXL_SWITCH_BW_Bps` are **assumed_sweepable** topology points, not directly measured constants from the cited CXL homepage.
- `UPMEM_HOST_H2D_MEASURED_BW_Bps` and `UPMEM_HOST_D2H_MEASURED_BW_Bps` are literature-backed anchors and remain sweepable in sensitivity analyses.
- DeepVariant internal 5-kernel split factors/byte factors are explicit modeling assumptions layered on cited public stage definitions.
- `tiling_model_by_template`, `pim_mode_by_stage_by_template`, and `pim_mode_effects` are explicit **assumed_sweepable** controls for optional regroup/glue/barrier and mode-dependent PIM behavior.
- Recommended `pim_mode_effects` sweeps: compute/mem multipliers in +/-20% to +/-40% bands and `command_overhead_s` in x0.5 to x2.0 bands.
- Validation overlay link constants are applied per run through an injected catalog; base `sources.LINKS` remains unchanged across runs/tests.

## How To Audit A Parameter

1. Locate config key/value in `configs/runs.yaml`.
2. Check `sources.PARAMETER_PROVENANCE[config_key-or-logical-id]`.
3. Follow `source` into `CITED_VALUES`, measured CSV references, or workload derivation fields.
4. Confirm classification (`measured`, `spec_or_literature_backed`, `derived_workload`, `assumed_sweepable`) matches report narrative.
