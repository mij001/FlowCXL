# Modeling Notes

## Scope

- Model includes transfer time and deterministic queueing only.
- Model excludes compute time.
- Concurrency comes from multiple chunks (`num_chunks` in `{1, 8}`) sharing resources.

## Link Resource Modes

- `shared_link = false` (duplex): direction-specific link resources (`*_h2d`, `*_d2h`).
- `shared_link = true` (shared): one link resource is used for both directions (`pcie_shared` or `cxl_shared`).

Shared mode is a modeling choice to represent worst-case serialized bidirectional link contention.

## CXL Operating Points

Two representative points are used from Melody's measured ranges:

- `CXL_LOCAL`: `214 ns`, `52 GB/s` (best local point)
- `CXL_REMOTE`: `621 ns`, `13 GB/s` (remote/worst-side representative point)

These are fixed points selected to keep design space small while covering local and remote regimes.

## Dataset Boundary Coherence

`PROFILE_ONT_100Gbases` is explicitly a representative boundary profile assembled from multiple cited sources:

- ONT raw and FASTQ boundaries from Nanopore data slides.
- Filtering ratio from TargetCall.
- Later BAM/VCF boundaries from GIAB/NA12878 representative sizes.

This profile is used as a transfer-boundary stress case, not as a single-source end-to-end measured dataset.

`PROFILE_ILLUMINA_NA12878` uses the NA12878 input boundary and GIAB representative BAM/VCF boundaries.
