"""Cited constants, DeepVariant profile defaults, and run vocabulary."""

from __future__ import annotations

from math import prod
from typing import Dict, Mapping

MB = 10**6
GB = 10**9
TB = 10**12

SCENARIO_CPU_ONLY = "cpu_only"
SCENARIO_PIM_HOST_BOUNCE = "pim_host_bounce"
SCENARIO_PIM_FLOWCXL_DIRECT = "pim_flowcxl_direct"

SCENARIOS = (
    SCENARIO_CPU_ONLY,
    SCENARIO_PIM_HOST_BOUNCE,
    SCENARIO_PIM_FLOWCXL_DIRECT,
)

DEVICE_CPU = "cpu"
DEVICE_PIM = "pim"

PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE = "deepvariant_3stage"
DEEPVARIANT_STAGE_NAMES = ("make_examples", "call_variants", "postprocess_variants")

PROFILE_DV_ILLUMINA_WGS_30X = "PROFILE_DV_ILLUMINA_WGS_30X"
PROFILE_DV_ILLUMINA_WES_100X = "PROFILE_DV_ILLUMINA_WES_100X"

LINK_PCIE_GEN4_X16 = "PCIe Gen4 x16"
LINK_CXL_LOCAL = "CXL_LOCAL"
LINK_CXL_REMOTE = "CXL_REMOTE"

PCIE4_X16_BW_Bps = 32e9
PCIE_WIRE_LATENCY_s = 900e-9
PCIE_ENQUEUE_OVERHEAD_s = 1.2e-6
PCIE_DRIVER_OVERHEAD_s = 7.0e-6
PCIE_FIXED_OVERHEAD_s = PCIE_ENQUEUE_OVERHEAD_s + PCIE_DRIVER_OVERHEAD_s + PCIE_WIRE_LATENCY_s

CXL_LOCAL_LAT_s = 214e-9
CXL_LOCAL_BW_Bps = 52e9

CXL_REMOTE_LAT_s = 621e-9
CXL_REMOTE_BW_Bps = 13e9

_REQUIRED_PROFILE_KEYS = (
    "covered_bases",
    "coverage_x",
    "candidate_density_per_base_at_ref_coverage",
    "candidate_density_ref_coverage_x",
    "aligned_bytes_per_covered_base",
    "example_shape",
    "example_element_bytes",
    "call_output_bytes_per_example",
    "postprocess_output_bytes_per_example",
    "cpu_reference_total_runtime_s_1x",
    "cpu_stage_time_share_1x",
)


def _validate_stage_shares(stage_shares: Mapping[str, float]) -> None:
    missing = [stage for stage in DEEPVARIANT_STAGE_NAMES if stage not in stage_shares]
    if missing:
        raise ValueError(f"missing stage share keys: {missing}")
    share_sum = sum(float(stage_shares[stage]) for stage in DEEPVARIANT_STAGE_NAMES)
    if abs(share_sum - 1.0) > 1e-3:
        raise ValueError(f"cpu_stage_time_share_1x must sum to 1.0, got {share_sum}")
    for stage in DEEPVARIANT_STAGE_NAMES:
        if float(stage_shares[stage]) <= 0.0:
            raise ValueError(f"cpu_stage_time_share_1x[{stage}] must be > 0")


def _derive_num_examples(params: Mapping[str, float]) -> int:
    covered_bases = float(params["covered_bases"])
    coverage_x = float(params["coverage_x"])
    density = float(params["candidate_density_per_base_at_ref_coverage"])
    ref_coverage_x = float(params["candidate_density_ref_coverage_x"])
    if ref_coverage_x <= 0:
        raise ValueError("candidate_density_ref_coverage_x must be > 0")
    num_examples = int(round(covered_bases * density * (coverage_x / ref_coverage_x)))
    return max(1, num_examples)


def _derive_boundaries_bytes(params: Mapping[str, float]) -> list[int]:
    num_examples = _derive_num_examples(params)
    x0 = int(
        round(
            float(params["covered_bases"])
            * float(params["coverage_x"])
            * float(params["aligned_bytes_per_covered_base"])
        )
    )
    x1 = int(num_examples * prod(int(dim) for dim in params["example_shape"]) * int(params["example_element_bytes"]))
    x2 = int(num_examples * int(params["call_output_bytes_per_example"]))
    x3 = int(num_examples * int(params["postprocess_output_bytes_per_example"]))
    return [x0, x1, x2, x3]


def _build_deepvariant_profile(profile_id: str, params: Dict[str, object], description: str) -> Dict[str, object]:
    missing = [key for key in _REQUIRED_PROFILE_KEYS if key not in params]
    if missing:
        raise ValueError(f"profile {profile_id} missing required keys: {missing}")

    _validate_stage_shares(params["cpu_stage_time_share_1x"])
    boundaries = _derive_boundaries_bytes(params)
    return {
        "boundaries_bytes": boundaries,
        "description": description,
        "stage_names": list(DEEPVARIANT_STAGE_NAMES),
        "parameters": dict(params),
        "num_examples_1x": _derive_num_examples(params),
    }


DEEPVARIANT_PROFILE_PARAMETERS: Dict[str, Dict[str, object]] = {
    PROFILE_DV_ILLUMINA_WGS_30X: {
        "covered_bases": 3_100_000_000,
        "coverage_x": 30,
        "candidate_density_per_base_at_ref_coverage": 0.0020,
        "candidate_density_ref_coverage_x": 30,
        "aligned_bytes_per_covered_base": 1.2,
        "example_shape": [100, 147, 10],
        "example_element_bytes": 1,
        "call_output_bytes_per_example": 12,
        "postprocess_output_bytes_per_example": 40,
        "cpu_reference_total_runtime_s_1x": 36792.0,
        "cpu_stage_time_share_1x": {
            "make_examples": 0.3082,
            "call_variants": 0.6389,
            "postprocess_variants": 0.0529,
        },
    },
    PROFILE_DV_ILLUMINA_WES_100X: {
        "covered_bases": 50_000_000,
        "coverage_x": 100,
        "candidate_density_per_base_at_ref_coverage": 0.0020,
        "candidate_density_ref_coverage_x": 30,
        "aligned_bytes_per_covered_base": 1.2,
        "example_shape": [100, 147, 10],
        "example_element_bytes": 1,
        "call_output_bytes_per_example": 12,
        "postprocess_output_bytes_per_example": 40,
        "cpu_reference_total_runtime_s_1x": 558.0,
        "cpu_stage_time_share_1x": {
            "make_examples": 0.8136,
            "call_variants": 0.1057,
            "postprocess_variants": 0.0806,
        },
    },
}

DATASET_PROFILES = {
    PROFILE_DV_ILLUMINA_WGS_30X: _build_deepvariant_profile(
        profile_id=PROFILE_DV_ILLUMINA_WGS_30X,
        params=DEEPVARIANT_PROFILE_PARAMETERS[PROFILE_DV_ILLUMINA_WGS_30X],
        description="DeepVariant WGS (Illumina 30x) with derived make_examples/call/postprocess boundaries.",
    ),
    PROFILE_DV_ILLUMINA_WES_100X: _build_deepvariant_profile(
        profile_id=PROFILE_DV_ILLUMINA_WES_100X,
        params=DEEPVARIANT_PROFILE_PARAMETERS[PROFILE_DV_ILLUMINA_WES_100X],
        description="DeepVariant WES (Illumina 100x) with derived make_examples/call/postprocess boundaries.",
    ),
}

LINKS = {
    LINK_PCIE_GEN4_X16: {
        "bandwidth_Bps": PCIE4_X16_BW_Bps,
        "latency_s": PCIE_FIXED_OVERHEAD_s,
        "how_used": "Used by host-link transfer equation.",
    },
    LINK_CXL_LOCAL: {
        "bandwidth_Bps": CXL_LOCAL_BW_Bps,
        "latency_s": CXL_LOCAL_LAT_s,
        "how_used": "Used by direct PIM-to-PIM transfer equation (local point).",
    },
    LINK_CXL_REMOTE: {
        "bandwidth_Bps": CXL_REMOTE_BW_Bps,
        "latency_s": CXL_REMOTE_LAT_s,
        "how_used": "Used by direct PIM-to-PIM transfer equation (remote point).",
    },
}

CITED_VALUES = {
    "PCIE4_X16_BW_Bps": {
        "value": PCIE4_X16_BW_Bps,
        "url": "https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf",
        "quote": "Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32",
        "how_used": "Host-link one-way bandwidth for B/BW.",
    },
    "PCIE_WIRE_LATENCY_s": {
        "value": PCIE_WIRE_LATENCY_s,
        "url": "https://web.stanford.edu/class/cs244/papers/neugebauer-sigcomm18.pdf",
        "quote": "PCIe contributing around 900 ns.",
        "how_used": "Wire component in PCIe fixed transfer cost.",
    },
    "PCIE_ENQUEUE_OVERHEAD_s": {
        "value": PCIE_ENQUEUE_OVERHEAD_s,
        "url": "https://mrmgroup.cs.princeton.edu/papers/dlustigHPCA13.pdf",
        "quote": "1.2 us to enqueue",
        "how_used": "Software enqueue component in PCIe fixed cost.",
    },
    "PCIE_DRIVER_OVERHEAD_s": {
        "value": PCIE_DRIVER_OVERHEAD_s,
        "url": "https://mrmgroup.cs.princeton.edu/papers/dlustigHPCA13.pdf",
        "quote": "7 us to process",
        "how_used": "Driver processing component in PCIe fixed cost.",
    },
    "CXL_LOCAL_LAT_s": {
        "value": CXL_LOCAL_LAT_s,
        "url": "https://huaicheng.github.io/p/asplos25-melody.pdf",
        "quote": "average latency and bandwidth are 214-394ns and 18-52GB/s",
        "how_used": "Representative local latency point for CXL direct transfers.",
    },
    "CXL_LOCAL_BW_Bps": {
        "value": CXL_LOCAL_BW_Bps,
        "url": "https://huaicheng.github.io/p/asplos25-melody.pdf",
        "quote": "average latency and bandwidth are 214-394ns and 18-52GB/s",
        "how_used": "Representative local bandwidth point for CXL direct transfers.",
    },
    "CXL_REMOTE_LAT_s": {
        "value": CXL_REMOTE_LAT_s,
        "url": "https://huaicheng.github.io/s/asplos25-melody-slides.pdf",
        "quote": "locally-attached ... 200-400ns ... switch(es) ... approximately 600ns",
        "how_used": "Representative remote-ish latency point for CXL direct transfers.",
    },
    "CXL_REMOTE_BW_Bps": {
        "value": CXL_REMOTE_BW_Bps,
        "url": "https://huaicheng.github.io/p/asplos25-melody.pdf",
        "quote": "remote entries show higher latency and reduced bandwidth (~13-14GB/s)",
        "how_used": "Representative remote bandwidth point for CXL direct transfers.",
    },
    "DEEPVARIANT_STAGE_NAMES": {
        "value": list(DEEPVARIANT_STAGE_NAMES),
        "url": "https://github.com/google/deepvariant",
        "quote": "make_examples, call_variants, postprocess_variants",
        "how_used": "Defines fixed 3-stage DeepVariant pipeline in simulator.",
    },
    "DEEPVARIANT_EXAMPLE_SHAPE": {
        "value": [100, 147, 10],
        "url": "https://github.com/google/deepvariant/releases",
        "quote": "example shape [100, 147, 10]",
        "how_used": "Tensor materialization size seed for stage-1 output bytes.",
    },
    "DEEPVARIANT_TIMING_BREAKDOWN_CONTEXT": {
        "value": {
            "wgs_runtime_s": DEEPVARIANT_PROFILE_PARAMETERS[PROFILE_DV_ILLUMINA_WGS_30X][
                "cpu_reference_total_runtime_s_1x"
            ],
            "wes_runtime_s": DEEPVARIANT_PROFILE_PARAMETERS[PROFILE_DV_ILLUMINA_WES_100X][
                "cpu_reference_total_runtime_s_1x"
            ],
        },
        "url": "https://developer.nvidia.com/blog/accelerating-deepvariant/",
        "quote": "make_examples and call_variants dominate runtime depending on hardware path.",
        "how_used": "Calibration context for stage runtime shares at 1x.",
    },
    "PARABRICKS_DV_CONTEXT": {
        "value": "qualitative",
        "url": "https://developer.nvidia.com/blog/accelerate-genomic-analysis-for-any-sequencer-with-parabricks-v4-2/",
        "quote": "accelerates DeepVariant and end-to-end variant calling runtime.",
        "how_used": "Context that hardware acceleration materially shifts call_variants throughput.",
    },
}

CITATIONS = {key: {"url": value["url"], "quote": value["quote"]} for key, value in CITED_VALUES.items()}
