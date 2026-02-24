"""Cited constants, workload profiles, and run vocabulary."""

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
PIPELINE_TEMPLATE_TPCH_3OP = "tpch_3op"

DEEPVARIANT_STAGE_NAMES = ("make_examples", "call_variants", "postprocess_variants")
TPCH_STAGE_NAMES = ("scan_filter_project", "join", "groupby_agg")

PROFILE_DV_ILLUMINA_WGS_30X = "PROFILE_DV_ILLUMINA_WGS_30X"
PROFILE_DV_ILLUMINA_WES_100X = "PROFILE_DV_ILLUMINA_WES_100X"
PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE = "PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE"
PROFILE_TPCH_SF100_HIGH_INTERMEDIATE = "PROFILE_TPCH_SF100_HIGH_INTERMEDIATE"

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

_REQUIRED_DEEPVARIANT_PROFILE_KEYS = (
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

_REQUIRED_TPCH_PROFILE_KEYS = (
    "scale_factor",
    "lineitem_rows_per_sf",
    "scan_input_row_bytes",
    "scan_projected_row_bytes",
    "scan_selectivity",
    "join_fanout",
    "join_output_row_bytes",
    "agg_reduction_ratio",
    "agg_output_row_bytes",
)


def _validate_stage_shares(stage_shares: Mapping[str, float], stage_names: tuple[str, ...]) -> None:
    missing = [stage for stage in stage_names if stage not in stage_shares]
    if missing:
        raise ValueError(f"missing stage share keys: {missing}")
    share_sum = sum(float(stage_shares[stage]) for stage in stage_names)
    if abs(share_sum - 1.0) > 1e-3:
        raise ValueError(f"stage shares must sum to 1.0, got {share_sum}")
    for stage in stage_names:
        if float(stage_shares[stage]) <= 0.0:
            raise ValueError(f"stage share for {stage} must be > 0")


def _derive_deepvariant_num_examples(params: Mapping[str, float]) -> int:
    covered_bases = float(params["covered_bases"])
    coverage_x = float(params["coverage_x"])
    density = float(params["candidate_density_per_base_at_ref_coverage"])
    ref_coverage_x = float(params["candidate_density_ref_coverage_x"])
    if ref_coverage_x <= 0:
        raise ValueError("candidate_density_ref_coverage_x must be > 0")
    num_examples = int(round(covered_bases * density * (coverage_x / ref_coverage_x)))
    return max(1, num_examples)


def _derive_deepvariant_boundaries_bytes(params: Mapping[str, float]) -> list[int]:
    num_examples = _derive_deepvariant_num_examples(params)
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
    missing = [key for key in _REQUIRED_DEEPVARIANT_PROFILE_KEYS if key not in params]
    if missing:
        raise ValueError(f"profile {profile_id} missing required keys: {missing}")

    _validate_stage_shares(params["cpu_stage_time_share_1x"], DEEPVARIANT_STAGE_NAMES)
    boundaries = _derive_deepvariant_boundaries_bytes(params)
    return {
        "pipeline_template": PIPELINE_TEMPLATE_DEEPVARIANT_3STAGE,
        "boundaries_bytes": boundaries,
        "description": description,
        "stage_names": list(DEEPVARIANT_STAGE_NAMES),
        "parameters": dict(params),
        "num_examples_1x": _derive_deepvariant_num_examples(params),
    }


def _derive_tpch_rows(params: Mapping[str, object]) -> Dict[str, int]:
    rows_scan_in = int(round(float(params["scale_factor"]) * float(params["lineitem_rows_per_sf"])))
    rows_scan_in = max(1, rows_scan_in)
    rows_scan_out = max(1, int(round(rows_scan_in * float(params["scan_selectivity"]))))
    rows_join_out = max(1, int(round(rows_scan_out * float(params["join_fanout"]))))
    rows_agg_out = max(1, int(round(rows_join_out * float(params["agg_reduction_ratio"]))))
    return {
        "rows_scan_in": rows_scan_in,
        "rows_scan_out": rows_scan_out,
        "rows_join_out": rows_join_out,
        "rows_agg_out": rows_agg_out,
    }


def _derive_tpch_boundaries_bytes(params: Mapping[str, object]) -> list[int]:
    rows = _derive_tpch_rows(params)
    x0 = int(rows["rows_scan_in"] * int(params["scan_input_row_bytes"]))
    x1 = int(rows["rows_scan_out"] * int(params["scan_projected_row_bytes"]))
    x2 = int(rows["rows_join_out"] * int(params["join_output_row_bytes"]))
    x3 = int(rows["rows_agg_out"] * int(params["agg_output_row_bytes"]))
    return [x0, x1, x2, x3]


def _build_tpch_profile(profile_id: str, params: Dict[str, object], description: str) -> Dict[str, object]:
    missing = [key for key in _REQUIRED_TPCH_PROFILE_KEYS if key not in params]
    if missing:
        raise ValueError(f"profile {profile_id} missing required keys: {missing}")

    rows = _derive_tpch_rows(params)
    boundaries = _derive_tpch_boundaries_bytes(params)
    return {
        "pipeline_template": PIPELINE_TEMPLATE_TPCH_3OP,
        "boundaries_bytes": boundaries,
        "description": description,
        "stage_names": list(TPCH_STAGE_NAMES),
        "parameters": dict(params),
        "derived_rows_1x": rows,
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

TPCH_PROFILE_PARAMETERS: Dict[str, Dict[str, object]] = {
    PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE: {
        "scale_factor": 100,
        "lineitem_rows_per_sf": 6_000_000,
        "scan_input_row_bytes": 64,
        "scan_projected_row_bytes": 24,
        "scan_selectivity": 0.35,
        "join_fanout": 1.8,
        "join_output_row_bytes": 48,
        "agg_reduction_ratio": 0.15,
        "agg_output_row_bytes": 32,
    },
    PROFILE_TPCH_SF100_HIGH_INTERMEDIATE: {
        "scale_factor": 100,
        "lineitem_rows_per_sf": 6_000_000,
        "scan_input_row_bytes": 64,
        "scan_projected_row_bytes": 24,
        "scan_selectivity": 0.90,
        "join_fanout": 6.0,
        "join_output_row_bytes": 56,
        "agg_reduction_ratio": 0.20,
        "agg_output_row_bytes": 32,
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
    PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE: _build_tpch_profile(
        profile_id=PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE,
        params=TPCH_PROFILE_PARAMETERS[PROFILE_TPCH_SF100_MODERATE_INTERMEDIATE],
        description="TPC-H-like 3-operator profile with moderate intermediate growth.",
    ),
    PROFILE_TPCH_SF100_HIGH_INTERMEDIATE: _build_tpch_profile(
        profile_id=PROFILE_TPCH_SF100_HIGH_INTERMEDIATE,
        params=TPCH_PROFILE_PARAMETERS[PROFILE_TPCH_SF100_HIGH_INTERMEDIATE],
        description="TPC-H-like 3-operator profile with high intermediate growth for bounce stress.",
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
        "how_used": "Defines fixed 3-stage DeepVariant pipeline template.",
    },
    "DEEPVARIANT_EXAMPLE_SHAPE": {
        "value": [100, 147, 10],
        "url": "https://github.com/google/deepvariant/releases",
        "quote": "example shape [100, 147, 10]",
        "how_used": "Tensor materialization seed for DeepVariant stage-1 output bytes.",
    },
    "TPCH_SCHEMA_CONTEXT": {
        "value": "TPC-H",
        "url": "https://www.tpc.org/tpch/",
        "quote": "TPC-H is a decision support benchmark.",
        "how_used": "Workload context for OLAP scan/join/aggregation pipeline.",
    },
    "GPUDIRECT_STAGING_CONTEXT": {
        "value": "qualitative",
        "url": "https://developer.nvidia.com/blog/gpudirect-storage/",
        "quote": "Direct paths avoid extra CPU memory copies.",
        "how_used": "Analogy for host-bounce elimination with direct device-to-device movement.",
    },
    "HYBRID_GPU_DB_CONTEXT": {
        "value": "qualitative",
        "url": "https://www.microsoft.com/en-us/research/publication/relational-query-processing-on-opencl-based-fpgas/",
        "quote": "Transfers can dominate accelerator query pipelines.",
        "how_used": "Context for transfer bottlenecks in analytical pipelines.",
    },
    "OLAP_MEMORY_BOUND_CONTEXT": {
        "value": "qualitative",
        "url": "https://www.microsoft.com/en-us/research/publication/relational-query-processing-on-opencl-based-fpgas/",
        "quote": "Operator placement is sensitive to host-device transfer overhead.",
        "how_used": "Supports memory/transfer bottleneck framing for OLAP stage modeling.",
    },
    "UPMEM_SCAN_CONTEXT": {
        "value": "qualitative",
        "url": "https://link.springer.com/article/10.1007/s11227-024-06378-8",
        "quote": "PIM scan performance is sensitive to data movement.",
        "how_used": "PIM counterpart context for scan/filter stage.",
    },
    "PID_JOIN_CONTEXT": {
        "value": "qualitative",
        "url": "https://arxiv.org/abs/2303.07591",
        "quote": "Processing-in-DIMM joins accelerate relational joins.",
        "how_used": "PIM counterpart context for join stage.",
    },
    "DARWIN_ANALYTICS_CONTEXT": {
        "value": "qualitative",
        "url": "https://pure.kaist.ac.kr/en/publications/darwin-a-dram-based-adaptive-in-memory-computing-architecture-for",
        "quote": "In-memory analytics architecture targets data analytics operators.",
        "how_used": "PIM counterpart context for aggregation stage.",
    },
    "DARWIN_OPERATOR_GAIN_CONTEXT": {
        "value": "qualitative",
        "url": "https://pure.kaist.ac.kr/en/publications/darwin-a-dram-based-adaptive-in-memory-computing-architecture-for",
        "quote": "Reports large operator-level throughput gains versus CPU baselines.",
        "how_used": "Context for using higher effective PIM throughput in OLAP stages.",
    },
    "PID_JOIN_REAL_DIMM_CONTEXT": {
        "value": "qualitative",
        "url": "https://arxiv.org/abs/2303.07591",
        "quote": "Join design and evaluation target real UPMEM DIMM constraints.",
        "how_used": "Context for join-stage PIM acceleration assumptions.",
    },
}

CITATIONS = {key: {"url": value["url"], "quote": value["quote"]} for key, value in CITED_VALUES.items()}
