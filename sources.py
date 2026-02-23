"""Cited constants, dataset profiles, and run vocabulary for pipeline experiments."""

from __future__ import annotations

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

TARGETCALL_KEEP_FRACTION = 0.0529

PROFILE_ONT_100Gbases = "PROFILE_ONT_100Gbases"
PROFILE_ILLUMINA_NA12878 = "PROFILE_ILLUMINA_NA12878"

ONT_X0 = 2 * TB
ONT_X1 = 112 * GB
ONT_X2 = int(round(ONT_X1 * TARGETCALL_KEEP_FRACTION))
ONT_X3 = 147 * GB
ONT_X4 = 128 * MB

ILLUMINA_X0 = 28 * GB
ILLUMINA_X1 = 147 * GB
ILLUMINA_X2 = 128 * MB

DATASET_PROFILES = {
    PROFILE_ONT_100Gbases: {
        "boundaries_bytes": [ONT_X0, ONT_X1, ONT_X2, ONT_X3, ONT_X4],
        "description": "ONT profile with basecall/filter/align/variant boundaries",
    },
    PROFILE_ILLUMINA_NA12878: {
        "boundaries_bytes": [ILLUMINA_X0, ILLUMINA_X1, ILLUMINA_X2],
        "description": "Illumina NA12878 profile with align/variant boundaries",
    },
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
    "ONT_FAST5_X0": {
        "value": ONT_X0,
        "url": "https://media.tghn.org/medialibrary/2020/09/Introduction_to_Nanopore_Data_analysis_-_Alp_Aydin.pdf",
        "quote": "fast5 = 2 terrabytes",
        "how_used": "ONT boundary X0.",
    },
    "ONT_FASTQ_GZ_X1": {
        "value": ONT_X1,
        "url": "https://media.tghn.org/medialibrary/2020/09/Introduction_to_Nanopore_Data_analysis_-_Alp_Aydin.pdf",
        "quote": "fastq gzipped = 112 gigabytes",
        "how_used": "ONT boundary X1.",
    },
    "TARGETCALL_KEEP_FRACTION": {
        "value": TARGETCALL_KEEP_FRACTION,
        "url": "https://www.frontiersin.org/journals/genetics/articles/10.3389/fgene.2024.1429306/full",
        "quote": "filters out 94.71% of the off-target reads",
        "how_used": "Derived keep fraction for ONT boundary X2.",
    },
    "GIAB_BAM_X": {
        "value": 147 * GB,
        "url": "https://digital.library.adelaide.edu.au/dspace/bitstream/2440/136736/1/Lan2022_PhD.pdf",
        "quote": ".bam ... 147 GB",
        "how_used": "Boundary proxy for aligned BAM size.",
    },
    "GIAB_VCF_X": {
        "value": 128 * MB,
        "url": "https://digital.library.adelaide.edu.au/dspace/bitstream/2440/136736/1/Lan2022_PhD.pdf",
        "quote": ".vcf.gz ... 128 MB",
        "how_used": "Boundary proxy for final VCF size.",
    },
    "ILLUMINA_X0": {
        "value": ILLUMINA_X0,
        "url": "https://oak.chosun.ac.kr/bitstream/2020.oak/18470/2/Constructing%20an%20ethnic-specific%20variant%20calling%20workflow%20based%20on%20a%20systematic%20comparison%20of%20multipl.pdf",
        "quote": "Since the NA12878 data was 28GB",
        "how_used": "Illumina boundary X0.",
    },
}

CITATIONS = {key: {"url": value["url"], "quote": value["quote"]} for key, value in CITED_VALUES.items()}
