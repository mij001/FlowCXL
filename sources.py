"""Fixed parameters and citations for the Flow-CXL transfer-only staging model."""

from __future__ import annotations

GB = 10**9
TB = 10**12

NUM_STAGES = 4

PAYLOAD_FASTQ_BYTES = 100 * GB
PAYLOAD_RAW_BYTES = 1 * TB

PAYLOADS = (
    ("FASTQ_100GB", PAYLOAD_FASTQ_BYTES),
    ("RAW_1TB", PAYLOAD_RAW_BYTES),
)

SCENARIO_BOUNCE = "conventional_host_bounce"
SCENARIO_CHAIN = "flowcxl_chain"

PCIE4_X16_BW_Bps = 32e9
PCIE_L_s = 0.0

CXL_LOCAL_L_s = 214e-9
CXL_LOCAL_BW_Bps = 52e9

CXL_REMOTE_L_s = 621e-9
CXL_REMOTE_BW_Bps = 13e9

LINKS = {
    "PCIe Gen4 x16": {
        "bandwidth_Bps": PCIE4_X16_BW_Bps,
        "latency_s": PCIE_L_s,
        "citation_id": "microchip_pcie_gen4_x16",
    },
    "CXL Local": {
        "bandwidth_Bps": CXL_LOCAL_BW_Bps,
        "latency_s": CXL_LOCAL_L_s,
        "citation_id": "melody_local",
    },
    "CXL Remote": {
        "bandwidth_Bps": CXL_REMOTE_BW_Bps,
        "latency_s": CXL_REMOTE_L_s,
        "citation_id": "melody_remote",
    },
}

RUN_LINK_SCENARIOS = (
    ("PCIe Gen4 x16", SCENARIO_BOUNCE),
    ("CXL Local", SCENARIO_BOUNCE),
    ("CXL Local", SCENARIO_CHAIN),
    ("CXL Remote", SCENARIO_BOUNCE),
    ("CXL Remote", SCENARIO_CHAIN),
)

CITATIONS = {
    "microchip_pcie_gen4_x16": {
        "url": "https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf",
        "quote": "4.0 ... Per-Link (16-Lane) Maximum One-Way Data Rate ... ~32 and PCIe uses dual simplex ... simultaneous two-way communication.",
    },
    "melody_local": {
        "url": "https://huaicheng.github.io/p/asplos25-melody.pdf",
        "quote": "CXL devices' average latency and bandwidth are 214-394ns and 18-52GB/s (Table 1, column 'Local').",
    },
    "melody_remote": {
        "url": "https://huaicheng.github.io/p/asplos25-melody.pdf",
        "quote": "Table 1 remote entries show higher latency and reduced bandwidth (remote up to ~621ns; bandwidth ~13-14GB/s).",
    },
    "melody_slides": {
        "url": "https://huaicheng.github.io/s/asplos25-melody-slides.pdf",
        "quote": "latencies ... locally-attached ... 200-400ns ... switch(es) ... approximately 600ns.",
    },
    "flowcxl_user_pdf": {
        "url": "Source: Flow-CXL proposal PDF provided by user",
        "quote": "1 TB Raw Data ... 100 GB FASTQ is moved between the host and the PIM units ... Flow-CXL ... reduce ... to essentially just the initial load ... eliminating ... Host Bounce. Also: four-stage genomic processing pipeline.",
    },
}
