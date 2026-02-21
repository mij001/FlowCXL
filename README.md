# Flow-CXL Contention-Aware Transfer Model

Small Python repo for quantifying host-bounce staging costs and Flow-CXL chain savings under contention.

## What is modeled

- Transfer fixed costs and bandwidth
- Deterministic queueing on shared resources
- Multi-chunk contention (`num_chunks`: 1 and 8)
- Duplex and shared-link resource modes (`shared_link`: false/true)

## What is not modeled

- Compute time
- Any uncited parameter

## Repository layout

- `sources.py`: all cited constants, dataset boundaries, and citation metadata
- `simulator.py`: resource contention scheduler and scenario simulation
- `configs/runs.yaml`: fixed run matrix
- `run.py`: executes runs and writes artifacts
- `report.py`: plots and markdown report generation
- `docs/equations.md`: equations and scheduler rules
- `docs/modeling.md`: modeling choices and CXL point selection
- `tests/test_simulator.py`: minimal correctness tests
- `tests/conftest.py`: pytest import-path bootstrap

## Requirements

- Python 3.10+
- `pyyaml`
- `pandas`
- `matplotlib`

Install:

```bash
pip install -r requirements.txt
```

## Run

```bash
python run.py
python report.py
```

Artifacts:

- `artifacts/metrics.csv`
- `artifacts/traces.csv`
- `artifacts/traces.yaml`
- `artifacts/report/plot_makespan_by_scenario.png`
- `artifacts/report/plot_total_bytes_by_scenario.png`
- `artifacts/report/plot_speedup_cxl_bounce_vs_chain.png`
- `artifacts/report/plot_queue_total_blocking.png`
- `artifacts/report/plot_queue_time_by_resource_attributed.png`
- `artifacts/report/plot_resource_utilization_heatmap.png`
- `artifacts/report/report.md`

## Tests

```bash
python -m unittest discover -s tests -v
```

## Citations (URLs)

- https://ww1.microchip.com/downloads/en/DeviceDoc/00003818.pdf
- https://web.stanford.edu/class/cs244/papers/neugebauer-sigcomm18.pdf
- https://mrmgroup.cs.princeton.edu/papers/dlustigHPCA13.pdf
- https://huaicheng.github.io/p/asplos25-melody.pdf
- https://huaicheng.github.io/s/asplos25-melody-slides.pdf
- https://media.tghn.org/medialibrary/2020/09/Introduction_to_Nanopore_Data_analysis_-_Alp_Aydin.pdf
- https://www.frontiersin.org/journals/genetics/articles/10.3389/fgene.2024.1429306/full
- https://digital.library.adelaide.edu.au/dspace/bitstream/2440/136736/1/Lan2022_PhD.pdf
- https://oak.chosun.ac.kr/bitstream/2020.oak/18470/2/Constructing%20an%20ethnic-specific%20variant%20calling%20workflow%20based%20on%20a%20systematic%20comparison%20of%20multipl.pdf
