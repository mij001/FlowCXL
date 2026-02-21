# Equations

This model includes transfer costs and deterministic queueing from shared resources.
Compute time is out of scope.

## Transfer time

Per transfer:

\[
T = T_{fixed} + \frac{B}{BW}
\]

- `B`: bytes transferred
- `BW`: one-way bandwidth in bytes/s

PCIe fixed cost:

\[
T_{fixed,pcie} = T_{enqueue} + T_{driver} + T_{wire}
\]

\[
T_{pcie}(B) = T_{fixed,pcie} + \frac{B}{BW_{pcie}}
\]

CXL fixed cost:

\[
T_{cxl}(B) = L_{cxl} + \frac{B}{BW_{cxl}}
\]

## Resource scheduling and queue attribution

For an operation requested at `t_req` on resources `R`:

\[
wait_r = \max(0, t_{free,r} - t_{req})
\]
\[
blocking\_wait = \max_{r \in R}(wait_r)
\]
\[
t_{start} = t_{req} + blocking\_wait
\]
\[
t_{end} = t_{start} + duration
\]

Resource updates:

\[
t_{free,r} \leftarrow t_{end}, \quad busy\_time_r \leftarrow busy\_time_r + duration
\]

Queue attribution rule in this repo:

- Identify bottleneck winner resources where `wait_r == blocking_wait`.
- Split `blocking_wait` equally across winners.
- Add only the attributed share to each winner resource queue counter.

Totals reported:

- `queue_total_blocking_s`: sum of `blocking_wait` across operations.
- `queue_total_attributed_s`: sum of per-resource attributed queue times.

## Two-resource gating for PCIe

PCIe H2D requires DMA + link:

- Duplex mode: `dma_h2d` and `pcie_h2d`
- Shared-link mode: `dma_h2d` and `pcie_shared`

PCIe D2H requires DMA + link:

- Duplex mode: `dma_d2h` and `pcie_d2h`
- Shared-link mode: `dma_d2h` and `pcie_shared`

CXL uses only link resources:

- Duplex mode: `cxl_h2d` or `cxl_d2h`
- Shared-link mode: `cxl_shared`

## Completion time

Run completion is event makespan:

\[
T_{makespan} = \max\limits_{events}(t_{end})
\]

## Bytes moved

For boundaries `[X0, X1, ..., XN]` and `num_chunks = k`:

Bounce (`pim_no_cxl_bounce` or `pim_cxl_bounce`):

\[
bytes = k \cdot \sum_{i=1}^{N}(X_{i-1}+X_i)
\]

Chain (`pim_cxl_chain`):

\[
bytes = k \cdot (X_0 + X_N)
\]
