# Equations

This repository models only transfer/staging time on the host link.
Compute time is out of scope.

## Per-transfer time

\[
T = L + \frac{B}{BW}
\]

- `B`: bytes transferred
- `BW`: one-way link bandwidth in bytes/second
- `L`: fixed transfer latency in seconds

## Total transfer time

\[
T_{total} = \sum_{k=1}^{N} T_k
\]

Where `N` is the number of transfers in a scenario.

## Queueing / contention

Queueing is fixed to `0` in this model. Transfers are serialized.

## Scenarios

- `conventional_host_bounce`: for each stage (4 total), one `H2D` transfer and one `D2H` transfer.
  - Total transfers: `2 * num_stages = 8`
- `flowcxl_chain`: one initial `H2D` and one final `D2H` transfer.
  - Total transfers: `2`

## Derived outputs

- `total_bytes_moved_over_host_link` (stored as `total_bytes_moved`)
- `total_transfer_time_s`
- `speedup = time_bounce / time_chain`
