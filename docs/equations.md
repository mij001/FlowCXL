# Equations

This model includes stage compute, inter-stage transfers, tile-level resource contention, makespan, and energy.

## Stage-size scaling

For original boundaries:

\[
[X_0, X_1, \dots, X_N]
\]

and multiplier \(m\), each boundary is scaled with integer rounding while preserving:

\[
\sum_i X_i' = round\left(m \cdot \sum_i X_i\right)
\]

## Tiling

Tile count:

\[
K = \max\left(1, \left\lceil \frac{\max_i(X_i')}{tile\_size} \right\rceil \right)
\]

Each boundary \(X_i'\) is partitioned exactly into tile bytes:

\[
X_i' = \sum_{k=1}^{K} X_{i,k}
\]

## Compute duration

For stage \(s\), tile \(k\), compute rate \(R_s\) (bytes/s):

\[
T_{compute}(s,k) = \frac{X_{s-1,k}}{R_s}
\]

CPU scenario uses CPU stage rates. PIM scenarios use PIM stage rates.

## Transfer duration

Host-link transfer (PCIe style fixed cost):

\[
T_{host}(B) = T_{fixed,host} + \frac{B}{BW_{host}}
\]

Direct CXL transfer:

\[
T_{cxl}(B) = L_{cxl} + \frac{B}{BW_{cxl}}
\]

## Resource-pool scheduling

Each operation requests one resource pool with limited capacity (units/channels). For request time \(t_{req}\):

\[
t_{start} = \max(t_{req}, t_{free,earliest})
\]
\[
t_{end} = t_{start} + duration
\]

The chosen slot updates:

\[
t_{free,slot} \leftarrow t_{end}
\]

## Scenario transfer paths

- `cpu_only`: compute only, no transfers.
- `pim_host_bounce`:
  - ingress: host \(H2D\) to stage 1
  - between stages: \(D2H\) then \(H2D\)
  - egress: host \(D2H\) from final stage
- `pim_flowcxl_direct`:
  - ingress: host \(H2D\) to stage 1
  - between stages: direct CXL transfer
  - egress: host \(D2H\) from final stage

## Completion time

Run completion is the maximum tile completion time:

\[
T_{makespan} = \max_k(t_{end,k})
\]

## Energy

Per resource:

\[
E_r = busy\_time_r \cdot P_r
\]

Totals:

\[
E_{compute} = \sum_{r \in compute} E_r
\]
\[
E_{transfer} = \sum_{r \in transfer} E_r
\]
\[
E_{total} = E_{compute} + E_{transfer}
\]
