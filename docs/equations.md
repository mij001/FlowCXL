# Equations

This model includes DeepVariant stage-size derivation, stage compute calibration, transition-aware transfers, tile-level resource contention, makespan, and energy.

## DeepVariant boundary derivation

Pipeline stages are fixed:

\[
S = [make\_examples, call\_variants, postprocess\_variants]
\]

Given profile parameters:

- covered bases \(B_{cov}\)
- coverage \(C\)
- candidate density at reference coverage \(d_{ref}\)
- reference coverage \(C_{ref}\)
- aligned bytes per covered base \(b_{aln}\)
- example shape \([h,w,c]\)
- element bytes \(b_{elem}\)
- call output bytes per example \(b_{call}\)
- postprocess output bytes per example \(b_{post}\)

Number of examples:

\[
N_{ex} = round\left(B_{cov} \cdot d_{ref} \cdot \frac{C}{C_{ref}}\right)
\]

Boundary bytes at \(1x\):

\[
X_0 = round(B_{cov} \cdot C \cdot b_{aln})
\]
\[
X_1 = N_{ex} \cdot h \cdot w \cdot c \cdot b_{elem}
\]
\[
X_2 = N_{ex} \cdot b_{call}
\]
\[
X_3 = N_{ex} \cdot b_{post}
\]

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

## Streaming admission

Only a bounded number of tiles are admitted at once:

\[
K_{active,0} = \min(K, max\_inflight\_tiles)
\]

When tile \(k\) completes its final operation at \(t_{done,k}\), one new tile is admitted at:

\[
t_{admit,next} = t_{done,k}
\]

## Compute duration

For stage \(s\), tile \(k\), compute rate \(R_s\) (bytes/s):

\[
T_{compute}(s,k) = \frac{X_{s-1,k}}{R_s}
\]

### CPU calibration from runtime shares

Given CPU reference runtime \(T_{cpu,ref}\) and stage share \(q_s\):

\[
T_{cpu,stage}(s) = T_{cpu,ref} \cdot q_s
\]

With \(U_{cpu}\) CPU units per stage and stage input bytes \(X_{s-1}\), per-unit CPU rate:

\[
R_{cpu}(s) = \frac{X_{s-1}}{U_{cpu} \cdot T_{cpu,stage}(s)}
\]

Given configured PIM speedup \(a_s\) for stage \(s\):

\[
R_{pim}(s) = R_{cpu}(s) \cdot a_s
\]

`stage_overrides` can explicitly replace any per-stage unit/rate/power value and takes precedence.

## Transfer duration

Host-link transfer (PCIe style fixed cost):

\[
T_{host}(B) = T_{fixed,host} + \frac{B}{BW_{host}}
\]

Direct CXL transfer:

\[
T_{cxl}(B) = L_{cxl} + \frac{B}{BW_{cxl}}
\]

Host-touch operation for bounced intermediates:

\[
T_{touch}(B) = T_{touch,fixed} + \frac{B}{BW_{touch}}
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

Transitions are selected from scenario stage-device maps.

For adjacent stages \(s \to s+1\):

- `cpu -> cpu`: no transfer op
- `cpu -> pim`: \(H2D\_stage\)
- `pim -> cpu`: \(D2H\)
- `pim -> pim` in `pim_host_bounce`: \(D2H \to HOST\_TOUCH \to H2D\_stage\)
- `pim -> pim` in `pim_flowcxl_direct`: \(CXL\_direct\)

Ingress \(H2D\_ingress\) appears only when stage 1 device is `pim`.
Final egress \(D2H\) appears only when last stage device is `pim`.

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

## Bottleneck lower-bound diagnostics

For any resource pool \(r\) with capacity \(C_r\) and busy time \(busy\_time_r\):

\[
LB_r = \frac{busy\_time_r}{C_r}
\]

Reported aggregate lower bounds:

\[
LB_{compute\_stage\_max} = \max_{r \in compute\_pools}(LB_r)
\]
\[
LB_{host\_h2d\_ingress} = LB_{pool(host\_h2d\_ingress)}
\]
\[
LB_{host\_h2d\_stage} = LB_{pool(host\_h2d\_stage)}
\]
\[
LB_{host\_d2h} = LB_{pool(host\_d2h)}
\]
\[
LB_{host\_link} = \max(LB_{host\_h2d\_ingress}, LB_{host\_h2d\_stage}, LB_{host\_d2h})
\]
\[
LB_{host\_touch} = LB_{pool(host\_touch)}
\]
\[
LB_{cxl\_direct} = LB_{pool(cxl\_direct)}
\]

`dominant_lb_component` is the component with largest lower bound in a run.
