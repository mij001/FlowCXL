# Equations

This model includes boundary derivation, stage compute calibration, tiled scheduling, transfer contention, makespan, and energy.

## 1) Boundary derivation by template

### DeepVariant (`deepvariant_3stage`)

Given:

- covered bases `B_cov`
- coverage `C`
- candidate density at reference coverage `d_ref`
- reference coverage `C_ref`
- aligned bytes per covered base `b_aln`
- example shape `[h, w, c]`
- example element bytes `b_elem`
- call output bytes/example `b_call`
- postprocess output bytes/example `b_post`

Examples:

\[
N_{ex} = round\left(B_{cov} \cdot d_{ref} \cdot \frac{C}{C_{ref}}\right)
\]

Boundaries (`X0..X3`):

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

### TPC-H OLAP (`tpch_3op`)

Given:

- scale factor `SF`
- base rows per SF `R_sf`
- scan input row bytes `b_scan_in`
- projected row bytes `b_scan_out`
- scan selectivity `s_scan`
- join fanout `f_join`
- join output row bytes `b_join_out`
- aggregation reduction ratio `r_agg`
- aggregation output row bytes `b_agg_out`

Row counts:

\[
R_{scan\_in} = round(SF \cdot R_{sf})
\]
\[
R_{scan\_out} = max(1, round(R_{scan\_in} \cdot s_{scan}))
\]
\[
R_{join\_out} = max(1, round(R_{scan\_out} \cdot f_{join}))
\]
\[
R_{agg\_out} = max(1, round(R_{join\_out} \cdot r_{agg}))
\]

Boundaries (`X0..X3`):

\[
X_0 = R_{scan\_in} \cdot b_{scan\_in}
\]
\[
X_1 = R_{scan\_out} \cdot b_{scan\_out}
\]
\[
X_2 = R_{join\_out} \cdot b_{join\_out}
\]
\[
X_3 = R_{agg\_out} \cdot b_{agg\_out}
\]

## 2) Stage-size scaling

For multiplier `m`, each boundary is scaled with integer conservation:

\[
\sum_i X'_i = round\left(m \cdot \sum_i X_i\right)
\]

## 3) Tiling

\[
K = max\left(1, \left\lceil \frac{\max_i(X'_i)}{tile\_size} \right\rceil \right)
\]

Each boundary is partitioned exactly:

\[
X'_i = \sum_{k=1}^{K} X_{i,k}
\]

## 4) Streaming admission

Initial active tiles:

\[
K_{active,0} = min(K, max\_inflight\_tiles)
\]

When tile `k` completes final op at `t_done,k`, admit one new tile at that time.

## 5) Compute calibration

### DeepVariant path

Given CPU reference total runtime `T_cpu_ref` and stage share `q_s`:

\[
T_{cpu,stage}(s) = T_{cpu,ref} \cdot q_s
\]

With stage input bytes `X_{s-1}` and CPU units `U_cpu`:

\[
R_{cpu}(s) = \frac{X_{s-1}}{U_{cpu} \cdot T_{cpu,stage}(s)}
\]

### TPC-H path

`R_cpu(s)` is taken directly from `cpu_stage_unit_compute_Bps_by_template[tpch_3op][s]`.

### PIM rate (all templates)

Given template stage speedup `a_s`:

\[
R_{pim}(s) = R_{cpu}(s) \cdot a_s
\]

Tile compute duration:

\[
T_{compute}(s,k) = \frac{X_{s-1,k}}{R_s}
\]

## 5b) Bytes touched and memory ceiling

For tile `k` at stage `s`:

- `bytes_in = X_{s-1,k}`
- `bytes_out = X_{s,k}`
- factors: `f_in`, `f_out`, `f_amp`

\[
bytes\_touched(s,k) = f_{amp}(s)\cdot\left(f_{in}(s)\cdot bytes_{in} + f_{out}(s)\cdot bytes_{out}\right)
\]

If memory ceiling is enabled for the template, stage memory bandwidth is shared across stage units:

\[
BW_{mem,unit}(s) = \frac{BW_{mem,stage}(s)}{U_s}
\]

\[
T_{mem}(s,k) = \frac{bytes\_touched(s,k)}{BW_{mem,unit}(s)}
\]

Final compute-op duration:

\[
T_{stage}(s,k) = max(T_{compute}(s,k), T_{mem}(s,k))
\]

If memory ceiling is disabled for the template:

\[
T_{stage}(s,k) = T_{compute}(s,k)
\]

## 6) Transfer duration

Host-link transfer:

\[
T_{host}(B) = T_{fixed,host} + \frac{B}{BW_{host}}
\]

Direct CXL transfer:

\[
T_{cxl}(B) = L_{cxl} + \frac{B}{BW_{cxl}}
\]

Host-touch:

\[
T_{touch}(B) = T_{touch,fixed} + \frac{B}{BW_{touch}}
\]

## 7) Transition-aware transfer graph

For stage transition `s -> s+1`:

- `cpu -> cpu`: no transfer op
- `cpu -> pim`: `host_h2d_stage`
- `pim -> cpu`: `host_d2h`
- `pim -> pim` in bounce: `host_d2h -> HOST_TOUCH -> host_h2d_stage`
- `pim -> pim` in direct: `cxl_direct`

Ingress `host_h2d_ingress` is added only when stage 1 is on PIM.
Egress `host_d2h` is added only when final stage is on PIM.

## 8) Resource-pool scheduling

Each op reserves one pool slot:

\[
t_{start} = max(t_{req}, t_{free,earliest})
\]
\[
t_{end} = t_{start} + duration
\]

## 9) Makespan

\[
T_{makespan} = \max_k(t_{end,k})
\]

## 10) Energy

Per resource `r`:

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

## 11) LB diagnostics

Pool lower bound:

\[
LB_r = \frac{busy\_time_r}{capacity_r}
\]

Reported:

- `lb_compute_stage_max_s = max(compute pool LBs)`
- `lb_host_h2d_ingress_s`
- `lb_host_h2d_stage_s`
- `lb_host_d2h_s`
- `lb_host_link_s = max(lb_host_h2d_ingress_s, lb_host_h2d_stage_s, lb_host_d2h_s)`
- `lb_host_touch_s`
- `lb_cxl_direct_s`

`dominant_lb_component` is the max among `{compute_stage_max, host_link, host_touch, cxl_direct}`.
