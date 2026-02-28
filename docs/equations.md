# Equations

This document is the executable model math contract for boundary derivation, compute/memory service, transfer timing, and energy.

## 1) Boundary Derivation

### DeepVariant (`deepvariant_3stage`, public 3-stage, internal 5-kernel)

Public stages:

- `make_examples`
- `call_variants`
- `postprocess_variants`

Execution kernels:

- `make_examples_frontend`
- `make_examples_tensorize`
- `call_variants_infer`
- `call_variants_post`
- `postprocess_variants`

Given:

- covered bases `B_cov`
- coverage `C`
- candidate density `d_ref` at reference coverage `C_ref`
- aligned bytes/base `b_aln`
- example shape `[h,w,c]`
- example element bytes `b_elem`
- frontend bytes/example `b_front`
- tensor overhead factor `f_tensor`
- infer output bytes/example `b_infer`
- call-post output bytes/example `b_call_post`
- final postprocess output bytes/example `b_post`

\[
N_{ex}=round\left(B_{cov}\cdot d_{ref}\cdot\frac{C}{C_{ref}}\right)
\]

Execution boundaries (`X0..X5`):

\[
X_0=round(B_{cov}\cdot C\cdot b_{aln})
\]
\[
X_1=round(N_{ex}\cdot b_{front})
\]
\[
X_2=round(N_{ex}\cdot h\cdot w\cdot c\cdot b_{elem}\cdot f_{tensor})
\]
\[
X_3=round(N_{ex}\cdot b_{infer})
\]
\[
X_4=round(N_{ex}\cdot b_{call\_post})
\]
\[
X_5=round(N_{ex}\cdot b_{post})
\]

Kernel runtime-share split from public shares:

\[
q_{front}=q_{make}\cdot f_{make\_front}
\]
\[
q_{tensor}=q_{make}\cdot (1-f_{make\_front})
\]
\[
q_{infer}=q_{call}\cdot f_{call\_infer}
\]
\[
q_{call\_post}=q_{call}\cdot (1-f_{call\_infer})
\]
\[
q_{post}=q_{postprocess}
\]

### TPC-H (`tpch_3op`)

Given:

- scale factor `SF`
- base rows/SF `R_sf`
- scan selectivity `s_scan`
- join fanout `f_join`
- agg reduction `r_agg`
- row widths `b_scan_in,b_scan_out,b_join_out,b_agg_out`

\[
R_{scan\_in}=round(SF\cdot R_{sf})
\]
\[
R_{scan\_out}=max(1, round(R_{scan\_in}\cdot s_{scan}))
\]
\[
R_{join\_out}=max(1, round(R_{scan\_out}\cdot f_{join}))
\]
\[
R_{agg\_out}=max(1, round(R_{join\_out}\cdot r_{agg}))
\]

\[
X_0=R_{scan\_in}\cdot b_{scan\_in},\;
X_1=R_{scan\_out}\cdot b_{scan\_out},\;
X_2=R_{join\_out}\cdot b_{join\_out},\;
X_3=R_{agg\_out}\cdot b_{agg\_out}
\]

## 2) Scaling, Tiling, Admission

Stage multiplier `m` uses integer-conserving boundary scaling:

\[
\sum_i X'_i = round\left(m\cdot \sum_i X_i\right)
\]

\[
K=max\left(1,\left\lceil \frac{\max_i(X'_i)}{tile\_size}\right\rceil\right)
\]

Each boundary is exactly partitioned:

\[
X'_i=\sum_{k=1}^{K} X_{i,k}
\]

Streaming admission:

- initial admitted tiles: `min(K, max_inflight_tiles)`
- each final tile completion admits exactly one next tile.

Optional retiling model (`tiling_model_by_template.<template>.enabled=true`) replaces global `K`
with boundary-local tile domains:

\[
D_b = \{tile\_count_b,\;bytes_{b,t}\}
\]

Boundary mappings:

- `IDENTITY`: `producer(i) -> consumer(i)`
- `GROUP_K_TO_1`: `consumer(j)` waits on producers `j*K ... j*K+K-1`
- `SPLIT_1_TO_M`: producer `i` emits `M` split contributions
- `REPARTITION_HASH`: each producer contributes to a materialized shuffle barrier

Boundary mapping keys use stage transitions, not numeric indices:

- `"<src_stage>-><dst_stage>"` (for example `join->groupby_agg`)

`REPARTITION_HASH` v1 global materialized readiness:

\[
ready(boundary\ b)\iff received\_producers_b = total\_producers_b
\]

After boundary `b` is ready, consumer partition bytes are deterministically split:

\[
bytes_{b,\;consumer} = split\left(\sum_{producer} bytes_{producer}\cdot output\_amplification,\;N_{consumers}\right)
\]

Barrier wait decomposition for a consumer tile:

\[
T_{barrier,dep}=max(0,\;t_{latest\_contrib}-t_{first\_contrib})
\]
\[
T_{glue,queue}=max(0,\;t_{glue,start}-t_{latest\_contrib})
\]
\[
T_{barrier,total}=T_{barrier,dep}+T_{glue,queue}
\]

Compatibility alias:

\[
T_{barrier}=T_{barrier,total}
\]

Glue core cost:

\[
T_{glue}=glue\_fixed + max\left(\frac{bytes_{touched}}{glue\_compute\_Bps},\frac{bytes_{touched}}{glue\_mem\_Bps}\right)
\]

and optional glue transfer addend:

\[
T_{glue,total}=T_{glue}+T_{glue,transfer}
\]

## 3) Compute And Memory Service

Per stage `s`, tile `k`:

\[
T_{compute}(s,k)=\frac{X_{s-1,k}}{R_{device,s}}
\]

\[
bytes\_touched(s,k)=f_{amp,s}\cdot\left(f_{in,s}\cdot X_{s-1,k}+f_{out,s}\cdot X_{s,k}\right)
\]

Memory service (CPU/PIM first-class systems):

\[
miss=max(10^{-6},1-row\_hit)
\]
\[
BW_{lat}=\frac{MLP\cdot CL}{(latency\_ns\cdot 10^{-9})\cdot miss}
\]
\[
BW_{service}=
\begin{cases}
BW_{peak}, & access=sequential\_scan\\
min(BW_{peak}, BW_{lat}), & access\in\{hash\_probe,hash\_build,groupby\_update\}
\end{cases}
\]
\[
BW_{service,adj}=BW_{service}/penalty\_multiplier
\]

Offered load and queueing:

\[
BW_{offered}=\frac{bytes\_touched}{max(T_{compute},\epsilon)}
\]
\[
\rho=min\left(\rho_{cap},\frac{BW_{offered}}{BW_{service,adj}}\right)
\]
\[
Q=1+queue\_\alpha\cdot\frac{\rho}{1-\rho}
\]
\[
BW_{eff,stage}=BW_{service,adj}/Q
\]
\[
BW_{eff,unit}=BW_{eff,stage}/units
\]
\[
T_{mem}=\frac{bytes\_touched}{BW_{eff,unit}}
\]
\[
T_{stage}=max(T_{compute},T_{mem})
\]

PIM mode-dependent adjustment (`NONE`, `BANK`, `BANK_GROUP`, `BUFFER`) for PIM stages:

\[
R'_{pim,s}=R_{pim,s}\cdot m^{compute}_{mode}
\]
\[
BW'_{pim,s}=BW_{pim,s}\cdot m^{mem}_{mode}
\]
\[
T'_{stage}=T_{stage}+overhead^{cmd}_{mode}
\]

When memory system is disabled for template: `T_stage = T_compute`.

## 4) Transfer Equations

Directional host links:

\[
T_{host\_h2d}(B)=lat_{h2d}+\frac{B}{BW_{h2d}}
\]
\[
T_{host\_d2h}(B)=lat_{d2h}+\frac{B}{BW_{d2h}}
\]

Host touch:

\[
T_{touch}(B)=touch\_fixed+\frac{B}{touch\_BW}
\]

CPU materialize:

\[
T_{mat}(B)=mat\_fixed+\frac{B}{mat\_BW}
\]

Retention handoff:

\[
T_{retain}=retain\_fixed+\frac{retain\_metadata\_bytes}{retain\_local\_BW}
\]

Ingressless rule:

- for configured scenarios, skip the first host->PIM transfer per tile (`host_h2d_ingress` or first `host_h2d_stage`).

## 5) CXL Direct Processor Sharing

Direct path uses symmetric processor-sharing with slot cap.

- total direct service bandwidth:
\[
BW_{total}=BW_{link}\cdot striping\_factor
\]
- active transfers at time `t`: `N(t)` with `N(t)\le slots`
- each active transfer instantaneous rate:
\[
r_i(t)=BW_{total}/N(t)
\]

Admission overhead (before data service):

\[
T_{issue}=dma\_issue\_fixed / u_{out}
\]
\[
u_{out}=min\left(1,\frac{dma\_outstanding\_per\_vc}{full\_bw\_outstanding\_threshold}\right)
\]

Data service advances continuously; overlapping transfers symmetrically slow all active flows.

## 6) Lower-Bound Diagnostics

\[
lb_{pool}=busy\_time\_s/capacity
\]

\[
lb_{compute\_stage\_max}=max(lb_{cpu/pim\_stage\_pools}, lb_{cpu\_materialize})
\]
\[
lb_{host\_link}=max(lb_{host\_h2d\_ingress}, lb_{host\_h2d\_stage}, lb_{host\_d2h})
\]

Dominant component:

\[
dominant=\arg\max\{lb_{compute\_stage\_max},lb_{host\_link},lb_{host\_touch},lb_{cxl\_direct}\}
\]

## 7) Energy

\[
E_{pool}=busy\_time\_s \cdot power\_W
\]

\[
E_{compute}=\sum E_{stage\_compute}+E_{cpu\_materialize}
\]
\[
E_{transfer}=E_{h2d\_ingress}+E_{h2d\_stage}+E_{d2h}+E_{cxl\_direct}+E_{host\_touch}
\]
\[
E_{total}=E_{compute}+E_{transfer}
\]

## 8) Calibration Fit And Residual Metrics

Measured rows are first aggregated by group:

\[
g=(path,\ payload,\ concurrency)
\]
\[
T^{measured}_{g}=Agg\left(\{time\_s\}_{g}\right),\quad Agg\in\{median,mean\}
\]
\[
p50_g=Q_{0.50}(\{time_s\}_g),\quad p95_g=Q_{0.95}(\{time_s\}_g),\quad p99_g=Q_{0.99}(\{time_s\}_g)
\]
\[
error_g = T^{measured}_g - T^{sim}_g
\]
\[
MAE = \frac{1}{|G|}\sum_g |error_g|
\]
\[
MAPE = \frac{100}{|G|}\sum_g \left|\frac{T^{measured}_g - T^{sim}_g}{max(T^{measured}_g,\epsilon)}\right|
\]

Per-path fit model (reference concurrency):

\[
T_{path}(bytes)=latency_{path}+\frac{bytes}{BW_{path}}
\]

with `latency_path` and `BW_path` fitted from measured points using linear regression over payload size.

Host-touch provenance:

\[
host\_touch\_source \in \{measured\_stream,\ derived\_from\_bounce\}
\]

If `host_touch_source=derived_from_bounce`:

\[
T_{touch,est}(bytes)=T_{bounce,measured}(bytes)-T_{d2h,fit}(bytes)-T_{h2d,fit}(bytes)
\]
\[
T_{touch,fit}(bytes)=host\_touch\_fixed\_s+\frac{bytes}{host\_touch\_Bps}
\]

Negative `T_touch,est` policy:

\[
T^{\prime}_{touch,est}=
\begin{cases}
\text{drop point}, & mode=drop \\
0, & mode=clamp\_to\_zero \\
\epsilon, & mode=clamp\_to\_epsilon
\end{cases}
\]

Direct-path provenance status:

\[
direct\_status \in \{measured,\ crosscheck\_only,\ cited\_sweep\_only\}
\]

For cited+sweep posture, the envelope is recorded as:
\[
latency_{ns}\in[214,394],\quad bandwidth_{GB/s}\in[18,52]
\]
\[
switch\_latency_{ns}\approx 600,\quad hop\_latency\_sweep,\ bottleneck\_factor\_sweep \text{ configured}
\]

Cross-check acceptance compares direct scheduler MAPE against configured tolerance.

## 9) PCIe Ceiling Sanity Check

One-way theoretical PCIe ceiling under configured generation/lane width:

\[
BW_{theoretical,oneway}=BW_{per\_lane}(gen)\cdot lanes
\]
\[
BW_{threshold}=BW_{theoretical,oneway}\cdot utilization\_cap
\]

Host-path fit is flagged if:

\[
BW_{fit,path} > BW_{threshold}
\]

This is a one-way sanity guard derived from a bidirectional table approximation (per-lane rate x lane width, then utilization cap), not a full throughput model.
