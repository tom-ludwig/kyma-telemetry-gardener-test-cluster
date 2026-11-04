# Load Test Results — TM Gateway — Persistent Queue — Outage — 2026-11-04

Shape F1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s offered, all to the gateway), 10 tenants, 2m warm-up, 10m steady, tool image `loadtest:6ced6c245098f99b15b349efbd9e46ceeee5cb37`, `-component=gateway`, `-health=outage`: every pipeline of every tenant exports to an endpoint that does not exist, so nothing leaves the gateway and every log item stays in its exporter queue. Load node `n4-highcpu-4` (3.92 CPU, 6.7 GiB, 39 GiB disk free) shared by the gateway and all generators.

Both Telemetry Manager images are 1.74.1 with the `1.74.1-loadtest-mem` memory settings plus a persistent sending queue for the gateway's log exporters (`file_storage` extension, `sizer: bytes`, directory `/var/tmp/telemetry-otlp-gateway/queue`). Trace and metric exporters keep the in-memory queue (256 batches split across pipelines, 25 each at 10 tenants).

| Run | Image | Volume at `/var/tmp` | Queue limit per log exporter | Volume limit |
|-----|-------|----------------------|------------------------------|--------------|
| 1   | `ghcr.io/louis-kauer/telemetry-manager:1.74.1-persistence-hostpath` | hostPath, node `/var/tmp` | 5 GiB | none (node disk) |
| 2   | `ghcr.io/louis-kauer/telemetry-manager:1.74.1-persistence-emptyDir` | emptyDir | 5 GiB ÷ 10 = 512 MiB | `sizeLimit: 5Gi` for the pod |

Both runs have the run ID `tm-f1-A-t010-outage-r1`; run 1 started at 16:09:53 UTC, run 2 at 16:33:03 UTC. Raw records: `raw-results-tm-gateway-persistent-queue-outage.txt`, in the same order.

## Log queue size per pipeline

Largest `otelcol_exporter_queue_size` of each log exporter over the steady window, in bytes (`queue_size_max_by_pipeline`).

| Pipeline | Run 1 hostPath | Run 2 emptyDir |
|----------|----------------|----------------|
| p-000    | 644 658 666    | 534 569 330    |
| p-001    | 639 366 216    | 536 686 310    |
| p-002    | 646 775 646    | 535 627 820    |
| p-003    | 640 424 706    | 531 393 860    |
| p-004    | 636 190 746    | 536 686 310    |
| p-005    | 640 424 706    | 533 510 840    |
| p-006    | 638 307 726    | 536 686 310    |
| p-007    | 642 541 686    | 536 686 310    |
| p-008    | 641 483 196    | 534 569 330    |
| p-009    | 644 658 666    | 534 569 330    |
| **Sum**  | **6.41 GB (5.97 GiB)** | **5.35 GB (4.98 GiB)** |
| Fill of the queue limit | 12 % | 99–100 % |

## What happened

**Run 1, hostPath.** Each pipeline's queue grew evenly to about 0.6 GiB, 12 % of its own 5 GiB limit. No limit was reached: the gateway was not evicted or restarted and refused nothing. The node's root filesystem grew from about 9.2 GiB to 15.1 GiB used during the run (Grafana, `/` used), the same 5.9–6 GiB the queues report, so the queue files on disk are about as large as their logical size. The files stay on the node after the run.

**Run 2, emptyDir.** All ten queues filled to their 512 MiB limit at about the same time. Their sum, 4.98 GiB, left only about 17 MiB below the 5Gi `sizeLimit`, and the files on disk need slightly more than their logical size. At 16:44:19 UTC (676 s after load start, 7m16s into steady state) kubelet evicted the gateway pod: `Usage of EmptyDir volume "queue" exceeds the limit "5Gi".` The emptyDir was deleted with the pod, so every pipeline's queued logs (about 5 GiB) were lost. The DaemonSet started a new gateway pod with empty queues 2 s later.

## Findings

- The `queue_size` limit applies to each pipeline's exporter on its own. With hostPath nothing caps the total: 10 pipelines can hold 10 × 5 GiB, bounded only by the node disk.
- The emptyDir `sizeLimit` caps the whole pod. When the pipelines' queues together use more disk than the limit, kubelet evicts the gateway and all pipelines lose their queued data at once, not only the pipeline that caused it.
- Splitting the volume evenly across pipelines (5 GiB ÷ N) is not enough: the queue files need a little more disk than their logical size, so the sum must stay below the `sizeLimit` with headroom.
- Trace and metric pipelines filled their in-memory queues (25 batches each) and dropped data in both runs (`enqueue_failed`); they use no disk.

## Signal throughput (steady-state)

| Run | Volume   | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Evicted | Valid |
|-----|----------|-----------|--------------|-------------|---------|---------|-------|
| 1   | hostPath | 4 364     | 6 482        | 8 347       | 0       | No      | ✓ ⚠   |
| 2   | emptyDir | ≈ 3 990 ¹ | 14 260 ²     | 17 838 ²    | 0       | Yes ³   | ✓ ⚠   |

> Both runs: hard limits H3 (send failures) and H4 (exported below offered) on all signals, as intended in outage mode. The `n4-highcpu-4` node delivered only 60–90 % of the offered load.

> ¹ From the evicted pod's receiver counter (2 623 300 log records at 16:44:00, 657 s after load start). The tool reported 9 513/s.

> ² The tool sums the rate of the evicted pod and its replacement over the window, so the received rates and the CPU average of run 2 are too high.

> ³ From the Kubernetes event. The tool reported `evicted: false`: the DaemonSet replaced the evicted pod before the tool's pod-status check saw it.

## Collector resource usage

| Run | Volume   | CPU avg  | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|----------|----------|---------|---------------|--------------|----------|-----|
| 1   | hostPath | 0.631    | 0.849   | 1 862         | 1 395        | 0        | No  |
| 2   | emptyDir | 1.280 ²  | 0.861   | 1 797         | 1 536        | 0        | No  |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 10      | 33        | 248                 | 30        | 46 432         |
| 2   | 10      | 33        | 248                 | 30        | 46 422         |