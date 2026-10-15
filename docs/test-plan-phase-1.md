# Phase 1: Raw OTel Collector Pipeline Scalability Test

**Goal:** check that a plain upstream OTel Collector (`otelcol-contrib`, hand-written config, no telemetry-manager) can handle **1, 3, 5, 10, 20, 50, and 100 pipelines/exporters per signal**, and find out early whether **naive fan-out** works or whether we need **routing**.
Legend: **[V]** verified (docs or code, Oct 2026), **[P]** proposed value, **[A]** assumption or unverified.

## 1. Facts that shape the test

| Fact                                                                                                                                                                                                                                                                                                                                                        | Impact                                                                                                                |
| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| A receiver shared by N pipelines sends every item into all N pipelines, and does so synchronously. Each pipeline gets its own processor instances, so one blocked pipeline blocks the receiver for all of them. [V, OTel architecture docs]                                                                                                                 | Fan-out costs about N× the processing work. A single faulty exporter can stall every pipeline, so test 1-of-N faulty. |
| Every exporter brings its own `sending_queue` (default `num_consumers: 10`, `queue_size: 1000` requests), its own retry loop (5 s initial, 30 s max interval, 300 s max elapsed), and its own HTTP or gRPC client. [V, exporterhelper README]                                                                                                               | This per-exporter cost is the thing under test. It is the same whatever the target endpoint is.                       |
| Today, telemetry-manager uses one shared OTLP receiver fanned out to N full pipelines. On the gateway, each traces or logs pipeline runs its own `memory_limiter` instance, `k8sattributes`, transforms, `batch` (512 items, 10 s), and exporter queue (`256/N` batches). Metric pipelines already share enrichment through connectors. [V, repo `6b45ca0`] | The fan-out variant mirrors this shape. The routing variant mirrors the metrics-gateway pattern.                      |
| The `routing` **connector** (contrib, alpha) routes on OTTL conditions such as `resource.attributes["k8s.namespace.name"] == "ns-7"`. It evaluates its `table` in order (`action: move` by default).                                                                                                                                                        | Use the connector. The cost of matching grows with the number of table entries, so measure it at N = 100.             |

## Backend

**Use one backend instance per signal (VictoriaMetrics, VictoriaLogs, VictoriaTraces single-node), with N distinct exporters.** The collector-side cost (queue, consumers, client, batching, retries) is created per exporter, whatever the target. One backend is enough as long as both of these hold:

1. **Each pipeline has its own exporter**: distinct component IDs (`otlphttp/p-000` … `otlphttp/p-099`) and **distinct endpoints**.
2. **The backend is verified not to be the bottleneck**

Gotchas:

- **Faulting a single exporter:** backpressure tests need one specific exporter to fail. With distinct Services this is simple: point `p-000`'s Service at a fault backend. The repo's `dependencies/fault-backend` speaks OTLP HTTP and gRPC and injects 503, 429, or delay [V]. An Istio `VirtualService` fault per host also works.
- **Per-pipeline attribution in the backend:**
  - VictoriaLogs: `headers: {VL-Extra-Fields: pipeline=p-007}` [V].
  - VictoriaTraces: `VT-Extra-Fields` [V].
  - VictoriaMetrics: `?extra_label=pipeline=p-007` on the endpoint URL. The parameter exists [V], but OTLP-path support is unverified [A].
  - Fallback: a per-pipeline `resource` processor that sets `pipeline.id`. Note this adds one processor per pipeline.

## 3. Test shapes and matrix

| ID                                        | Shape (per signal)                                                                                                                                                                                                                                 | What it answers                             |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------- |
| **F1 – fan-out (telemetry-manager-like)** | `otlp` receiver shared by N pipelines, each pipeline = `memory_limiter` → `filter` (keep own namespace) → `batch` → `otlphttp/p-i` (`sending_queue.queue_size = 256/N` to mirror telemetry-manager). `k8sattributes` optional, off by default [P]. | Is today's design viable at N = 100?        |
| **R1 – routing**                          | `in` pipeline: `otlp` → `memory_limiter` → `batch` → `routing` connector (N entries on `resource.attributes["k8s.namespace.name"]`, `action: move`). N output pipelines: `routing` → `otlphttp/p-i`.                                               | Does routing remove the N× processing cost? |

| Dimension | Values                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| N         | 1, 3, 5, 10, 20, 50, 100                                                                                                        |
| Health    | healthy; **1-of-N faulty** (`p-000` → fault-backend, 70% 503 + 1 s delay) at N = 5, 20, 100;                                    |
| Signals   | traces, metrics, logs (one signal per run)                                                                                      |
| Load      | **A (primary):** constant total `R` split across N namespaces. **B:** constant `R/5` per pipeline, stop at the first hard limit |

Generators set `k8s.namespace.name` directly, so no `k8sattributes` overwrite risk. With telemetrygen, use `--otlp-attributes k8s.namespace.name="ns-i"` and one small Deployment per namespace (one attribute value per process [A]). Rates [P]: traces 12,000 spans/s, logs 8,000 logs/s × 2 KB, metrics ≈ 6,000 points/s. These are about 50% of the single-pipeline throughput telemetry-manager measured.

## 4. Metrics per run (all averaged or maxed over the steady-state window `W`)

| Metric                                  | Name / query                                                                                                       | Aggregation          |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | -------------------- |
| Received                                | `sum(rate(otelcol_receiver_accepted_<T>_total[W]))`                                                                | avg                  |
| Refused (memory limiter / backpressure) | `sum(increase(otelcol_receiver_refused_<T>_total[W]))`                                                             | total                |
| Exported total and per pipeline         | `sum by (exporter)(rate(otelcol_exporter_sent_<T>_total[W]))`                                                      | sum, min, max        |
| Enqueue failed (queue full)             | `otelcol_exporter_enqueue_failed_<T>_total`                                                                        | increase             |
| Send failed (after retries)             | `otelcol_exporter_send_failed_<T>_total`                                                                           | increase             |
| Queue utilization                       | `max(otelcol_exporter_queue_size / otelcol_exporter_queue_capacity)`                                               | avg, max, time > 0.8 |
| Collector RSS / heap                    | `otelcol_process_memory_rss_bytes` [V]; `otelcol_process_runtime_heap_alloc_bytes` [A]                             | avg, max             |
| Pod memory                              | `container_memory_working_set_bytes{container="otelcol"}` (cAdvisor)                                               | max                  |
| Pod CPU                                 | `rate(container_cpu_usage_seconds_total{container="otelcol"}[W])`                                                  | avg, p95 of 1m rates |
| Restarts / OOM                          | `kube_pod_container_status_restarts_total`, `kube_pod_container_status_last_terminated_reason{reason="OOMKilled"}` | any                  |
| Startup                                 | Pod Ready time minus container start time                                                                          | s                    |
| Backend sanity                          | backend CPU; backend ingest rate vs. exported                                                                      | max, ratio           |

`<T>` = `spans` | `metric_points` | `log_records`. Collector telemetry runs at `service.telemetry.metrics.level: detailed`.

## 5. Run protocol

- **Cluster:** 2 × n1-standard-4 (same as the existing benchmarks). One collector replica pinned to node A, `requests = limits` with 4Gi memory and 2 CPU [P]; generators and backends run on node B.
- **Phases:** 5 min warm-up (not measured), **20 min steady state** (measured), 3 min cool-down (record queue drain time).
- **Repetitions:** one run per data point; 3 runs at N = 5 and at the first N that hits a limit.
- **Hygiene:** fresh collector, backend and generator pods per run, Prometheus queries pinned to the window (`@ end`).

## 6. Limit-reached criteria (ordered; record the first one triggered and when)

| #   | Class | ID                   | Condition                                                                                                                       |
| --- | ----- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Hard  | H1 OOM               | any `OOMKilled`                                                                                                                 |
| 2   | Hard  | H2 Restart / startup | any restart, or config load to Ready > 60 s [P]                                                                                 |
| 3   | Hard  | H3 Data loss         | healthy pipelines: `rate(enqueue_failed[5m]) > 0` or `rate(send_failed[5m]) > 0` for ≥ 5 min, or backend count < 99% of offered |
| 4   | Hard  | H4 Throughput        | Load A: exported < 95% of offered for ≥ 5 min                                                                                   |
| 5   | Soft  | S1 Throttling        | `rate(otelcol_receiver_refused_<T>_total[5m]) > 0` for ≥ 5 min                                                                  |
| 6   | Soft  | S2 Queue             | `queue_size / queue_capacity > 0.8` for ≥ 5 min on a **healthy** exporter                                                       |
| 7   | Soft  | S3 Memory            | working set > 80% of limit for ≥ 5 min                                                                                          |
| 8   | Soft  | S4 CPU               | p95 CPU ≥ 90% of the CPU limit                                                                                                  |
| 9   | Soft  | S5 Isolation         | 1-of-N faulty: healthy pipelines' exported rate drops > 5% vs. the healthy run                                                  |

In faulty runs, S1, S2, and H3 on the faulty exporter are expected. Judge H1, H2, H4, and S5.

## 7. Result record (one per run)

```yaml
run: {id: f1-logs-A-n050-healthy-r1, date: , collector_image: otelcol-contrib:<ver>, backend: victorialogs:<ver>}
setup: {shape: F1, signal: logs, load: A, pipelines: 50, health: healthy, offered_per_s: 8000, item_bytes: 2000,
        cpu_limit: 2, mem_limit_mib: 4096, queue_size_per_exporter: 5}
config: {bytes: , service_pipelines: , processor_instances: , exporters: , startup_s: }
results: {received_per_s: , exported_per_s: , per_pipeline_min_max: [ , ], refused: , enqueue_failed: , send_failed: ,
          queue_util_max: , queue_drain_s: , rss_max_mib: , cpu_avg: , cpu_p95: , restarts: , oom: ,
          backend_cpu_max: , backend_ingest_ratio: }
limit: {first: , at_s: , all: []}
valid: true   # false if backend not verified as non-bottleneck
```

## 8. Out of scope here (phase 2, only if phase 1 looks good)

- telemetry-manager in Kyma: generated config size (Log Agent reaches about 562 KB at N = 100, while the ConfigMap limit is 1 MiB), the 5-pipeline cap (`--unlimited-pipelines`), reconcile and rollout time, and self-monitor load.
- Product-specific buffers: gateway `256/N` queue split, Log Agent 200 MB per-pipeline in-memory queue, gateway memory-limit formula and cap, VPA.
- Agents (filelog receiver per pipeline, Metric Agent), and gRPC exporter parity.
