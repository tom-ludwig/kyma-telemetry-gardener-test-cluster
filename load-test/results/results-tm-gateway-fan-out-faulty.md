# Load Test Results — TM Gateway — Fan-Out — Faulty — 2026-10-30

Shape F1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total, all to the gateway), 10m steady, tool image `loadtest:76ca1e15c10540d9d705c9e7c442221b4af0b77c`, Telemetry Manager `ghcr.io/louis-kauer/telemetry-manager:1.74.1-loadtest-mem` (1.74.1 with agent memory request 1536Mi, gateway memory request 1Gi and the log agent memory_limiter at 1s / 75 / 15) with VPA on, `-component=gateway`, load node `n4-highcpu-32` (31.92 CPU, 61.7 GiB) shared by the gateway, both agents and all generators. Faulty: pipeline `p-000` of each signal exports to the fault backend (`503:70`, delays `503:1000,200:1000`). Pipeline, processor and exporter counts include the two keep-alive pipelines (`LogPipeline/test-pipeline`, `MetricPipeline/test-pipeline`).

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 2       | 5 996     | 9 000        | 10 000      | 0       | ✓ ⚠   |
| 2   | 3       | 5 996     | 9 000        | 9 998       | 0       | ✓ ⚠   |
| 3   | 5       | 5 998     | 8 999        | 10 000      | 0       | ✓ ⚠   |
| 4   | 10      | 6 000     | 9 003        | 10 000      | 0       | ✓ ⚠   |
| 5   | 20      | 5 995     | 9 000        | 9 999       | 0       | ✓ ⚠   |
| 6   | 30      | 5 992     | 9 005        | 9 989       | 0       | ✓ ⚠   |
| 7   | 50      | 6 000     | 9 017        | 10 000      | 0       | ✓ ⚠   |
| 8   | 100     | 6 000     | 9 034        | 10 001      | 0       | ✓ ⚠   |

> All runs: the tool reports hard limit H3 and soft limit S2 on all three signals at t=120s. For `-target=tm` the tool does not split healthy from faulty exporters (`SelectorsFor` in `internal/result/result.go`), so the enqueue failures and full queue of the faulty `p-000` exporter count as healthy. vm-single shows enqueue failures only on the `p-000` exporters and none on the healthy ones (see Isolation), so these are not collector limits and the runs were not repeated.

> Run 8: the gateway config read by the tool once all pipelines reported ConfigurationGenerated had 294 pipelines and 291 exporters instead of 303 and 300; the remaining pipelines were added later, since healthy throughput reached 99.5–100 % over the steady window.

## Isolation

Healthy = all gateway exporters except `p-000`, read from vm-single over the steady window (the tool has no faulty split for TM). Healthy delivered = healthy exported/s ÷ (rx/s × (N-1)/N). Send failed, enqueue failed and queue util are for the healthy exporters only.

| Run | Tenants | Healthy exported/s (logs · metrics · traces) | Healthy delivered (logs · metrics · traces) | Faulty p-000 exported/s (logs · metrics · traces) | Send failed | Enqueue failed | Queue util max |
|-----|---------|----------------------------------------------|---------------------------------------------|---------------------------------------------------|-------------|----------------|----------------|
| 1   | 2       | 2 999 · 4 500 · 5 000                        | 100 · 100 · 100 %                           | 134 · 148 · 159                                   | 0           | 0              | 0              |
| 2   | 3       | 3 997 · 5 998 · 6 666                        | 100 · 100 · 100 %                           | 151 · 154 · 153                                   | 0           | 0              | 0              |
| 3   | 5       | 4 799 · 7 199 · 8 001                        | 100 · 100 · 100 %                           | 141 · 153 · 160                                   | 0           | 0              | 0              |
| 4   | 10      | 5 407 · 8 098 · 9 001                        | 100 · 99.9 · 100 %                          | 172 · 154 · 139                                   | 0           | 0              | 0              |
| 5   | 20      | 5 693 · 8 543 · 9 500                        | 100 · 99.9 · 100 %                          | 167 · 144 · 178                                   | 0           | 0              | 0              |
| 6   | 30      | 5 791 · 8 694 · 9 661                        | 100 · 99.9 · 100 %                          | 99 · 100 · 137                                    | 0           | 0              | 0              |
| 7   | 50      | 5 889 · 8 821 · 9 784                        | 100 · 99.8 · 99.8 %                         | 77 · 70 · 55                                      | 0           | 0              | 0              |
| 8   | 100     | 5 908 · 8 909 · 9 968                        | 99.5 · 99.6 · 100 %                         | 32 · 23 · 24                                      | 0           | 0              | 0              |

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 2       | 0.279   | 0.317   | 553           | 559          | 0        | No  | 2 048           |
| 2   | 3       | 0.313   | 0.383   | 497           | 500          | 0        | No  | 2 048           |
| 3   | 5       | 0.395   | 0.463   | 421           | 410          | 0        | No  | 2 048           |
| 4   | 10      | 0.557   | 0.625   | 385           | 399          | 0        | No  | 2 048           |
| 5   | 20      | 0.854   | 1.009   | 427           | 400          | 0        | No  | 2 048           |
| 6   | 30      | 1.358   | 1.525   | 517           | 529          | 0        | No  | 2 048           |
| 7   | 50      | 1.911   | 2.233   | 862           | 1 021        | 0        | No  | 2 048           |
| 8   | 100     | 3.431   | 4.090   | 1 273         | 1 634        | 0        | No  | 2 048           |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 2       | 11        | 71                  | 8         | 20 630         |
| 2   | 3       | 14        | 95                  | 11        | 24 033         |
| 3   | 5       | 20        | 143                 | 17        | 30 843         |
| 4   | 10      | 35        | 263                 | 32        | 47 868         |
| 5   | 20      | 64        | 492                 | 61        | 80 683         |
| 6   | 30      | 94        | 740                 | 91        | 114 937        |
| 7   | 50      | 153       | 1 209               | 150       | 181 743        |
| 8   | 100     | 294       | 2 350               | 291       | 342 212        |
