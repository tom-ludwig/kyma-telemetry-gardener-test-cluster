# Load Test Results — Transforms — Fan-Out — Faulty — 2026-10-23

Shape F1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total), 10m steady, collector `otel/opentelemetry-collector-contrib:0.161.0` with the TM gateway processing, collector node `n1-standard-4` (3.92 CPU, 13.5 GiB), load node `n4-highcpu-8`. Faulty: pipeline `p-000` of each signal exports to the fault backend (`503:70`, delays `503:1000,200:1000`).

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 2       | 5 965     | 8 974        | 9 993       | 0       | ✓     |
| 2   | 20      | 5 942     | 8 885        | 9 978       | 0       | ✓     |
| 3   | 50      | 5 523     | 8 327        | 10 000      | 0       | ✓ ⚠   |

> Run 3: hard limit H4 hit on metrics and logs at t=120s (healthy pipelines exported < 95 % of the offered healthy share). Same limit level as the healthy 50-tenant run, whose logs and metrics also fell ~5 % short.

## Isolation

Healthy delivered = `exported_healthy_per_s` ÷ (rx/s × (N-1)/N). Vs healthy run = `exported_healthy_per_s` ÷ (healthy run's `exported_per_s` × (N-1)/N); no healthy run at 2 tenants.

| Run | Tenants | Healthy exported/s (logs · metrics · traces) | Healthy delivered (logs · metrics · traces) | Vs healthy run (logs · metrics · traces) | Faulty p-000 exported/s (logs · metrics · traces) | Send failed | Enqueue failed | Queue util max |
|-----|---------|----------------------------------------------|---------------------------------------------|------------------------------------------|---------------------------------------------------|-------------|----------------|----------------|
| 1   | 2       | 2 982 · 4 487 · 4 996                        | 100 · 100 · 100 %                           | —                                        | 165 · 143 · 158                                   | 0           | 0              | 0.008          |
| 2   | 20      | 5 650 · 8 431 · 9 485                        | 100 · 99.9 · 100 %                          | 99.9 · 99.7 · 100 %                      | 144 · 160 · 143                                   | 0           | 0              | 0.083          |
| 3   | 50      | 5 404 · 8 164 · 9 784                        | 99.8 · 100 · 99.8 %                         | 96.6 · 97.1 · 99.6 %                     | 62 · 47 · 62                                      | 0           | 0              | 0.2            |

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|---------|---------|---------|---------------|--------------|----------|-----|
| 1   | 2       | 0.558   | 0.630   | 724           | 562          | 0        | No  |
| 2   | 20      | 1.357   | 1.624   | 540           | 433          | 0        | No  |
| 3   | 50      | 2.675   | 3.230   | 977           | 1 061        | 0        | No  |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 2       | 7         | 32                  | 6         | 9 130          |
| 2   | 20      | 61        | 284                 | 60        | 49 966         |
| 3   | 50      | 151       | 704                 | 150       | 117 886        |
