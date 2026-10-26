# Load Test Results — Transforms — Routing — Faulty — 2026-10-23

Shape R1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total), 10m steady, collector `otel/opentelemetry-collector-contrib:0.161.0` with the TM gateway processing, collector node `n1-standard-4` (3.92 CPU, 13.5 GiB), load node `n4-highcpu-8`. Faulty: pipeline `p-000` of each signal exports to the fault backend (`503:70`, delays `503:1000,200:1000`).

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 2       | 5 975     | 8 985        | 9 998       | 0       | ✓     |
| 2   | 20      | 5 997     | 8 989        | 10 000      | 0       | ✓     |
| 3   | 50      | 5 997     | 8 997        | 10 000      | 0       | ✓     |

## Isolation

Healthy delivered = `exported_healthy_per_s` ÷ (rx/s × (N-1)/N). Vs healthy run = `exported_healthy_per_s` ÷ (healthy run's `exported_per_s` × (N-1)/N); no healthy run at 2 tenants.

| Run | Tenants | Healthy exported/s (logs · metrics · traces) | Healthy delivered (logs · metrics · traces) | Vs healthy run (logs · metrics · traces) | Faulty p-000 exported/s (logs · metrics · traces) | Send failed | Enqueue failed | Queue util max |
|-----|---------|----------------------------------------------|---------------------------------------------|------------------------------------------|---------------------------------------------------|-------------|----------------|----------------|
| 1   | 2       | 2 988 · 4 493 · 4 999                        | 100 · 100 · 100 %                           | —                                        | 167 · 149 · 150                                   | 0           | 0              | 0.008          |
| 2   | 20      | 5 693 · 8 544 · 9 495                        | 99.9 · 100 · 99.9 %                         | 99.9 · 100 · 100 %                       | 148 · 148 · 157                                   | 0           | 0              | 0.083          |
| 3   | 50      | 5 896 · 8 806 · 9 784                        | 100.3 · 99.9 · 99.8 %                       | 100 · 99.8 · 100 %                       | 55 · 58 · 68                                      | 0           | 0              | 0.2            |

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|---------|---------|---------|---------------|--------------|----------|-----|
| 1   | 2       | 0.503   | 0.568   | 721           | 553          | 0        | No  |
| 2   | 20      | 0.639   | 0.779   | 436           | 337          | 0        | No  |
| 3   | 50      | 0.612   | 0.689   | 614           | 606          | 0        | No  |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 2       | 9         | 21                  | 6         | 9 066          |
| 2   | 20      | 63        | 75                  | 60        | 43 692         |
| 3   | 50      | 153       | 165                 | 150       | 101 262        |
