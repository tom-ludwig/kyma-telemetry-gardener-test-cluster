# Load Test Results — Fan-Out — 2026-10-21

Shape F1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total), 10m steady, collector `otel/opentelemetry-collector-contrib:0.161.0`, collector node `n1-standard-4` (3.92 CPU, 13.5 GiB).

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 50      | 5 962     | 8 843        | 10 000      | 0       | ✓     |
| 2   | 100     | 5 526     | 8 372        | 10 007      | 0       | ✓ ⚠   |

> Run 2: hard limit hit on metrics at t=120s (H4). Logs/traces throughput degraded (~8% logs, ~7% metrics drop vs offered). queue_util_max 0.5 across all signals.

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|---------|---------|---------|---------------|--------------|----------|-----|
| 1   | 50      | 1.620   | 1.820   | 676           | 704          | 0        | No  |
| 2   | 100     | 2.826   | 3.354   | 1 066         | 1 164        | 0        | No  |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 50      | 150       | 450                 | 150       | 103 889        |
| 2   | 100     | 300       | 900                 | 300       | 206 789        |
