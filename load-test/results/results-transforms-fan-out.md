# Load Test Results — Transforms — Fan-Out — 2026-10-23

Shape F1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total), 10m steady, collector `otel/opentelemetry-collector-contrib:0.161.0` with the TM gateway processing (`k8s_attributes`, transforms), collector node `n1-standard-4` (3.92 CPU, 13.5 GiB), load node `n4-highcpu-8`.

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 1       | 5 983     | 8 990        | 9 997       | 0       | ✓     |
| 2   | 10      | 5 986     | 8 965        | 9 998       | 0       | ✓     |
| 3   | 20      | 5 947     | 8 899        | 9 991       | 0       | ✓     |
| 4   | 50      | 5 709     | 8 582        | 10 000      | 0       | ✓     |
| 5   | 100     | 3 545     | 4 796        | 9 096       | 0       | ✓ ⚠   |

> Run 5: hard limit H4 hit on metrics and logs at t=120s, on traces at t=180s; soft limit S4 at t=720s. Throughput dropped to ~59% of offered for logs, ~53% for metrics, ~91% for traces. queue_util_max 1.0.

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|---------|---------|---------|---------------|--------------|----------|-----|
| 1   | 1       | 0.556   | 0.620   | 279           | 128          | 0        | No  |
| 2   | 10      | 0.921   | 1.129   | 370           | 251          | 0        | No  |
| 3   | 20      | 1.303   | 1.481   | 488           | 393          | 0        | No  |
| 4   | 50      | 2.468   | 2.969   | 922           | 905          | 0        | No  |
| 5   | 100     | 3.466   | 3.981   | 1 878         | 1 925        | 0        | No  |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 4         | 18                  | 3         | 6 858          |
| 2   | 10      | 31        | 144                 | 30        | 27 276         |
| 3   | 20      | 61        | 284                 | 60        | 49 966         |
| 4   | 50      | 151       | 704                 | 150       | 117 886        |
| 5   | 100     | 301       | 1 404               | 300       | 231 186        |