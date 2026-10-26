# Load Test Results — Transforms — Routing — 2026-10-23

Shape R1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total), 10m steady, collector `otel/opentelemetry-collector-contrib:0.161.0` with the TM gateway processing (`k8s_attributes`, transforms), collector node `n1-standard-4` (3.92 CPU, 13.5 GiB), load node `n4-highcpu-8`.

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 1       | 5 979     | 8 984        | 9 997       | 0       | ✓     |
| 2   | 10      | 5 999     | 8 993        | 9 999       | 0       | ✓     |
| 3   | 20      | 5 996     | 8 986        | 9 990       | 0       | ✓     |
| 4   | 50      | 5 999     | 8 998        | 10 000      | 0       | ✓     |
| 5   | 100     | 6 000     | 9 000        | 10 004      | 0       | ✓     |

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|---------|---------|---------|---------------|--------------|----------|-----|
| 1   | 1       | 0.537   | 0.646   | 271           | 124          | 0        | No  |
| 2   | 10      | 0.579   | 0.674   | 324           | 220          | 0        | No  |
| 3   | 20      | 0.620   | 0.747   | 384           | 288          | 0        | No  |
| 4   | 50      | 0.578   | 0.700   | 584           | 610          | 0        | No  |
| 5   | 100     | 0.635   | 0.765   | 972           | 1 114        | 0        | No  |

## Config size

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 6         | 18                  | 3         | 7 139          |
| 2   | 10      | 33        | 45                  | 30        | 24 452         |
| 3   | 20      | 63        | 75                  | 60        | 43 692         |
| 4   | 50      | 153       | 165                 | 150       | 101 262        |
| 5   | 100     | 303       | 315                 | 300       | 197 312        |
