# Load Test Results — TM All — Fan-Out — 2026-10-28

Shape F1, load A (10 000 traces/s; 9 000 metrics/s = 2 000 gateway + 7 000 metric agent; 6 000 logs/s = 1 000 gateway + 5 000 log agent total), 10m steady, Telemetry Manager `ghcr.io/tom-ludwig/telemetry-manager/telemetry-manager:latest` with VPA on, load node `n4-highcpu-16` (15.92 CPU, 30 GiB) shared by the gateway, both agents and all generators. Pipeline, processor and exporter counts include the two keep-alive pipelines (`LogPipeline/test-pipeline`, `MetricPipeline/test-pipeline`).

## Signal throughput (steady-state)

| Run | Tenants | Traces rx/s | GW metrics rx/s | GW logs rx/s | Log agent rx/s | Metric agent rx/s | Refused | Valid |
|-----|---------|-------------|-----------------|--------------|----------------|-------------------|---------|-------|
| 1   | 1       | 9 996       | 2 000           | 999          | 4 993          | 7 411             | 0       | ✓     |
| 2   | 3       | 9 996       | 1 999           | 998          | 4 988          | 7 415             | 0       | ✓     |
| 3   | 5       | 9 988       | 2 001           | 1 000        | 4 993          | 7 424             | 0       | ✓     |
| 4   | 10      | 9 960       | 2 004           | 1 000        | 4 981          | 7 440             | 0       | ✓     |
| 5   | 20      | 9 968       | 2 008           | 1 000        | 4 992          | 7 472             | 0       | ✓     |

> 30 tenants: no clean run, sweep stopped. Attempt 1 (warmup 2m): hard limit H4 on log-agent/logs at t=120s, 1 102 of 5 000 logs/s received, nothing refused. Attempt 2 (warmup 5m): H4 on log-agent/logs at t=300s, 1 885 logs/s received, nothing refused. Log agent at 5.9 CPU avg, RSS 463 MiB of 473 MiB limit, node CPU max 68 %, no restarts or OOM.

## Collector resource usage

### Gateway

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 1       | 0.168   | 0.183   | 167           | 111          | 0        | No  | 976             |
| 2   | 3       | 0.211   | 0.250   | 188           | 157          | 0        | No  | 909             |
| 3   | 5       | 0.253   | 0.289   | 204           | 155          | 0        | No  | 909             |
| 4   | 10      | 0.348   | 0.396   | 283           | 255          | 0        | No  | 909             |
| 5   | 20      | 0.529   | 0.658   | 370           | 369          | 0        | No  | 845             |

### Log agent

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 1       | 0.355   | 0.423   | 156           | 102          | 0        | No  | 473             |
| 2   | 3       | 0.966   | 1.193   | 181           | 111          | 0        | No  | 473             |
| 3   | 5       | 1.681   | 1.996   | 200           | 179          | 0        | No  | 312             |
| 4   | 10      | 3.532   | 4.109   | 292           | 341          | 0        | No  | 430             |
| 5   | 20      | 7.152   | 8.737   | 426           | 394          | 0        | No  | 430             |

### Metric agent

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 1       | 0.050   | 0.057   | 597           | 551          | 0        | No  | 845             |
| 2   | 3       | 0.069   | 0.088   | 604           | 566          | 0        | No  | 845             |
| 3   | 5       | 0.085   | 0.097   | 602           | 607          | 0        | No  | 909             |
| 4   | 10      | 0.124   | 0.151   | 583           | 532          | 0        | No  | 1 121           |
| 5   | 20      | 0.193   | 0.216   | 538           | 503          | 0        | No  | 1 121           |

## Config size

### Gateway

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 8         | 47                  | 5         | 17 359         |
| 2   | 3       | 14        | 95                  | 11        | 24 373         |
| 3   | 5       | 20        | 143                 | 17        | 31 387         |
| 4   | 10      | 35        | 263                 | 32        | 48 922         |
| 5   | 20      | 65        | 503                 | 62        | 83 992         |

### Log agent

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 2         | 15                  | 2         | 16 704         |
| 2   | 3       | 4         | 31                  | 4         | 28 542         |
| 3   | 5       | 6         | 47                  | 6         | 40 380         |
| 4   | 10      | 11        | 87                  | 11        | 69 975         |
| 5   | 20      | 21        | 167                 | 21        | 129 165        |

### Metric agent

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 5         | 29                  | 2         | 22 116         |
| 2   | 3       | 7         | 45                  | 4         | 25 698         |
| 3   | 5       | 9         | 61                  | 6         | 29 280         |
| 4   | 10      | 14        | 101                 | 11        | 38 235         |
| 5   | 20      | 24        | 181                 | 21        | 56 145         |
