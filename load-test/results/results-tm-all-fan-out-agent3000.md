# Load Test Results — TM All — Fan-Out — Log Agent 3 000 — 2026-10-29

Shape F1, load A (10 000 traces/s; 9 000 metrics/s = 2 000 gateway + 7 000 metric agent; 6 000 logs/s = 3 000 gateway + 3 000 log agent total), 10m steady, tool image `loadtest:76ca1e15c10540d9d705c9e7c442221b4af0b77c`, Telemetry Manager `ghcr.io/louis-kauer/telemetry-manager:1.74.1-loadtest-mem` (1.74.1 with agent memory request 1536Mi, gateway memory request 1Gi and the log agent memory_limiter at 1s / 75 / 15) with VPA on, load node `n4-highcpu-32` (31.92 CPU, 61.7 GiB) shared by the gateway, both agents and all generators. Pipeline, processor and exporter counts include the two keep-alive pipelines (`LogPipeline/test-pipeline`, `MetricPipeline/test-pipeline`).

## Signal throughput (steady-state)

| Run | Tenants | Traces rx/s | GW metrics rx/s | GW logs rx/s | Log agent rx/s | Metric agent rx/s | Refused | Valid |
|-----|---------|-------------|-----------------|--------------|----------------|-------------------|---------|-------|
| 1   | 1       | 10 000      | 2 000           | 2 998        | 2 999          | 7 322             | 0       | ✓     |
| 2   | 3       | 9 999       | 2 000           | 2 997        | 2 999          | 7 325             | 0       | ✓     |
| 3   | 5       | 9 999       | 2 002           | 3 000        | 3 000          | 7 332             | 0       | ✓     |
| 4   | 10      | 9 997       | 2 004           | 3 000        | 2 999          | 7 346             | 0       | ✓     |
| 5   | 20      | 9 991       | 2 008           | 3 000        | 3 000          | 7 373             | 0       | ✓     |
| 6   | 30      | 9 985       | 1 991           | 3 000        | 2 999          | 7 390             | 0       | ✓     |
| 7   | 50      | 10 000      | 2 019           | 3 000        | 3 000          | 7 455             | 0       | ✓     |
| 8   | 100     | 9 993       | 2 036           | 2 993        | 2 849          | 7 595             | 0       | ✓     |

> Run 8: no limit hit, but node CPU peaked at 88.5 % (log agent 21.6 CPU avg, 26.5 p95), just below the ~90 % node saturation threshold.

## Collector resource usage

### Gateway

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 1       | 0.189   | 0.221   | 200           | 160          | 0        | No  | 2 048           |
| 2   | 3       | 0.238   | 0.285   | 211           | 186          | 0        | No  | 2 048           |
| 3   | 5       | 0.282   | 0.330   | 228           | 213          | 0        | No  | 2 048           |
| 4   | 10      | 0.381   | 0.440   | 265           | 229          | 0        | No  | 2 048           |
| 5   | 20      | 0.585   | 0.666   | 383           | 436          | 0        | No  | 2 048           |
| 6   | 30      | 0.798   | 0.945   | 499           | 568          | 0        | No  | 2 048           |
| 7   | 50      | 1.289   | 1.459   | 914           | 1 056        | 0        | No  | 2 048           |
| 8   | 100     | 2.422   | 2.769   | 1 278         | 1 532        | 0        | No  | 2 048           |

### Log agent

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 1       | 0.205   | 0.245   | 154           | 90           | 0        | No  | 3 072           |
| 2   | 3       | 0.511   | 0.627   | 188           | 134          | 0        | No  | 3 072           |
| 3   | 5       | 0.865   | 0.955   | 221           | 183          | 0        | No  | 3 072           |
| 4   | 10      | 1.725   | 2.095   | 310           | 295          | 0        | No  | 3 072           |
| 5   | 20      | 4.003   | 4.582   | 440           | 541          | 0        | No  | 3 072           |
| 6   | 30      | 6.151   | 6.692   | 616           | 760          | 0        | No  | 3 072           |
| 7   | 50      | 10.391  | 11.818  | 857           | 1 227        | 0        | No  | 3 072           |
| 8   | 100     | 21.643  | 26.505  | 1 692         | 2 289        | 0        | No  | 3 072           |

### Metric agent

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM | Mem limit (MiB) |
|-----|---------|---------|---------|---------------|--------------|----------|-----|-----------------|
| 1   | 1       | 0.047   | 0.058   | 612           | 601          | 0        | No  | 3 072           |
| 2   | 3       | 0.062   | 0.081   | 590           | 573          | 0        | No  | 3 072           |
| 3   | 5       | 0.073   | 0.095   | 628           | 659          | 0        | No  | 3 072           |
| 4   | 10      | 0.100   | 0.114   | 549           | 518          | 0        | No  | 3 072           |
| 5   | 20      | 0.163   | 0.195   | 575           | 525          | 0        | No  | 3 072           |
| 6   | 30      | 0.219   | 0.255   | 586           | 520          | 0        | No  | 3 072           |
| 7   | 50      | 0.355   | 0.430   | 636           | 572          | 0        | No  | 3 072           |
| 8   | 100     | 0.801   | 0.957   | 821           | 810          | 0        | No  | 3 072           |

## Config size

### Gateway

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 8         | 47                  | 5         | 17 324         |
| 2   | 3       | 14        | 95                  | 11        | 24 321         |
| 3   | 5       | 20        | 143                 | 17        | 31 323         |
| 4   | 10      | 35        | 263                 | 32        | 48 828         |
| 5   | 20      | 65        | 503                 | 62        | 83 838         |
| 6   | 30      | 95        | 743                 | 92        | 118 756        |
| 7   | 50      | 155       | 1 223               | 152       | 188 716        |
| 8   | 100     | 305       | 2 423               | 302       | 363 616        |

### Log agent

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 2         | 15                  | 2         | 16 689         |
| 2   | 3       | 4         | 31                  | 4         | 28 527         |
| 3   | 5       | 6         | 47                  | 6         | 40 365         |
| 4   | 10      | 11        | 87                  | 11        | 69 960         |
| 5   | 20      | 21        | 167                 | 21        | 129 150        |
| 6   | 30      | 31        | 247                 | 31        | 188 340        |
| 7   | 50      | 51        | 407                 | 51        | 306 720        |
| 8   | 100     | 101       | 807                 | 101       | 602 670        |

### Metric agent

| Run | Tenants | Pipelines | Processor instances | Exporters | Config (bytes) |
|-----|---------|-----------|---------------------|-----------|----------------|
| 1   | 1       | 5         | 29                  | 2         | 22 069         |
| 2   | 3       | 7         | 45                  | 4         | 25 645         |
| 3   | 5       | 9         | 61                  | 6         | 29 223         |
| 4   | 10      | 14        | 101                 | 11        | 38 168         |
| 5   | 20      | 24        | 181                 | 21        | 56 058         |
| 6   | 30      | 34        | 261                 | 31        | 73 917         |
| 7   | 50      | 54        | 421                 | 51        | 109 677        |
| 8   | 100     | 104       | 821                 | 101       | 199 077        |
