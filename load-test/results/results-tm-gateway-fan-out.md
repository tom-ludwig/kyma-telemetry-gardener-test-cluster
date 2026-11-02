# Load Test Results — TM Gateway — Fan-Out — 2026-10-26 to 2026-10-29

Shape F1, load A (6 000 logs/s, 9 000 metrics/s, 10 000 traces/s total, all to the gateway), healthy, Telemetry Manager Helm chart 1.74.1 with the pipeline limit lifted and VPA on, `-target=tm`. Two setups, as used in `result.md` section 3:

- Runs 1-4 (1-10 tenants, 2026-10-26/27): tool commit `d46909f0`, warmup 2m, 10m steady, gateway on `n1-standard-4` (3.92 CPU, 13.5 GiB), load node `n4-highcpu-8`.
- Runs 5-8 (20-100 tenants, 2026-10-29): tool commit `7678af0f`, warmup 3m, 10m steady, gateway and generators pinned with `-node-selector` to an `n4-highcpu-16` load node, metric `forward` connector and exporter queue sized independently of the tenant count (instead of TM's `max(1, 256 / tenants)`).

Raw data: `raw-results-new-telemetry-manager.txt` (line 319: 1T, 323: 3T, 454: 5T, 3: 10T, 458: 20T, 464: 30T, 460: 50T, 462: 100T).

## Signal throughput (steady-state)

| Run | Tenants | Logs rx/s | Metrics rx/s | Traces rx/s | Refused | Valid |
|-----|---------|-----------|--------------|-------------|---------|-------|
| 1   | 1       | 5 994     | 8 992        | 9 992       | 0       | ✓     |
| 2   | 3       | 5 993     | 8 995        | 9 994       | 0       | ✓     |
| 3   | 5       | 5 996     | 8 997        | 9 994       | 0       | ✓     |
| 4   | 10      | 5 998     | 8 997        | 9 993       | 0       | ✓     |
| 5   | 20      | 5 991     | 8 994        | 9 997       | 0       | ✓     |
| 6   | 30      | 5 997     | 9 008        | 9 990       | 0       | ✓     |
| 7   | 50      | 6 000     | 9 012        | 10 000      | 0       | ✓     |
| 8   | 100     | 5 991     | 9 022        | 10 002      | 0       | ✓     |

> All runs: no limit hit, no enqueue or send failures, queue util max ≤ 0.04.

> Superseded runs in the raw file, not used here: the first 20T, 30T, 50T and 100T runs on the `n1-standard-4` setup (2026-10-26). 20T delivered 100 % (CPU 1.06, RSS 284 MiB); 30T, 50T and 100T hit H4 at t=120s with receiver refusals (30T: 1 872 logs/s, 2 801 metrics/s; 50T and 100T < 3 % delivered). A 100T run on 2026-10-28 with 15m warmup hit H3 on metrics at t=900s (CPU 3.72, RSS 1 392 MiB).

## Collector resource usage

| Run | Tenants | CPU avg | CPU p95 | RSS max (MiB) | WS max (MiB) | Restarts | OOM |
|-----|---------|---------|---------|---------------|--------------|----------|-----|
| 1   | 1       | 0.303   | 0.365   | 167           | 124          | 0        | No  |
| 2   | 3       | 0.379   | 0.462   | 186           | 133          | 0        | No  |
| 3   | 5       | 0.447   | 0.512   | 211           | 176          | 0        | No  |
| 4   | 10      | 0.607   | 0.716   | 248           | 213          | 0        | No  |
| 5   | 20      | 0.888   | 0.994   | 376           | 377          | 0        | No  |
| 6   | 30      | 1.131   | 1.235   | 466           | 504          | 0        | No  |
| 7   | 50      | 1.876   | 2.132   | 703           | 676          | 0        | No  |
| 8   | 100     | 4.408   | 5.103   | 795           | 721          | 0        | No  |

## Config size

The tool did not record the config for these runs (all zero). Processor instances below are taken from the gateway config of the faulty sweep (`results-tm-gateway-fan-out-faulty.md`), which has the same pipeline shape. Its 2, 3, 5 and 10 tenant configs fit 24 processor instances per tenant plus 23 (including the two keep-alive pipelines) exactly; its 20-100 tenant configs were read before all pipelines were reconciled, so the counts below use the formula.

| Run | Tenants | Processor instances |
|-----|---------|---------------------|
| 1   | 1       | 47                  |
| 2   | 3       | 95                  |
| 3   | 5       | 143                 |
| 4   | 10      | 263                 |
| 5   | 20      | 503                 |
| 6   | 30      | 743                 |
| 7   | 50      | 1 223               |
| 8   | 100     | 2 423               |
