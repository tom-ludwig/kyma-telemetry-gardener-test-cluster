# Load-Test Tooling

Runs the phase 1 pipeline-scalability load test against a plain OpenTelemetry Collector (no telemetry-manager) as a Job in the cluster. Every run deploys one collector with one traces, one metrics and one logs pipeline per tenant, loads it with telemetrygen, measures it through the self-monitoring `vm-single`, and prints one result line.

## Prerequisites

- Node roles:
  - one node labelled `load-test/role=collector` and tainted `load-test/role=collector:NoSchedule` (only the collector runs there);
  - at least one node labelled `load-test/role=load` (Job, generators, fault backend).
- Deployed by Flux: backends in `load-test-backends`, namespace `otel-load-test`, ServiceAccount and RBAC `loadtest`.
- Image `ghcr.io/tom-ludwig/kyma-telemetry-gardener-test-cluster/loadtest:main`, built by `.github/workflows/loadtest-image.yaml` on every change under `load-test/tooling/`.

## Start a run

Set the args in [`job.yaml`](job.yaml), then:

```sh
kubectl create -f load-test/tooling/job.yaml
kubectl -n otel-load-test logs -f job/loadtest
```

The Job has a fixed name, so only one run can be active: a second `kubectl create` fails with "already exists" while the first Job exists. A finished Job is deleted after 5 minutes; its log stays in VictoriaLogs.

If `create` reports "already exists", check the Job first:

```sh
kubectl -n otel-load-test get job loadtest
```

Delete it (`kubectl -n otel-load-test delete job loadtest`) only when its status is `Complete` or `Failed`. Deleting a running Job stops its run.

Examples for `args`:

```yaml
args: [-matrix]                                   # whole matrix
args: [-matrix, -only=f1-A-*]                     # part of the matrix
args: [-shape=F1, -tenants=20, -load=A]           # one run
args: [-shape=R1, -tenants=50, -load=B, -rate=500]
args: [-shape=F1, -tenants=3, -warmup=1m, -steady=3m, -cooldown=1m]   # short dry run
```

| Flag                              | Default                                         | Meaning                                                                                                                                                                                            |
| --------------------------------- | ----------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-shape`                          | `F1`                                            | `F1`: every pipeline gets every item and filters its tenant. `R1`: one input pipeline routes each item to its tenant's pipeline.                                                                   |
| `-tenants`                        | `1`                                             | Tenants; each has 3 pipelines (traces, metrics, logs).                                                                                                                                             |
| `-load`                           | `A`                                             | `A`: fixed total per signal split across tenants. `B`: `-rate` per tenant.                                                                                                                         |
| `-rate`                           | `1000`                                          | Load B: items/s per signal per tenant.                                                                                                                                                             |
| `-health`                         | `healthy`                                       | `faulty`: pipeline `p-000` of each signal exports to the fault backend.                                                                                                                            |
| `-rep`                            | `1`                                             | Repetition number.                                                                                                                                                                                 |
| `-matrix`                         | off                                             | Binary search per shape × load (1, then 100 tenants, then halve), faulty runs at 5, 20 and 100 tenants, each after the healthy run of the same size, 3 repetitions at 5 tenants and at each limit. |
| `-only`                           |                                                 | Matrix: only run IDs matching this glob.                                                                                                                                                           |
| `-warmup`, `-steady`, `-cooldown` | `5m`, `20m`, `3m`                               | Run phases; only `-steady` is measured.                                                                                                                                                            |
| `-vm-url`                         | `http://vmsingle-vm-single.monitoring.svc:8428` | Self-monitoring vm-single.                                                                                                                                                                         |
| `-skip-preflight`                 | off                                             | Skip preflight cluster capacity, backend readiness, and disk checks.                                                                                                                               |

## Results

Each run prints one JSON line on stdout: `{"msg":"loadtest result","result":{…}}`. Progress messages go to stderr. Find the lines:

- in the Job log: `kubectl -n otel-load-test logs job/loadtest | grep '"loadtest result"'`
- in Grafana, VictoriaLogs datasource: `kubernetes.pod_namespace:otel-load-test "loadtest result"`

Run IDs: `<shape>-<load>-t<tenants>-<health>-r<rep>`, for example `f1-A-t050-healthy-r1` or `r1-B1000-t005-faulty-r1`. `result.run.steady_start` and `steady_end` are the measured window; collector series carry the label `loadtest_kyma_project_io_run=<run-id>`.

The record holds the run's inputs (`run.args`, `run.tool_commit`, phases, generator and fault backend settings, node sizes in `env`), config size, per-signal throughput and failures, collector memory and CPU, backend checks, and the limits hit (`H1`–`H4` hard, `S1`–`S5` soft).

## What stops a run

- **Preflight** (nothing is created): node roles missing, collector node not free, load nodes without room for the generators, a backend not ready, not enough backend or vm-single disk. Objects left over from an earlier, killed run are removed first.
- **Watchdog** (every 30 s, result gets `valid: false` and the reason): a backend restarts or runs below 5 % free disk, vm-single is unreachable, or the generators deliver less than 95 % of the planned rate while the collector refuses nothing and the load node runs at ≥ 90 % CPU.
- A collector OOM kill, eviction or restart does not stop the run; it is a result (H1/H2), read from the pod status.
- A shortfall with a non-saturated load node is a collector limit (H3/H4), not a broken run.

In `-matrix` mode any of these stops the whole matrix. A new Job starts the matrix from the beginning; use `-only` to skip finished runs.

## Layout

```
cmd/loadtest/main.go          flags, start
internal/
├── spec/                     run spec, tenants, rates, binary search
│   └── signals.yaml          per-signal settings: backend, port, OTLP path, load A rate, log body size
├── manifests/                Kubernetes objects and collector config per run
│   ├── collector-config.yaml static part of the collector config (from the telemetry-manager OTLP gateway)
│   ├── collector.yaml        collector ServiceAccount, Deployment, Services, VMPodScrape
│   ├── generator.yaml        telemetrygen Deployment template
│   ├── fault-backend.yaml    fault backend for faulty runs
│   └── pipeline-service.yaml Service template, one per pipeline
├── result/                   vm-single queries, limits, result record
└── runner/                   run protocol, preflight, watchdog, matrix
job.yaml                      Job that starts the tool
```

RBAC for the Job: `load-test/deployments/tooling/berlin-gcp-europe-west-1/` (`serviceaccount.yaml`, `rbac.yaml`), deployed by Flux.
