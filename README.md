# Kyma Telemetry Gardener Test Cluster

This repository manages the deployment and configuration for the performance testing of kyma telemetry module on Gardener Shoot clusters using **Flux Operator**.

## Repository Layout

```
.
├── clusters/
│   └── berlin-gcp-europe-west-1/
│       ├── flux-system/
│       │   ├── flux-instance.yaml
│       │   └── kustomization.yaml
│       └── kustomizations.yaml
│
├── infrastructure/
│   ├── configs/
│   └── deployments/
│       └── cert-manager/
│
├── load-test/
│   └── deployments/
│       └── backends/ # VMSingle, VLSingle, VTSingle that receive the load-test data
│
├── monitoring/
│   └── deployments/
│
└── shared/
    ├── namespaces/
    └── sources/ # Shared Flux HelmRepository and OCI HelmRepository definitions
```

## Installation

1. Install the Flux Operator in the Gardener cluster:

```bash
helm install flux-operator oci://ghcr.io/controlplaneio-fluxcd/charts/flux-operator \
  --namespace flux-system \
  --create-namespace
```

1. Deploy FluxInstance

Apply the Flux instance manifest:

```bash
kubectl apply -k clusters/berlin-gcp-europe-west-1/flux-system
```

## Monitoring

For cluster monitoring the Victoria Stack is used, that means the VictoriaMetrics Operator deploys all components to scrape and store cluster metrics and logs.
For this load test we explictily don't use a standard Kyma `MetricsPipeline` becuase we want to benchmark the `telemetry-module` it self.

VictoriaMetrics Agent which is a lightweight prometheus like scraper, scrapes metrics and pushes them to `VMSingle` instance used for internal monitoring.
VictoriaLogs Agent is a lightweight logs scraper, which collects all logs and pushes them to `VLSingle` instance.
Both `VMSingle` and `VLSingle` are queryable from within Grafana.

Node metrics come from the node-exporter that Gardener already runs in `kube-system`; a `VMServiceScrape` picks it up.

## Load Test Backends

For the actual load test the OpenTelemetry Collectors will _NOT_ push their data to the monitoring instances. They export to dedicated backends in the `load-test-backends` namespace:

| Signal  | Backend    | Endpoint                                                                                 |
| ------- | ---------- | ---------------------------------------------------------------------------------------- |
| Metrics | `VMSingle` | `http://vmsingle-load-test.load-test-backends.svc:8428/opentelemetry/v1/metrics` (OTLP/HTTP) |
| Logs    | `VLSingle` | `http://vlsingle-load-test.load-test-backends.svc:9428/insert/opentelemetry/v1/logs` (OTLP/HTTP) |
| Traces  | `VTSingle` | `vtsingle-load-test.load-test-backends.svc:4317` (OTLP/gRPC) or `:10428/insert/opentelemetry/v1/traces` (OTLP/HTTP) |

All three keep data for 1 day. They are queryable in Grafana through the `* LoadTest` datasources.

Namespaces per phase:

- `load-test-backends`: backends, shared by all phases.
- `otel-load-test`: phase 1, plain OpenTelemetry Collector gateway and load generators.
