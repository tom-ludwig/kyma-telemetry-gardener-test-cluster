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
├── monitoring/
│   └── deployments/
│
└── shared/
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

For the actual load test the OpenTelemetry Collectors will _NOT_ push their data to those instaces. For them, seperate instances are to be issues.
