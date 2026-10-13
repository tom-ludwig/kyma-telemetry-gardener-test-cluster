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
