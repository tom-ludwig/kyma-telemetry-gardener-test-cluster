# GitHub Sub-Issues: Pipeline Scalability

Sub-issues for the epic [kyma-project/telemetry-manager#3900](https://github.com/kyma-project/telemetry-manager/issues/3900) (Pipeline Scalability: Lift the 5-Pipeline Limit).

## Scope

1. Phase: Plain OTel Collector (without Kyma TM)
2. Phase: Kyma TM: OTLP Gateway, log agent, and metric agent

## 1. Test Plan

[Assignee: @tom-ludwig]

Define which metrics etc. each load test run collects, so that results from different runs are comparable.

The epic already defines the pipeline counts (1, 3, 5, 10, 20, 50, 100 per signal type) and the report topics. The existing [benchmark setup](https://github.com/kyma-project/telemetry-manager/blob/main/docs/contributor/benchmarks/README.md) already defines cluster size, test duration, backpressure simulation, and the basic result metrics.

### Tasks

- [ ] List the metrics to collect per run (for example, received and exported items per second, exporter queue size, dropped items, Pod memory and CPU, restarts and OOM kills).
- [ ] Decide the load model: constant total load split across N pipelines, or constant load per pipeline.
- [ ] Decide how much data (GB) to generate and pass through the collector per run.
- [ ] Define when a run counts as "limit reached" (for example, first OOM kill, first dropped data, or exporter queue staying full).
- [ ] Decide the pipeline shape: one backend per pipeline or a shared backend, with or without transform and filter.

### Acceptance Criteria

- [ ] Test plan is documented and agreed on by the test team.

## 2. Load Test Tooling

[Assignee: @louis-kauer]

Check the `.sh` script in its current-state, and decide how to extend it for a parameterized pipeline count.

Current state: [`hack/load-tests/run-load-test.sh`](https://github.com/kyma-project/telemetry-manager/blob/main/hack/load-tests/run-load-test.sh) supports a single multi-pipeline option (`-m true`). It applies a static manifest that adds two extra pipelines, so only 1 and 3 pipelines are possible. OTel logs and the metric agent have no multi-pipeline variant.

### Phases

1. **Hard-coded Go script**: Creates the pipelines and the load generators for a given run.
2. **Operator-like Go binary as Kubernetes Job**: Runs in the cluster, applies the test infra and pipeline CRs, and starts load generation.

### Tasks

- [ ] Explore the existing [`hack/load-tests/run-load-test.sh`](https://github.com/kyma-project/telemetry-manager/blob/main/hack/load-tests/run-load-test.sh) script (current-state).
- [ ] Clarify the scope of the `.sh` script: just Pods and load test creation, or also Gardener and VM creation.
- [ ] Support a configurable pipeline count per signal type.
- [ ] Support all components in scope: OTLP Gateway (traces, metrics, logs), log agent, and metric agent.
- [ ] Deploy Telemetry Manager with `--unlimited-pipelines`, so that runs above 5 pipelines are possible.

### Acceptance Criteria

- [ ] A run with any pipeline count from the test plan can be started with one command or one manifest.

## 3. Grafana Dashboard

[Assignee: @tom-ludwig]

Provide a Grafana dashboard to compare load test runs.

### Tasks

- [ ] Fix the y-axis auto-scaling problem, so that panels from different runs can be compared visually.
- [ ] Add a filter for runs (for example, Run 1, Run 2, Run N), so that one run or a selection of runs can be shown.

### Open Questions

- How is a run identified in the metrics (for example, a `run` label set by the load test tooling)?

### Acceptance Criteria

- [ ] Dashboard is versioned in this repository and deployed with Flux.
- [ ] Dashboard shows all metrics from the test plan, filterable by run.

## 4. Infrastructure Setup (for hard-coded Go script)

[Assignees: @louis-kauer in collab w/@tom-ludwig]

Provide the infrastructure that the load test tooling (the hard-coded Go script) runs against.

### Tasks

- [ ] Support a plain OTel Collector setup (phase 1)
- [ ] Create VM / Grafana resources and push to flux repo.

### Acceptance Criteria

- [ ] Flux repo with all components deployed.