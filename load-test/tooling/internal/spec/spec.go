package spec

import (
	_ "embed"
	"fmt"
	"slices"
	"strings"

	"sigs.k8s.io/yaml"
)

const (
	ShapeF1 = "F1" // fan-out: every pipeline gets every item and filters its tenant
	ShapeR1 = "R1" // routing: one input pipeline routes each item to its tenant

	LoadA = "A" // fixed total per signal, split across tenants
	LoadB = "B" // -rate per signal per tenant

	Healthy = "healthy"
	Faulty  = "faulty"

	// TargetOTel tests a plain OTel collector we build ourselves; TargetTM tests the managed
	// Telemetry Manager gateway via LogPipeline/MetricPipeline/TracePipeline CRs we deploy.
	TargetOTel = "otel"
	TargetTM   = "tm"

	// Components under test. otel has only the gateway; tm also has the node agents and all
	// three together.
	ComponentGateway     = "gateway"
	ComponentLogAgent    = "log-agent"
	ComponentMetricAgent = "metric-agent"
	ComponentAll         = "all"

	MaxTenants = 100
	SearchGap  = 5

	// TM gateway: one shared OTLP DaemonSet in kyma-system for all signals.
	// (The per-signal runtime/prometheus scraping DaemonSets are disabled in these tests.)
	TMNamespace       = "kyma-system"
	TMGatewayEndpoint = "telemetry-otlp.kyma-system.svc:4317"
	TMGatewayApp      = "telemetry-otlp-gateway" // app.kubernetes.io/name of the shared OTLP gateway
	// TMBackendNamespace holds the per-run nop debug backend TM pipelines export to.
	TMBackendNamespace = "tm-pipeline-test"
	// ScrapeIntervalS is TM's default Prometheus scrape interval; metric generators size their
	// series so that one scrape per interval yields the planned data points/s.
	ScrapeIntervalS = 30
	// MetricPortBase is the port of tenant 0's metric generator; tenant i serves on base+i.
	MetricPortBase = 9001
)

// TMApps is the app.kubernetes.io/name of each TM component's pods.
var TMApps = map[string]string{
	ComponentGateway:     TMGatewayApp,
	ComponentLogAgent:    "telemetry-log-agent",
	ComponentMetricAgent: "telemetry-metric-agent",
}

// agentSignal is the one signal each node agent collects.
var agentSignal = map[string]string{
	ComponentLogAgent:    "logs",
	ComponentMetricAgent: "metrics",
}

//go:embed signals.yaml
var signalsYAML []byte

// Signal holds everything that differs between traces, metrics and logs (signals.yaml).
type Signal struct {
	Name      string `json:"name"`
	Item      string `json:"item"`
	FilterKey string `json:"filterKey"`
	TotalRate int    `json:"totalRate"`
	BodyBytes int    `json:"bodyBytes"`
}

// Signals is parsed once from the embedded file; a broken file is a build mistake, so it panics.
var Signals = mustLoadSignals(signalsYAML)

func mustLoadSignals(raw []byte) []Signal {
	signals, err := loadSignals(raw)
	if err != nil {
		panic(fmt.Sprintf("signals.yaml: %v", err))
	}
	return signals
}

func loadSignals(raw []byte) ([]Signal, error) {
	var file struct {
		Signals []Signal `json:"signals"`
	}
	if err := yaml.UnmarshalStrict(raw, &file); err != nil {
		return nil, err
	}
	if len(file.Signals) == 0 {
		return nil, fmt.Errorf("no signals")
	}
	for _, s := range file.Signals {
		if s.Name == "" || s.Item == "" || s.FilterKey == "" || s.TotalRate <= 0 {
			return nil, fmt.Errorf("signal %q: name, item, filterKey and totalRate are required", s.Name)
		}
	}
	return file.Signals, nil
}

// RunSpec is one test run: the signals of its components with Tenants pipelines each.
type RunSpec struct {
	Target    string // otel (plain collector) or tm (Telemetry Manager gateway)
	Component string // gateway, log-agent, metric-agent or all; empty means gateway
	Shape     string
	Tenants   int
	Load      string
	Health    string
	Rep       int
	Rate      int // load B only: items/s per signal per tenant
}

// ID is the run's name, e.g. otel-f1-A-t050-healthy-r1 or tm-log-agent-f1-B1000-t050-healthy-r1.
// Gateway runs carry no component, so their IDs stay as before.
func (s RunSpec) ID() string {
	load := s.Load
	if s.Load == LoadB {
		load = fmt.Sprintf("B%d", s.Rate)
	}
	target := s.Target
	if !s.gatewayOnly() {
		target += "-" + s.Component
	}
	return fmt.Sprintf("%s-%s-%s-t%03d-%s-r%d", target, strings.ToLower(s.Shape), load, s.Tenants, s.Health, s.Rep)
}

func (s RunSpec) gatewayOnly() bool { return s.Component == "" || s.Component == ComponentGateway }

// Components lists the components under test; all expands to gateway and both agents.
func (s RunSpec) Components() []string {
	if s.Component == ComponentAll {
		return []string{ComponentGateway, ComponentLogAgent, ComponentMetricAgent}
	}
	if s.gatewayOnly() {
		return []string{ComponentGateway}
	}
	return []string{s.Component}
}

// Has reports whether component c is under test in this run.
func (s RunSpec) Has(c string) bool { return slices.Contains(s.Components(), c) }

// Unit is one measured signal flow: a component and one signal it carries.
type Unit struct {
	Component string
	Signal    Signal
}

// Key names the unit in results: the signal for the gateway (as before), component/signal for agents.
func (u Unit) Key() string {
	if u.Component == ComponentGateway {
		return u.Signal.Name
	}
	return u.Component + "/" + u.Signal.Name
}

// Units lists the run's signal flows: the gateway carries all signals, each agent one.
func (s RunSpec) Units() []Unit {
	var units []Unit
	for _, c := range s.Components() {
		for _, sig := range Signals {
			if c != ComponentGateway && agentSignal[c] != sig.Name {
				continue
			}
			units = append(units, Unit{Component: c, Signal: sig})
		}
	}
	return units
}

func (s RunSpec) Validate() error {
	switch {
	case s.Target != TargetOTel && s.Target != TargetTM:
		return fmt.Errorf("-target must be otel or tm, got %q", s.Target)
	case !s.gatewayOnly() && s.Component != ComponentLogAgent && s.Component != ComponentMetricAgent && s.Component != ComponentAll:
		return fmt.Errorf("-component must be gateway, log-agent, metric-agent or all, got %q", s.Component)
	case s.Target == TargetOTel && !s.gatewayOnly():
		return fmt.Errorf("-target otel supports only -component gateway, got %q", s.Component)
	case !s.gatewayOnly() && s.Health != Healthy:
		return fmt.Errorf("-component %s supports only -health healthy, got %q", s.Component, s.Health)
	case s.Shape != ShapeF1 && s.Shape != ShapeR1:
		return fmt.Errorf("-shape must be F1 or R1, got %q", s.Shape)
	case s.Target == TargetTM && s.Shape != ShapeF1:
		return fmt.Errorf("-target tm supports only -shape F1 (Telemetry Manager has no routing), got %q", s.Shape)
	case s.Load != LoadA && s.Load != LoadB:
		return fmt.Errorf("-load must be A or B, got %q", s.Load)
	case s.Health != Healthy && s.Health != Faulty:
		return fmt.Errorf("-health must be healthy or faulty, got %q", s.Health)
	case s.Tenants < 1 || s.Tenants > 999:
		return fmt.Errorf("-tenants must be 1..999, got %d", s.Tenants)
	case s.Rep < 1:
		return fmt.Errorf("-rep must be >= 1, got %d", s.Rep)
	case s.Load == LoadB && s.Rate < 1:
		return fmt.Errorf("-rate must be >= 1, got %d", s.Rate)
	}
	return nil
}

// TenantRate is the generator rate of one tenant for one signal.
func (s RunSpec) TenantRate(sig Signal) int {
	if s.Load == LoadB {
		return s.Rate
	}
	return max(1, sig.TotalRate/s.Tenants)
}

// OfferedRate is the planned total items/s of one signal.
func (s RunSpec) OfferedRate(sig Signal) int { return s.TenantRate(sig) * s.Tenants }

// QueueSize mirrors TM: 256 batches split over the pipelines of one signal.
func (s RunSpec) QueueSize() int { return max(1, 256/s.Tenants) }

// HACK: quick-fix hardcoding 4Gi instead of a proper dynamic calculation; revisit later
const CollectorMemoryLimitMi = 4096

// MemoryLimitMi returns the fixed collector memory limit (4 GiB).
func (s RunSpec) MemoryLimitMi() int { return CollectorMemoryLimitMi }

func TenantID(i int) string   { return fmt.Sprintf("tenant-%03d", i) }
func PipelineID(i int) string { return fmt.Sprintf("p-%03d", i) }

// NextSearch returns the next tenant count of the binary search, or done.
// passed holds the outcome (true = no hard limit) of each tenant count run so far.
func NextSearch(passed map[int]bool) (next int, done bool) {
	if _, ok := passed[1]; !ok {
		return 1, false
	}
	if !passed[1] {
		return 0, true // limit already at 1 tenant
	}
	ok, ran := passed[MaxTenants]
	if !ran {
		return MaxTenants, false
	}
	if ok {
		return 0, true
	}
	lo, hi := 1, MaxTenants
	for n, ok := range passed {
		if ok && n > lo {
			lo = n
		}
		if !ok && n < hi {
			hi = n
		}
	}
	if hi-lo <= SearchGap {
		return 0, true
	}
	return (lo + hi) / 2, false
}

// SmallestFailing returns the smallest tenant count with a hard limit, or 0.
func SmallestFailing(passed map[int]bool) int {
	min := 0
	for n, ok := range passed {
		if !ok && (min == 0 || n < min) {
			min = n
		}
	}
	return min
}
