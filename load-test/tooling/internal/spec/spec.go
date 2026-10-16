package spec

import (
	_ "embed"
	"fmt"
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

	MaxTenants = 100
	SearchGap  = 5
)

//go:embed signals.yaml
var signalsYAML []byte

// Signal holds everything that differs between traces, metrics and logs (signals.yaml).
type Signal struct {
	Name           string `json:"name"`
	Item           string `json:"item"`
	Backend        string `json:"backend"`
	Port           int    `json:"port"`
	Path           string `json:"path"`
	EndpointKey    string `json:"endpointKey"`
	FilterKey      string `json:"filterKey"`
	Header         string `json:"header"`
	IngestMetric   string `json:"ingestMetric"`
	IngestFilter   string `json:"ingestFilter"`
	FreeDiskMetric string `json:"freeDiskMetric"`
	TotalRate      int    `json:"totalRate"`
	BodyBytes      int    `json:"bodyBytes"`
	StoredBytes    int    `json:"storedBytes"`
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
		if s.Name == "" || s.Backend == "" || s.Port <= 0 || s.TotalRate <= 0 || s.StoredBytes <= 0 {
			return nil, fmt.Errorf("signal %q: name, backend, port, totalRate and storedBytes are required", s.Name)
		}
	}
	return file.Signals, nil
}

// RunSpec is one test run: all three signals with Tenants pipelines each.
type RunSpec struct {
	Shape   string
	Tenants int
	Load    string
	Health  string
	Rep     int
	Rate    int // load B only: items/s per signal per tenant
}

// ID is the run's name, e.g. f1-A-t050-healthy-r1 or f1-B1000-t050-healthy-r1.
func (s RunSpec) ID() string {
	load := s.Load
	if s.Load == LoadB {
		load = fmt.Sprintf("B%d", s.Rate)
	}
	return fmt.Sprintf("%s-%s-t%03d-%s-r%d", strings.ToLower(s.Shape), load, s.Tenants, s.Health, s.Rep)
}

func (s RunSpec) Validate() error {
	switch {
	case s.Shape != ShapeF1 && s.Shape != ShapeR1:
		return fmt.Errorf("-shape must be F1 or R1, got %q", s.Shape)
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

// MemoryLimitMi is the TM gateway formula: 750Mi + 1000Mi per pipeline (all signals).
func (s RunSpec) MemoryLimitMi() int { return 750 + 1000*len(Signals)*s.Tenants }

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
