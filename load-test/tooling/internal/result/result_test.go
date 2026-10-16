package result

import (
	"testing"
	"time"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
)

func steps(start time.Time, from, to int) []time.Time {
	var out []time.Time
	for i := from; i <= to; i++ {
		out = append(out, start.Add(time.Duration(i)*step))
	}
	return out
}

func TestFirstHeld(t *testing.T) {
	t0 := time.Unix(0, 0)
	// exactly 5 min: 20 steps of 15 s after the first
	if got, ok := FirstHeld(steps(t0, 0, 20), step, hold); !ok || !got.Equal(t0) {
		t.Errorf("exactly 5 min: %v %v", got, ok)
	}
	// one step short
	if _, ok := FirstHeld(steps(t0, 0, 19), step, hold); ok {
		t.Error("4:45 counted as held")
	}
	// gap restarts the stretch
	times := append(steps(t0, 0, 10), steps(t0, 12, 40)...)
	if got, ok := FirstHeld(times, step, hold); !ok || !got.Equal(t0.Add(12*step)) {
		t.Errorf("after gap: %v %v", got, ok)
	}
	if _, ok := FirstHeld(nil, step, hold); ok {
		t.Error("empty counted as held")
	}
}

func TestLimitsOrderAndHard(t *testing.T) {
	var l Limits
	l.Add(Hit{ID: "S2", AtS: 300})
	if l.Hard {
		t.Error("soft limit marked hard")
	}
	l.Add(Hit{ID: "H1", AtS: 100})
	if !l.Hard || l.First.ID != "H1" {
		t.Errorf("first %+v hard %t", l.First, l.Hard)
	}
}

// The faulty exporter's successful sends must not hide a drop of the healthy pipelines.
func TestIsolationUsesHealthyExporters(t *testing.T) {
	s := spec.RunSpec{Shape: spec.ShapeF1, Tenants: 5, Load: spec.LoadA, Health: spec.Faulty, Rep: 1}
	healthy := &Record{Signals: map[string]SignalResult{"traces": {ExportedPerS: 1000}}}
	// healthy pipelines: 740/s (baseline 800/s, -7.5 %); with p-000's partial sends the total looks fine
	faulty := &Record{Signals: map[string]SignalResult{"traces": {ExportedPerS: 800, ExportedHealthyPerS: 740}}}
	AddIsolation(faulty, s, healthy, Window{})
	if faulty.Limit.First == nil || faulty.Limit.First.ID != "S5" {
		t.Errorf("want S5, got %+v", faulty.Limit)
	}
}

func TestCollectorLimits(t *testing.T) {
	at := 120.0
	rec := &Record{Collector: CollectorResult{Restarts: 1, RestartAtS: &at, OOMKilled: true, OOMAtS: &at}}
	addCollectorLimits(rec, 1500)
	if !rec.Limit.Hard || len(rec.Limit.All) != 2 || rec.Limit.First.AtS != 120 {
		t.Errorf("limits %+v", rec.Limit)
	}
	evicted := &Record{Collector: CollectorResult{Evicted: true}}
	addCollectorLimits(evicted, 1500)
	if evicted.Limit.First == nil || evicted.Limit.First.ID != "H1" {
		t.Errorf("eviction: %+v", evicted.Limit)
	}
}
