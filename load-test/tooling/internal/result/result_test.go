package result

import (
	"strings"
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

func TestSelectorsForTarget(t *testing.T) {
	u := spec.Unit{Component: spec.ComponentGateway, Signal: spec.Signals[0]}
	otel := SelectorsFor(spec.RunSpec{Target: spec.TargetOTel, Shape: spec.ShapeF1, Tenants: 5, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}, u)
	if !strings.Contains(otel.Run, RunPromLabel) {
		t.Errorf("otel Run selector lacks run label: %q", otel.Run)
	}
	tm := SelectorsFor(spec.RunSpec{Target: spec.TargetTM, Shape: spec.ShapeF1, Tenants: 5, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}, u)
	if strings.Contains(tm.Run, RunPromLabel) {
		t.Errorf("tm Run selector must not carry a run label: %q", tm.Run)
	}
	gw := `namespace="kyma-system",pod=~"telemetry-otlp-gateway-.*"`
	if tm.Run != gw || tm.Exp != gw || tm.Healthy != gw {
		t.Errorf("tm selectors must all be the gateway selector: %+v", tm)
	}
	agent := SelectorsFor(spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentLogAgent}, spec.Unit{Component: spec.ComponentLogAgent, Signal: spec.Signals[2]})
	if agent.Run != `namespace="kyma-system",pod=~"telemetry-log-agent-.*"` {
		t.Errorf("log agent selector %q", agent.Run)
	}
}

// Agents run on every node; only the agent-node agent's cadvisor series may count.
func TestContainerSelAgentsOnAgentNode(t *testing.T) {
	if got := containerSelFor(spec.TargetTM, spec.ComponentMetricAgent, "n1"); !strings.Contains(got, `node="n1"`) {
		t.Errorf("metric agent container selector %q", got)
	}
	if got := containerSelFor(spec.TargetTM, spec.ComponentGateway, "n1"); strings.Contains(got, "node=") {
		t.Errorf("gateway container selector must cover all gateway pods: %q", got)
	}
}

func TestCollectorPerComponent(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentAll}
	var rec Record
	rec.SetCollector(s, spec.ComponentGateway, CollectorResult{CPUAvg: 1})
	rec.SetCollector(s, spec.ComponentLogAgent, CollectorResult{CPUAvg: 2})
	if rec.Collector.CPUAvg != 1 || rec.Agents[spec.ComponentLogAgent].CPUAvg != 2 {
		t.Errorf("collector %+v agents %+v", rec.Collector, rec.Agents)
	}
	if name, res := rec.collector(s, spec.ComponentLogAgent); name != spec.ComponentLogAgent || res.CPUAvg != 2 {
		t.Errorf("collector(log-agent) = %q %+v", name, res)
	}
	single := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentLogAgent}
	var one Record
	one.SetCollector(single, spec.ComponentLogAgent, CollectorResult{CPUAvg: 3})
	if one.Collector.CPUAvg != 3 || one.Agents != nil {
		t.Errorf("single agent run: collector %+v agents %+v", one.Collector, one.Agents)
	}
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
	addCollectorLimits(rec, "", rec.Collector, 1500)
	if !rec.Limit.Hard || len(rec.Limit.All) != 2 || rec.Limit.First.AtS != 120 {
		t.Errorf("limits %+v", rec.Limit)
	}
	evicted := &Record{Collector: CollectorResult{Evicted: true}}
	addCollectorLimits(evicted, "", evicted.Collector, 1500)
	if evicted.Limit.First == nil || evicted.Limit.First.ID != "H1" {
		t.Errorf("eviction: %+v", evicted.Limit)
	}
}
