package runner

import (
	"context"
	"strings"
	"testing"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
)

// fakeRunner fails every run from limit tenants on with a hard limit.
func fakeRunner(limit int) (*Runner, *[]string) {
	var ran []string
	r := &Runner{done: map[string]*result.Record{}}
	r.runFn = func(_ context.Context, s spec.RunSpec) (*result.Record, error) {
		ran = append(ran, s.ID())
		return &result.Record{Valid: true, Limit: result.Limits{Hard: s.Tenants >= limit}}, nil
	}
	return r, &ran
}

func TestMatrixRunsEachIDOnce(t *testing.T) {
	r, ran := fakeRunner(43)
	if err := r.Matrix(context.Background(), spec.TargetOTel, spec.ComponentGateway, 1000, ""); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	faulty := 0
	for _, id := range *ran {
		if seen[id] {
			t.Errorf("%s ran twice", id)
		}
		seen[id] = true
		if strings.Contains(id, "-faulty-") {
			faulty++
		}
	}
	if faulty != 6 {
		t.Errorf("faulty runs: %d, want 6", faulty)
	}
	for _, id := range []string{
		"otel-f1-A-t005-healthy-r2", "otel-f1-A-t005-healthy-r3", "otel-r1-A-t005-healthy-r3",
		"otel-f1-A-t043-healthy-r2", "otel-f1-B1000-t043-healthy-r3", "otel-r1-B1000-t043-healthy-r2",
	} {
		if !seen[id] {
			t.Errorf("missing repetition %s", id)
		}
	}
	search := (*ran)[:7]
	want := []string{"t001", "t100", "t050", "t025", "t037", "t043", "t040"}
	for i, id := range search {
		if !strings.Contains(id, want[i]) {
			t.Errorf("search step %d: %s, want %s", i, id, want[i])
		}
	}
}

// Every faulty run needs its healthy baseline before it, for S5.
func TestMatrixHealthyBeforeFaulty(t *testing.T) {
	r, ran := fakeRunner(1000)
	if err := r.Matrix(context.Background(), spec.TargetOTel, spec.ComponentGateway, 1000, ""); err != nil {
		t.Fatal(err)
	}
	pos := map[string]int{}
	for i, id := range *ran {
		pos[id] = i
	}
	for _, id := range *ran {
		if !strings.Contains(id, "-faulty-") {
			continue
		}
		baseline := strings.Replace(id, "-faulty-", "-healthy-", 1)
		if p, ok := pos[baseline]; !ok || p > pos[id] {
			t.Errorf("%s has no healthy run %s before it", id, baseline)
		}
	}
}

func TestMatrixAgentRunsCarryComponent(t *testing.T) {
	r, ran := fakeRunner(1000)
	if err := r.Matrix(context.Background(), spec.TargetTM, spec.ComponentLogAgent, 1000, ""); err != nil {
		t.Fatal(err)
	}
	for _, id := range *ran {
		if !strings.HasPrefix(id, "tm-log-agent-f1-") {
			t.Errorf("log-agent matrix ran %s", id)
		}
	}
	if err := r.Matrix(context.Background(), spec.TargetOTel, spec.ComponentLogAgent, 1000, ""); err == nil {
		t.Error("otel log-agent matrix accepted")
	}
}

func TestMatrixOnlyFilter(t *testing.T) {
	r, ran := fakeRunner(1000)
	if err := r.Matrix(context.Background(), spec.TargetOTel, spec.ComponentGateway, 1000, "otel-r1-A-*"); err != nil {
		t.Fatal(err)
	}
	for _, id := range *ran {
		if !strings.HasPrefix(id, "otel-r1-A-") {
			t.Errorf("-only otel-r1-A-* ran %s", id)
		}
	}
}
