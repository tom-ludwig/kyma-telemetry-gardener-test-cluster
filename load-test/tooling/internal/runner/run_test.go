package runner

import (
	"slices"
	"testing"
	"time"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/manifests"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
)

func TestNewRecordInputs(t *testing.T) {
	r := &Runner{warmup: time.Minute, steady: 3 * time.Minute, cooldown: time.Minute, args: []string{"-health=faulty", "-tenants=5"}}
	s := spec.RunSpec{Shape: spec.ShapeF1, Tenants: 5, Load: spec.LoadA, Health: spec.Faulty, Rep: 1}
	n := nodes{env: result.Env{CollectorNode: result.NodeInfo{Name: "a", CPU: 3.92}}}
	rec, err := r.newRecord(s, manifests.ConfigStats{}, 0, result.Window{}, n)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rec.Run.Args, r.args) || rec.Run.ToolCommit != result.ToolCommit {
		t.Errorf("run: args %v, commit %q", rec.Run.Args, rec.Run.ToolCommit)
	}
	if rec.Setup.PhasesS != (result.Phases{Warmup: 60, Steady: 180, Cooldown: 60}) {
		t.Errorf("phases %+v", rec.Setup.PhasesS)
	}
	// load A, 5 tenants: traces 12000/5 = 2400/s per tenant, 500 per worker
	if rec.Setup.Generator.WorkersPerTenant["traces"] != 5 || rec.Setup.Generator.LogBodyBytes != 2000 {
		t.Errorf("generator %+v", rec.Setup.Generator)
	}
	if rec.Setup.Fault == nil || rec.Setup.Fault.Rules != "503:70" || rec.Setup.Fault.Delays != "503:1000,200:1000" {
		t.Errorf("fault %+v", rec.Setup.Fault)
	}
	if rec.Env.CollectorNode.CPU != 3.92 {
		t.Errorf("env %+v", rec.Env)
	}

	s.Health = spec.Healthy
	rec, err = r.newRecord(s, manifests.ConfigStats{}, 0, result.Window{}, n)
	if err != nil {
		t.Fatal(err)
	}
	if rec.Setup.Fault != nil {
		t.Errorf("healthy run has fault setup %+v", rec.Setup.Fault)
	}
}