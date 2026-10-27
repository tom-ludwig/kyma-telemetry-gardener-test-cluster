package spec

import "testing"

func TestRunID(t *testing.T) {
	cases := map[string]RunSpec{
		"otel-f1-A-t050-healthy-r1":    {Target: TargetOTel, Shape: ShapeF1, Tenants: 50, Load: LoadA, Health: Healthy, Rep: 1, Rate: 1000},
		"otel-r1-B1000-t005-faulty-r3": {Target: TargetOTel, Shape: ShapeR1, Tenants: 5, Load: LoadB, Health: Faulty, Rep: 3, Rate: 1000},
		"tm-f1-A-t050-healthy-r1":      {Target: TargetTM, Shape: ShapeF1, Tenants: 50, Load: LoadA, Health: Healthy, Rep: 1, Rate: 1000},
	}
	for want, s := range cases {
		if got := s.ID(); got != want {
			t.Errorf("ID() = %q, want %q", got, want)
		}
	}
}

func TestRates(t *testing.T) {
	traces := Signals[0]
	a := RunSpec{Tenants: 100, Load: LoadA}
	if got := a.TenantRate(traces); got != traces.TotalRate/100 {
		t.Errorf("load A rate = %d, want %d", got, traces.TotalRate/100)
	}
	b := RunSpec{Tenants: 100, Load: LoadB, Rate: 1000}
	if got := b.OfferedRate(traces); got != 100000 {
		t.Errorf("load B offered = %d, want 100000", got)
	}
	if got := (RunSpec{Tenants: 100}).QueueSize(); got != 2 {
		t.Errorf("queue size at 100 tenants = %d, want 2", got)
	}
	if got := (RunSpec{Tenants: 300}).QueueSize(); got != 1 {
		t.Errorf("queue size floor = %d, want 1", got)
	}
	if got := (RunSpec{Tenants: 100}).MemoryLimitMi(); got != 4096 {
		t.Errorf("memory limit = %d, want 4096", got)
	}
}

// simulate runs the search against a collector that fails from limit tenants on.
func simulate(limit int) (runs []int, failing int) {
	passed := map[int]bool{}
	for {
		n, done := NextSearch(passed)
		if done {
			return runs, SmallestFailing(passed)
		}
		runs = append(runs, n)
		passed[n] = n < limit
	}
}

func TestSearch(t *testing.T) {
	runs, failing := simulate(1000)
	if len(runs) != 2 || failing != 0 {
		t.Errorf("100 passes: runs %v, failing %d; want [1 100], 0", runs, failing)
	}
	runs, failing = simulate(37)
	if failing < 37 || failing > 37+SearchGap || len(runs) > 8 {
		t.Errorf("limit at 37: runs %v, smallest failing %d", runs, failing)
	}
	runs, failing = simulate(1)
	if len(runs) != 1 || failing != 1 {
		t.Errorf("limit at 1: runs %v, failing %d", runs, failing)
	}
}

func TestValidate(t *testing.T) {
	base := RunSpec{Target: TargetTM, Shape: ShapeF1, Tenants: 5, Load: LoadA, Health: Healthy, Rep: 1}
	if err := base.Validate(); err != nil {
		t.Errorf("valid tm run rejected: %v", err)
	}
	faulty := RunSpec{Target: TargetTM, Shape: ShapeF1, Tenants: 5, Load: LoadA, Health: Faulty, Rep: 1}
	if err := faulty.Validate(); err != nil {
		t.Errorf("valid tm faulty run rejected: %v", err)
	}
	reject := map[string]RunSpec{
		"bad target": {Target: "x", Shape: ShapeF1, Tenants: 5, Load: LoadA, Health: Healthy, Rep: 1},
		"tm+R1":      {Target: TargetTM, Shape: ShapeR1, Tenants: 5, Load: LoadA, Health: Healthy, Rep: 1},
	}
	for name, s := range reject {
		if err := s.Validate(); err == nil {
			t.Errorf("%s: want error, got nil", name)
		}
	}
}

func TestLoadSignalsStrict(t *testing.T) {
	if _, err := loadSignals([]byte("signals:\n  - name: traces\n    typo: 1\n")); err == nil {
		t.Error("unknown key accepted")
	}
	if len(Signals) != 3 {
		t.Errorf("want 3 signals, got %d", len(Signals))
	}
}
