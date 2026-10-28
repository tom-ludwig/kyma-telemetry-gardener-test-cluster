package runner

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"context"
	"fmt"
	"log"
	"path"
)

var (
	shapes        = []string{spec.ShapeF1, spec.ShapeR1}
	loads         = []string{spec.LoadA, spec.LoadB}
	faultyTenants = []int{5, 20, 100}
	repTenants    = 5
	reps          = 3
)

// Matrix runs the binary search per shape × load, then the fixed faulty runs and the repetitions.
// A stopped matrix starts from the beginning; use -only to skip finished runs.
// target tm supports F1 and healthy only, so its routing search and the faulty block are skipped.
func (r *Runner) Matrix(ctx context.Context, target, component string, rate int, only string) error {
	first := spec.RunSpec{Target: target, Component: component, Shape: spec.ShapeF1, Tenants: 1, Load: spec.LoadA, Health: spec.Healthy, Rep: 1, Rate: rate}
	if err := first.Validate(); err != nil {
		return err
	}
	matrixShapes := shapes
	if target == spec.TargetTM {
		matrixShapes = []string{spec.ShapeF1}
	}
	log.Printf("matrix: target %s, search per shape × load (1, then %d tenants, then halve), faulty at %v tenants, %d reps at %d tenants and at each limit",
		target, spec.MaxTenants, faultyTenants, reps, repTenants)
	var repeat []spec.RunSpec // healthy specs to repeat: smallest failing run per search, then 5 tenants
	for _, shape := range matrixShapes {
		for _, load := range loads {
			failing, err := r.search(ctx, target, component, shape, load, rate, only)
			if err != nil {
				return err
			}
			if failing > 0 {
				repeat = append(repeat, spec.RunSpec{Target: target, Component: component, Shape: shape, Tenants: failing, Load: load, Health: spec.Healthy, Rate: rate})
			}
		}
	}
	if target == spec.TargetOTel {
		for _, shape := range matrixShapes {
			for _, n := range faultyTenants {
				faulty := spec.RunSpec{Target: target, Shape: shape, Tenants: n, Load: spec.LoadA, Health: spec.Faulty, Rep: 1, Rate: rate}
				healthy := faulty
				healthy.Health = spec.Healthy
				// S5 compares with the healthy run; reused if the search already ran it
				if _, err := r.runOrSkip(ctx, healthy, only); err != nil {
					return err
				}
				if _, err := r.runOrSkip(ctx, faulty, only); err != nil {
					return err
				}
			}
		}
	}
	for _, shape := range matrixShapes {
		repeat = append(repeat, spec.RunSpec{Target: target, Component: component, Shape: shape, Tenants: repTenants, Load: spec.LoadA, Health: spec.Healthy, Rate: rate})
	}
	for _, s := range repeat {
		for rep := 1; rep <= reps; rep++ {
			s.Rep = rep
			if _, err := r.runOrSkip(ctx, s, only); err != nil {
				return err
			}
		}
	}
	return nil
}

// search returns the smallest tenant count with a hard limit, or 0 if all passed.
func (r *Runner) search(ctx context.Context, target, component, shape, load string, rate int, only string) (int, error) {
	passed := map[int]bool{}
	for {
		n, done := spec.NextSearch(passed)
		if done {
			return spec.SmallestFailing(passed), nil
		}
		rec, err := r.runOrSkip(ctx, spec.RunSpec{Target: target, Component: component, Shape: shape, Tenants: n, Load: load, Health: spec.Healthy, Rep: 1, Rate: rate}, only)
		if err != nil {
			return 0, err
		}
		if rec == nil {
			return spec.SmallestFailing(passed), nil // filtered out by -only: this search stops here
		}
		passed[n] = !rec.Limit.Hard
		log.Printf("search %s-%s-%s: %d tenants %s", target, shape, load, n, map[bool]string{true: "passed", false: "hit a hard limit"}[passed[n]])
	}
}

// runOrSkip returns the record of a run that already ran in this process, skips runs
// excluded by -only (nil record) and runs the rest.
// An invalid run stops the matrix: its cause is the infra, so the next runs would fail too.
func (r *Runner) runOrSkip(ctx context.Context, s spec.RunSpec, only string) (*result.Record, error) {
	if rec, ok := r.done[s.ID()]; ok {
		return rec, nil
	}
	if match, _ := path.Match(only, s.ID()); only != "" && !match {
		return nil, nil
	}
	rec, err := r.runFn(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", s.ID(), err)
	}
	if !rec.Valid {
		return nil, fmt.Errorf("%s invalid: %s", s.ID(), rec.InvalidReason)
	}
	r.done[s.ID()] = rec
	return rec, nil
}
