package runner

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/manifests"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"context"
	"fmt"
	"log"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Runner struct {
	k                        client.Client
	p                        result.Prom
	warmup, steady, cooldown time.Duration
	genCPU, genMemory        resource.Quantity
	done                     map[string]*result.Record // records of this process by run ID
	runFn                    func(context.Context, spec.RunSpec) (*result.Record, error)
}

func New(k client.Client, p result.Prom, warmup, steady, cooldown time.Duration) (*Runner, error) {
	cpu, memory, err := manifests.GeneratorRequests()
	if err != nil {
		return nil, err
	}
	r := &Runner{
		k: k, p: p, warmup: warmup, steady: steady, cooldown: cooldown,
		genCPU: cpu, genMemory: memory, done: map[string]*result.Record{},
	}
	r.runFn = r.Run
	return r, nil
}

// Run executes one run and prints its result record.
func (r *Runner) Run(ctx context.Context, s spec.RunSpec) (*result.Record, error) {
	relayConf, stats, err := manifests.BuildConfig(s)
	if err != nil {
		return nil, err
	}
	n, err := r.preflight(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("preflight: %w", err)
	}
	defer r.cleanup(context.WithoutCancel(ctx), s)

	log.Printf("%s: fresh backends", s.ID())
	if err := r.restartBackends(ctx); err != nil {
		return nil, err
	}
	if err := r.applyCollector(ctx, s, relayConf); err != nil {
		return nil, err
	}
	startup, err := r.waitCollector(ctx)
	if err != nil {
		return nil, err
	}

	gens, err := manifests.Generators(s, r.warmup+r.steady)
	if err != nil {
		return nil, err
	}
	if err := r.create(ctx, gens); err != nil {
		return nil, err
	}
	start := time.Now()
	w := result.Window{Start: start, SteadyStart: start.Add(r.warmup), SteadyEnd: start.Add(r.warmup + r.steady)}
	log.Printf("%s: load on, measuring %s – %s", s.ID(), w.SteadyStart.Format(time.TimeOnly), w.SteadyEnd.Format(time.TimeOnly))

	rec := newRecord(s, stats, startup, w)
	watch := &collectorWatch{start: start}
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	if reason := waitOrStop(ctx, r.watchdog(watchCtx, s, w, n, watch), w.SteadyEnd); reason != "" {
		return rec, r.invalid(rec, reason)
	}
	stopWatch()

	if err := r.delete(ctx, gens); err != nil {
		return nil, err
	}
	drain := r.waitDrain(ctx, s, w.SteadyEnd)
	r.observeCollector(ctx, watch) // restarts after the last watchdog tick
	w.End = time.Now()
	rec.Run.End = w.End

	if err := r.fill(ctx, s, w, n, rec, watch, drain); err != nil {
		return nil, err
	}
	return rec, result.PrintRecord(rec)
}

func newRecord(s spec.RunSpec, stats manifests.ConfigStats, startup float64, w result.Window) *result.Record {
	rec := &result.Record{
		Run: result.RunInfo{ID: s.ID(), Date: time.Now().UTC(), CollectorImage: manifests.CollectorImage(),
			Start: w.Start, SteadyStart: w.SteadyStart, SteadyEnd: w.SteadyEnd},
		Setup: result.Setup{Shape: s.Shape, Load: s.Load, Tenants: s.Tenants, Health: s.Health,
			RatePerTenant: map[string]int{}, OfferedPerS: map[string]int{},
			MemLimitMiB: s.MemoryLimitMi(), QueueSizePerExporter: s.QueueSize()},
		Config:  result.ConfigRecord{ConfigStats: stats, StartupS: startup},
		Signals: map[string]result.SignalResult{},
		Limit:   result.Limits{All: []result.Hit{}},
		Valid:   true,
	}
	for _, sig := range spec.Signals {
		rec.Setup.RatePerTenant[sig.Name] = s.TenantRate(sig)
		rec.Setup.OfferedPerS[sig.Name] = s.OfferedRate(sig)
	}
	return rec
}

func (r *Runner) invalid(rec *result.Record, reason string) error {
	rec.Valid, rec.InvalidReason = false, reason
	if err := result.PrintRecord(rec); err != nil {
		return err
	}
	return fmt.Errorf("run invalid: %s", reason)
}

// waitOrStop waits until t; it returns early with the watchdog's reason.
func waitOrStop(ctx context.Context, stop <-chan string, t time.Time) string {
	select {
	case reason := <-stop:
		return reason
	case <-ctx.Done():
		return "interrupted"
	case <-time.After(time.Until(t)):
		return ""
	}
}

// fill queries vm-single for all record values and evaluates the limits.
func (r *Runner) fill(ctx context.Context, s spec.RunSpec, w result.Window, n nodes, rec *result.Record, watch *collectorWatch, drain *float64) error {
	collector, err := r.p.QueryCollector(ctx, s, w)
	if err != nil {
		return err
	}
	collector.QueueDrainS = drain
	watch.apply(&collector)
	rec.Collector = collector
	for _, sig := range spec.Signals {
		res, err := r.p.QuerySignal(ctx, s, sig, w)
		if err != nil {
			return err
		}
		rec.Signals[sig.Name] = res
		if res.BackendIngestRatio < 0.99 {
			rec.Valid = false
			rec.InvalidReason = fmt.Sprintf("%s backend stored %.0f %% of exported", sig.Name, 100*res.BackendIngestRatio)
		}
	}
	if err := r.p.EvalLimits(ctx, s, w, rec, n.nodeACores); err != nil {
		return err
	}
	healthy := s
	healthy.Health, healthy.Rep = spec.Healthy, 1
	result.AddIsolation(rec, s, r.done[healthy.ID()], w)
	return nil
}

// restartBackends deletes the backend pods; their Deployments create fresh ones.
func (r *Runner) restartBackends(ctx context.Context) error {
	old, err := r.backendPods(ctx)
	if err != nil {
		return err
	}
	for _, p := range old {
		if err := r.k.Delete(ctx, &p); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	var pods map[string]corev1.Pod
	err = poll(ctx, 5*time.Minute, "fresh backends", func() (bool, error) {
		pods, err = r.backendPods(ctx)
		if err != nil {
			return false, err
		}
		for _, sig := range spec.Signals {
			p, ok := pods[sig.Backend]
			if !ok || p.UID == old[sig.Backend].UID || !podReady(p) {
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return err
	}
	// the watchdog reads the new pods' disk metrics; they reach vm-single only after a scrape
	return poll(ctx, 3*time.Minute, "fresh backend metrics", func() (bool, error) {
		for _, sig := range spec.Signals {
			q := fmt.Sprintf(`min(%s{namespace=%q,pod=%q})`, sig.FreeDiskMetric, manifests.BackendNamespace, pods[sig.Backend].Name)
			_, ok, err := r.p.Value(ctx, q, time.Now())
			if err != nil || !ok {
				return false, err
			}
		}
		return true, nil
	})
}

func (r *Runner) applyCollector(ctx context.Context, s spec.RunSpec, relayConf []byte) error {
	svcs, err := manifests.PipelineServices(s)
	if err != nil {
		return err
	}
	objs := svcs
	if s.Health == spec.Faulty {
		fault, err := manifests.FaultBackend(s)
		if err != nil {
			return err
		}
		objs = append(objs, fault)
	}
	collector, err := manifests.CollectorObjects(s, relayConf)
	if err != nil {
		return err
	}
	return r.create(ctx, append(objs, collector...))
}

// waitCollector returns the startup time: pod Ready minus container start.
func (r *Runner) waitCollector(ctx context.Context) (float64, error) {
	var startup float64
	err := poll(ctx, 5*time.Minute, "collector Ready", func() (bool, error) {
		var pods corev1.PodList
		if err := r.k.List(ctx, &pods, client.InNamespace(manifests.LoadNamespace), client.MatchingLabels{"app.kubernetes.io/name": "otelcol"}); err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			ready, ok := readySince(p)
			if !ok || len(p.Status.ContainerStatuses) == 0 || p.Status.ContainerStatuses[0].State.Running == nil {
				continue
			}
			startup = ready.Sub(p.Status.ContainerStatuses[0].State.Running.StartedAt.Time).Seconds()
			return true, nil
		}
		return false, nil
	})
	return startup, err
}

func readySince(p corev1.Pod) (time.Time, bool) {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue {
			return c.LastTransitionTime.Time, true
		}
	}
	return time.Time{}, false
}

// waitDrain measures how long the exporter queues need to empty after the load stops,
// and always waits the full cool-down so the last scrapes land in vm-single.
func (r *Runner) waitDrain(ctx context.Context, s spec.RunSpec, loadStop time.Time) *float64 {
	var drained *float64
	end := loadStop.Add(r.cooldown)
	query := fmt.Sprintf(`sum(otelcol_exporter_queue_size{%s=%q})`, result.RunPromLabel, s.ID())
	for time.Now().Before(end) && ctx.Err() == nil {
		v, ok, err := r.p.Value(ctx, query, time.Now())
		if drained == nil && err == nil && ok && v == 0 {
			d := time.Since(loadStop).Seconds()
			drained = &d
		}
		time.Sleep(5 * time.Second)
	}
	return drained
}

func (r *Runner) create(ctx context.Context, objs []client.Object) error {
	for _, o := range objs {
		if err := r.k.Create(ctx, o); err != nil {
			return fmt.Errorf("create %T %s: %w", o, o.GetName(), err)
		}
	}
	return nil
}

func (r *Runner) delete(ctx context.Context, objs []client.Object) error {
	for _, o := range objs {
		if err := r.k.Delete(ctx, o, client.PropagationPolicy("Background")); client.IgnoreNotFound(err) != nil {
			return err
		}
	}
	return nil
}

// cleanup removes every object of the run.
func (r *Runner) cleanup(ctx context.Context, s spec.RunSpec) {
	if err := r.removeObjects(ctx, client.MatchingLabels(manifests.RunLabels(s))); err != nil {
		log.Printf("%s: cleanup incomplete: %v", s.ID(), err)
	}
}

// selector matches run objects: one run (MatchingLabels) or any run (HasLabels).
type selector interface {
	client.ListOption
	client.DeleteAllOfOption
}

// removeObjects deletes the matching objects in both namespaces and waits until their pods are gone.
func (r *Runner) removeObjects(ctx context.Context, labels selector) error {
	for _, ns := range []string{manifests.LoadNamespace, manifests.BackendNamespace} {
		r.deleteAll(ctx, ns, labels)
	}
	return poll(ctx, 3*time.Minute, "run objects removed", func() (bool, error) {
		var pods corev1.PodList
		err := r.k.List(ctx, &pods, labels)
		return len(pods.Items) == 0, err
	})
}

func (r *Runner) deleteAll(ctx context.Context, ns string, labels selector) {
	bg := client.PropagationPolicy("Background")
	scrape := &unstructured.Unstructured{}
	scrape.SetGroupVersionKind(schema.GroupVersionKind{Group: "operator.victoriametrics.com", Version: "v1beta1", Kind: "VMPodScrape"})
	for _, obj := range []client.Object{&appsv1.Deployment{}, &corev1.ConfigMap{}, &corev1.ServiceAccount{}, scrape} {
		if err := r.k.DeleteAllOf(ctx, obj, client.InNamespace(ns), labels, bg); err != nil && !apierrors.IsNotFound(err) {
			log.Printf("cleanup %T in %s: %v", obj, ns, err)
		}
	}
	// Services do not support delete-collection.
	var svcs corev1.ServiceList
	if err := r.k.List(ctx, &svcs, client.InNamespace(ns), labels); err != nil {
		log.Printf("cleanup services in %s: %v", ns, err)
		return
	}
	for i := range svcs.Items {
		if err := r.k.Delete(ctx, &svcs.Items[i]); client.IgnoreNotFound(err) != nil {
			log.Printf("cleanup service %s: %v", svcs.Items[i].Name, err)
		}
	}
}

func poll(ctx context.Context, timeout time.Duration, what string, done func() (bool, error)) error {
	deadline := time.Now().Add(timeout)
	for {
		ok, err := done()
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %s waiting for %s", timeout, what)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
}
