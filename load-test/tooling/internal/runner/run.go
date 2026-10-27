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
	args                     []string // flags as passed, for the result record
	genCPU, genMemory        resource.Quantity
	done                     map[string]*result.Record // records of this process by run ID
	runFn                    func(context.Context, spec.RunSpec) (*result.Record, error)
	skipPreflight            bool
}

func New(k client.Client, p result.Prom, warmup, steady, cooldown time.Duration, args []string, skipPreflight bool) (*Runner, error) {
	cpu, memory, err := manifests.GeneratorRequests()
	if err != nil {
		return nil, err
	}
	r := &Runner{
		k: k, p: p, warmup: warmup, steady: steady, cooldown: cooldown, args: args,
		genCPU: cpu, genMemory: memory, done: map[string]*result.Record{},
		skipPreflight: skipPreflight,
	}
	r.runFn = r.Run
	return r, nil
}

// Run executes one run and prints its result record.
func (r *Runner) Run(ctx context.Context, s spec.RunSpec) (*result.Record, error) {
	var configParts [][]byte
	var stats manifests.ConfigStats
	if s.Target == spec.TargetOTel {
		var err error
		configParts, stats, err = manifests.BuildConfig(s)
		if err != nil {
			return nil, err
		}
	}
	n, err := r.preflight(ctx, s)
	if err != nil {
		return nil, fmt.Errorf("preflight: %w", err)
	}
	defer r.cleanup(context.WithoutCancel(ctx), s)

	var startup float64
	if s.Target == spec.TargetTM {
		if err := r.applyTM(ctx, s); err != nil {
			return nil, err
		}
		if err := r.waitPipelines(ctx, s); err != nil {
			return nil, err
		}
	} else {
		if err := r.applyCollector(ctx, s, configParts); err != nil {
			return nil, err
		}
		if err := r.waitDebugBackend(ctx, manifests.BackendNamespace); err != nil {
			return nil, err
		}
		if startup, err = r.waitCollector(ctx); err != nil {
			return nil, err
		}
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

	rec, err := r.newRecord(s, stats, startup, w, n)
	if err != nil {
		return nil, err
	}
	watches := newWatches(s, start)
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	if reason := waitOrStop(ctx, r.watchdog(watchCtx, s, w, n, watches), w.SteadyEnd); reason != "" {
		return rec, r.invalid(rec, reason)
	}
	stopWatch()

	if err := r.delete(ctx, gens); err != nil {
		return nil, err
	}
	drain := r.waitDrain(ctx, s, w.SteadyEnd)
	r.observeCollectors(ctx, s, n, watches) // restarts after the last watchdog tick
	w.End = time.Now()
	rec.Run.End = w.End

	if err := r.fill(ctx, s, w, n, rec, watches, drain); err != nil {
		return nil, err
	}
	return rec, result.PrintRecord(rec)
}

// newRecord holds the run's inputs; the measured values are filled in later.
func (r *Runner) newRecord(s spec.RunSpec, stats manifests.ConfigStats, startup float64, w result.Window, n nodes) (*result.Record, error) {
	genImage, err := manifests.GeneratorImage()
	if err != nil {
		return nil, err
	}
	collectorImage := ""
	if s.Target == spec.TargetOTel {
		collectorImage = manifests.CollectorImage()
	}
	rec := &result.Record{
		Run: result.RunInfo{ID: s.ID(), Args: r.args, ToolCommit: result.ToolCommit, Date: time.Now().UTC(),
			CollectorImage: collectorImage, Start: w.Start, SteadyStart: w.SteadyStart, SteadyEnd: w.SteadyEnd},
		Setup: result.Setup{Target: s.Target, Component: s.Components()[0], Shape: s.Shape, Load: s.Load, Tenants: s.Tenants, Health: s.Health,
			RatePerTenant: map[string]int{}, OfferedPerS: map[string]int{},
			MemLimitMiB: s.MemoryLimitMi(), QueueSizePerExporter: s.QueueSize(),
			PhasesS:   result.Phases{Warmup: r.warmup.Seconds(), Steady: r.steady.Seconds(), Cooldown: r.cooldown.Seconds()},
			Generator: result.GeneratorSetup{Image: genImage, WorkersPerTenant: map[string]int{}}},
		Env:     n.env,
		Config:  result.ConfigRecord{ConfigStats: stats, StartupS: startup},
		Signals: map[string]result.SignalResult{},
		Limit:   result.Limits{All: []result.Hit{}},
		Valid:   true,
	}
	for _, u := range s.Units() {
		rec.Setup.RatePerTenant[u.Key()] = s.TenantRate(u.Signal)
		rec.Setup.OfferedPerS[u.Key()] = s.OfferedRate(u.Signal)
		rec.Setup.Generator.LogBodyBytes = max(rec.Setup.Generator.LogBodyBytes, u.Signal.BodyBytes)
		if u.Component == spec.ComponentGateway {
			rec.Setup.Generator.WorkersPerTenant[u.Key()] = manifests.Workers(s.TenantRate(u.Signal))
		}
	}
	if s.Health != spec.Faulty {
		return rec, nil
	}
	fault, err := faultSetup(s)
	if err != nil {
		return nil, err
	}
	rec.Setup.Fault = &fault
	return rec, nil
}

func faultSetup(s spec.RunSpec) (result.FaultSetup, error) {
	d, err := manifests.FaultBackend(s)
	if err != nil {
		return result.FaultSetup{}, err
	}
	c := d.Spec.Template.Spec.Containers[0]
	f := result.FaultSetup{Image: c.Image}
	for _, e := range c.Env {
		switch e.Name {
		case "FAULT_RULES":
			f.Rules = e.Value
		case "FAULT_DELAYS":
			f.Delays = e.Value
		}
	}
	return f, nil
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
func (r *Runner) fill(ctx context.Context, s spec.RunSpec, w result.Window, n nodes, rec *result.Record, watches map[string]*collectorWatch, drain *float64) error {
	for _, c := range s.Components() {
		collector, err := r.p.QueryCollector(ctx, s, c, w)
		if err != nil {
			return err
		}
		collector.QueueDrainS = drain
		watches[c].apply(&collector)
		rec.SetCollector(s, c, collector)
	}
	for _, u := range s.Units() {
		res, err := r.p.QuerySignal(ctx, s, u, w)
		if err != nil {
			return err
		}
		rec.Signals[u.Key()] = res
	}
	if err := r.p.EvalLimits(ctx, s, w, rec, n.env.CollectorNode.CPU); err != nil {
		return err
	}
	// S5 isolation compares a faulty run with its healthy baseline.
	if s.Target == spec.TargetOTel {
		healthy := s
		healthy.Health, healthy.Rep = spec.Healthy, 1
		result.AddIsolation(rec, s, r.done[healthy.ID()], w)
	}
	return nil
}

// waitDebugBackend waits until the per-run nop debug collector pod is Ready in ns, so the
// collector under test has somewhere to export to before load starts.
func (r *Runner) waitDebugBackend(ctx context.Context, ns string) error {
	return poll(ctx, 5*time.Minute, "debug backend Ready", func() (bool, error) {
		var pods corev1.PodList
		if err := r.k.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{"app": manifests.DebugBackendName}); err != nil {
			return false, err
		}
		for _, p := range pods.Items {
			if p.DeletionTimestamp == nil && podReady(p) {
				return true, nil
			}
		}
		return false, nil
	})
}

// applyTM deploys the per-run nop debug backend and the Telemetry Manager pipeline CRs.
// In faulty runs the fault backend is also deployed; tenant 0's pipelines target it.
func (r *Runner) applyTM(ctx context.Context, s spec.RunSpec) error {
	pipelines, err := manifests.TMPipelineObjects(s)
	if err != nil {
		return err
	}
	objs := manifests.DebugBackendObjects(s, spec.TMBackendNamespace)
	if s.Health == spec.Faulty {
		fault, err := manifests.FaultBackendInNamespace(s, spec.TMBackendNamespace)
		if err != nil {
			return err
		}
		faultSvc, err := manifests.FaultBackendService(s, spec.TMBackendNamespace)
		if err != nil {
			return err
		}
		objs = append(objs, fault, faultSvc)
	}
	sm, err := manifests.GatewayServiceMonitor()
	if err != nil {
		return err
	}
	objs = append(objs, sm)
	return r.create(ctx, append(objs, pipelines...))
}

// waitPipelines waits until every pipeline CR reports ConfigurationGenerated=True and the
// debug backend is Ready, so the gateway has the run's config before load starts.
func (r *Runner) waitPipelines(ctx context.Context, s spec.RunSpec) error {
	pipelines, err := manifests.TMPipelineObjects(s)
	if err != nil {
		return err
	}
	return poll(ctx, 5*time.Minute, "pipelines ConfigurationGenerated", func() (bool, error) {
		for _, p := range pipelines {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(p.GetObjectKind().GroupVersionKind())
			if err := r.k.Get(ctx, client.ObjectKeyFromObject(p), u); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			if !conditionTrue(u, "ConfigurationGenerated") {
				return false, nil
			}
		}
		return true, nil
	})
}

// conditionTrue reports whether status.conditions has type t with status "True".
func conditionTrue(u *unstructured.Unstructured, t string) bool {
	conds, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if m["type"] == t && m["status"] == "True" {
			return true
		}
	}
	return false
}

// applyCollector deploys the collector under test, its per-pipeline Services, the per-run
// nop debug backend they export to, and the fault backend for faulty runs.
func (r *Runner) applyCollector(ctx context.Context, s spec.RunSpec, configParts [][]byte) error {
	svcs, err := manifests.PipelineServices(s)
	if err != nil {
		return err
	}
	objs := append(svcs, manifests.DebugBackendObjects(s, manifests.BackendNamespace)...)
	if s.Health == spec.Faulty {
		fault, err := manifests.FaultBackend(s)
		if err != nil {
			return err
		}
		objs = append(objs, fault)
	}
	collector, err := manifests.CollectorObjects(s, configParts)
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
		if err := r.k.Patch(ctx, o, client.Apply, client.ForceOwnership, client.FieldOwner("loadtest")); err != nil {
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
	if err := r.removeAll(ctx, client.MatchingLabels(manifests.RunLabels(s)), s.Target); err != nil {
		log.Printf("%s: cleanup incomplete: %v", s.ID(), err)
	}
}

// selector matches run objects: one run (MatchingLabels) or any run (HasLabels).
type selector interface {
	client.ListOption
	client.DeleteAllOfOption
}

// tmPipelineGVKs are the cluster-scoped Telemetry Manager pipeline kinds the tool creates.
var tmPipelineGVKs = []schema.GroupVersionKind{
	{Group: "telemetry.kyma-project.io", Version: "v1beta1", Kind: "LogPipeline"},
	{Group: "telemetry.kyma-project.io", Version: "v1beta1", Kind: "MetricPipeline"},
	{Group: "telemetry.kyma-project.io", Version: "v1beta1", Kind: "TracePipeline"},
}

// removeAll removes run objects for the target: the otel namespaces always, plus the tm
// debug-backend namespace and the cluster-scoped pipeline CRs when the target is tm.
func (r *Runner) removeAll(ctx context.Context, labels selector, target string) error {
	if target == spec.TargetTM {
		bg := client.PropagationPolicy("Background")
		for _, gvk := range tmPipelineGVKs {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(gvk)
			if err := r.k.DeleteAllOf(ctx, u, labels, bg); err != nil && !apierrors.IsNotFound(err) {
				log.Printf("cleanup %s: %v", gvk.Kind, err)
			}
		}
		r.deleteAll(ctx, spec.TMBackendNamespace, labels)
	}
	return r.removeObjects(ctx, labels)
}

// removeObjects deletes the matching objects in the otel namespaces and waits until their pods are gone.
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
