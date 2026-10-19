package runner

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/manifests"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	monitoringNamespace = "monitoring"
	vmSinglePVC         = "vmsingle-vm-single"
	minFreeDisk         = 0.05
)

type nodes struct {
	collector string
	load      []string
	env       result.Env
}

// preflight stops a run before anything is created when the cluster cannot carry it.
// Objects of an earlier, killed run are removed first, so the collector node is free again.
func (r *Runner) preflight(ctx context.Context, s spec.RunSpec) (nodes, error) {
	if err := r.removeObjects(ctx, client.HasLabels{manifests.RunLabel}); err != nil {
		return nodes{}, fmt.Errorf("remove objects of an earlier run: %w", err)
	}
	n, err := r.checkNodes(ctx, s)
	if err != nil {
		return n, err
	}
	if err := r.checkBackendsReady(ctx); err != nil {
		return n, err
	}
	return n, r.checkDisks(ctx, s)
}

func (r *Runner) checkNodes(ctx context.Context, s spec.RunSpec) (nodes, error) {
	var n nodes
	var list corev1.NodeList
	if err := r.k.List(ctx, &list, client.HasLabels{manifests.RoleLabel}); err != nil {
		return n, err
	}
	loadAlloc, loadUsed := corev1.ResourceList{}, corev1.ResourceList{}
	for _, node := range list.Items {
		if node.Labels[manifests.RoleLabel] == "collector" {
			n.collector = node.Name
			n.env.CollectorNode = nodeInfo(node)
			warnMemory(s, node)
			continue
		}
		if node.Labels[manifests.RoleLabel] != "load" {
			continue
		}
		n.load = append(n.load, node.Name)
		n.env.LoadNodes = append(n.env.LoadNodes, nodeInfo(node))
		add(loadAlloc, node.Status.Allocatable)
		used, err := r.requestsOn(ctx, node.Name)
		if err != nil {
			return n, err
		}
		add(loadUsed, used)
	}
	if n.collector == "" || len(n.load) == 0 {
		return n, fmt.Errorf("need one node labelled %s=collector and at least one %s=load", manifests.RoleLabel, manifests.RoleLabel)
	}
	if err := r.checkCollectorNodeFree(ctx, n.collector); err != nil {
		return n, err
	}
	pods := int64(len(spec.Signals)) // 1 pod per signal
	need := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(pods*r.genCPU.MilliValue(), resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(pods*r.genMemory.Value(), resource.BinarySI),
	}
	for name, q := range need {
		free := loadAlloc[name].DeepCopy()
		free.Sub(loadUsed[name])
		if free.Cmp(q) < 0 {
			return n, fmt.Errorf("load nodes have %s %s free, generators need %s", free.String(), name, q.String())
		}
	}
	return n, nil
}

func nodeInfo(node corev1.Node) result.NodeInfo {
	return result.NodeInfo{
		Name:        node.Name,
		MachineType: node.Labels[corev1.LabelInstanceTypeStable],
		CPU:         node.Status.Allocatable.Cpu().AsApproximateFloat64(),
		MemoryGiB:   node.Status.Allocatable.Memory().AsApproximateFloat64() / (1 << 30),
	}
}

func warnMemory(s spec.RunSpec, node corev1.Node) {
	limit := resource.MustParse(fmt.Sprintf("%dMi", s.MemoryLimitMi()))
	if limit.Cmp(*node.Status.Allocatable.Memory()) <= 0 {
		return
	}
	log.Printf("warning: collector memory limit %s is above node %s allocatable %s; node memory is the real limit",
		limit.String(), node.Name, node.Status.Allocatable.Memory().String())
}

// checkCollectorNodeFree: only DaemonSet pods may run next to the collector.
func (r *Runner) checkCollectorNodeFree(ctx context.Context, node string) error {
	var pods corev1.PodList
	if err := r.k.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node}); err != nil {
		return err
	}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed || ownedByDaemonSet(p) {
			continue
		}
		return fmt.Errorf("collector node %s runs %s/%s; only DaemonSet pods are allowed there", node, p.Namespace, p.Name)
	}
	return nil
}

func ownedByDaemonSet(p corev1.Pod) bool {
	for _, o := range p.OwnerReferences {
		if o.Kind == "DaemonSet" {
			return true
		}
	}
	return false
}

func (r *Runner) requestsOn(ctx context.Context, node string) (corev1.ResourceList, error) {
	var pods corev1.PodList
	if err := r.k.List(ctx, &pods, client.MatchingFields{"spec.nodeName": node}); err != nil {
		return nil, err
	}
	sum := corev1.ResourceList{}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, c := range p.Spec.Containers {
			add(sum, c.Resources.Requests)
		}
	}
	return sum, nil
}

func add(sum, l corev1.ResourceList) {
	for name, q := range l {
		cur := sum[name]
		cur.Add(q)
		sum[name] = cur
	}
}

func (r *Runner) backendPods(ctx context.Context) (map[string]corev1.Pod, error) {
	var pods corev1.PodList
	if err := r.k.List(ctx, &pods, client.InNamespace(manifests.BackendNamespace), client.MatchingLabels{"app.kubernetes.io/instance": "load-test"}); err != nil {
		return nil, err
	}
	byBackend := map[string]corev1.Pod{}
	for _, p := range pods.Items {
		if p.DeletionTimestamp != nil {
			continue
		}
		byBackend[p.Labels["app.kubernetes.io/name"]] = p
	}
	return byBackend, nil
}

func (r *Runner) checkBackendsReady(ctx context.Context) error {
	pods, err := r.backendPods(ctx)
	if err != nil {
		return err
	}
	for _, sig := range spec.Signals {
		p, ok := pods[sig.Backend]
		if !ok || !podReady(p) {
			return fmt.Errorf("backend %s-load-test is not ready", sig.Backend)
		}
	}
	return nil
}

func podReady(p corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// checkDisks: each backend must hold this run's data, vm-single must keep 5 % free.
func (r *Runner) checkDisks(ctx context.Context, s spec.RunSpec) error {
	seconds := (r.warmup + r.steady).Seconds()
	for _, sig := range spec.Signals {
		free, _, err := r.freeDisk(ctx, manifests.BackendNamespace, sig.Backend+"-load-test", sig.FreeDiskMetric)
		if err != nil {
			return err
		}
		need := float64(s.OfferedRate(sig)*sig.StoredBytes) * seconds
		if free < need {
			return fmt.Errorf("%s-load-test has %.1f GiB free, run needs about %.1f GiB", sig.Backend, free/(1<<30), need/(1<<30))
		}
	}
	free, capacity, err := r.freeDisk(ctx, monitoringNamespace, vmSinglePVC, "vm_free_disk_space_bytes")
	if err != nil {
		return err
	}
	if free < minFreeDisk*capacity {
		return fmt.Errorf("vm-single has only %.0f MiB free", free/(1<<20))
	}
	return nil
}

// freeDisk returns free bytes from the backend's own metric and the PVC capacity.
func (r *Runner) freeDisk(ctx context.Context, ns, pvc, metric string) (free, capacity float64, err error) {
	var claim corev1.PersistentVolumeClaim
	if err := r.k.Get(ctx, types.NamespacedName{Namespace: ns, Name: pvc}, &claim); err != nil {
		return 0, 0, fmt.Errorf("PVC %s/%s: %w", ns, pvc, err)
	}
	capacity = claim.Status.Capacity.Storage().AsApproximateFloat64()
	free, err = r.p.Required(ctx, fmt.Sprintf(`min(%s{namespace=%q})`, metric, ns), time.Now())
	return free, capacity, err
}

// watchdog checks every 30 s and returns the reason to stop the run; it records collector restarts.
func (r *Runner) watchdog(ctx context.Context, s spec.RunSpec, w result.Window, n nodes, watch *collectorWatch) <-chan string {
	stop := make(chan string, 1)
	baseline, _ := r.backendPods(ctx)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			reason := r.watchdogCheck(ctx, s, w, n, baseline, watch)
			if reason == "" {
				continue
			}
			stop <- reason
			return
		}
	}()
	return stop
}

func (r *Runner) watchdogCheck(ctx context.Context, s spec.RunSpec, w result.Window, n nodes, baseline map[string]corev1.Pod, watch *collectorWatch) string {
	if reason := r.checkBackendsStable(ctx, baseline); reason != "" {
		return reason
	}
	for _, sig := range spec.Signals {
		free, capacity, err := r.freeDisk(ctx, manifests.BackendNamespace, sig.Backend+"-load-test", sig.FreeDiskMetric)
		if err != nil {
			return "vm-single unreachable: " + err.Error()
		}
		if free < minFreeDisk*capacity {
			return fmt.Sprintf("%s-load-test disk below 5 %% free", sig.Backend)
		}
	}
	free, capacity, err := r.freeDisk(ctx, monitoringNamespace, vmSinglePVC, "vm_free_disk_space_bytes")
	if err != nil || free < minFreeDisk*capacity {
		return "vm-single unreachable or disk below 5 % free"
	}
	r.observeCollector(ctx, watch)
	if time.Now().Before(w.SteadyStart) {
		return ""
	}
	return r.checkGenerators(ctx, s, n)
}

func (r *Runner) checkBackendsStable(ctx context.Context, baseline map[string]corev1.Pod) string {
	pods, err := r.backendPods(ctx)
	if err != nil {
		return "cannot list backend pods: " + err.Error()
	}
	for _, sig := range spec.Signals {
		p, ok := pods[sig.Backend]
		if !ok || !podReady(p) || p.UID != baseline[sig.Backend].UID || podRestarts(p) > podRestarts(baseline[sig.Backend]) {
			return fmt.Sprintf("backend %s-load-test restarted or not ready", sig.Backend)
		}
	}
	return ""
}

func podRestarts(p corev1.Pod) int32 {
	var n int32
	for _, c := range p.Status.ContainerStatuses {
		n += c.RestartCount
	}
	return n
}

// collectorWatch records collector restarts, OOM kills and evictions from the pod status;
// cadvisor starts a new series per container start and misses them.
type collectorWatch struct {
	mu               sync.Mutex
	start            time.Time
	restarts         int32
	evicted          bool
	restartAt, oomAt *float64
}

func (r *Runner) observeCollector(ctx context.Context, watch *collectorWatch) {
	var pods corev1.PodList
	if err := r.k.List(ctx, &pods, client.InNamespace(manifests.LoadNamespace), client.MatchingLabels{"app.kubernetes.io/name": "otelcol"}); err != nil {
		log.Printf("watchdog: list collector pods: %v", err)
		return
	}
	watch.observe(pods.Items, time.Now())
}

func (c *collectorWatch) observe(pods []corev1.Pod, now time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := now.Sub(c.start).Seconds()
	var restarts int32
	for _, p := range pods {
		restarts += podRestarts(p)
		c.evicted = c.evicted || p.Status.Reason == "Evicted"
		if c.oomAt == nil && oomKilled(p) {
			c.oomAt = &at
		}
	}
	c.restarts = max(c.restarts, restarts)
	if c.restartAt == nil && c.restarts > 0 {
		c.restartAt = &at
	}
}

func (c *collectorWatch) apply(res *result.CollectorResult) {
	c.mu.Lock()
	defer c.mu.Unlock()
	res.Restarts, res.OOMKilled, res.Evicted = c.restarts, c.oomAt != nil, c.evicted
	res.RestartAtS, res.OOMAtS = c.restartAt, c.oomAt
}

func oomKilled(p corev1.Pod) bool {
	for _, c := range p.Status.ContainerStatuses {
		if t := c.LastTerminationState.Terminated; t != nil && t.Reason == "OOMKilled" {
			return true
		}
		if t := c.State.Terminated; t != nil && t.Reason == "OOMKilled" {
			return true
		}
	}
	return false
}

// checkGenerators stops the run when the generators, not the collector, limit the load.
func (r *Runner) checkGenerators(ctx context.Context, s spec.RunSpec, n nodes) string {
	loadCPU, _, err := r.p.Value(ctx, fmt.Sprintf(`max(1 - avg by (nodename) (rate(node_cpu_seconds_total{mode="idle",nodename=~%q}[1m])))`,
		strings.Join(n.load, "|")), time.Now())
	if err != nil {
		return ""
	}
	for _, sig := range spec.Signals {
		sel := result.SelectorsFor(s, sig)
		received, ok, err := r.p.Value(ctx, fmt.Sprintf(`sum(rate(otelcol_receiver_accepted_%s_total{%s}[1m]))`, sig.Item, sel.Run), time.Now())
		if err != nil || !ok {
			continue
		}
		refused, _, _ := r.p.Value(ctx, fmt.Sprintf(`sum(rate(otelcol_receiver_refused_%s_total{%s}[1m]))`, sig.Item, sel.Run), time.Now())
		if generatorsTooSlow(received, float64(s.OfferedRate(sig)), refused, loadCPU) {
			return fmt.Sprintf("%s generators send %.0f/s of planned %d/s on a saturated load node (CPU %.0f %%)",
				sig.Name, received, s.OfferedRate(sig), 100*loadCPU)
		}
	}
	return ""
}

// generatorsTooSlow: the collector gets < 95 % of the planned rate, refuses nothing and the
// load node is saturated. A CPU-bound collector also slows its senders; then the load node
// is not saturated and the shortfall is a collector result (H3/H4), not a broken run.
func generatorsTooSlow(received, offered, refused, loadNodeCPU float64) bool {
	return received < 0.95*offered && refused == 0 && loadNodeCPU >= 0.9
}
