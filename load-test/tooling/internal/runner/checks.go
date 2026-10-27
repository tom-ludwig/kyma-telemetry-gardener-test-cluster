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
	rbacv1 "k8s.io/api/rbac/v1"
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
	if err := r.removeAll(ctx, client.HasLabels{manifests.RunLabel}, s.Target); err != nil {
		return nodes{}, fmt.Errorf("remove objects of an earlier run: %w", err)
	}
	n, err := r.checkNodes(ctx, s)
	if err != nil {
		return n, err
	}
	// Without it k8s_attributes only logs "forbidden" and passes data through unenriched.
	// The managed TM gateway has its own RBAC, so this only guards the otel collector.
	if s.Target == spec.TargetOTel {
		var binding rbacv1.ClusterRoleBinding
		if err := r.k.Get(ctx, types.NamespacedName{Name: "otelcol"}, &binding); err != nil {
			return n, fmt.Errorf("ClusterRoleBinding otelcol for k8s_attributes (deployed by Flux): %w", err)
		}
	}
	if r.skipPreflight {
		log.Printf("%s: preflight capacity and disk checks skipped (-skip-preflight)", s.ID())
		return n, nil
	}
	// vm-single stores our own otelcol_* metrics; it must keep headroom. No test backends to
	// check: both targets export to the nop debug collector.
	return n, r.checkVMSingleDisk(ctx)
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
	if r.skipPreflight {
		return n, nil
	}
	if err := r.checkCollectorNodeFree(ctx, n.collector); err != nil {
		return n, err
	}
	var pods int64 // 1 telemetrygen pod per gateway signal; agent generators run on the collector node
	for _, u := range s.Units() {
		if u.Component == spec.ComponentGateway {
			pods++
		}
	}
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

func podReady(p corev1.Pod) bool {
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// checkVMSingleDisk fails the run when the self-monitoring vm-single that stores our
// otelcol_* metrics is below the free-space floor.
func (r *Runner) checkVMSingleDisk(ctx context.Context) error {
	free, capacity, err := r.vmSingleDisk(ctx)
	if err != nil {
		return err
	}
	if free < minFreeDisk*capacity {
		return fmt.Errorf("vm-single has only %.0f MiB free", free/(1<<20))
	}
	return nil
}

// vmSingleDisk returns vm-single's free bytes and PVC capacity.
func (r *Runner) vmSingleDisk(ctx context.Context) (free, capacity float64, err error) {
	var claim corev1.PersistentVolumeClaim
	if err := r.k.Get(ctx, types.NamespacedName{Namespace: monitoringNamespace, Name: vmSinglePVC}, &claim); err != nil {
		return 0, 0, fmt.Errorf("PVC %s/%s: %w", monitoringNamespace, vmSinglePVC, err)
	}
	capacity = claim.Status.Capacity.Storage().AsApproximateFloat64()
	free, err = r.p.Required(ctx, fmt.Sprintf(`min(vm_free_disk_space_bytes{namespace=%q})`, monitoringNamespace), time.Now())
	return free, capacity, err
}

// watchdog checks every 30 s and returns the reason to stop the run; it records collector restarts.
func (r *Runner) watchdog(ctx context.Context, s spec.RunSpec, w result.Window, n nodes, watches map[string]*collectorWatch) <-chan string {
	stop := make(chan string, 1)
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			reason := r.watchdogCheck(ctx, s, w, n, watches)
			if reason == "" {
				continue
			}
			stop <- reason
			return
		}
	}()
	return stop
}

func (r *Runner) watchdogCheck(ctx context.Context, s spec.RunSpec, w result.Window, n nodes, watches map[string]*collectorWatch) string {
	free, capacity, err := r.vmSingleDisk(ctx)
	if err != nil || free < minFreeDisk*capacity {
		return "vm-single unreachable or disk below 5 % free"
	}
	r.observeCollectors(ctx, s, n, watches)
	if time.Now().Before(w.SteadyStart) {
		return ""
	}
	return r.checkGenerators(ctx, s, n)
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

// newWatches holds one collectorWatch per component under test.
func newWatches(s spec.RunSpec, start time.Time) map[string]*collectorWatch {
	watches := map[string]*collectorWatch{}
	for _, c := range s.Components() {
		watches[c] = &collectorWatch{start: start}
	}
	return watches
}

func (r *Runner) observeCollectors(ctx context.Context, s spec.RunSpec, n nodes, watches map[string]*collectorWatch) {
	for c, watch := range watches {
		var pods corev1.PodList
		if err := r.k.List(ctx, &pods, collectorPods(s, c, n)...); err != nil {
			log.Printf("watchdog: list %s pods: %v", c, err)
			continue
		}
		watch.observe(pods.Items, time.Now())
	}
}

// collectorPods selects the pods of one component under test. Managed TM components are
// shared and long-lived; they are watched by their fixed labels. Agents run on every node,
// only the one on the collector node gets load.
func collectorPods(s spec.RunSpec, component string, n nodes) []client.ListOption {
	if s.Target != spec.TargetTM {
		return []client.ListOption{client.InNamespace(manifests.LoadNamespace), client.MatchingLabels{"app.kubernetes.io/name": "otelcol"}}
	}
	opts := []client.ListOption{client.InNamespace(spec.TMNamespace), client.MatchingLabels{"app.kubernetes.io/name": spec.TMApps[component]}}
	if component == spec.ComponentGateway {
		return opts
	}
	return append(opts, client.MatchingFields{"spec.nodeName": n.collector})
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
	// Agent generators share the collector node; their rate is checked by a calibration run.
	for _, u := range s.Units() {
		if u.Component != spec.ComponentGateway {
			continue
		}
		sel, sig := result.SelectorsFor(s, u), u.Signal
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
