package manifests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
)

func TestTMPipelineObjects(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Shape: spec.ShapeF1, Tenants: 2, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := TMPipelineObjects(s)
	if err != nil {
		t.Fatal(err)
	}
	// 3 signals × 2 tenants
	if len(objs) != 6 {
		t.Fatalf("got %d pipelines, want 6", len(objs))
	}
	kinds := map[string]int{}
	for _, o := range objs {
		u := o.(*unstructured.Unstructured)
		kinds[u.GetKind()]++
		if u.GetLabels()[RunLabel] != s.ID() {
			t.Errorf("%s %s has no run label", u.GetKind(), u.GetName())
		}
		if u.GetAPIVersion() != tmAPIVersion {
			t.Errorf("%s apiVersion %q", u.GetName(), u.GetAPIVersion())
		}
		filter, _, _ := unstructured.NestedSlice(u.Object, "spec", "filter")
		conds := filter[0].(map[string]any)["conditions"].([]any)
		if !strings.Contains(conds[0].(string), `resource.attributes["tenant"] !=`) {
			t.Errorf("%s filter %q", u.GetName(), conds[0])
		}
		proto, _, _ := unstructured.NestedString(u.Object, "spec", "output", "otlp", "protocol")
		if proto != "http" {
			t.Errorf("%s output protocol %q, want http", u.GetName(), proto)
		}
	}
	want := map[string]int{"LogPipeline": 2, "MetricPipeline": 2, "TracePipeline": 2}
	for k, n := range want {
		if kinds[k] != n {
			t.Errorf("%s: got %d, want %d", k, kinds[k], n)
		}
	}
}

func TestTMLogAgentPipeline(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentLogAgent, Shape: spec.ShapeF1, Tenants: 2, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := TMPipelineObjects(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("got %d pipelines, want 2 LogPipelines only", len(objs))
	}
	u := objs[1].(*unstructured.Unstructured)
	if u.GetKind() != "LogPipeline" {
		t.Fatalf("kind %s", u.GetKind())
	}
	filter, _, _ := unstructured.NestedSlice(u.Object, "spec", "filter")
	if cond := filter[0].(map[string]any)["conditions"].([]any)[0]; cond != `log.attributes["tenant"] != "tenant-001"` {
		t.Errorf("filter %q", cond)
	}
	otlp, _, _ := unstructured.NestedBool(u.Object, "spec", "input", "otlp", "enabled")
	runtime, _, _ := unstructured.NestedBool(u.Object, "spec", "input", "runtime", "enabled")
	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "input", "runtime", "containers", "include")
	if otlp || !runtime || len(containers) != 1 || containers[0] != LogGeneratorContainer {
		t.Errorf("input %v", u.Object["spec"].(map[string]any)["input"])
	}
}

func TestServiceMonitorPerComponent(t *testing.T) {
	sm, err := ServiceMonitor("telemetry-log-agent")
	if err != nil {
		t.Fatal(err)
	}
	u := sm.(*unstructured.Unstructured)
	sel, _, _ := unstructured.NestedStringMap(u.Object, "spec", "selector", "matchLabels")
	if u.GetName() != "telemetry-log-agent" || sel["app.kubernetes.io/name"] != "telemetry-log-agent" {
		t.Errorf("name %q selector %v", u.GetName(), sel)
	}
}

func TestLogAgentGenerator(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentLogAgent, Shape: spec.ShapeF1, Tenants: 3, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := Generators(s, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 1 {
		t.Fatalf("got %d generators, want 1", len(objs))
	}
	d := objs[0].(*appsv1.Deployment)
	if d.Spec.Template.Spec.NodeSelector[RoleLabel] != "collector" || d.Spec.Template.Labels[RunLabel] != s.ID() {
		t.Errorf("placement %v labels %v", d.Spec.Template.Spec.NodeSelector, d.Spec.Template.Labels)
	}
	script := d.Spec.Template.Spec.Containers[0].Args[0]
	logs := spec.Unit{Component: spec.ComponentLogAgent, Signal: spec.Signals[2]}
	for _, want := range []string{"seq 0 2", fmt.Sprintf("-b %d -r %d", logs.Signal.BodyBytes, s.TenantRate(logs)), `"tenant=$(printf 'tenant-%03d' $i)"`} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
}

func TestTMMetricAgentPipeline(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentMetricAgent, Shape: spec.ShapeF1, Tenants: 2, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := TMPipelineObjects(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 2 {
		t.Fatalf("got %d pipelines, want 2 MetricPipelines only", len(objs))
	}
	u := objs[1].(*unstructured.Unstructured)
	filter, _, _ := unstructured.NestedSlice(u.Object, "spec", "filter")
	if cond := filter[0].(map[string]any)["conditions"].([]any)[0]; cond != `resource.attributes["server.port"] != "9002"` {
		t.Errorf("filter %q", cond)
	}
	prom, _, _ := unstructured.NestedBool(u.Object, "spec", "input", "prometheus", "enabled")
	otlp, _, _ := unstructured.NestedBool(u.Object, "spec", "input", "otlp", "enabled")
	if !prom || otlp {
		t.Errorf("input %v", u.Object["spec"].(map[string]any)["input"])
	}
}

func TestMetricAgentGenerator(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentMetricAgent, Shape: spec.ShapeF1, Tenants: 3, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := Generators(s, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// 1 Deployment + 1 Service per tenant
	if len(objs) != 4 {
		t.Fatalf("got %d objects, want 4", len(objs))
	}
	d := objs[0].(*appsv1.Deployment)
	if len(d.Spec.Template.Annotations) != 0 {
		t.Errorf("generator pod must not be annotated: %v", d.Spec.Template.Annotations)
	}
	// 9000 dp/s over 3 tenants, 30 s scrapes, 10 gauges: 9000 series per tenant
	script := d.Spec.Template.Spec.Containers[0].Args[0]
	for _, want := range []string{"seq 0 2", "--port=$((9001 + i))", "--series-count=9000", "--series-interval=0"} {
		if !strings.Contains(script, want) {
			t.Errorf("script lacks %q:\n%s", want, script)
		}
	}
	svc := objs[3].(*corev1.Service)
	if svc.Annotations["prometheus.io/port"] != "9003" || svc.Spec.Selector["app"] != "gen-metric-agent" || svc.Labels[RunLabel] != s.ID() {
		t.Errorf("service %s: annotations %v selector %v labels %v", svc.Name, svc.Annotations, svc.Spec.Selector, svc.Labels)
	}
}

// A combined run serves gateway and agent data through one pipeline per signal and tenant.
func TestTMAllPipelines(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Component: spec.ComponentAll, Shape: spec.ShapeF1, Tenants: 2, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := TMPipelineObjects(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 6 {
		t.Fatalf("got %d pipelines, want 6", len(objs))
	}
	want := map[string]string{
		"logs-p-000":    `resource.attributes["tenant"] != "tenant-000" and log.attributes["tenant"] != "tenant-000"`,
		"metrics-p-000": `resource.attributes["tenant"] != "tenant-000" and resource.attributes["server.port"] != "9001"`,
		"traces-p-000":  `resource.attributes["tenant"] != "tenant-000"`,
	}
	for _, o := range objs {
		u := o.(*unstructured.Unstructured)
		cond, ok := want[u.GetName()]
		if !ok {
			continue
		}
		filter, _, _ := unstructured.NestedSlice(u.Object, "spec", "filter")
		if got := filter[0].(map[string]any)["conditions"].([]any)[0]; got != cond {
			t.Errorf("%s filter %q, want %q", u.GetName(), got, cond)
		}
		otlp, found, _ := unstructured.NestedBool(u.Object, "spec", "input", "otlp", "enabled")
		if found && !otlp {
			t.Errorf("%s: OTLP input off in a combined run", u.GetName())
		}
	}
	gens, err := Generators(s, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	// 3 telemetrygen + log generator + metric generator + 2 metric Services
	if len(gens) != 7 {
		t.Errorf("got %d generator objects, want 7", len(gens))
	}
}

func TestGeneratorEndpointTM(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Shape: spec.ShapeF1, Tenants: 1, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := Generators(s, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	script := objs[0].(*appsv1.Deployment).Spec.Template.Spec.Containers[0].Args[0]
	if !strings.Contains(script, spec.TMGatewayEndpoint) {
		t.Errorf("tm generator script must target %q, got %q", spec.TMGatewayEndpoint, script)
	}
}
