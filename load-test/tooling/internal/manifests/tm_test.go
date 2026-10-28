package manifests

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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

func TestGeneratorEndpointTM(t *testing.T) {
	s := spec.RunSpec{Target: spec.TargetTM, Shape: spec.ShapeF1, Tenants: 1, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := Generators(s, time.Minute, nil)
	if err != nil {
		t.Fatal(err)
	}
	script := objs[0].(*appsv1.Deployment).Spec.Template.Spec.Containers[0].Args[0]
	if !strings.Contains(script, spec.TMGatewayEndpoint) {
		t.Errorf("tm generator script must target %q, got %q", spec.TMGatewayEndpoint, script)
	}
}
