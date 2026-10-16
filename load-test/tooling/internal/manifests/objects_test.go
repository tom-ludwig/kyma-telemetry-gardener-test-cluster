package manifests

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
)

func TestCollectorObjects(t *testing.T) {
	s := spec.RunSpec{Shape: spec.ShapeF1, Tenants: 2, Load: spec.LoadA, Health: spec.Healthy, Rep: 1}
	objs, err := CollectorObjects(s, []byte("conf"))
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, o := range objs {
		if o.GetLabels()[RunLabel] != s.ID() {
			t.Errorf("%s %s has no run label", o.GetObjectKind().GroupVersionKind().Kind, o.GetName())
		}
		u, ok := o.(*unstructured.Unstructured)
		if !ok {
			kinds["ConfigMap"]++
			continue
		}
		kinds[u.GetKind()]++
		if u.GetKind() != "Deployment" {
			continue
		}
		containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
		limit, _, _ := unstructured.NestedString(containers[0].(map[string]any), "resources", "limits", "memory")
		if limit != "6750Mi" {
			t.Errorf("memory limit %q, want 6750Mi (750 + 3·2·1000)", limit)
		}
	}
	want := map[string]int{"ConfigMap": 1, "ServiceAccount": 1, "Deployment": 1, "Service": 2, "VMPodScrape": 1}
	for k, n := range want {
		if kinds[k] != n {
			t.Errorf("%s: got %d, want %d", k, kinds[k], n)
		}
	}
	if CollectorImage() != "otel/opentelemetry-collector-contrib:0.161.0" {
		t.Errorf("collector image %q", CollectorImage())
	}
}

func TestFaultyPipelineService(t *testing.T) {
	objs, err := PipelineServices(spec.RunSpec{Tenants: 3, Health: spec.Faulty, Rep: 1, Shape: spec.ShapeF1, Load: spec.LoadA})
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 9 {
		t.Fatalf("got %d services, want 9", len(objs))
	}
	faulty := 0
	for _, o := range objs {
		svc := o.(*corev1.Service)
		if svc.Spec.Selector["app"] != "fault-backend" {
			continue
		}
		faulty++
		if svc.Spec.Ports[0].TargetPort.IntVal != 4318 {
			t.Errorf("%s targets port %d, want 4318", svc.Name, svc.Spec.Ports[0].TargetPort.IntVal)
		}
	}
	if faulty != 3 {
		t.Errorf("services on the fault backend: %d, want 3 (p-000 of each signal)", faulty)
	}
}
