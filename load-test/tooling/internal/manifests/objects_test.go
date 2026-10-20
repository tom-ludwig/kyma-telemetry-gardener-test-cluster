package manifests

import (
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
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
		if limit != "4096Mi" {
			t.Errorf("memory limit %q, want 4096Mi", limit)
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

func TestGeneratorWorkers(t *testing.T) {
	for rate, want := range map[int][2]string{
		300:  {"--workers=1", "--rate=300"},
		1000: {"--workers=2", "--rate=500"},
		1001: {"--workers=3", "--rate=333.6666666666667"},
	} {
		s := spec.RunSpec{Shape: spec.ShapeF1, Tenants: 1, Load: spec.LoadB, Rate: rate, Health: spec.Healthy, Rep: 1}
		objs, err := Generators(s, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		script := objs[0].(*appsv1.Deployment).Spec.Template.Spec.Containers[0].Args[0]
		if !strings.Contains(script, want[0]) || !strings.Contains(script, want[1]) {
			t.Errorf("rate %d: script %q, want %v", rate, script, want)
		}
		if wantAttr := `tenant=\"$(printf 'tenant-%03d' $i)\"`; !strings.Contains(script, wantAttr) {
			t.Errorf("script missing %q", wantAttr)
		}
	}
}
