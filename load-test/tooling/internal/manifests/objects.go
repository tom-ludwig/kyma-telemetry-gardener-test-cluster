package manifests

import (
	"bytes"
	"errors"
	"io"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
	"k8s.io/apimachinery/pkg/api/resource"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"

	"embed"
	"fmt"
	"maps"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

//go:embed collector.yaml generator.yaml fault-backend.yaml pipeline-service.yaml
var manifestsFS embed.FS

const (
	LoadNamespace = "otel-load-test"
	RunLabel      = "loadtest.kyma-project.io/run"
	RoleLabel     = "load-test/role"
	tenantsPerPod = 10
	// items/s per telemetrygen worker, well below its measured ~1600/s ceiling
	itemsPerWorker = 500
)

func RunLabels(s spec.RunSpec) map[string]string { return map[string]string{RunLabel: s.ID()} }

// readTemplate decodes one embedded single-object manifest into obj.
func readTemplate(path string, obj any) error {
	raw, err := manifestsFS.ReadFile(path)
	if err != nil {
		return err
	}
	if err := yaml.UnmarshalStrict(raw, obj); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// CollectorObjects returns the static collector manifests with the per-run values set:
// the generated config, the run label and the TM memory limit.
func CollectorObjects(s spec.RunSpec, relayConf []byte) ([]client.Object, error) {
	raw, err := manifestsFS.ReadFile("collector.yaml")
	if err != nil {
		return nil, err
	}
	objs := []client.Object{&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "otelcol", Namespace: LoadNamespace, Labels: RunLabels(s)},
		Data:       map[string]string{"relay.conf": string(relayConf)},
	}}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(raw), 4096)
	for {
		u := &unstructured.Unstructured{}
		err := decoder.Decode(&u.Object)
		if errors.Is(err, io.EOF) {
			return objs, nil
		}
		if err != nil {
			return nil, fmt.Errorf("collector.yaml: %w", err)
		}
		if len(u.Object) == 0 {
			continue
		}
		u.SetLabels(merge(u.GetLabels(), RunLabels(s)))
		if err := setCollectorRunValues(u, s); err != nil {
			return nil, fmt.Errorf("collector.yaml %s: %w", u.GetKind(), err)
		}
		objs = append(objs, u)
	}
}

// GeneratorRequests returns the requests of one generator container, for capacity checks.
func GeneratorRequests() (cpu, memory resource.Quantity, err error) {
	var tmpl appsv1.Deployment
	if err := readTemplate("generator.yaml", &tmpl); err != nil {
		return cpu, memory, err
	}
	req := tmpl.Spec.Template.Spec.Containers[0].Resources.Requests
	return req[corev1.ResourceCPU], req[corev1.ResourceMemory], nil
}

// CollectorImage returns the image of the collector under test, for the result record.
func CollectorImage() string {
	objs, err := CollectorObjects(spec.RunSpec{Tenants: 1}, nil)
	if err != nil {
		return ""
	}
	for _, o := range objs {
		u, ok := o.(*unstructured.Unstructured)
		if !ok || u.GetKind() != "Deployment" {
			continue
		}
		containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
		image, _, _ := unstructured.NestedString(containers[0].(map[string]any), "image")
		return image
	}
	return ""
}

func setCollectorRunValues(u *unstructured.Unstructured, s spec.RunSpec) error {
	if u.GetKind() != "Deployment" {
		return nil
	}
	labels, _, err := unstructured.NestedStringMap(u.Object, "spec", "template", "metadata", "labels")
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedStringMap(u.Object, merge(labels, RunLabels(s)), "spec", "template", "metadata", "labels"); err != nil {
		return err
	}
	containers, _, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	if err != nil {
		return err
	}
	if len(containers) != 1 {
		return fmt.Errorf("expected one container, got %d", len(containers))
	}
	limit := fmt.Sprintf("%dMi", s.MemoryLimitMi())
	if err := unstructured.SetNestedField(containers[0].(map[string]any), limit, "resources", "limits", "memory"); err != nil {
		return err
	}
	return unstructured.SetNestedSlice(u.Object, containers, "spec", "template", "spec", "containers")
}

// PipelineServices gives every exporter its own endpoint; in faulty runs p-000 of each
// signal points at the fault backend.
func PipelineServices(s spec.RunSpec) ([]client.Object, error) {
	var tmpl corev1.Service
	if err := readTemplate("pipeline-service.yaml", &tmpl); err != nil {
		return nil, err
	}
	fault, err := FaultBackend(s)
	if err != nil {
		return nil, err
	}
	var objs []client.Object
	for _, sig := range spec.Signals {
		for i := range s.Tenants {
			objs = append(objs, pipelineService(&tmpl, fault, s, sig, i))
		}
	}
	return objs, nil
}

func pipelineService(tmpl *corev1.Service, fault *appsv1.Deployment, s spec.RunSpec, sig spec.Signal, i int) *corev1.Service {
	svc := tmpl.DeepCopy()
	svc.Name = fmt.Sprintf("%s-%s", sig.Name, spec.PipelineID(i))
	svc.Labels = merge(svc.Labels, RunLabels(s))
	svc.Spec.Selector = map[string]string{"app.kubernetes.io/name": sig.Backend, "app.kubernetes.io/instance": "load-test"}
	svc.Spec.Ports[0].Port = int32(sig.Port)
	svc.Spec.Ports[0].TargetPort = intstr.FromInt(sig.Port)
	if s.Health != spec.Faulty || i != 0 {
		return svc
	}
	svc.Spec.Selector = fault.Spec.Selector.MatchLabels
	svc.Spec.Ports[0].TargetPort = intstr.FromInt32(fault.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	return svc
}

// FaultBackend is the static fault backend Deployment with the run label.
func FaultBackend(s spec.RunSpec) (*appsv1.Deployment, error) {
	var d appsv1.Deployment
	if err := readTemplate("fault-backend.yaml", &d); err != nil {
		return nil, err
	}
	d.Labels = merge(d.Labels, RunLabels(s))
	d.Spec.Template.Labels = merge(d.Spec.Template.Labels, RunLabels(s))
	return &d, nil
}

// Generators: per signal ceil(N/10) Deployments, one telemetrygen container per tenant.
func Generators(s spec.RunSpec, duration time.Duration) ([]client.Object, error) {
	var tmpl appsv1.Deployment
	if err := readTemplate("generator.yaml", &tmpl); err != nil {
		return nil, err
	}
	var objs []client.Object
	for _, sig := range spec.Signals {
		for start := 0; start < s.Tenants; start += tenantsPerPod {
			objs = append(objs, generator(&tmpl, s, sig, start, duration))
		}
	}
	return objs, nil
}

func generator(tmpl *appsv1.Deployment, s spec.RunSpec, sig spec.Signal, start int, duration time.Duration) *appsv1.Deployment {
	d := tmpl.DeepCopy()
	d.Name = fmt.Sprintf("gen-%s-%d", sig.Name, start/tenantsPerPod)
	app := map[string]string{"app": d.Name}
	d.Labels = merge(app, RunLabels(s))
	d.Spec.Selector.MatchLabels = app
	d.Spec.Template.Labels = merge(app, RunLabels(s))

	base := tmpl.Spec.Template.Spec.Containers[0]
	d.Spec.Template.Spec.Containers = nil
	for i := start; i < min(start+tenantsPerPod, s.Tenants); i++ {
		d.Spec.Template.Spec.Containers = append(d.Spec.Template.Spec.Containers, generatorContainer(base, s, sig, i, duration))
	}
	return d
}

// generatorContainer copies the template container for one tenant of one signal.
// The template's first arg is the example signal and gets replaced.
func generatorContainer(base corev1.Container, s spec.RunSpec, sig spec.Signal, tenant int, duration time.Duration) corev1.Container {
	c := *base.DeepCopy()
	c.Name = fmt.Sprintf("%s-%s", sig.Name, spec.PipelineID(tenant))
	c.Args = append([]string{sig.Name}, base.Args[1:]...)
	// one telemetrygen worker delivers only ~1600 items/s, and --rate is per worker
	rate := s.TenantRate(sig)
	workers := max(1, (rate+itemsPerWorker-1)/itemsPerWorker)
	c.Args = append(c.Args,
		fmt.Sprintf("--workers=%d", workers),
		fmt.Sprintf("--rate=%g", float64(rate)/float64(workers)),
		"--duration="+duration.String(),
		fmt.Sprintf(`--otlp-attributes=tenant="%s"`, spec.TenantID(tenant)),
	)
	if sig.BodyBytes == 0 {
		return c
	}
	c.Args = append(c.Args, "--body="+strings.Repeat("x", sig.BodyBytes))
	return c
}

func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}
