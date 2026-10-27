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

//go:embed collector.yaml generator.yaml fault-backend.yaml pipeline-service.yaml gateway-service-monitor.yaml log-generator.yaml metric-generator.yaml
var manifestsFS embed.FS

const (
	LoadNamespace = "otel-load-test"
	RunLabel      = "loadtest.kyma-project.io/run"
	RoleLabel     = "load-test/role"

	// LogGeneratorContainer is the container name in log-generator.yaml that LogPipelines read.
	LogGeneratorContainer = "log-gen"

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
// one ConfigMap per config part, the run label and the TM memory limit.
func CollectorObjects(s spec.RunSpec, parts [][]byte) ([]client.Object, error) {
	raw, err := manifestsFS.ReadFile("collector.yaml")
	if err != nil {
		return nil, err
	}
	var objs []client.Object
	for i, part := range parts {
		objs = append(objs, &corev1.ConfigMap{
			TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
			ObjectMeta: metav1.ObjectMeta{Name: configMapName(i), Namespace: LoadNamespace, Labels: RunLabels(s)},
			Data:       map[string]string{configFile(i): string(part)},
		})
	}
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
		if err := setCollectorRunValues(u, s, len(parts)); err != nil {
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

// Workers is the telemetrygen worker count for one tenant's rate: one worker delivers
// only ~1600 items/s, and --rate is per worker.
func Workers(rate int) int { return max(1, (rate+itemsPerWorker-1)/itemsPerWorker) }

// GeneratorImage returns the telemetrygen image, for the result record.
func GeneratorImage() (string, error) {
	var tmpl appsv1.Deployment
	if err := readTemplate("generator.yaml", &tmpl); err != nil {
		return "", err
	}
	return tmpl.Spec.Template.Spec.Containers[0].Image, nil
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

func configMapName(i int) string { return fmt.Sprintf("otelcol-%d", i) }
func configFile(i int) string    { return fmt.Sprintf("relay-%d.conf", i) }

// setCollectorRunValues sets the run label, the memory limit and one --config arg
// plus one projected ConfigMap source per config part.
func setCollectorRunValues(u *unstructured.Unstructured, s spec.RunSpec, parts int) error {
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
	args := make([]any, parts)
	sources := make([]any, parts)
	for i := range parts {
		args[i] = "--config=file:/conf/" + configFile(i)
		sources[i] = map[string]any{"configMap": map[string]any{"name": configMapName(i)}}
	}
	if err := unstructured.SetNestedSlice(containers[0].(map[string]any), args, "args"); err != nil {
		return err
	}
	if err := unstructured.SetNestedSlice(u.Object, containers, "spec", "template", "spec", "containers"); err != nil {
		return err
	}
	volumes, _, err := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "volumes")
	if err != nil {
		return err
	}
	if len(volumes) != 1 {
		return fmt.Errorf("expected one volume, got %d", len(volumes))
	}
	if err := unstructured.SetNestedSlice(volumes[0].(map[string]any), sources, "projected", "sources"); err != nil {
		return err
	}
	return unstructured.SetNestedSlice(u.Object, volumes, "spec", "template", "spec", "volumes")
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

// pipelineService gives each exporter its own endpoint, all backed by the one nop debug
// collector. In faulty runs p-000 of each signal points at the fault backend instead.
func pipelineService(tmpl *corev1.Service, fault *appsv1.Deployment, s spec.RunSpec, sig spec.Signal, i int) *corev1.Service {
	svc := tmpl.DeepCopy()
	svc.Name = fmt.Sprintf("%s-%s", sig.Name, spec.PipelineID(i))
	svc.Labels = merge(svc.Labels, RunLabels(s))
	svc.Spec.Selector = map[string]string{"app": DebugBackendName}
	svc.Spec.Ports[0].Port = int32(DebugBackendPort)
	svc.Spec.Ports[0].TargetPort = intstr.FromInt(DebugBackendPort)
	if s.Health != spec.Faulty || i != 0 {
		return svc
	}
	svc.Spec.Selector = fault.Spec.Selector.MatchLabels
	svc.Spec.Ports[0].TargetPort = intstr.FromInt32(fault.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort)
	return svc
}

// FaultBackend is the static fault backend Deployment with the run label.
func FaultBackend(s spec.RunSpec) (*appsv1.Deployment, error) {
	return FaultBackendInNamespace(s, "")
}

// FaultBackendInNamespace is the fault backend Deployment with the run label, deployed in ns.
// When ns is empty the namespace from the template is used.
func FaultBackendInNamespace(s spec.RunSpec, ns string) (*appsv1.Deployment, error) {
	var d appsv1.Deployment
	if err := readTemplate("fault-backend.yaml", &d); err != nil {
		return nil, err
	}
	d.Labels = merge(d.Labels, RunLabels(s))
	d.Spec.Template.Labels = merge(d.Spec.Template.Labels, RunLabels(s))
	if ns != "" {
		d.Namespace = ns
	}
	return &d, nil
}

// FaultBackendService returns a Service in ns that exposes the fault backend pod.
func FaultBackendService(s spec.RunSpec, ns string) (*corev1.Service, error) {
	fault, err := FaultBackendInNamespace(s, ns)
	if err != nil {
		return nil, err
	}
	port := fault.Spec.Template.Spec.Containers[0].Ports[0].ContainerPort
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: "fault-backend", Namespace: ns, Labels: RunLabels(s)},
		Spec: corev1.ServiceSpec{
			Selector: fault.Spec.Selector.MatchLabels,
			Ports: []corev1.ServicePort{
				{Name: "http-otlp", Port: int32(port), TargetPort: intstr.FromInt32(port)},
			},
		},
	}
	return svc, nil
}

// Generators: per gateway signal 1 telemetrygen Deployment, per agent its own generator;
// each runs a shell loop over all tenants.
func Generators(s spec.RunSpec, duration time.Duration) ([]client.Object, error) {
	var tmpl appsv1.Deployment
	if err := readTemplate("generator.yaml", &tmpl); err != nil {
		return nil, err
	}
	var objs []client.Object
	for _, u := range s.Units() {
		if u.Component == spec.ComponentGateway {
			objs = append(objs, generator(&tmpl, s, u.Signal, duration))
			continue
		}
		gen, err := agentGenerators(s, u)
		if err != nil {
			return nil, err
		}
		objs = append(objs, gen...)
	}
	return objs, nil
}

// agentGeneratorTemplates holds the generator Deployment of each node agent; it runs on the
// collector node, because an agent only collects from its own node.
var agentGeneratorTemplates = map[string]string{
	spec.ComponentLogAgent:    "log-generator.yaml",
	spec.ComponentMetricAgent: "metric-generator.yaml",
}

// gaugesPerTenant is the avalanche metric count; the series per metric set the volume.
const gaugesPerTenant = 10

// AgentGeneratorImage returns the generator image of a node agent, for the result record.
func AgentGeneratorImage(component string) (string, error) {
	var d appsv1.Deployment
	if err := readTemplate(agentGeneratorTemplates[component], &d); err != nil {
		return "", err
	}
	return d.Spec.Template.Spec.Containers[0].Image, nil
}

// agentGenerators returns the generator objects of one node agent with the run label set.
func agentGenerators(s spec.RunSpec, u spec.Unit) ([]client.Object, error) {
	path, ok := agentGeneratorTemplates[u.Component]
	if !ok {
		return nil, fmt.Errorf("no generator for %s", u.Component)
	}
	var d appsv1.Deployment
	if err := readTemplate(path, &d); err != nil {
		return nil, err
	}
	d.Labels = merge(d.Labels, RunLabels(s))
	d.Spec.Template.Labels = merge(d.Spec.Template.Labels, RunLabels(s))
	if u.Component == spec.ComponentLogAgent {
		d.Spec.Template.Spec.Containers[0].Args = []string{logAgentScript(s, u.Signal)}
		return []client.Object{&d}, nil
	}
	d.Spec.Template.Spec.Containers[0].Args = []string{metricAgentScript(s, u.Signal)}
	return append([]client.Object{&d}, metricAgentServices(s, d.Spec.Selector.MatchLabels)...), nil
}

// metricAgentScript launches one avalanche per tenant on port MetricPortBase+i. Each serves
// gauges only with fixed series, so one scrape per interval yields the tenant's data points/s.
func metricAgentScript(s spec.RunSpec, sig spec.Signal) string {
	series := max(1, s.TenantRate(sig)*spec.ScrapeIntervalS/gaugesPerTenant)
	return fmt.Sprintf(`for i in $(seq 0 %d); do
  avalanche --port=$((%d + i)) \
    --gauge-metric-count=%d --counter-metric-count=0 --histogram-metric-count=0 \
    --native-histogram-metric-count=0 --summary-metric-count=0 \
    --series-count=%d --label-count=2 \
    --series-interval=0 --metric-interval=0 --series-change-interval=0 &
done
wait
`, s.Tenants-1, spec.MetricPortBase, gaugesPerTenant, series)
}

// metricAgentServices gives every tenant's avalanche its own scrape target: one Service per
// tenant on the generator pod, annotated with that tenant's port.
func metricAgentServices(s spec.RunSpec, selector map[string]string) []client.Object {
	var objs []client.Object
	for i := range s.Tenants {
		port := int32(spec.MetricPortBase + i)
		objs = append(objs, &corev1.Service{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("gen-metric-agent-%s", spec.PipelineID(i)),
				Namespace: LoadNamespace,
				Labels:    RunLabels(s),
				Annotations: map[string]string{
					"prometheus.io/scrape": "true",
					"prometheus.io/port":   fmt.Sprint(port),
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: selector,
				Ports:    []corev1.ServicePort{{Name: "metrics", Port: port, TargetPort: intstr.FromInt32(port)}},
			},
		})
	}
	return objs
}

// logAgentScript launches one stdout-log-generator per tenant. Each writes JSON lines of
// BodyBytes with its tenant as a field, which the log agent parses into log attributes.
// The generator has no duration flag; the tool deletes the Deployment when the load stops.
func logAgentScript(s spec.RunSpec, sig spec.Signal) string {
	return fmt.Sprintf(`for i in $(seq 0 %d); do
  stdout-log-generator --format=json -b %d -r %d -f "tenant=$(printf 'tenant-%%03d' $i)" &
done
wait
`, s.Tenants-1, sig.BodyBytes, s.TenantRate(sig))
}

// generatorEndpoint is the OTLP gRPC endpoint generators send to: our collector for otel,
// the managed Telemetry Manager gateway for tm.
func generatorEndpoint(s spec.RunSpec) string {
	if s.Target == spec.TargetTM {
		return spec.TMGatewayEndpoint
	}
	return "otelcol.otel-load-test.svc:4317"
}

func generator(tmpl *appsv1.Deployment, s spec.RunSpec, sig spec.Signal, duration time.Duration) *appsv1.Deployment {
	d := tmpl.DeepCopy()
	d.Name = fmt.Sprintf("gen-%s", sig.Name)
	app := map[string]string{"app": d.Name}
	d.Labels = merge(app, RunLabels(s))
	d.Spec.Selector.MatchLabels = app
	d.Spec.Template.Labels = merge(app, RunLabels(s))

	c := &d.Spec.Template.Spec.Containers[0]
	c.Name = sig.Name
	c.Args = []string{generatorScript(s, sig, duration)}
	return d
}

// generatorScript builds the shell script that launches one telemetrygen per tenant.
func generatorScript(s spec.RunSpec, sig spec.Signal, duration time.Duration) string {
	rate := s.TenantRate(sig)
	workers := Workers(rate)
	var b strings.Builder
	if sig.BodyBytes > 0 {
		fmt.Fprintf(&b, "BODY=$(printf '%%%ds' '' | tr ' ' 'x')\n", sig.BodyBytes)
	}
	bodyArg := ""
	if sig.BodyBytes > 0 {
		bodyArg = ` --body="$BODY"`
	}
	fmt.Fprintf(&b, `for i in $(seq 0 %d); do
  telemetrygen %s \
    --otlp-endpoint=%s \
    --otlp-insecure --allow-export-failures \
    --workers=%d --rate=%g \
    --duration=%s \
    --otlp-attributes=tenant=\"$(printf 'tenant-%%03d' $i)\"%s &
done
wait
`,
		s.Tenants-1,
		sig.Name,
		generatorEndpoint(s),
		workers, float64(rate)/float64(workers),
		duration,
		bodyArg,
	)
	return b.String()
}

func merge(a, b map[string]string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, a)
	maps.Copy(out, b)
	return out
}
