package manifests

import (
	_ "embed"
	"fmt"
	"slices"
	"strings"

	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

//go:embed debug-backend-config.yaml
var debugBackendConfig string

const (
	tmAPIVersion = "telemetry.kyma-project.io/v1beta1"
	// debugBackend is the nop collector pipelines export to; it only has to accept OTLP.
	debugBackend     = "debug-backend"
	debugBackendPort = 4318
)

// DebugBackendName is the Service/pod name of the per-run nop collector, exported so the
// otel exporter config and per-pipeline Services can target it.
const DebugBackendName = debugBackend

// DebugBackendPort is the nop collector's OTLP/HTTP port.
const DebugBackendPort = debugBackendPort

// tmKind maps a signal to its Telemetry Manager pipeline kind.
var tmKind = map[string]string{
	"logs":    "LogPipeline",
	"metrics": "MetricPipeline",
	"traces":  "TracePipeline",
}

const (
	faultBackendName = "fault-backend"
	faultBackendPort = 4318
)

// TMPipelineObjects returns the Telemetry Manager pipeline CRs for a run: one pipeline per
// signal and tenant, each keeping only its tenant (same F1 filter as the otel path) and
// exporting to the per-run nop debug backend. In faulty runs tenant 0 exports to the fault
// backend instead. The managed gateway and module are not touched.
func TMPipelineObjects(s spec.RunSpec) ([]client.Object, error) {
	debugEndpoint := fmt.Sprintf("http://%s.%s.svc:%d", debugBackend, spec.TMBackendNamespace, debugBackendPort)
	faultEndpoint := fmt.Sprintf("http://%s.%s.svc:%d", faultBackendName, spec.TMBackendNamespace, faultBackendPort)
	var objs []client.Object
	for _, sig := range spec.Signals {
		sources := sourcesOf(s, sig)
		if len(sources) == 0 {
			continue
		}
		kind, ok := tmKind[sig.Name]
		if !ok {
			return nil, fmt.Errorf("no Telemetry Manager kind for signal %q", sig.Name)
		}
		for i := range s.Tenants {
			endpoint := debugEndpoint
			if s.Health == spec.Faulty && i == 0 {
				endpoint = faultEndpoint
			}
			objs = append(objs, tmPipeline(s, sig, kind, i, endpoint, sources))
		}
	}
	return objs, nil
}

// sourcesOf lists the components that deliver sig to the run's pipelines: the gateway via OTLP,
// an agent via its own input. One pipeline per signal and tenant serves all of them.
func sourcesOf(s spec.RunSpec, sig spec.Signal) []string {
	var sources []string
	for _, u := range s.Units() {
		if u.Signal.Name == sig.Name {
			sources = append(sources, u.Component)
		}
	}
	return sources
}

// tenantCondition is true for items that are not tenant i's, by where each source puts the
// tenant: the gateway's generators on the resource, the log generator as a JSON field the log
// agent parses into log attributes, and the metric generator as its scrape port, which the
// prometheus receiver sets as the string resource attribute server.port.
func tenantCondition(source string, i int) string {
	if source == spec.ComponentLogAgent {
		return fmt.Sprintf(`log.attributes["tenant"] != %q`, spec.TenantID(i))
	}
	if source == spec.ComponentMetricAgent {
		return fmt.Sprintf(`resource.attributes["server.port"] != "%d"`, spec.MetricPortBase+i)
	}
	return fmt.Sprintf(`resource.attributes["tenant"] != %q`, spec.TenantID(i))
}

func tmPipeline(s spec.RunSpec, sig spec.Signal, kind string, i int, endpoint string, sources []string) *unstructured.Unstructured {
	// Drop every item that is not this tenant's, identical OTTL to the otel filter processor.
	// Each source carries only its own tenant key and a missing key compares unequal, so with
	// several sources an item is dropped only when no key matches.
	var conds []string
	for _, src := range sources {
		conds = append(conds, tenantCondition(src, i))
	}
	filter := []any{map[string]any{
		"conditions": []any{strings.Join(conds, " and ")},
	}}
	pipelineSpec := map[string]any{
		"filter": filter,
		"output": map[string]any{
			"otlp": map[string]any{
				"endpoint": map[string]any{"value": endpoint},
				"protocol": "http",
			},
		},
	}
	// MetricPipeline for the metric agent: only the prometheus input, only the generator namespace.
	if kind == "MetricPipeline" && slices.Contains(sources, spec.ComponentMetricAgent) {
		pipelineSpec["input"] = map[string]any{
			"prometheus": map[string]any{"enabled": true, "namespaces": map[string]any{"include": []any{LoadNamespace}}},
			"runtime":    map[string]any{"enabled": false},
			"istio":      map[string]any{"enabled": false},
			"otlp":       map[string]any{"enabled": slices.Contains(sources, spec.ComponentGateway)},
		}
	}
	// LogPipeline: OTLP for the gateway, the runtime input for the log agent, reading only the
	// log generator's container.
	if kind == "LogPipeline" {
		pipelineSpec["input"] = map[string]any{
			"otlp":    map[string]any{"enabled": slices.Contains(sources, spec.ComponentGateway)},
			"runtime": map[string]any{"enabled": false},
		}
	}
	if kind == "LogPipeline" && slices.Contains(sources, spec.ComponentLogAgent) {
		pipelineSpec["input"].(map[string]any)["runtime"] = map[string]any{
			"enabled":    true,
			"namespaces": map[string]any{"include": []any{LoadNamespace}},
			"containers": map[string]any{"include": []any{LogGeneratorContainer}},
		}
	}
	u := &unstructured.Unstructured{}
	u.SetAPIVersion(tmAPIVersion)
	u.SetKind(kind)
	u.SetName(fmt.Sprintf("%s-%s", sig.Name, spec.PipelineID(i)))
	u.SetLabels(RunLabels(s))
	u.Object["spec"] = pipelineSpec
	return u
}

// DebugBackendObjects returns the per-run nop collector that pipelines export to:
// a ConfigMap, a Deployment and a Service in the given namespace, run-labelled so cleanup
// removes them with the rest of the run. Both targets use it: otel in BackendNamespace,
// tm in TMBackendNamespace.
func DebugBackendObjects(s spec.RunSpec, namespace string) []client.Object {
	labels := RunLabels(s)
	meta := func() metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: debugBackend, Namespace: namespace, Labels: labels}
	}
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: meta(),
		Data:       map[string]string{"config.yaml": debugBackendConfig},
	}
	podLabels := merge(labels, map[string]string{"app": debugBackend})
	dep := &unstructured.Unstructured{}
	dep.SetAPIVersion("apps/v1")
	dep.SetKind("Deployment")
	dep.SetName(debugBackend)
	dep.SetNamespace(namespace)
	dep.SetLabels(labels)
	dep.Object["spec"] = map[string]any{
		"replicas": int64(1),
		"selector": map[string]any{"matchLabels": map[string]any{"app": debugBackend}},
		"template": map[string]any{
			"metadata": map[string]any{"labels": toAnyMap(podLabels)},
			"spec": map[string]any{
				"containers": []any{map[string]any{
					"name":  "otelcol",
					"image": debugBackendImage,
					"args":  []any{"--config=/conf/config.yaml"},
					"ports": []any{
						map[string]any{"containerPort": int64(4317)},
						map[string]any{"containerPort": int64(debugBackendPort)},
					},
					"volumeMounts": []any{map[string]any{"name": "config", "mountPath": "/conf"}},
				}},
				"volumes": []any{map[string]any{
					"name":      "config",
					"configMap": map[string]any{"name": debugBackend},
				}},
			},
		},
	}
	svc := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: meta(),
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": debugBackend},
			Ports: []corev1.ServicePort{
				{Name: "grpc", Port: 4317, TargetPort: intstr.FromInt32(4317)},
				{Name: "http", Port: debugBackendPort, TargetPort: intstr.FromInt32(debugBackendPort)},
			},
		},
	}
	return []client.Object{cm, dep, svc}
}

const debugBackendImage = "otel/opentelemetry-collector-contrib:0.123.0"

// ServiceMonitor returns a Prometheus ServiceMonitor that scrapes the metrics Service TM
// creates for one managed component (gateway or agent) on port 8888; app is its
// app.kubernetes.io/name.
func ServiceMonitor(app string) (client.Object, error) {
	u := &unstructured.Unstructured{}
	if err := readTemplate("gateway-service-monitor.yaml", &u.Object); err != nil {
		return nil, err
	}
	u.SetName(app)
	if err := unstructured.SetNestedStringMap(u.Object, map[string]string{"app.kubernetes.io/name": app}, "spec", "selector", "matchLabels"); err != nil {
		return nil, err
	}
	return u, nil
}

func toAnyMap(m map[string]string) map[string]any {
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}
