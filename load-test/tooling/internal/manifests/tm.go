package manifests

import (
	_ "embed"
	"fmt"

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
		kind, ok := tmKind[sig.Name]
		if !ok {
			return nil, fmt.Errorf("no Telemetry Manager kind for signal %q", sig.Name)
		}
		for i := range s.Tenants {
			endpoint := debugEndpoint
			if s.Health == spec.Faulty && i == 0 {
				endpoint = faultEndpoint
			}
			objs = append(objs, tmPipeline(s, sig, kind, i, endpoint))
		}
	}
	return objs, nil
}

func tmPipeline(s spec.RunSpec, sig spec.Signal, kind string, i int, endpoint string) *unstructured.Unstructured {
	// Drop every item that is not this tenant's, identical OTTL to the otel filter processor.
	filter := []any{map[string]any{
		"conditions": []any{fmt.Sprintf(`resource.attributes["tenant"] != %q`, spec.TenantID(i))},
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
	// Only LogPipeline exposes an input block; runtime log scraping stays off, OTLP only.
	if kind == "LogPipeline" {
		pipelineSpec["input"] = map[string]any{
			"otlp":    map[string]any{},
			"runtime": map[string]any{"enabled": false},
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
				{Name: "grpc", Port: 4317, TargetPort: intstr.FromInt(4317)},
				{Name: "http", Port: debugBackendPort, TargetPort: intstr.FromInt(debugBackendPort)},
			},
		},
	}
	return []client.Object{cm, dep, svc}
}

const debugBackendImage = "otel/opentelemetry-collector-contrib:0.123.0"

// GatewayServiceMonitor returns a Prometheus ServiceMonitor that scrapes the
// TM-managed OTLP gateway pods on port 8888.
func GatewayServiceMonitor() (client.Object, error) {
	u := &unstructured.Unstructured{}
	if err := readTemplate("gateway-service-monitor.yaml", &u.Object); err != nil {
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
