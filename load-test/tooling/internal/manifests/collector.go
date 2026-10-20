package manifests

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	_ "embed"
	"fmt"
	"slices"

	"sigs.k8s.io/yaml"
)

//go:embed collector-config.yaml
var baseConfigYAML []byte

const (
	BackendNamespace  = "load-test-backends"
	maxConfigMapBytes = 1 << 20
)

// ConfigStats goes into the result record.
type ConfigStats struct {
	Bytes              int `json:"bytes"`
	ServicePipelines   int `json:"service_pipelines"`
	ProcessorInstances int `json:"processor_instances"`
	Exporters          int `json:"exporters"`
}

type collectorConfig struct {
	processors, exporters, connectors, pipelines map[string]any
	processorInstances                           int
}

// BuildConfig returns the collector config for a run: the TM gateway base plus
// N pipelines per signal, shaped as F1 (fan-out + filter) or R1 (routing connector).
func BuildConfig(s spec.RunSpec) ([]byte, ConfigStats, error) {
	var base map[string]any
	if err := yaml.Unmarshal(baseConfigYAML, &base); err != nil {
		return nil, ConfigStats{}, fmt.Errorf("parse base config: %w", err)
	}
	c := collectorConfig{
		processors: base["processors"].(map[string]any),
		exporters:  map[string]any{},
		connectors: map[string]any{},
		pipelines:  map[string]any{},
	}
	for _, sig := range spec.Signals {
		for i := range s.Tenants {
			c.exporters[exporterName(sig, i)] = exporterConfig(s, sig, spec.PipelineID(i))
		}
		if s.Shape == spec.ShapeF1 {
			c.addFanOut(s, sig)
			continue
		}
		c.addRouting(s, sig)
	}

	base["exporters"] = c.exporters
	if len(c.connectors) > 0 {
		base["connectors"] = c.connectors
	}
	base["service"].(map[string]any)["pipelines"] = c.pipelines

	out, err := yaml.Marshal(base)
	if err != nil {
		return nil, ConfigStats{}, err
	}
	// The API server rejects ConfigMaps above 1 MiB; fail before anything is created.
	if len(out) > maxConfigMapBytes {
		return nil, ConfigStats{}, fmt.Errorf("collector config for %d tenants is %d KiB, above the ConfigMap limit of 1 MiB; use fewer tenants",
			s.Tenants, len(out)/1024)
	}
	return out, ConfigStats{
		Bytes:              len(out),
		ServicePipelines:   len(c.pipelines),
		ProcessorInstances: c.processorInstances,
		Exporters:          len(c.exporters),
	}, nil
}

// enrichment is the TM gateway processing per signal that changes telemetrygen data,
// run before the per-pipeline processors.
// TM sets kyma.input.name in its own input pipeline; with OTLP as the only input it runs here.
var enrichment = map[string][]any{
	"logs":    {"transform/set-observed-time-if-zero", "k8s_attributes", "transform/insert-cluster-attributes"},
	"traces":  {"k8s_attributes", "transform/insert-cluster-attributes"},
	"metrics": {"transform/set-kyma-input-name-otlp", "k8s_attributes", "transform/insert-cluster-attributes"},
}

// perPipeline is the TM processing each pipeline runs before its filter.
var perPipeline = map[string][]any{
	"metrics": {"transform/drop-kyma-attributes"},
}

// addFanOut: N pipelines share the otlp receiver; each enriches, then keeps only its tenant.
// Metrics follow TM: one pipeline enriches, then fans out to the N pipelines.
func (c *collectorConfig) addFanOut(s spec.RunSpec, sig spec.Signal) {
	receiver := "otlp"
	head := slices.Concat([]any{"memory_limiter"}, enrichment[sig.Name])
	if sig.Name == "metrics" {
		receiver = "forward/enrichment"
		c.connectors[receiver] = map[string]any{}
		c.pipelines["metrics/enrichment"] = map[string]any{
			"receivers":  []any{"otlp"},
			"processors": head,
			"exporters":  []any{receiver},
		}
		c.processorInstances += len(head)
		head = nil
	}
	for i := range s.Tenants {
		filter := fmt.Sprintf("filter/%s-%s", sig.Name, spec.PipelineID(i))
		c.processors[filter] = filterConfig(sig, spec.TenantID(i))
		processors := slices.Concat(head, perPipeline[sig.Name], []any{filter, "batch"})
		c.pipelines[pipelineName(sig, i)] = map[string]any{
			"receivers":  []any{receiver},
			"processors": processors,
			"exporters":  []any{exporterName(sig, i)},
		}
		c.processorInstances += len(processors)
	}
}

// addRouting: one input pipeline enriches, batches and routes each item to its tenant's pipeline,
// which batches again before export.
func (c *collectorConfig) addRouting(s spec.RunSpec, sig spec.Signal) {
	connector := "routing/" + sig.Name
	table := make([]any, 0, s.Tenants)
	for i := range s.Tenants {
		table = append(table, map[string]any{
			"context":   "resource",
			"condition": fmt.Sprintf(`attributes["tenant"] == %q`, spec.TenantID(i)),
			"action":    "move",
			"pipelines": []any{pipelineName(sig, i)},
		})
		c.pipelines[pipelineName(sig, i)] = map[string]any{
			"receivers":  []any{connector},
			"processors": []any{"batch"},
			"exporters":  []any{exporterName(sig, i)},
		}
		c.processorInstances++
	}
	c.connectors[connector] = map[string]any{"error_mode": "ignore", "table": table}
	processors := slices.Concat([]any{"memory_limiter"}, enrichment[sig.Name], perPipeline[sig.Name], []any{"batch"})
	c.pipelines[sig.Name+"/in"] = map[string]any{
		"receivers":  []any{"otlp"},
		"processors": processors,
		"exporters":  []any{connector},
	}
	c.processorInstances += len(processors)
}

func pipelineName(sig spec.Signal, i int) string {
	return fmt.Sprintf("%s/%s", sig.Name, spec.PipelineID(i))
}
func exporterName(sig spec.Signal, i int) string {
	return fmt.Sprintf("otlp_http/%s-%s", sig.Name, spec.PipelineID(i))
}

// exporterConfig mirrors TM's otlp_http exporter, pointed at the pipeline's own Service.
func exporterConfig(s spec.RunSpec, sig spec.Signal, pid string) map[string]any {
	url := fmt.Sprintf("http://%s-%s.%s.svc:%d%s", sig.Name, pid, BackendNamespace, sig.Port, sig.Path)
	cfg := map[string]any{
		"compression":   "gzip",
		"sending_queue": map[string]any{"enabled": true, "queue_size": s.QueueSize()},
		"retry_on_failure": map[string]any{
			"enabled": true, "initial_interval": "5s", "max_interval": "30s", "max_elapsed_time": "300s",
		},
	}
	if sig.Header == "" {
		// shortcut: extra_label on the OTLP path is unverified; switch to a resource processor if VM ignores it.
		cfg[sig.EndpointKey] = url + "?extra_label=pipeline=" + pid
		return cfg
	}
	cfg[sig.EndpointKey] = url
	cfg["headers"] = map[string]any{sig.Header: "pipeline=" + pid}
	return cfg
}

// filterConfig drops every item that does not belong to the tenant.
func filterConfig(sig spec.Signal, tid string) map[string]any {
	return map[string]any{
		"error_mode": "ignore",
		sig.FilterKey: []any{map[string]any{
			"conditions": []any{fmt.Sprintf(`resource.attributes["tenant"] != %q`, tid)},
		}},
	}
}
