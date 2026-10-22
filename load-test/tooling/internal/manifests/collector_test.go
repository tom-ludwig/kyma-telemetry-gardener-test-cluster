package manifests

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func parseConfig(t *testing.T, s spec.RunSpec) (map[string]any, ConfigStats) {
	t.Helper()
	raw, stats, err := BuildConfig(s)
	if err != nil {
		t.Fatal(err)
	}
	var cfg map[string]any
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return cfg, stats
}

func TestBuildConfigFanOut(t *testing.T) {
	for _, n := range []int{1, 100} {
		cfg, stats := parseConfig(t, spec.RunSpec{Shape: spec.ShapeF1, Tenants: n, Load: spec.LoadA})
		// logs: 6 and traces: 5 processors per pipeline; metrics: 4 in the enrichment pipeline, 3 per pipeline
		if stats.ServicePipelines != 3*n+1 || stats.Exporters != 3*n || stats.ProcessorInstances != 14*n+4 {
			t.Errorf("n=%d: stats %+v", n, stats)
		}
		filters := 0
		for name := range cfg["processors"].(map[string]any) {
			if strings.HasPrefix(name, "filter/") {
				filters++
			}
		}
		if filters != 3*n {
			t.Errorf("n=%d: %d filters, want %d", n, filters, 3*n)
		}
		connectors := cfg["connectors"].(map[string]any)
		if _, ok := connectors["forward/enrichment"]; !ok || len(connectors) != 1 {
			t.Errorf("n=%d: connectors %v, want only forward/enrichment", n, connectors)
		}
	}
}

func TestPipelineProcessorsDefined(t *testing.T) {
	for _, shape := range []string{spec.ShapeF1, spec.ShapeR1} {
		cfg, _ := parseConfig(t, spec.RunSpec{Shape: shape, Tenants: 2, Load: spec.LoadA})
		processors := cfg["processors"].(map[string]any)
		for name, p := range cfg["service"].(map[string]any)["pipelines"].(map[string]any) {
			list, _ := p.(map[string]any)["processors"].([]any)
			for _, proc := range list {
				if _, ok := processors[proc.(string)]; !ok {
					t.Errorf("%s %s: processor %s not defined", shape, name, proc)
				}
			}
		}
	}
	for _, sig := range spec.Signals {
		if len(enrichment[sig.Name]) == 0 {
			t.Errorf("signal %s has no enrichment chain", sig.Name)
		}
	}
}

func TestBuildConfigRouting(t *testing.T) {
	cfg, stats := parseConfig(t, spec.RunSpec{Shape: spec.ShapeR1, Tenants: 100, Load: spec.LoadA})
	// /in pipelines: 5 + 4 + 6 processors; one batch per tenant pipeline
	if stats.ServicePipelines != 3*100+3 || stats.Exporters != 300 || stats.ProcessorInstances != 3*100+15 {
		t.Errorf("stats %+v", stats)
	}
	table := cfg["connectors"].(map[string]any)["routing/logs"].(map[string]any)["table"].([]any)
	if len(table) != 100 {
		t.Errorf("routing table has %d entries", len(table))
	}
	pipeline := cfg["service"].(map[string]any)["pipelines"].(map[string]any)["logs/p-000"].(map[string]any)
	processors, ok := pipeline["processors"].([]any)
	if !ok || len(processors) != 1 || processors[0] != "batch" {
		t.Errorf("tenant pipeline processors: %v, want [batch]", pipeline["processors"])
	}
}

func TestExporterEndpoints(t *testing.T) {
	cfg, _ := parseConfig(t, spec.RunSpec{Shape: spec.ShapeF1, Tenants: 2, Load: spec.LoadA})
	exporters := cfg["exporters"].(map[string]any)
	logs := exporters["otlp_http/logs-p-001"].(map[string]any)
	if logs["logs_endpoint"] != "http://logs-p-001.load-test-backends.svc:9428/insert/opentelemetry/v1/logs" {
		t.Errorf("logs endpoint %v", logs["logs_endpoint"])
	}
	if logs["headers"].(map[string]any)["VL-Extra-Fields"] != "pipeline=p-001" {
		t.Errorf("logs headers %v", logs["headers"])
	}
	metrics := exporters["otlp_http/metrics-p-000"].(map[string]any)
	if !strings.HasSuffix(metrics["metrics_endpoint"].(string), "?extra_label=pipeline=p-000") {
		t.Errorf("metrics endpoint %v", metrics["metrics_endpoint"])
	}
	if q := logs["sending_queue"].(map[string]any)["queue_size"]; q != float64(128) {
		t.Errorf("queue size %v, want 128", q)
	}
}

func TestBuildConfigTooLarge(t *testing.T) {
	if _, _, err := BuildConfig(spec.RunSpec{Shape: spec.ShapeF1, Tenants: 600, Load: spec.LoadA}); err == nil {
		t.Error("600 tenants: want error above the 1 MiB ConfigMap limit")
	}
	if _, _, err := BuildConfig(spec.RunSpec{Shape: spec.ShapeF1, Tenants: 300, Load: spec.LoadA}); err != nil {
		t.Errorf("300 tenants: %v", err)
	}
}
