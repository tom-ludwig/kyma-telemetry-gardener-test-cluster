package manifests

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

func parseConfig(t *testing.T, s spec.RunSpec) (map[string]any, ConfigStats) {
	t.Helper()
	parts, stats, err := BuildConfig(s)
	if err != nil {
		t.Fatal(err)
	}
	return mergeParts(t, parts), stats
}

// mergeParts merges like the collector: maps deep, everything else replaced.
func mergeParts(t *testing.T, parts [][]byte) map[string]any {
	t.Helper()
	cfg := map[string]any{}
	for _, raw := range parts {
		var part map[string]any
		if err := yaml.Unmarshal(raw, &part); err != nil {
			t.Fatal(err)
		}
		deepMerge(cfg, part)
	}
	return cfg
}

func deepMerge(dst, src map[string]any) {
	for k, v := range src {
		sub, ok := v.(map[string]any)
		existing, exists := dst[k].(map[string]any)
		if !ok || !exists {
			dst[k] = v
			continue
		}
		deepMerge(existing, sub)
	}
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
	// Every exporter points at its per-pipeline Service, backed by the nop debug collector on
	// the OTLP/HTTP port. The exporter appends /v1/<signal> itself, so only the base is set.
	if logs["endpoint"] != "http://logs-p-001.load-test-backends.svc:4318" {
		t.Errorf("logs endpoint %v", logs["endpoint"])
	}
	if logs["compression"] != "gzip" {
		t.Errorf("logs compression %v, want gzip", logs["compression"])
	}
	metrics := exporters["otlp_http/metrics-p-000"].(map[string]any)
	if metrics["endpoint"] != "http://metrics-p-000.load-test-backends.svc:4318" {
		t.Errorf("metrics endpoint %v", metrics["endpoint"])
	}
	if q := logs["sending_queue"].(map[string]any)["queue_size"]; q != float64(128) {
		t.Errorf("queue size %v, want 128", q)
	}
}

func TestBuildConfigSplit(t *testing.T) {
	for _, shape := range []string{spec.ShapeF1, spec.ShapeR1} {
		parts, stats, err := BuildConfig(spec.RunSpec{Shape: shape, Tenants: 1000, Load: spec.LoadA})
		if err != nil {
			t.Fatalf("%s: %v", shape, err)
		}
		if len(parts) < 3 || stats.ConfigMaps != len(parts) {
			t.Errorf("%s: %d parts, stats %+v; want more than one tenant part", shape, len(parts), stats)
		}
		for i, p := range parts {
			if len(p) > maxConfigMapBytes {
				t.Errorf("%s: part %d is %d bytes, above 1 MiB", shape, i, len(p))
			}
		}
		if pipelines := mergeParts(t, parts)["service"].(map[string]any)["pipelines"].(map[string]any); len(pipelines) != stats.ServicePipelines {
			t.Errorf("%s: merged %d pipelines, want %d", shape, len(pipelines), stats.ServicePipelines)
		}
	}
}

func TestSplitConfigMergesBack(t *testing.T) {
	base := map[string]any{"receivers": map[string]any{"otlp": map[string]any{}}}
	sections := map[string]map[string]any{
		"processors": {"batch": map[string]any{"timeout": "1s"}, "filter/a": map[string]any{"error_mode": "ignore"}},
		"exporters":  {"debug/a": map[string]any{}, "debug/b": map[string]any{}},
		"pipelines": {
			"traces/a": map[string]any{"receivers": []any{"otlp"}, "processors": []any{"filter/a", "batch"}, "exporters": []any{"debug/a"}},
			"traces/b": map[string]any{"receivers": []any{"otlp"}, "processors": []any{"batch"}, "exporters": []any{"debug/b"}},
		},
	}
	whole, err := splitConfig(base, sections, maxConfigMapBytes)
	if err != nil {
		t.Fatal(err)
	}
	// 200 bytes fits one pipeline per part
	split, err := splitConfig(base, sections, 200)
	if err != nil {
		t.Fatal(err)
	}
	if len(whole) != 2 || len(split) < 4 {
		t.Errorf("got %d and %d parts, want 2 and at least 4", len(whole), len(split))
	}
	if !reflect.DeepEqual(mergeParts(t, whole), mergeParts(t, split)) {
		t.Error("merged split parts differ from the unsplit config")
	}
	if _, err := splitConfig(base, sections, 10); err == nil {
		t.Error("want error for an entry above the budget")
	}
}

func TestTMConfigStats(t *testing.T) {
	raw := []byte(`receivers:
  otlp: {}
processors:
  batch: {}
  filter/p-000: {}
exporters:
  otlp_http/p-000: {}
  otlp_http/p-001: {}
service:
  pipelines:
    metrics/p-000:
      receivers: [otlp]
      processors: [batch, filter/p-000]
      exporters: [otlp_http/p-000]
    metrics/p-001:
      receivers: [otlp]
      processors: [batch]
      exporters: [otlp_http/p-001]
`)
	got, err := TMConfigStats(raw)
	if err != nil {
		t.Fatal(err)
	}
	want := ConfigStats{Bytes: len(raw), ConfigMaps: 1, ServicePipelines: 2, ProcessorInstances: 3, Exporters: 2}
	if got != want {
		t.Errorf("got %+v, want %+v", got, want)
	}
}
