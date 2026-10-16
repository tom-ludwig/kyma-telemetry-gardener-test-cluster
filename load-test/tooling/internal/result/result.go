package result

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/manifests"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"time"

	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

const (
	// VMPodScrape copies the pod label loadtest.kyma-project.io/run under this sanitized name.
	RunPromLabel = "loadtest_kyma_project_io_run"
	collectorSel = `namespace="otel-load-test",container="otelcol"`
	step         = 15 * time.Second
	hold         = 5 * time.Minute
)

type Record struct {
	Run           RunInfo                 `json:"run"`
	Setup         Setup                   `json:"setup"`
	Config        ConfigRecord            `json:"config"`
	Signals       map[string]SignalResult `json:"signals"`
	Collector     CollectorResult         `json:"collector"`
	Limit         Limits                  `json:"limit"`
	Valid         bool                    `json:"valid"`
	InvalidReason string                  `json:"invalid_reason,omitempty"`
}

type RunInfo struct {
	ID             string    `json:"id"`
	Date           time.Time `json:"date"`
	CollectorImage string    `json:"collector_image"`
	Start          time.Time `json:"start"`        // generators started
	SteadyStart    time.Time `json:"steady_start"` // Window W, for Grafana
	SteadyEnd      time.Time `json:"steady_end"`
	End            time.Time `json:"end"` // cool-down done
}

type Setup struct {
	Shape                string         `json:"shape"`
	Load                 string         `json:"load"`
	Tenants              int            `json:"tenants"`
	Health               string         `json:"health"`
	RatePerTenant        map[string]int `json:"rate_per_tenant"`
	OfferedPerS          map[string]int `json:"offered_per_s"`
	CPULimit             *string        `json:"cpu_limit"` // none, as in TM
	MemLimitMiB          int            `json:"mem_limit_mib"`
	QueueSizePerExporter int            `json:"queue_size_per_exporter"`
}

type ConfigRecord struct {
	manifests.ConfigStats
	StartupS float64 `json:"startup_s"`
}

type SignalResult struct {
	ReceivedPerS        float64    `json:"received_per_s"`
	ExportedPerS        float64    `json:"exported_per_s"`
	ExportedHealthyPerS float64    `json:"exported_healthy_per_s"` // without the faulty p-000 exporter
	PerPipelineMinMax   [2]float64 `json:"per_pipeline_min_max"`
	Refused             float64    `json:"refused"`
	EnqueueFailed       float64    `json:"enqueue_failed"`
	SendFailed          float64    `json:"send_failed"`
	QueueUtilMax        float64    `json:"queue_util_max"`
	BackendIngestPerS   float64    `json:"backend_ingest_per_s"`
	BackendIngestRatio  float64    `json:"backend_ingest_ratio"`
	BackendCPUMax       float64    `json:"backend_cpu_max"`
}

type CollectorResult struct {
	RSSMaxMiB        float64 `json:"rss_max_mib"`
	WorkingSetMaxMiB float64 `json:"working_set_max_mib"`
	CPUAvg           float64 `json:"cpu_avg"`
	CPUP95           float64 `json:"cpu_p95"`
	// From the pod status, watched every 30 s.
	Restarts    int32    `json:"restarts"`
	RestartAtS  *float64 `json:"restart_at_s"` // first restart, seconds after generator start
	OOMKilled   bool     `json:"oom_killed"`
	OOMAtS      *float64 `json:"oom_at_s"`
	Evicted     bool     `json:"evicted"`
	QueueDrainS *float64 `json:"queue_drain_s"` // null: queues not empty within cool-down
}

type Hit struct {
	ID     string  `json:"id"`
	Signal string  `json:"signal,omitempty"`
	AtS    float64 `json:"at_s"` // seconds after generator start
}

type Limits struct {
	First *Hit  `json:"first"`
	All   []Hit `json:"all"`
	Hard  bool  `json:"hard"` // any of H1–H4
}

func (l *Limits) Add(h Hit) {
	l.All = append(l.All, h)
	sort.Slice(l.All, func(i, j int) bool { return l.All[i].AtS < l.All[j].AtS })
	l.First = &l.All[0]
	l.Hard = l.Hard || h.ID[0] == 'H'
}

// Prom queries the self-monitoring vm-single through the Prometheus API.
type Prom struct{ API promv1.API }

// value returns the first sample of an instant query; ok is false when the query has no data.
func (p Prom) Value(ctx context.Context, q string, at time.Time) (v float64, ok bool, err error) {
	res, _, _, err := p.API.Query(ctx, q, at)
	if err != nil {
		return 0, false, fmt.Errorf("query %s: %w", q, err)
	}
	vec, isVec := res.(model.Vector)
	if !isVec || len(vec) == 0 {
		return 0, false, nil
	}
	return float64(vec[0].Value), true, nil
}

// required fails the run when the query has no data; the record never gets invented zeros.
func (p Prom) Required(ctx context.Context, q string, at time.Time) (float64, error) {
	v, ok, err := p.Value(ctx, q, at)
	if err != nil {
		return 0, err
	}
	if !ok {
		return 0, fmt.Errorf("no data: %s", q)
	}
	return v, nil
}

// counter is for failure counters that only appear after the first event. Called only
// after required queries proved the run's series are scraped, so no data means zero events.
func (p Prom) Counter(ctx context.Context, q string, at time.Time) (float64, error) {
	v, _, err := p.Value(ctx, q, at)
	return v, err
}

// trueSteps returns the timestamps where a filtering query (e.g. `x > 0`) has data.
func (p Prom) TrueSteps(ctx context.Context, q string, from, to time.Time) ([]time.Time, error) {
	res, _, _, err := p.API.QueryRange(ctx, q, promv1.Range{Start: from, End: to, Step: step})
	if err != nil {
		return nil, fmt.Errorf("query %s: %w", q, err)
	}
	seen := map[time.Time]bool{}
	for _, series := range res.(model.Matrix) {
		for _, sample := range series.Values {
			seen[sample.Timestamp.Time()] = true
		}
	}
	times := make([]time.Time, 0, len(seen))
	for t := range seen {
		times = append(times, t)
	}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	return times, nil
}

// FirstHeld returns the start of the first stretch of consecutive steps lasting at least d.
func FirstHeld(times []time.Time, step, d time.Duration) (time.Time, bool) {
	if len(times) == 0 {
		return time.Time{}, false
	}
	start := times[0]
	for i := 1; i < len(times); i++ {
		if times[i].Sub(times[i-1]) > step+step/2 {
			start = times[i]
		}
		if times[i].Sub(start) >= d {
			return start, true
		}
	}
	return time.Time{}, false
}

type Window struct{ Start, SteadyStart, SteadyEnd, End time.Time }

func (w Window) promRange() string {
	return fmt.Sprintf("%ds", int(w.SteadyEnd.Sub(w.SteadyStart).Seconds()))
}

type Selectors struct{ Run, Exp, Healthy, Backend string }

func SelectorsFor(s spec.RunSpec, sig spec.Signal) Selectors {
	run := fmt.Sprintf(`%s=%q`, RunPromLabel, s.ID())
	exp := fmt.Sprintf(`%s,exporter=~"otlp_http/%s-p-.*"`, run, sig.Name)
	healthy := exp
	if s.Health == spec.Faulty {
		healthy = fmt.Sprintf(`%s,exporter!="otlp_http/%s-p-000"`, exp, sig.Name)
	}
	backend := fmt.Sprintf(`namespace=%q,pod=~"%s-load-test-.*"`, manifests.BackendNamespace, sig.Backend)
	return Selectors{Run: run, Exp: exp, Healthy: healthy, Backend: backend}
}

// signalResult runs the per-signal queries over the steady Window W.
func (p Prom) QuerySignal(ctx context.Context, s spec.RunSpec, sig spec.Signal, w Window) (SignalResult, error) {
	sel, W, at, t := SelectorsFor(s, sig), w.promRange(), w.SteadyEnd, sig.Item
	var r SignalResult
	var err error
	if r.ReceivedPerS, err = p.Required(ctx, fmt.Sprintf(`sum(rate(otelcol_receiver_accepted_%s_total{%s}[%s]))`, t, sel.Run, W), at); err != nil {
		return r, err
	}
	if r.ExportedPerS, err = p.Required(ctx, fmt.Sprintf(`sum(rate(otelcol_exporter_sent_%s_total{%s}[%s]))`, t, sel.Exp, W), at); err != nil {
		return r, err
	}
	perPipeline := fmt.Sprintf(`sum by (exporter) (rate(otelcol_exporter_sent_%s_total{%s}[%s]))`, t, sel.Healthy, W)
	if r.PerPipelineMinMax[0], err = p.Required(ctx, "min("+perPipeline+")", at); err != nil {
		return r, err
	}
	if r.PerPipelineMinMax[1], err = p.Required(ctx, "max("+perPipeline+")", at); err != nil {
		return r, err
	}
	ingest := sel.Backend
	if sig.IngestFilter != "" {
		ingest += "," + sig.IngestFilter
	}
	if r.BackendIngestPerS, err = p.Required(ctx, fmt.Sprintf(`sum(rate(%s{%s}[%s]))`, sig.IngestMetric, ingest, W), at); err != nil {
		return r, err
	}
	if r.BackendCPUMax, err = p.Required(ctx, fmt.Sprintf(`max_over_time(sum(rate(container_cpu_usage_seconds_total{%s,container!=""}[1m]))[%s:%s])`, sel.Backend, W, step), at); err != nil {
		return r, err
	}
	if r.Refused, err = p.Counter(ctx, fmt.Sprintf(`sum(increase(otelcol_receiver_refused_%s_total{%s}[%s]))`, t, sel.Run, W), at); err != nil {
		return r, err
	}
	if r.EnqueueFailed, err = p.Counter(ctx, fmt.Sprintf(`sum(increase(otelcol_exporter_enqueue_failed_%s_total{%s}[%s]))`, t, sel.Healthy, W), at); err != nil {
		return r, err
	}
	if r.SendFailed, err = p.Counter(ctx, fmt.Sprintf(`sum(increase(otelcol_exporter_send_failed_%s_total{%s}[%s]))`, t, sel.Healthy, W), at); err != nil {
		return r, err
	}
	if r.QueueUtilMax, err = p.Counter(ctx, fmt.Sprintf(`max_over_time(max(otelcol_exporter_queue_size{%[1]s} / otelcol_exporter_queue_capacity{%[1]s})[%[2]s:%[3]s])`, sel.Healthy, W, step), at); err != nil {
		return r, err
	}
	r.ExportedHealthyPerS = r.ExportedPerS
	if s.Health == spec.Faulty {
		// the faulty exporter's successful sends land in the fault backend, not in this backend
		if r.ExportedHealthyPerS, err = p.Required(ctx, fmt.Sprintf(`sum(rate(otelcol_exporter_sent_%s_total{%s}[%s]))`, t, sel.Healthy, W), at); err != nil {
			return r, err
		}
	}
	r.BackendIngestRatio = r.BackendIngestPerS / max(r.ExportedHealthyPerS, 1)
	return r, nil
}

func (p Prom) QueryCollector(ctx context.Context, s spec.RunSpec, w Window) (CollectorResult, error) {
	run := fmt.Sprintf(`%s=%q`, RunPromLabel, s.ID())
	W, at := w.promRange(), w.SteadyEnd
	const mib = 1 << 20
	var r CollectorResult
	var err error
	if r.RSSMaxMiB, err = p.Required(ctx, fmt.Sprintf(`max(max_over_time(otelcol_process_memory_rss_bytes{%s}[%s])) / %d`, run, W, mib), at); err != nil {
		return r, err
	}
	if r.WorkingSetMaxMiB, err = p.Required(ctx, fmt.Sprintf(`max(max_over_time(container_memory_working_set_bytes{%s}[%s])) / %d`, collectorSel, W, mib), at); err != nil {
		return r, err
	}
	if r.CPUAvg, err = p.Required(ctx, fmt.Sprintf(`sum(rate(container_cpu_usage_seconds_total{%s}[%s]))`, collectorSel, W), at); err != nil {
		return r, err
	}
	if r.CPUP95, err = p.Required(ctx, fmt.Sprintf(`quantile_over_time(0.95, sum(rate(container_cpu_usage_seconds_total{%s}[1m]))[%s:%s])`, collectorSel, W, step), at); err != nil {
		return r, err
	}
	return r, nil
}

// limits evaluates H1–H4 and S1–S4; S5 needs the healthy record and is added by the caller.
func (p Prom) EvalLimits(ctx context.Context, s spec.RunSpec, w Window, rec *Record, nodeACores float64) error {
	at := func(t time.Time) float64 { return t.Sub(w.Start).Seconds() }
	addCollectorLimits(rec, at(w.End))
	if rec.Config.StartupS > 60 {
		rec.Limit.Add(Hit{ID: "H2", AtS: 0})
	}
	limitBytes := float64(s.MemoryLimitMi()) * (1 << 20)
	held := []struct{ id, q string }{
		{"S3", fmt.Sprintf(`max(container_memory_working_set_bytes{%s}) > %f`, collectorSel, 0.8*limitBytes)},
	}
	for _, sig := range spec.Signals {
		sel, t := SelectorsFor(s, sig), sig.Item
		offered := float64(s.OfferedRate(sig))
		if s.Health == spec.Faulty {
			offered = offered * float64(s.Tenants-1) / float64(s.Tenants)
		}
		held = append(held,
			struct{ id, q string }{"H3:" + sig.Name, fmt.Sprintf(`(sum(rate(otelcol_exporter_enqueue_failed_%[1]s_total{%[2]s}[5m])) > 0) or (sum(rate(otelcol_exporter_send_failed_%[1]s_total{%[2]s}[5m])) > 0)`, t, sel.Healthy)},
			struct{ id, q string }{"S1:" + sig.Name, fmt.Sprintf(`sum(rate(otelcol_receiver_refused_%s_total{%s}[5m])) > 0`, t, sel.Run)},
			struct{ id, q string }{"S2:" + sig.Name, fmt.Sprintf(`max(otelcol_exporter_queue_size{%[1]s} / otelcol_exporter_queue_capacity{%[1]s}) > 0.8`, sel.Healthy)},
		)
		if s.Load == spec.LoadA {
			held = append(held, struct{ id, q string }{"H4:" + sig.Name, fmt.Sprintf(`sum(rate(otelcol_exporter_sent_%s_total{%s}[1m])) < %f`, t, sel.Healthy, 0.95*offered)})
		}
		if rec.Signals[sig.Name].BackendIngestPerS < 0.99*offered {
			rec.Limit.Add(Hit{ID: "H3", Signal: sig.Name, AtS: at(w.SteadyEnd)})
		}
	}
	for _, c := range held {
		times, err := p.TrueSteps(ctx, c.q, w.SteadyStart, w.SteadyEnd)
		if err != nil {
			return err
		}
		start, ok := FirstHeld(times, step, hold)
		if !ok {
			continue
		}
		id, signal := splitID(c.id)
		rec.Limit.Add(Hit{ID: id, Signal: signal, AtS: at(start)})
	}
	if nodeACores > 0 && rec.Collector.CPUP95 >= 0.9*nodeACores {
		rec.Limit.Add(Hit{ID: "S4", AtS: at(w.SteadyEnd)})
	}
	return nil
}

// addCollectorLimits: H1 for an OOM kill or eviction, H2 for any restart (pod status).
func addCollectorLimits(rec *Record, endS float64) {
	c := rec.Collector
	if c.OOMAtS != nil {
		rec.Limit.Add(Hit{ID: "H1", AtS: *c.OOMAtS})
	}
	if c.Evicted && c.OOMAtS == nil {
		rec.Limit.Add(Hit{ID: "H1", AtS: endS})
	}
	if c.RestartAtS != nil {
		rec.Limit.Add(Hit{ID: "H2", AtS: *c.RestartAtS})
	}
}

func splitID(id string) (string, string) {
	for i := range id {
		if id[i] == ':' {
			return id[:i], id[i+1:]
		}
	}
	return id, ""
}

// AddIsolation adds S5 for faulty runs: healthy pipelines export > 5 % less than in the healthy run.
// healthy is the healthy run of the same shape, tenants and load from this process, or nil.
func AddIsolation(rec *Record, s spec.RunSpec, healthy *Record, w Window) {
	if s.Health != spec.Faulty || healthy == nil {
		return
	}
	share := float64(s.Tenants-1) / float64(s.Tenants)
	for name, r := range rec.Signals {
		base := healthy.Signals[name].ExportedPerS * share
		if base > 0 && r.ExportedHealthyPerS < 0.95*base {
			rec.Limit.Add(Hit{ID: "S5", Signal: name, AtS: w.SteadyEnd.Sub(w.Start).Seconds()})
		}
	}
}

// PrintRecord writes the record as one JSON line to stdout; VLAgent ships it to VictoriaLogs.
// Progress logs go to stderr, so stdout carries only results.
func PrintRecord(rec *Record) error {
	out, err := json.Marshal(struct {
		Msg    string  `json:"msg"`
		Result *Record `json:"result"`
	}{"loadtest result", rec})
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(os.Stdout, string(out))
	return err
}
