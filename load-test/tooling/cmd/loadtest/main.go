// Command tooling runs the phase 1 pipeline-scalability load test against a plain OTel collector.
package main

import (
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/result"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/runner"
	"github.com/tom-ludwig/kyma-telemetry-gardener-test-cluster/load-test/tooling/internal/spec"

	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/api"
	promv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

func main() {
	var s spec.RunSpec
	flag.StringVar(&s.Target, "target", spec.TargetOTel, "otel (plain collector we build) or tm (Telemetry Manager gateway via pipeline CRs)")
	flag.StringVar(&s.Shape, "shape", spec.ShapeF1, "F1 (fan-out) or R1 (routing); tm supports F1 only")
	flag.IntVar(&s.Tenants, "tenants", 1, "tenants; each has one traces, metrics and logs pipeline")
	flag.StringVar(&s.Load, "load", spec.LoadA, "A (fixed total split across tenants) or B (-rate per tenant)")
	flag.StringVar(&s.Health, "health", spec.Healthy, "healthy or faulty (p-000 of each signal goes to the fault backend)")
	flag.IntVar(&s.Rep, "rep", 1, "repetition number, part of the run ID")
	flag.IntVar(&s.Rate, "rate", 1000, "load B: items/s per signal per tenant")
	matrix := flag.Bool("matrix", false, "run the whole matrix (binary search, faulty runs, repetitions)")
	only := flag.String("only", "", "matrix: only run IDs matching this glob, e.g. 'otel-f1-A-*'")
	vmURL := flag.String("vm-url", "http://vmsingle-vm-single.monitoring.svc:8428", "self-monitoring vm-single")
	warmup := flag.Duration("warmup", 5*time.Minute, "warm-up, not measured")
	steady := flag.Duration("steady", 20*time.Minute, "measured steady state")
	cooldown := flag.Duration("cooldown", 3*time.Minute, "cool-down after the load stops")
	skipPreflight := flag.Bool("skip-preflight", false, "skip preflight capacity, backend readiness and disk checks")
	nodeSelector := flag.String("node-selector", "", "key=value node selector applied to all created pods (e.g. kubernetes.io/hostname=node1)")
	flag.Parse()

	ns, err := parseNodeSelector(*nodeSelector)
	if err != nil {
		log.Fatal(err)
	}
	if err := run(s, *matrix, *only, *vmURL, *warmup, *steady, *cooldown, *skipPreflight, ns); err != nil {
		log.Fatal(err)
	}
}

func run(s spec.RunSpec, matrix bool, only, vmURL string, warmup, steady, cooldown time.Duration, skipPreflight bool, nodeSelector map[string]string) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	k, err := kubeClient()
	if err != nil {
		return err
	}
	promClient, err := api.NewClient(api.Config{Address: vmURL})
	if err != nil {
		return err
	}
	r, err := runner.New(k, result.Prom{API: promv1.NewAPI(promClient)}, warmup, steady, cooldown, os.Args[1:], skipPreflight, nodeSelector)
	if err != nil {
		return err
	}
	if matrix {
		return r.Matrix(ctx, s.Target, s.Rate, only)
	}
	if err := s.Validate(); err != nil {
		return err
	}
	rec, err := r.Run(ctx, s)
	if err != nil {
		return err
	}
	log.Printf("%s: done, valid=%t, first limit %+v", rec.Run.ID, rec.Valid, rec.Limit.First)
	return nil
}

func parseNodeSelector(s string) (map[string]string, error) {
	if s == "" {
		return nil, nil
	}
	parts := strings.SplitN(s, "=", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return nil, fmt.Errorf("-node-selector %q: must be key=value", s)
	}
	return map[string]string{parts[0]: parts[1]}, nil
}

func kubeClient() (client.Client, error) {
	cfg, err := config.GetConfig()
	if err != nil {
		return nil, err
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		return nil, err
	}
	return client.New(cfg, client.Options{Scheme: scheme})
}
