package runner

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
)

func TestGeneratorsTooSlow(t *testing.T) {
	cases := []struct {
		name                                string
		received, offered, refused, loadCPU float64
		want                                bool
	}{
		{"collector CPU-bound, load node idle", 50_000, 300_000, 0, 0.4, false},
		{"load node saturated", 50_000, 300_000, 0, 0.95, true},
		{"collector refuses data", 50_000, 300_000, 10, 0.95, false},
		{"full rate", 299_000, 300_000, 0, 0.95, false},
	}
	for _, c := range cases {
		if got := generatorsTooSlow(c.received, c.offered, c.refused, c.loadCPU); got != c.want {
			t.Errorf("%s: got %t, want %t", c.name, got, c.want)
		}
	}
}

func TestCollectorWatch(t *testing.T) {
	start := time.Unix(0, 0)
	w := &collectorWatch{start: start}
	healthy := corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{RestartCount: 0}}}}
	w.observe([]corev1.Pod{healthy}, start.Add(30*time.Second))
	if w.restartAt != nil || w.oomAt != nil {
		t.Fatal("restart recorded without restart")
	}
	oom := corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
		RestartCount:         1,
		LastTerminationState: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled"}},
	}}}}
	w.observe([]corev1.Pod{oom}, start.Add(90*time.Second))
	w.observe([]corev1.Pod{oom}, start.Add(120*time.Second))
	if w.restarts != 1 || w.restartAt == nil || *w.restartAt != 90 || w.oomAt == nil || *w.oomAt != 90 {
		t.Errorf("restarts %d restartAt %v oomAt %v, want 1, 90, 90", w.restarts, w.restartAt, w.oomAt)
	}
}