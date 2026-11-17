package metrics

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPercentilesNearestRank(t *testing.T) {
	h := NewHistogram(1000)
	for i := 1; i <= 100; i++ {
		h.Observe(time.Duration(i) * time.Millisecond)
	}
	s := h.Snapshot()
	if s.Count != 100 || s.P50 != 50*time.Millisecond || s.P90 != 90*time.Millisecond ||
		s.P99 != 99*time.Millisecond || s.Max != 100*time.Millisecond {
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestRingBufferKeepsMostRecent(t *testing.T) {
	h := NewHistogram(10)
	for i := 1; i <= 25; i++ {
		h.Observe(time.Duration(i))
	}
	s := h.Snapshot()
	if s.Count != 25 || s.Max != 25 || s.P50 != 20 { // window holds 16..25
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestEmptySnapshot(t *testing.T) {
	if s := NewHistogram(4).Snapshot(); s.Count != 0 || s.P99 != 0 {
		t.Fatalf("snapshot=%+v", s)
	}
}

func TestRegistryConcurrent(t *testing.T) {
	r := NewRegistry(100)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				r.Inc("hits")
				r.Hist("lat").Observe(time.Microsecond)
			}
		}()
	}
	wg.Wait()
	if r.Counter("hits") != 5000 {
		t.Fatalf("hits=%d", r.Counter("hits"))
	}
	if !strings.Contains(r.Report(), "lat") {
		t.Fatal("report missing histogram")
	}
}
