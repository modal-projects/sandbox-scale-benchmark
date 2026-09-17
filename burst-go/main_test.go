package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRecordMinMax(t *testing.T) {
	var lo, hi atomic.Int64
	var wg sync.WaitGroup
	for _, v := range []int64{500, 100, 900, 300, 700} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recordMin(&lo, v)
			recordMax(&hi, v)
		}()
	}
	wg.Wait()
	if lo.Load() != 100 {
		t.Errorf("min = %d, want 100", lo.Load())
	}
	if hi.Load() != 900 {
		t.Errorf("max = %d, want 900", hi.Load())
	}
}

func TestCreateWindow(t *testing.T) {
	t.Cleanup(func() {
		firstCreateNs.Store(0)
		lastCreateNs.Store(0)
		created.Store(0)
	})

	// Fewer than two creates: no window, no rate.
	if d, r := createWindow(); d != 0 || r != 0 {
		t.Errorf("empty window = (%v, %v), want (0, 0)", d, r)
	}

	// 5000 creates spread over 5s is 1000/s, regardless of any later hold.
	base := time.Now().UnixNano()
	firstCreateNs.Store(base)
	lastCreateNs.Store(base + int64(5*time.Second))
	created.Store(5000)
	d, r := createWindow()
	if d != 5*time.Second {
		t.Errorf("span = %v, want 5s", d)
	}
	if r != 1000 {
		t.Errorf("sustained = %v/s, want 1000/s", r)
	}
}

func TestParseWindowLine(t *testing.T) {
	first, last, n, ok := parseWindowLine("#window 100 5000000100 1250")
	if !ok || first != 100 || last != 5000000100 || n != 1250 {
		t.Fatalf("got (%d, %d, %d, %v)", first, last, n, ok)
	}
	for _, bad := range []string{"#window 1 2", "#window a b c", "progress: created 5", "#peak 10"} {
		if _, _, _, ok := parseWindowLine(bad); ok {
			t.Errorf("parsed %q, want rejection", bad)
		}
	}
}

func TestParseFailAndPeakLines(t *testing.T) {
	reason, n, ok := parseFailLine("#fail 42 create sandbox: rpc error with spaces")
	if !ok || n != 42 || reason != "create sandbox: rpc error with spaces" {
		t.Fatalf("got (%q, %d, %v)", reason, n, ok)
	}
	if p, ok := parsePeakLine("#peak 1250"); !ok || p != 1250 {
		t.Fatalf("peak = (%d, %v)", p, ok)
	}
	if _, ok := parsePeakLine("#peak notanumber"); ok {
		t.Error("parsed non-numeric peak")
	}
}

func TestParseDursLine(t *testing.T) {
	op, batch, ok := parseDursLine("#durs create 1000000,2000000")
	if !ok || op != "create" || len(batch) != 2 {
		t.Fatalf("got (%q, %v, %v)", op, batch, ok)
	}
	if batch[0] != time.Millisecond || batch[1] != 2*time.Millisecond {
		t.Errorf("batch = %v", batch)
	}
}

func TestRateLimiterShape(t *testing.T) {
	for _, tc := range []struct {
		perSecond  int
		wantBurst  int
		wantPeriod time.Duration
	}{
		{5, 1, 200 * time.Millisecond},
		{100, 1, 10 * time.Millisecond},
		{2000, 20, 10 * time.Millisecond},
		{10000, 100, 10 * time.Millisecond},
	} {
		burst := max(tc.perSecond/100, 1)
		period := time.Duration(int64(time.Second) * int64(burst) / int64(tc.perSecond))
		if burst != tc.wantBurst || period != tc.wantPeriod {
			t.Errorf("rate %d: burst=%d period=%v, want burst=%d period=%v",
				tc.perSecond, burst, period, tc.wantBurst, tc.wantPeriod)
		}
	}
}
