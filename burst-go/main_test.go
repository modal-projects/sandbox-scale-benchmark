package main

import (
	"context"
	"errors"
	"fmt"
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

func TestParseCountLine(t *testing.T) {
	for _, tc := range []struct {
		line, prefix string
		want         int64
		ok           bool
	}{
		{"#retries 7", "#retries ", 7, true},
		{"#peak 1250", "#peak ", 1250, true},
		{"#retries x", "#retries ", 0, false},
		{"#peak 3", "#retries ", 0, false},
	} {
		n, ok := parseCountLine(tc.line, tc.prefix)
		if ok != tc.ok || n != tc.want {
			t.Errorf("parseCountLine(%q, %q) = (%d, %v), want (%d, %v)", tc.line, tc.prefix, n, ok, tc.want, tc.ok)
		}
	}
}

func TestBadExitIsNotRetried(t *testing.T) {
	// runCmd must be able to tell "the command ran and failed" apart from a
	// transport failure, or it would retry deterministic failures.
	var exit badExit
	if !errors.As(error(badExit{code: 3}), &exit) || exit.code != 3 {
		t.Fatal("badExit is not recoverable via errors.As")
	}
	if errors.As(fmt.Errorf("exec: %w", context.DeadlineExceeded), &exit) {
		t.Error("a transport failure was classified as a bad exit")
	}
}

func TestExecBudget(t *testing.T) {
	// The SDK rejects a fractional Timeout and reads 0 as "no timeout", so the
	// budget must always be whole seconds and never zero.
	t.Run("no deadline falls back to the flag", func(t *testing.T) {
		d, ok := execBudget(context.Background(), 2*time.Minute)
		if !ok || d != 2*time.Minute {
			t.Fatalf("got (%v, %v)", d, ok)
		}
	})

	t.Run("truncates to whole seconds and never exceeds the budget", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		d, ok := execBudget(ctx, 2*time.Minute)
		if !ok {
			t.Fatal("budget unexpectedly exhausted")
		}
		if d%time.Second != 0 {
			t.Errorf("timeout %v is not a whole number of seconds", d)
		}
		if d <= 0 || d > 2*time.Minute {
			t.Errorf("timeout %v outside (0, 2m]", d)
		}
	})

	t.Run("sub-second remainder gives up rather than sending zero", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
		defer cancel()
		if d, ok := execBudget(ctx, 2*time.Minute); ok {
			t.Errorf("got (%v, true), want give-up: zero would mean no timeout", d)
		}
	})

	t.Run("expired context gives up", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
		defer cancel()
		if _, ok := execBudget(ctx, 2*time.Minute); ok {
			t.Error("expired context still yielded a budget")
		}
	})
}
