package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHistoryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nested", "history.jsonl")
	if runs, err := loadHistory(path); err != nil || runs != nil {
		t.Fatalf("missing file = (%v, %v), want empty history", runs, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	for _, r := range []runRecord{
		{Time: now, Total: 1000, Ready: 1000},
		{Time: now, Total: 2000, Ready: 1500},
	} {
		if err := appendHistory(path, r); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("not json\n") //nolint:errcheck
	f.Close()                   //nolint:errcheck

	runs, err := loadHistory(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 2 || runs[0].Total != 1000 || !runs[0].Time.Equal(now) || runs[1].Ready != 1500 {
		t.Errorf("runs = %+v", runs)
	}
}

func TestLargestCleanTotal(t *testing.T) {
	now := time.Now()
	recent := now.Add(-time.Hour)
	runs := []runRecord{
		{Time: recent, Total: 1000, Ready: 1000},
		{Time: recent, Total: 2000, Ready: 1900}, // 95% ready: clean
		{Time: recent, Total: 5000, Ready: 4000}, // 80% ready: not clean
		{Time: recent, Total: 6000, Ready: 6000, FailedShards: 1},
		{Time: recent, Total: 7000, Ready: 7000, Interrupted: true},
		{Time: now.Add(-historyWindow - time.Hour), Total: 9000, Ready: 9000}, // too old
	}
	if got := largestCleanTotal(runs, now); got != 2000 {
		t.Errorf("largest clean = %d, want 2000", got)
	}
	if got := largestCleanTotal(nil, now); got != 0 {
		t.Errorf("empty history = %d, want 0", got)
	}
}

func TestPreflightConfirm(t *testing.T) {
	for _, tc := range []struct {
		name               string
		total, proven      int
		interactive, force bool
		input              string
		want               bool
	}{
		{"within step", 2000, 1000, true, false, "", true},
		{"within no-history max", 500, 0, true, false, "", true},
		{"y runs", 5500, 1000, true, false, "y\n", true},
		{"YES runs", 5500, 1000, true, false, "YES\n", true},
		{"n aborts", 5500, 1000, true, false, "n\n", false},
		{"enter aborts", 5500, 1000, true, false, "\n", false},
		{"EOF aborts", 5500, 1000, true, false, "", false},
		{"reprompts on junk", 5500, 1000, true, false, "maybe\ny\n", true},
		{"no history asks", 5500, 0, true, false, "n\n", false},
		{"non-interactive aborts", 5500, 1000, false, false, "y\n", false},
		{"force runs", 5500, 1000, false, true, "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := preflightConfirm(tc.total, tc.proven, tc.interactive, tc.force, strings.NewReader(tc.input), io.Discard)
			if got != tc.want {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPreflightConfirmMessage(t *testing.T) {
	var out strings.Builder
	preflightConfirm(5500, 1000, true, false, strings.NewReader("y\n"), &out)
	for _, want := range []string{
		"was 1,000. Proposed test is 5,500 (5.5x previous test); up to 2,000 runs without confirmation.",
		"Run with -total 5,500 anyway? [y/N]",
		"Proceeding with 5,500 (WARN: 5.5x previous test)",
	} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestCommas(t *testing.T) {
	for n, want := range map[int]string{0: "0", 999: "999", 1000: "1,000", 123456: "123,456", 1234567: "1,234,567", -1000: "-1,000", -100: "-100"} {
		if got := commas(n); got != want {
			t.Errorf("commas(%d) = %q, want %q", n, got, want)
		}
	}
}

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

	if d, r := createWindow(); d != 0 || r != 0 {
		t.Errorf("empty window = (%v, %v), want (0, 0)", d, r)
	}

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

func TestSignalPacket(t *testing.T) {
	got := signalPacket("tok", 0x01020304, 1)
	want := []byte{'t', 'o', 'k', 4, 3, 2, 1, 1}
	if string(got) != string(want) {
		t.Fatalf("packet = %v, want %v", got, want)
	}
	if p := signalPacket("", 10, 0); string(p) != string([]byte{10, 0, 0, 0, 0}) {
		t.Fatalf("empty-token packet = %v", p)
	}
}

func TestShardSignalBase(t *testing.T) {
	// 10 Sandboxes over 3 shards: shard 0 takes the remainder (4), then 3, 3.
	// Shards must tile the id range exactly, with no overlap and no gap.
	const base, total, shards = 100, 10, 3
	want := []int{100, 104, 107}
	for i, w := range want {
		if got := shardSignalBase(base, total, shards, i); got != w {
			t.Fatalf("shard %d base = %d, want %d", i, got, w)
		}
	}
	if end := shardSignalBase(base, total, shards, shards-1) + total/shards; end != base+total {
		t.Fatalf("last shard ends at %d, want %d", end, base+total)
	}
}

func TestSignallerNilIsNoop(t *testing.T) {
	var s *signaller
	s.on(1) // must not panic when signals are disabled
	s.off(1)
	s.flush()
	if s, err := newSignaller("", 7777, "", 1, 0); s != nil || err != nil {
		t.Fatalf("no host = (%v, %v), want disabled", s, err)
	}
	if _, err := newSignaller("127.0.0.1", 7777, "", 1, 0); err == nil {
		t.Fatal("host without token should be rejected")
	}
	if _, err := newSignaller("127.0.0.1", 7777, "tok", 0, 0); err == nil {
		t.Fatal("repeat 0 should be rejected")
	}
}

func TestSignallerSendsDatagrams(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close() //nolint:errcheck
	port := pc.LocalAddr().(*net.UDPAddr).Port
	s, err := newSignaller("127.0.0.1", port, "devtok", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	s.on(2570) // 0x0a0a: bytes that would split a line-based sender
	s.off(2570)
	buf := make([]byte, 64)
	for _, want := range [][]byte{signalPacket("devtok", 2570, 1), signalPacket("devtok", 2570, 0)} {
		pc.SetReadDeadline(time.Now().Add(5 * time.Second)) //nolint:errcheck
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			t.Fatal(err)
		}
		if string(buf[:n]) != string(want) {
			t.Fatalf("datagram = %v, want %v", buf[:n], want)
		}
	}
}

func TestSignallerRepeats(t *testing.T) {
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close() //nolint:errcheck
	port := pc.LocalAddr().(*net.UDPAddr).Port
	s, err := newSignaller("127.0.0.1", port, "devtok", 3, 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	s.off(7)
	s.flush() // every repeat must be on the wire once flush returns
	want := signalPacket("devtok", 7, 0)
	buf := make([]byte, 64)
	for i := 0; i < 3; i++ {
		pc.SetReadDeadline(time.Now().Add(time.Second)) //nolint:errcheck
		n, _, err := pc.ReadFrom(buf)
		if err != nil {
			t.Fatalf("repeat %d: %v", i, err)
		}
		if string(buf[:n]) != string(want) {
			t.Fatalf("repeat %d = %v, want %v", i, buf[:n], want)
		}
	}
	pc.SetReadDeadline(time.Now().Add(100 * time.Millisecond)) //nolint:errcheck
	if n, _, err := pc.ReadFrom(buf); err == nil {
		t.Fatalf("unexpected 4th datagram %v", buf[:n])
	}
}
