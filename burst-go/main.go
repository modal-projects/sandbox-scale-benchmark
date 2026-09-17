// Command burst load-tests Modal Sandbox creation at burst scale.
//
// Each session creates a default-sized Sandbox, runs a short shell command in
// it, holds the Sandbox for -lifetime so the run reaches a concurrency
// plateau, and terminates it. The report shows p50/p90/p99 for:
//
//	create   the Sandbox create call
//	exec     the shell command, from exec start to exit
//	ready    create start until the command finished, i.e. time to a usable Sandbox
//
// It also reports peak concurrent live Sandboxes, which is the number any
// concurrency claim actually rests on.
//
// Authentication works like any Modal client: the active profile in
// ~/.modal.toml, or MODAL_TOKEN_ID and MODAL_TOKEN_SECRET (MODAL_PROFILE and
// MODAL_ENVIRONMENT are honored).
//
// Sandboxes go through the v2 backend via Sandboxes.ExperimentalCreate, which
// forces v2 unconditionally. Sandboxes.Create also reaches v2, but only when
// MODAL_SANDBOX_V2=1 or sandbox_v2 = true is set in the profile; calling
// ExperimentalCreate directly means a missing flag cannot silently turn this
// into a v1 benchmark.
//
// This file is self-contained. From an empty directory containing only main.go:
//
/*
   go mod init burst && go mod tidy
   CGO_ENABLED=0 go build -o burst .
   ./burst -total 500 -rate 100 -lifetime 5m
*/
//
// (CGO_ENABLED=0 produces a static binary, required for -shards: runners run
// this same binary, baked into their Image.)
//
// -shards needs that runner Image published first, because the Go SDK cannot
// add a local file to an Image. Build it with the bundled Python helper, which
// tags the Image with the binary's SHA-256 prefix:
//
/*
   CGO_ENABLED=0 go build -o burst .
   uv run python build_runner_image.py burst
   ./burst -total 20000 -rate 2000 -lifetime 5m -shards 8
*/
//
// The driver derives the same tag from its own binary, so an Image built from
// a different build is a lookup failure rather than a stale benchmark.
//
// When doing large runs, please let someone at Modal know.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	toml "github.com/pelletier/go-toml/v2"

	modal "github.com/modal-labs/modal-client/go"
)

// Two file reads and a checksum: enough to prove the Sandbox is usable, with
// nothing to install into the image and no build step to cache.
// Must match RUNNER_PATH in build_runner_image.py.
const runnerBinaryPath = "/usr/local/bin/burst"

const defaultCmd = "cat /etc/os-release > /tmp/osrel && " +
	"wc -l < /proc/meminfo > /tmp/meminfo.lines && " +
	"cksum /tmp/osrel | cut -d' ' -f1 > /tmp/osrel.cksum && " +
	"test -s /tmp/osrel.cksum && test -s /tmp/meminfo.lines"

var (
	total         = flag.Int("total", 500, "Sandboxes to create")
	rate          = flag.Int("rate", 100, "cap on Sandbox creates started per second across the whole run (0 = uncapped)")
	concurrency   = flag.Int("concurrency", 0, "sessions running in parallel (0 = sized automatically from -rate and -lifetime)")
	lifetime      = flag.Duration("lifetime", 5*time.Minute, "how long each Sandbox is held up, measured from its create")
	imageTag      = flag.String("image", "alpine:3.21", "registry tag for the workload Sandboxes")
	runnerImage   = flag.String("runner-image", "burst-runner", "published Image name holding this binary, for -shards runners")
	workloadCmd   = flag.String("cmd", defaultCmd, "shell command to run in each Sandbox; empty skips the exec (BURST_CMD overrides)")
	appName       = flag.String("app", "sandbox-burst-load-test", "Modal App name to create Sandboxes in")
	shards        = flag.Int("shards", 0, "spread the run across this many runner Sandboxes on Modal (0 = run directly from this machine)")
	progressEvery = flag.Duration("progress", 2*time.Second, "how often to print a progress line; rates are measured over this window")
	startAt       = flag.Int64("start-at", 0, "unix nanos at which to begin creating (set by the -shards driver; 0 = start immediately)")
	startDelay    = flag.Duration("start-delay", 45*time.Second, "with -shards, how long runners get to boot before every shard starts creating at once; 0 disables the barrier")
	printImageRef = flag.Bool("print-image-ref", false, "print the runner Image ref this binary needs, and the binary's path, then exit")
	runnerCPU     = flag.Float64("runner-cpu", 4, "CPU cores per -shards runner Sandbox")
	runnerMemory  = flag.Int("runner-memory", 4096, "MiB of memory per -shards runner Sandbox")
)

var (
	// created counts Sandboxes that exist; ready counts ones whose workload
	// also finished. They are separate because a create that succeeds and then
	// fails its exec is still a create, and folding the two would make the
	// create rate lag by the exec duration.
	created  atomic.Int64
	ready    atomic.Int64
	failures atomic.Int64
	live     atomic.Int64
	peakLive atomic.Int64

	// Bounds of the create phase, as Unix nanos. Sustained create rate is
	// measured over this window, not over total elapsed time, which would be
	// diluted by the -lifetime hold.
	firstCreateNs atomic.Int64
	lastCreateNs  atomic.Int64

	// Set when this process started load after its synchronized start time had
	// already passed, which means its samples skew any aggregate.
	missedBarrier atomic.Bool

	// We hard limit the create rate to get an accurate benchmark.
	bootLimiter <-chan struct{}
)

// timeoutMargin is added to -lifetime for the Sandbox's own Timeout, so the
// platform reaps a Sandbox whose session died without terminating it.
const timeoutMargin = 5 * time.Minute

const slowThreshold = 5 * time.Second

// Modal object IDs make otherwise-identical failures unique; strip them so the
// same kind of failure groups together.
var idRe = regexp.MustCompile(`\b[a-z]{2}-[A-Za-z0-9]{8,}\b`)

func normalizeReason(msg string) string {
	msg = strings.ReplaceAll(idRe.ReplaceAllString(msg, "<id>"), "\n", " ")
	if len(msg) > 160 {
		msg = msg[:160]
	}
	return msg
}

type failureCounts struct {
	mu     sync.Mutex
	counts map[string]int
}

func (f *failureCounts) add(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts == nil {
		f.counts = map[string]int{}
	}
	f.counts[normalizeReason(err.Error())]++
}

func (f *failureCounts) addCount(reason string, n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.counts == nil {
		f.counts = map[string]int{}
	}
	f.counts[reason] += n
}

func (f *failureCounts) report(emitRaw bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.counts) == 0 {
		return
	}
	type kv struct {
		reason string
		n      int
	}
	rows := make([]kv, 0, len(f.counts))
	for reason, n := range f.counts {
		rows = append(rows, kv{reason, n})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].n > rows[j].n })
	if emitRaw {
		for _, r := range rows {
			fmt.Printf("#fail %d %s\n", r.n, r.reason)
		}
		return
	}
	fmt.Printf("\nfailure reasons:\n")
	for i, r := range rows {
		if i == 10 {
			fmt.Printf("  ... and %d more kinds\n", len(rows)-10)
			break
		}
		fmt.Printf("  %6d  %s\n", r.n, r.reason)
	}
}

func recordPeak(n int64) { recordMax(&peakLive, n) }

func recordMax(dst *atomic.Int64, n int64) {
	for {
		old := dst.Load()
		if n <= old || dst.CompareAndSwap(old, n) {
			return
		}
	}
}

func recordMin(dst *atomic.Int64, n int64) {
	for {
		old := dst.Load()
		if old != 0 && n >= old {
			return
		}
		if dst.CompareAndSwap(old, n) {
			return
		}
	}
}

// createWindow returns the span from the first create to the last, and the
// sustained creates/s over it. Zero duration means fewer than two creates.
func createWindow() (time.Duration, float64) {
	first, last := firstCreateNs.Load(), lastCreateNs.Load()
	if first == 0 || last <= first {
		return 0, 0
	}
	d := time.Duration(last - first)
	return d, float64(created.Load()) / d.Seconds()
}

type metrics struct {
	mu        sync.Mutex
	durations map[string][]time.Duration
}

func (m *metrics) record(op string, d time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.durations[op] = append(m.durations[op], d)
}

// report prints a human readable table, or raw samples for a driver to merge.
func (m *metrics) report(emitRaw bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, op := range []string{"create", "exec", "ready"} {
		durations := m.durations[op]
		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		if !emitRaw {
			fmt.Printf("%-8s n=%-8d p50=%-9s p90=%-9s p99=%-9s max=%s\n",
				op, len(durations), pct(durations, 0.50), pct(durations, 0.90), pct(durations, 0.99), pct(durations, 1))
			continue
		}
		const chunk = 1000
		for start := 0; start < len(durations); start += chunk {
			batch := durations[start:min(start+chunk, len(durations))]
			ns := make([]string, len(batch))
			for i, d := range batch {
				ns[i] = strconv.FormatInt(d.Nanoseconds(), 10)
			}
			fmt.Printf("#durs %s %s\n", op, strings.Join(ns, ","))
		}
	}
}

// parseDursLine parses "#durs <op> <ns>,<ns>,...".
func parseDursLine(line string) (string, []time.Duration, bool) {
	if !strings.HasPrefix(line, "#durs ") {
		return "", nil, false
	}
	fields := strings.Fields(line)
	if len(fields) != 3 {
		return "", nil, false
	}
	values := strings.Split(fields[2], ",")
	batch := make([]time.Duration, 0, len(values))
	for _, v := range values {
		ns, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return "", nil, false
		}
		batch = append(batch, time.Duration(ns))
	}
	return fields[1], batch, true
}

// parseFailLine parses "#fail <count> <reason...>".
func parseFailLine(line string) (string, int, bool) {
	if !strings.HasPrefix(line, "#fail ") {
		return "", 0, false
	}
	fields := strings.SplitN(line, " ", 3)
	if len(fields) != 3 {
		return "", 0, false
	}
	n, err := strconv.Atoi(fields[1])
	if err != nil {
		return "", 0, false
	}
	return fields[2], n, true
}

// parseWindowLine parses "#window <firstNs> <lastNs> <created>".
func parseWindowLine(line string) (first, last, n int64, ok bool) {
	if !strings.HasPrefix(line, "#window ") {
		return 0, 0, 0, false
	}
	fields := strings.Fields(line)
	if len(fields) != 4 {
		return 0, 0, 0, false
	}
	var err error
	if first, err = strconv.ParseInt(fields[1], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if last, err = strconv.ParseInt(fields[2], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	if n, err = strconv.ParseInt(fields[3], 10, 64); err != nil {
		return 0, 0, 0, false
	}
	return first, last, n, true
}

// parsePeakLine parses "#peak <n>".
func parsePeakLine(line string) (int64, bool) {
	if !strings.HasPrefix(line, "#peak ") {
		return 0, false
	}
	n, err := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, "#peak ")), 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// aggregateDurations merges every shard's raw samples so the driver can compute
// true percentiles over the combined distribution.
type aggregateDurations struct {
	mu        sync.Mutex
	byOp      map[string][]time.Duration
	fails     failureCounts
	peakSum   atomic.Int64
	shardsIn  atomic.Int64
	winFirst  atomic.Int64
	winLast   atomic.Int64
	winCreate atomic.Int64
	missed    atomic.Int64
}

func (a *aggregateDurations) add(op string, batch []time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.byOp == nil {
		a.byOp = map[string][]time.Duration{}
	}
	a.byOp[op] = append(a.byOp[op], batch...)
}

func (a *aggregateDurations) report() {
	a.mu.Lock()
	byOp := a.byOp
	a.mu.Unlock()
	if len(byOp) == 0 {
		return
	}
	fmt.Printf("\nall shards combined:\n")
	if missed := a.missed.Load(); missed > 0 {
		fmt.Printf("WARNING: %d of %d shards started late and missed the synchronized start;\n"+
			"         the combined rate below understates the real burst. Raise -start-delay.\n",
			missed, a.shardsIn.Load())
	}
	// The create window spans every shard's first create to the last, so the
	// rate below is the real aggregate, not a sum of per-shard averages.
	if first, last := a.winFirst.Load(), a.winLast.Load(); first != 0 && last > first {
		span := time.Duration(last - first)
		fmt.Printf("created %d Sandboxes in %s of creates (%.0f/s sustained)\n",
			a.winCreate.Load(), span.Round(time.Millisecond),
			float64(a.winCreate.Load())/span.Seconds())
	}
	// Shards burst together but are not synchronised, so the sum of their peaks
	// is an upper bound on true global peak concurrency, not a measurement.
	fmt.Printf("peak live Sandboxes (sum of %d shard peaks, upper bound): %d\n",
		a.shardsIn.Load(), a.peakSum.Load())
	merged := &metrics{durations: byOp}
	merged.report(false)
	a.fails.report(false)
}

// pct returns the nearest-rank percentile (index ceil(p*n)-1) of an ascending
// slice; pct(s, 1) is the max.
func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	i = min(max(i, 0), len(sorted)-1)
	return sorted[i].Round(time.Millisecond)
}

// newRateLimiter yields one permit per create. Permits are refilled in batches
// so the timer period stays coarse even at high rates, where a tick per create
// would be sub-millisecond. It can undershoot the target rate but never exceed
// it, which is the safe direction for a cap.
func newRateLimiter(ctx context.Context, perSecond int) <-chan struct{} {
	if perSecond <= 0 {
		return nil
	}
	burst := max(perSecond/100, 1)
	period := time.Duration(int64(time.Second) * int64(burst) / int64(perSecond))
	ch := make(chan struct{}, burst*2)
	go func() {
		t := time.NewTicker(period)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for range burst {
					select {
					case ch <- struct{}{}:
					default: // nobody waiting; drop rather than bank capacity
					}
				}
			}
		}
	}()
	return ch
}

// sleepUntil waits for t, or until ctx is cancelled. time.Sleep would make
// Ctrl-C block for the whole -lifetime on every live session.
func sleepUntil(ctx context.Context, t time.Time) {
	d := time.Until(t)
	if d <= 0 {
		return
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
	}
}

// runCmd executes a shell command in the Sandbox and checks its exit code. It
// never reads stdout, so the SDK's lazy output streams are never opened.
func runCmd(ctx context.Context, sb *modal.Sandbox, cmd string) error {
	proc, err := sb.Exec(ctx, []string{"sh", "-c", cmd}, nil)
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	exitCode, err := proc.Wait(ctx, nil)
	if err != nil {
		return fmt.Errorf("wait for exec: %w", err)
	}
	if exitCode != 0 {
		return fmt.Errorf("command exited with code %d", exitCode)
	}
	return nil
}

// session runs one Sandbox through its whole life: create, workload, hold,
// terminate.
func session(ctx context.Context, mc *modal.Client, app *modal.App, image *modal.Image, cmd string, m *metrics) error {
	if bootLimiter != nil {
		select {
		case <-bootLimiter:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	start := time.Now()
	// No CPU or MemoryMiB: a default-sized Sandbox is what the capacity buffer
	// is sized in, so asking for anything else measures a different thing.
	sb, err := mc.Sandboxes.ExperimentalCreate(ctx, app, image, &modal.SandboxCreateParams{
		Command: []string{"sleep", "infinity"},
		Timeout: *lifetime + timeoutMargin,
	})
	if err != nil {
		return fmt.Errorf("create sandbox: %w", err)
	}
	createdAt := time.Now()
	createTook := createdAt.Sub(start)
	m.record("create", createTook)
	created.Add(1)
	recordMin(&firstCreateNs, start.UnixNano())
	recordMax(&lastCreateNs, createdAt.UnixNano())
	recordPeak(live.Add(1))
	defer func() {
		sb.Terminate(context.WithoutCancel(ctx), nil) //nolint:errcheck // best-effort cleanup
		live.Add(-1)
	}()
	if createTook > slowThreshold {
		log.Printf("slow create: sandbox %s took %s", sb.SandboxID, createTook.Round(time.Millisecond))
	}

	// An empty -cmd skips the exec entirely. The first exec on a Sandbox both
	// waits for the container to boot and opens a per-Sandbox connection, so
	// skipping it isolates raw create throughput from those costs.
	if cmd != "" {
		execStart := time.Now()
		if err := runCmd(ctx, sb, cmd); err != nil {
			return fmt.Errorf("workload on %s: %w", sb.SandboxID, err)
		}
		done := time.Now()
		m.record("exec", done.Sub(execStart))
		m.record("ready", done.Sub(start))
	}
	ready.Add(1)

	// Hold the Sandbox for the rest of its lifetime, so the run reaches a real
	// concurrency plateau instead of only measuring create throughput.
	sleepUntil(ctx, createdAt.Add(*lifetime))
	return nil
}

// ownBinaryTag is the SHA-256 prefix of the running binary, used as the runner
// Image tag so the driver and its runners always execute identical code.
func ownBinaryTag() (string, error) {
	exePath, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("locate own binary: %w", err)
	}
	f, err := os.Open(exePath)
	if err != nil {
		return "", fmt.Errorf("open own binary: %w", err)
	}
	defer f.Close() //nolint:errcheck // read-only
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash own binary: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil))[:12], nil
}

// shardEnv builds the env vars runner Sandboxes need to authenticate exactly
// like this process: explicit MODAL_* env vars win, otherwise the selected
// profile in ~/.modal.toml is forwarded.
func shardEnv() (map[string]string, error) {
	// MODAL_SANDBOX_V2 is deliberately not forwarded: runners execute this same
	// binary, which calls ExperimentalCreate and is already on v2.
	env := map[string]string{}
	for _, k := range []string{"MODAL_TOKEN_ID", "MODAL_TOKEN_SECRET", "MODAL_SERVER_URL", "MODAL_ENVIRONMENT"} {
		if v := os.Getenv(k); v != "" {
			env[k] = v
		}
	}
	if env["MODAL_TOKEN_ID"] != "" && env["MODAL_TOKEN_SECRET"] != "" {
		return env, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("find home dir: %w", err)
	}
	data, err := os.ReadFile(filepath.Join(home, ".modal.toml"))
	if err != nil {
		return nil, fmt.Errorf("read ~/.modal.toml: %w", err)
	}
	var cfg map[string]struct {
		ServerURL   string `toml:"server_url"`
		TokenID     string `toml:"token_id"`
		TokenSecret string `toml:"token_secret"`
		Environment string `toml:"environment"`
		Active      bool   `toml:"active"`
	}
	if err := toml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse ~/.modal.toml: %w", err)
	}
	name := os.Getenv("MODAL_PROFILE")
	if name == "" {
		for n, p := range cfg {
			if p.Active {
				name = n
				break
			}
		}
	}
	p, ok := cfg[name]
	if !ok || p.TokenID == "" || p.TokenSecret == "" {
		return nil, fmt.Errorf("no usable token credentials for profile %q in ~/.modal.toml", name)
	}
	env["MODAL_TOKEN_ID"], env["MODAL_TOKEN_SECRET"] = p.TokenID, p.TokenSecret
	if p.ServerURL != "" {
		env["MODAL_SERVER_URL"] = p.ServerURL
	}
	if env["MODAL_ENVIRONMENT"] == "" && p.Environment != "" {
		env["MODAL_ENVIRONMENT"] = p.Environment
	}
	return env, nil
}

// runShard boots one runner Sandbox from the published runner Image and runs
// the load test there with the shard's slice of the load, relaying its output
// line by line.
func runShard(ctx context.Context, mc *modal.Client, app *modal.App, runner *modal.Image, idx int, env map[string]string, shardTotal, shardConcurrency, shardRate int, startAtNs int64, agg *aggregateDurations) error {
	// Runners are load generators, not the thing under test, so they get real
	// resources rather than the default Sandbox shape.
	createStart := time.Now()
	sb, err := mc.Sandboxes.ExperimentalCreate(ctx, app, runner, &modal.SandboxCreateParams{
		Command:   []string{"sleep", "infinity"},
		CPU:       *runnerCPU,
		MemoryMiB: *runnerMemory,
		Timeout:   8 * time.Hour, // safety net; terminated explicitly below
	})
	if err != nil {
		return fmt.Errorf("create runner sandbox: %w", err)
	}
	createTook := time.Since(createStart)
	defer sb.Terminate(context.WithoutCancel(ctx), nil) //nolint:errcheck // best-effort cleanup

	// On interrupt, terminate the runner right away: that stops the remote load
	// test and ends the output streams below, which would otherwise keep
	// blocking (they don't notice context cancellation).
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			sb.Terminate(context.WithoutCancel(ctx), nil) //nolint:errcheck // best-effort
		case <-watchDone:
		}
	}()

	// The binary is already in the Image, so starting a shard is one exec.
	cmd := []string{runnerBinaryPath,
		"-total", strconv.Itoa(shardTotal),
		"-concurrency", strconv.Itoa(shardConcurrency),
		"-rate", strconv.Itoa(shardRate),
		"-lifetime", lifetime.String(),
		"-progress", progressEvery.String(),
		"-start-at", strconv.FormatInt(startAtNs, 10),
		"-image", *imageTag,
		"-app", *appName,
	}
	run, err := sb.Exec(ctx, cmd, &modal.SandboxExecParams{Env: env})
	if err != nil {
		return fmt.Errorf("start load test: %w", err)
	}
	log.Printf("[shard %d] running on %s (runner create %s) (-total %d -concurrency %d -rate %d)",
		idx, sb.SandboxID, createTook.Round(time.Millisecond), shardTotal, shardConcurrency, shardRate)

	var pipes sync.WaitGroup
	for _, stream := range []io.Reader{run.Stdout, run.Stderr} {
		pipes.Add(1)
		go func() {
			defer pipes.Done()
			scanner := bufio.NewScanner(stream)
			scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
			for scanner.Scan() {
				line := scanner.Text()
				if op, batch, ok := parseDursLine(line); ok {
					agg.add(op, batch)
					continue
				}
				if reason, n, ok := parseFailLine(line); ok {
					agg.fails.addCount(reason, n)
					continue
				}
				if n, ok := parsePeakLine(line); ok {
					agg.peakSum.Add(n)
					agg.shardsIn.Add(1)
					continue
				}
				if first, last, n, ok := parseWindowLine(line); ok {
					recordMin(&agg.winFirst, first)
					recordMax(&agg.winLast, last)
					agg.winCreate.Add(n)
					continue
				}
				if strings.HasPrefix(line, "#missed ") {
					agg.missed.Add(1)
					continue
				}
				fmt.Printf("[shard %d] %s\n", idx, line)
			}
		}()
	}
	pipes.Wait()
	code, err := run.Wait(ctx, nil)
	if err != nil {
		return fmt.Errorf("wait for load test: %w", err)
	}
	if code != 0 {
		return fmt.Errorf("load test exited with code %d", code)
	}
	return nil
}

// runShards splits the run evenly across *shards* runner Sandboxes. Sharding
// exists because a single machine bottlenecks on per-Sandbox connection setup:
// the first Exec on a Sandbox fetches a per-task command-router URL and dials
// it, so every Sandbox costs one DNS lookup plus one TLS handshake, and that
// connection stays open until the Sandbox is terminated. Runners each bring
// their own resolver, connection budget, and NIC.
func runShards(ctx context.Context, mc *modal.Client, app *modal.App, runner *modal.Image) {
	env, err := shardEnv()
	if err != nil {
		log.Fatalf("collect credentials for runners: %v", err)
	}
	// Marks the child as a shard so it emits machine-readable samples, and
	// carries the workload command without shell quoting problems.
	env["BURST_SHARD"] = "1"
	// Sent as-is, including empty, so -cmd "" reaches the runners too.
	env["BURST_CMD"] = *workloadCmd
	env["BURST_CMD_SET"] = "1"
	agg := &aggregateDurations{}

	perTotal := *total / *shards
	perConcurrency := max(*concurrency / *shards, 1)
	perRate := 0
	if *rate > 0 {
		perRate = max(*rate / *shards, 1)
	}
	// Every shard begins creating at the same instant. Without this, a runner
	// that boots late stretches the aggregate create window and collapses the
	// combined rate, which is not a property of Modal at all.
	startAtNs := int64(0)
	if *startDelay > 0 {
		startAtNs = time.Now().Add(*startDelay).UnixNano()
	}
	barrier := "none, shards start as they boot"
	if startAtNs > 0 {
		barrier = "in " + startDelay.String()
	}
	log.Printf("launching %d runner Sandboxes (aggregate: -total %d -concurrency %d -rate %d -lifetime %s), synchronized start %s",
		*shards, *total, *concurrency, *rate, lifetime.String(), barrier)

	start := time.Now()
	var wg sync.WaitGroup
	var failedShards atomic.Int64
	for i := range *shards {
		wg.Add(1)
		go func() {
			defer wg.Done()
			shardTotal := perTotal
			if i == 0 {
				shardTotal += *total % *shards
			}
			if err := runShard(ctx, mc, app, runner, i, env, shardTotal, perConcurrency, perRate, startAtNs, agg); err != nil {
				failedShards.Add(1)
				log.Printf("[shard %d] %v", i, err)
			}
		}()
	}
	wg.Wait()
	log.Printf("all %d shards finished in %s (%d failed)", *shards, time.Since(start).Round(time.Second), failedShards.Load())
	agg.report()
}

func main() {
	flag.Usage = func() {
		fmt.Fprint(flag.CommandLine.Output(), `burst load-tests Modal Sandbox creation at burst scale.

Each session creates a default-sized Sandbox, runs a short shell command in it,
holds it for -lifetime, and terminates it. Prints p50/p90/p99 for create, exec
and ready (create start to command finished), plus peak live Sandboxes.

Authentication: the active profile in ~/.modal.toml, or MODAL_TOKEN_ID and
MODAL_TOKEN_SECRET (MODAL_PROFILE / MODAL_ENVIRONMENT are honored). Sandboxes
always use the v2 backend, regardless of MODAL_SANDBOX_V2.

Examples:

  burst -total 500 -rate 100 -lifetime 5m
  burst -total 20000 -rate 2000 -lifetime 5m -shards 8

With -shards, the runner image must ship CA certificates or the runners cannot
reach Modal. alpine:3.21 does; debian:bookworm-slim does not.

Flags:

`)
		flag.PrintDefaults()
	}
	flag.Parse()

	// Answers "what do I publish?" without touching the network, and works
	// under `go run`, where the binary lives in a temporary build directory.
	if *printImageRef {
		tag, err := ownBinaryTag()
		if err != nil {
			log.Fatalf("%v", err)
		}
		exePath, _ := os.Executable()
		fmt.Printf("%s:%s\t%s\n", *runnerImage, tag, exePath)
		return
	}

	cmd := *workloadCmd
	if os.Getenv("BURST_CMD_SET") != "" {
		cmd = os.Getenv("BURST_CMD")
	}
	isShard := os.Getenv("BURST_SHARD") != ""

	if *concurrency == 0 {
		// A session occupies a worker for -lifetime plus create and exec, so by
		// Little's law holding -rate needs rate*(lifetime+overhead) in flight.
		// Never more than -total, since that bounds sessions outright.
		if *rate > 0 {
			need := float64(*rate) * (lifetime.Seconds() + 10)
			*concurrency = min(*total, max(int(need), 1))
		} else {
			*concurrency = min(*total, 1000)
		}
	}

	if *rate > 0 {
		log.Printf("plan: %d Sandboxes at up to %d/s (~%s of creates), lifetime %s, concurrency %d, app %q",
			*total, *rate, (time.Duration(*total / *rate) * time.Second).Round(time.Second),
			lifetime.String(), *concurrency, *appName)
	} else {
		log.Printf("plan: %d Sandboxes, UNCAPPED rate, lifetime %s, concurrency %d, app %q",
			*total, lifetime.String(), *concurrency, *appName)
		log.Printf("warning: -rate 0 pushes as hard as -concurrency allows; prefer setting a rate")
	}
	if !isShard && *shards == 0 && *rate > 100 {
		log.Printf("warning: rates above ~100/s from one machine can bottleneck on its DNS resolver and connection budget; consider -shards %d", (*rate+249)/250)
	}
	if *concurrency > 100_000 {
		log.Printf("warning: concurrency %d means that many live Sandboxes and open connections on one host; consider more -shards", *concurrency)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	// After the first interrupt starts a graceful shutdown, restore default
	// signal handling so a second Ctrl-C kills the process outright.
	go func() {
		<-ctx.Done()
		stop()
	}()

	// Each modal.Client multiplexes all control-plane RPCs over one HTTP/2
	// connection, which caps concurrent in-flight requests. Spread workers
	// across one client per ~100 workers so connections don't throttle creates.
	// This helps creates only: the first Exec on a Sandbox opens its own
	// connection regardless.
	// The driver in -shards mode only creates runners and relays their output;
	// the load runs inside them, so it needs one connection, not hundreds.
	poolSize := min((*concurrency+99)/100, 256)
	if *shards > 0 {
		poolSize = 1
	}
	clients := make([]*modal.Client, poolSize)
	for i := range clients {
		c, err := modal.NewClient()
		if err != nil {
			log.Fatalf("create Modal client: %v", err)
		}
		clients[i] = c
	}
	mc := clients[0]

	app, err := mc.Apps.FromName(ctx, *appName, &modal.AppFromNameParams{CreateIfMissing: true})
	if err != nil {
		log.Fatalf("get or create App: %v", err)
	}

	// Resolve the workload image once up front, so per-session timings exclude it.
	base, err := mc.Images.FromRegistry(*imageTag, nil).Build(ctx, app, nil)
	if err != nil {
		log.Fatalf("build workload image %q: %v", *imageTag, err)
	}

	if *shards > 0 {
		tag, err := ownBinaryTag()
		if err != nil {
			log.Fatalf("%v", err)
		}
		ref := fmt.Sprintf("%s:%s", *runnerImage, tag)
		runner, err := mc.Images.FromName(ctx, ref, nil)
		if err != nil {
			log.Fatalf("runner Image %q not found: %v\n"+
				"  publish it for this exact binary with:\n"+
				"    uv run python build_runner_image.py <path to this binary>", ref, err)
		}
		log.Printf("runners will boot Image %s", ref)
		runShards(ctx, mc, app, runner)
		return
	}

	// Hold until the shared start instant so every shard bursts together. The
	// limiter is created afterwards, or it would bank permits while waiting.
	if *startAt > 0 {
		target := time.Unix(0, *startAt)
		if d := time.Until(target); d > 0 {
			log.Printf("holding %s for the synchronized start", d.Round(time.Millisecond))
			sleepUntil(ctx, target)
		} else {
			missedBarrier.Store(true)
			log.Printf("WARNING: synchronized start was %s ago; starting now, this shard skews the aggregate",
				(-d).Round(time.Millisecond))
		}
	}
	bootLimiter = newRateLimiter(ctx, *rate)

	log.Printf("creating %d Sandboxes at concurrency %d", *total, *concurrency)

	m := &metrics{durations: make(map[string][]time.Duration)}
	fails := &failureCounts{}
	start := time.Now()

	// Rates are per tick, not since start: a cumulative average smears the
	// burst into the -lifetime hold and can never show a mid-run throttle.
	go func() {
		t := time.NewTicker(*progressEvery)
		defer t.Stop()
		lastCreated, lastReady, lastTick := int64(0), int64(0), start
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				c, r := created.Load(), ready.Load()
				window := now.Sub(lastTick).Seconds()
				var createRate, readyRate float64
				if window > 0 {
					createRate = float64(c-lastCreated) / window
					readyRate = float64(r-lastReady) / window
				}
				log.Printf("progress: created %d (%.0f/s) | ready %d (%.0f/s) | live %d (peak %d) | failed %d",
					c, createRate, r, readyRate, live.Load(), peakLive.Load(), failures.Load())
				lastCreated, lastReady, lastTick = c, r, now
			}
		}
	}()

	var next atomic.Int64
	var wg sync.WaitGroup
	for w := range *concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := clients[w%len(clients)]
			for ctx.Err() == nil {
				n := next.Add(1)
				if n > int64(*total) {
					return
				}
				if err := session(ctx, client, app, base, cmd, m); err != nil && ctx.Err() == nil {
					failures.Add(1)
					fails.add(err)
					log.Printf("session %d: %v", n, err)
				}
			}
		}()
	}
	wg.Wait()

	elapsed := time.Since(start)
	if isShard {
		fmt.Printf("#peak %d\n", peakLive.Load())
		fmt.Printf("#window %d %d %d\n", firstCreateNs.Load(), lastCreateNs.Load(), created.Load())
		if missedBarrier.Load() {
			fmt.Printf("#missed 1\n")
		}
		m.report(true)
		fails.report(true)
		return
	}
	createSpan, sustained := createWindow()
	fmt.Printf("\ncreated %d Sandboxes in %s of creates (%.0f/s sustained), peak %d live, %d ready, %d failed\n",
		created.Load(), createSpan.Round(time.Millisecond), sustained,
		peakLive.Load(), ready.Load(), failures.Load())
	fmt.Printf("wall %s, which includes the %s lifetime hold\n\n", elapsed.Round(time.Second), lifetime.String())
	m.report(false)
	fails.report(false)
}
