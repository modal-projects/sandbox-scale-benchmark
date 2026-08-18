// Parameters come from the environment (set by the Python orchestrator); the
// per-shard result is written to stdout as one JSON object.
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	modal "github.com/modal-labs/modal-client/go"
)

//go:embed workload.sh
var workloadScript string

const (
	sqliteSrcURL = "https://sqlite.org/2026/sqlite-src-3530200.zip"
	sqliteDir    = "sqlite-src-3530200"
)

// Image for the inner sandboxes: Debian + build toolchain + TCL, with the SQLite
// source fetched and ./configure'd (but NOT built — each sandbox compiles fresh).
var sqliteSetup = []string{
	"RUN apt-get update && apt-get install -y --no-install-recommends " +
		"build-essential tcl-dev zlib1g-dev curl unzip ca-certificates && rm -rf /var/lib/apt/lists/*",
	"RUN curl -fsSL -o /tmp/src.zip " + sqliteSrcURL +
		" && cd /tmp && unzip -q src.zip && mv " + sqliteDir + " /sqlite && cd /sqlite && ./configure >/dev/null",
	"RUN useradd -m tester && chown -R tester:tester /sqlite",
}

type result struct {
	Status    string  `json:"status"` // success | build_failed | create_failed | workload_error
	SandboxID *string `json:"sandbox_id,omitempty"`
	CreateMs  int     `json:"create_ms"`
	BuildMs   *int    `json:"build_ms,omitempty"`
	Error     *string `json:"error,omitempty"`
}

type shardOutput struct {
	ShardIndex         int      `json:"shard_index"`
	ShardSize          int      `json:"shard_size"`
	BaseIdx            int      `json:"base_idx"`
	SetupMs            int      `json:"setup_ms"`
	CreateStartEpochMs int64    `json:"create_start_epoch_ms"`
	AllCreatedEpochMs  int64    `json:"all_created_epoch_ms"`
	BuildStartEpochMs  int64    `json:"build_start_epoch_ms"`
	BuildDoneEpochMs   int64    `json:"build_done_epoch_ms"`
	SandboxesCreated   int      `json:"sandboxes_created"`
	Results            []result `json:"results"`
	Error              string   `json:"error,omitempty"`
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func short(err error) string {
	s := err.Error()
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}

func fail(msg string) {
	out, _ := json.Marshal(shardOutput{
		ShardIndex: envInt("SHARD_INDEX", 0),
		ShardSize:  envInt("SHARD_SIZE", 0),
		BaseIdx:    envInt("BASE_IDX", 0),
		Error:      msg,
		Results:    []result{},
	})
	fmt.Println(string(out))
	os.Exit(0) // orchestrator reads the error from JSON, not the exit code
}

type pooledClient struct {
	mc    *modal.Client
	app   *modal.App
	image *modal.Image
}

func main() {
	programStart := time.Now()
	ctx := context.Background()

	size := envInt("SHARD_SIZE", 100)
	group := envInt("GROUP_SIZE", 50)
	rampMs := envInt("RAMP_MS", 0)
	sbTimeout := time.Duration(envInt("SANDBOX_TIMEOUT_S", 3600)) * time.Second
	waitForShardCreates := envInt("WAIT_FOR_SHARD_CREATES", 0) != 0
	appName := env("SANDBOX_APP_NAME", "modal-burst-sandboxes")

	clients := envInt("CLIENTS", 0)
	if clients <= 0 {
		clients = (size + 99) / 100 // ~100 inner sandboxes per connection
	}
	if clients < 1 {
		clients = 1
	}
	if clients > size {
		clients = size
	}

	// Build the client/connection pool and the (cached) SQLite image — all
	// before the clock starts, so create/workload timings exclude one-time
	// auth + imageget.
	pool := make([]pooledClient, clients)
	setupErrs := make([]error, clients)
	var setupWg sync.WaitGroup
	for i := range pool {
		setupWg.Add(1)
		go func(i int) {
			defer setupWg.Done()
			mc, err := modal.NewClient()
			if err != nil {
				setupErrs[i] = fmt.Errorf("NewClient: %w", err)
				return
			}
			app, err := mc.Apps.FromName(ctx, appName, &modal.AppFromNameParams{CreateIfMissing: true})
			if err != nil {
				setupErrs[i] = fmt.Errorf("Apps.FromName: %w", err)
				return
			}
			image, err := mc.Images.FromRegistry("debian:bookworm-slim", nil).
				DockerfileCommands(sqliteSetup, nil).
				Build(ctx, app, nil)
			if err != nil {
				setupErrs[i] = fmt.Errorf("Image.Build: %w", err)
				return
			}
			pool[i] = pooledClient{mc: mc, app: app, image: image}
		}(i)
	}
	setupWg.Wait()
	for _, e := range setupErrs {
		if e != nil {
			fail(e.Error())
		}
	}
	setupMs := int(time.Since(programStart).Milliseconds())

	// Ramp: spread the creates evenly over rampMs by pausing between groups of
	// `group`. rampMs=0 fires every create at once.
	numGroups := 1
	if group > 0 {
		numGroups = (size + group - 1) / group
	}
	var groupDelay time.Duration
	if rampMs > 0 && numGroups > 1 {
		groupDelay = time.Duration(rampMs/(numGroups-1)) * time.Millisecond
	}

	sandboxes := make([]*modal.Sandbox, size)
	createMs := make([]int, size)
	results := make([]result, size)
	buildStarts := make([]time.Time, size)
	buildDones := make([]time.Time, size)
	var buildWg sync.WaitGroup
	startBuild := func(idx int, sb *modal.Sandbox) {
		buildWg.Add(1)
		go func() {
			defer buildWg.Done()
			buildStarts[idx] = time.Now()
			r := runWorkload(ctx, sb)
			buildDones[idx] = time.Now()
			r.CreateMs = createMs[idx]
			sid := sb.SandboxID
			r.SandboxID = &sid
			results[idx] = r
			sb.Terminate(ctx, nil)
		}()
	}

	createStart := time.Now()
	var createWg sync.WaitGroup
	for off := 0; off < size; off++ {
		createWg.Add(1)
		go func(idx int, p pooledClient) {
			defer createWg.Done()
			t := time.Now()
			sb, err := p.mc.Sandboxes.ExperimentalCreate(ctx, p.app, p.image, &modal.SandboxCreateParams{
				Command: []string{"sleep", "infinity"},
				Timeout: sbTimeout,
			})
			createMs[idx] = int(time.Since(t).Milliseconds())
			if err != nil {
				msg := short(fmt.Errorf("Sandboxes.ExperimentalCreate: %w", err))
				results[idx] = result{Status: "create_failed", CreateMs: createMs[idx], Error: &msg}
				return
			}
			if sb == nil {
				msg := "sandbox create returned no sandbox"
				results[idx] = result{Status: "create_failed", CreateMs: createMs[idx], Error: &msg}
				return
			}
			sandboxes[idx] = sb
			if !waitForShardCreates {
				startBuild(idx, sb)
			}
		}(off, pool[off%clients])
		if groupDelay > 0 && (off+1)%group == 0 && off+1 < size {
			time.Sleep(groupDelay)
		}
	}
	createWg.Wait()
	allCreated := time.Now()

	created := 0
	for idx, sb := range sandboxes {
		if sb != nil {
			created++
			if waitForShardCreates {
				startBuild(idx, sb)
			}
		}
	}

	buildWg.Wait()
	var buildStart time.Time
	var buildDone time.Time
	for idx := range buildStarts {
		if !buildStarts[idx].IsZero() && (buildStart.IsZero() || buildStarts[idx].Before(buildStart)) {
			buildStart = buildStarts[idx]
		}
		if !buildDones[idx].IsZero() && (buildDone.IsZero() || buildDones[idx].After(buildDone)) {
			buildDone = buildDones[idx]
		}
	}
	if buildStart.IsZero() {
		buildStart = allCreated
		buildDone = allCreated
	}

	out, _ := json.Marshal(shardOutput{
		ShardIndex:         envInt("SHARD_INDEX", 0),
		ShardSize:          size,
		BaseIdx:            envInt("BASE_IDX", 0),
		SetupMs:            setupMs,
		CreateStartEpochMs: createStart.UnixMilli(),
		AllCreatedEpochMs:  allCreated.UnixMilli(),
		BuildStartEpochMs:  buildStart.UnixMilli(),
		BuildDoneEpochMs:   buildDone.UnixMilli(),
		SandboxesCreated:   created,
		Results:            results,
	})
	fmt.Println(string(out))
}

func runWorkload(ctx context.Context, sb *modal.Sandbox) result {
	proc, err := sb.Exec(
		ctx,
		[]string{"bash", "-c", workloadScript},
		&modal.SandboxExecParams{Stdout: modal.Pipe, Stderr: modal.Ignore},
	)
	if err != nil {
		msg := short(fmt.Errorf("Sandbox.Exec: %w", err))
		return result{Status: "workload_error", Error: &msg}
	}
	out, err := io.ReadAll(proc.Stdout)
	if err != nil {
		msg := short(fmt.Errorf("io.ReadAll: %w", err))
		return result{Status: "workload_error", Error: &msg}
	}
	if _, err := proc.Wait(ctx, nil); err != nil {
		msg := short(fmt.Errorf("ContainerProcess.Wait: %w", err))
		return result{Status: "workload_error", Error: &msg}
	}
	if r, ok := parseWorkload(out); ok {
		return r
	}
	msg := "workload produced no JSON"
	return result{Status: "workload_error", Error: &msg}
}

func parseWorkload(out []byte) (result, bool) {
	lines := strings.Split(string(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		s := strings.TrimSpace(lines[i])
		if !strings.HasPrefix(s, "{") {
			continue
		}
		var r result
		if json.Unmarshal([]byte(s), &r) == nil && r.Status != "" {
			return r, true
		}
	}
	return result{}, false
}
