package main

import (
	"net"
	"os"
	"os/exec"
	"testing"
	"time"
)

func TestParseWorkload(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		status  string
		buildMs int
		ok      bool
	}{
		{
			name:    "success",
			output:  "build output\n{\"status\":\"success\",\"build_ms\":123}\n",
			status:  "success",
			buildMs: 123,
			ok:      true,
		},
		{
			name:    "failure",
			output:  "{\"status\":\"build_failed\",\"build_ms\":45,\"error\":\"make testfixture\"}\n",
			status:  "build_failed",
			buildMs: 45,
			ok:      true,
		},
		{name: "missing", output: "build output\n", ok: false},
		{name: "invalid", output: "{\"build_ms\":123}\n", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, ok := parseWorkload([]byte(tt.output))
			if ok != tt.ok {
				t.Fatalf("parseWorkload ok = %v, want %v", ok, tt.ok)
			}
			if !ok {
				return
			}
			if result.Status != tt.status {
				t.Errorf("status = %q, want %q", result.Status, tt.status)
			}
			if result.BuildMs == nil || *result.BuildMs != tt.buildMs {
				t.Errorf("build_ms = %v, want %d", result.BuildMs, tt.buildMs)
			}
		})
	}
}

func TestSignallerPacketAndEnv(t *testing.T) {
	s := &signaller{host: "h", port: 7777, token: "devtok"}
	got := s.packet(0x010203, 1)
	want := []byte{'d', 'e', 'v', 't', 'o', 'k', 0x03, 0x02, 0x01, 0x00, 1}
	if string(got) != string(want) {
		t.Fatalf("packet = %v, want %v", got, want)
	}
	env := s.env(42)
	if env["SIGNAL_ID"] != "42" || env["SIGNAL_HOST"] != "h" || env["SIGNAL_PORT"] != "7777" || env["SIGNAL_TOKEN"] != "devtok" {
		t.Fatalf("env = %v", env)
	}
	if (&signaller{}).env(1) != nil {
		t.Fatal("disabled signaller should not inject env")
	}
}

// The embedded workload.sh must emit exactly the datagram the Go side does.
func TestWorkloadSignalDatagram(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not available")
	}
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	_, port, _ := net.SplitHostPort(pc.LocalAddr().String())

	cmd := exec.Command("bash", "-c", workloadScript)
	cmd.Env = append(os.Environ(),
		"SIGNAL_HOST=127.0.0.1", "SIGNAL_PORT="+port, "SIGNAL_TOKEN=devtok", "SIGNAL_ID=66051")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("bash: %v\n%s", err, out)
	}
	if _, ok := parseWorkload(out); !ok {
		t.Fatalf("workload output not parseable: %s", out)
	}

	s := &signaller{token: "devtok"}
	pc.SetReadDeadline(time.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	n, _, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatalf("waiting for ON datagram: %v", err)
	}
	if string(buf[:n]) != string(s.packet(66051, 1)) {
		t.Fatalf("datagram = %v, want %v", buf[:n], s.packet(66051, 1))
	}
	// Only ON comes from the workload; OFF is the shard's job after Terminate.
	pc.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	if n, _, err := pc.ReadFrom(buf); err == nil {
		t.Fatalf("unexpected extra datagram %v", buf[:n])
	}
}
