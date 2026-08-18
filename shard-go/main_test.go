package main

import "testing"

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
