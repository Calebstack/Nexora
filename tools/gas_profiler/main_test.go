package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const sampleLog = `running 2 tests
GAS_PROFILE {"contract":"bridge_adapter","function":"release","case":"lock/2_of_3","instructions":9000000,"mem_bytes":2000000,"read_bytes":300,"write_bytes":80,"fee_stroops":1200}
test gas::gas_profile_release ... ok
GAS_PROFILE {"contract":"attestation","function":"get_attestation","case":"single","instructions":400000,"mem_bytes":1000000,"read_bytes":100,"write_bytes":0,"fee_stroops":90}
`

func TestParseSamplesSortsAndSkipsNoise(t *testing.T) {
	samples, err := parseSamples(strings.NewReader(sampleLog))
	if err != nil {
		t.Fatalf("parseSamples: %v", err)
	}
	if len(samples) != 2 {
		t.Fatalf("got %d samples, want 2", len(samples))
	}
	if samples[0].Contract != "attestation" || samples[1].Contract != "bridge_adapter" {
		t.Fatalf("samples not sorted by key: %+v", samples)
	}
	if samples[1].Instructions != 9_000_000 || samples[1].FeeStroops != 1200 {
		t.Fatalf("fields not parsed: %+v", samples[1])
	}
}

func TestParseSamplesRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"empty":     "running 0 tests\n",
		"malformed": "GAS_PROFILE {not json}\n",
		"duplicate": strings.Repeat("GAS_PROFILE {\"contract\":\"a\",\"function\":\"f\",\"case\":\"c\"}\n", 2),
	}
	for name, log := range cases {
		if _, err := parseSamples(strings.NewReader(log)); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestWarningsFlagsOverBudgetFunctions(t *testing.T) {
	r := Report{Limits: NetworkLimits, Samples: []Sample{
		{Contract: "c", Function: "cheap", Case: "x", Instructions: 75_000_000, MemBytes: 1},
		{Contract: "c", Function: "hot", Case: "x", Instructions: 75_000_001},
		{Contract: "c", Function: "heavy", Case: "x", MemBytes: 40_000_000},
	}}

	got := warnings(r, 75)

	if len(got) != 2 {
		t.Fatalf("got %d warnings, want 2: %v", len(got), got)
	}
	if !strings.Contains(got[0], "c/hot/x") || !strings.Contains(got[1], "c/heavy/x") {
		t.Fatalf("unexpected warnings: %v", got)
	}
}

func TestCompareFlagsGrowthBeyondThreshold(t *testing.T) {
	baseline := Report{Samples: []Sample{
		{Contract: "c", Function: "a", Case: "x", Instructions: 1000, MemBytes: 1000},
		{Contract: "c", Function: "b", Case: "x", Instructions: 1000, MemBytes: 1000},
		{Contract: "c", Function: "gone", Case: "x", Instructions: 1000},
	}}
	current := Report{Samples: []Sample{
		{Contract: "c", Function: "a", Case: "x", Instructions: 1150, MemBytes: 1000},
		{Contract: "c", Function: "b", Case: "x", Instructions: 1151, MemBytes: 1200},
		{Contract: "c", Function: "new", Case: "x", Instructions: 999_999},
	}}

	got := compare(baseline, current, 15)

	if len(got) != 2 {
		t.Fatalf("got %d regressions, want 2: %v", len(got), got)
	}
	if got[0].Key != "c/b/x" || got[0].Metric != "CPU instructions" {
		t.Errorf("unexpected first regression: %v", got[0])
	}
	if got[1].Key != "c/b/x" || got[1].Metric != "memory bytes" {
		t.Errorf("unexpected second regression: %v", got[1])
	}
}

func TestMarkdownGroupsByContract(t *testing.T) {
	samples, err := parseSamples(strings.NewReader(sampleLog))
	if err != nil {
		t.Fatalf("parseSamples: %v", err)
	}
	r := Report{Limits: NetworkLimits, Samples: samples}

	md := markdown(r, nil, warnings(r, 75), nil, 15)

	for _, want := range []string{"## attestation", "## bridge_adapter", "| `release` | lock/2_of_3 | 9000000 | 9.00% |", "No function exceeds the warning threshold."} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown missing %q:\n%s", want, md)
		}
	}
}

func TestRunWritesReportsAndFailsOnRegression(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "run.log")
	if err := os.WriteFile(logPath, []byte(sampleLog), 0o600); err != nil {
		t.Fatal(err)
	}
	jsonPath := filepath.Join(dir, "report.json")
	mdPath := filepath.Join(dir, "report.md")

	var stdout, stderr bytes.Buffer
	if code := run([]string{"-input", logPath, "-json", jsonPath, "-markdown", mdPath}, &stdout, &stderr); code != 0 {
		t.Fatalf("run exited %d: %s", code, stderr.String())
	}

	var written Report
	data, err := os.ReadFile(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &written); err != nil || len(written.Samples) != 2 {
		t.Fatalf("bad JSON report (%v): %s", err, data)
	}

	// A baseline where every function was 20% cheaper makes the run regress.
	for i := range written.Samples {
		written.Samples[i].Instructions = written.Samples[i].Instructions * 80 / 100
	}
	baselineData, _ := json.Marshal(written)
	baselinePath := filepath.Join(dir, "baseline.json")
	if err := os.WriteFile(baselinePath, baselineData, 0o600); err != nil {
		t.Fatal(err)
	}

	stderr.Reset()
	if code := run([]string{"-input", logPath, "-baseline", baselinePath, "-markdown", mdPath}, &stdout, &stderr); code != 1 {
		t.Fatalf("run exited %d, want 1 for a regression", code)
	}
	if !strings.Contains(stderr.String(), "regression:") {
		t.Errorf("stderr missing regression line: %s", stderr.String())
	}
}
