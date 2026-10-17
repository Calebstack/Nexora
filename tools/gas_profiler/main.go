// Command gas_profiler benchmarks the Soroban contracts in contracts/soroban
// against the Stellar network's per-transaction resource limits.
//
// Each contract crate has a gas module whose tests (named gas_profile_*) invoke
// every contract function across varying input sizes and storage states and
// print one GAS_PROFILE line per invocation with the CPU instructions, memory,
// ledger read/write bytes and fee the Soroban host metered for it. This tool
// runs those tests (or reads a saved log), then:
//
//   - writes a Markdown comparison table and a JSON report;
//   - warns when a function uses more than 75% of a transaction's CPU or memory
//     budget;
//   - compares against a baseline JSON report and exits 1 when any function
//     regressed by more than 15%.
//
// It depends only on the standard library so it keeps working while other
// packages in the module do not compile.
//
// Usage:
//
//	gas_profiler -markdown gas-report.md -json gas-report.json
//	gas_profiler -baseline contracts/soroban/gas-baseline.json -max-regression 15
//	gas_profiler -input cargo-test.log -json gas-report.json
//
// Costs come from the native test environment: they are deterministic and
// comparable between runs, but they omit VM instantiation, so they are a
// regression signal rather than a quote for mainnet fees.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sort"
	"strings"
)

const logPrefix = "GAS_PROFILE "

// Limits are the per-transaction resource ceilings of the Stellar network.
type Limits struct {
	Instructions uint64 `json:"instructions"`
	MemBytes     uint64 `json:"mem_bytes"`
	ReadBytes    uint64 `json:"read_bytes"`
	WriteBytes   uint64 `json:"write_bytes"`
}

// NetworkLimits are the current pubnet transaction limits.
var NetworkLimits = Limits{
	Instructions: 100_000_000,
	MemBytes:     41_943_040,
	ReadBytes:    200_000,
	WriteBytes:   132_096,
}

// Sample is the metered cost of one contract function invocation.
type Sample struct {
	Contract     string `json:"contract"`
	Function     string `json:"function"`
	Case         string `json:"case"`
	Instructions uint64 `json:"instructions"`
	MemBytes     uint64 `json:"mem_bytes"`
	ReadBytes    uint64 `json:"read_bytes"`
	WriteBytes   uint64 `json:"write_bytes"`
	FeeStroops   int64  `json:"fee_stroops"`
}

func (s Sample) key() string { return s.Contract + "/" + s.Function + "/" + s.Case }

// Report is the exportable result of one profiling run.
type Report struct {
	Limits  Limits   `json:"limits"`
	Samples []Sample `json:"samples"`
}

// Regression is a metric that grew past the allowed percentage.
type Regression struct {
	Key      string
	Metric   string
	Baseline uint64
	Current  uint64
	Percent  float64
}

func (r Regression) String() string {
	return fmt.Sprintf("%s: %s grew %.1f%% (%d -> %d)", r.Key, r.Metric, r.Percent, r.Baseline, r.Current)
}

// parseSamples extracts the GAS_PROFILE lines from cargo test output.
func parseSamples(r io.Reader) ([]Sample, error) {
	var samples []Sample
	seen := map[string]bool{}

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		idx := strings.Index(line, logPrefix)
		if idx < 0 {
			continue
		}
		var s Sample
		if err := json.Unmarshal([]byte(line[idx+len(logPrefix):]), &s); err != nil {
			return nil, fmt.Errorf("malformed gas profile line %q: %w", line, err)
		}
		if seen[s.key()] {
			return nil, fmt.Errorf("duplicate gas profile for %s", s.key())
		}
		seen[s.key()] = true
		samples = append(samples, s)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read gas profile output: %w", err)
	}
	if len(samples) == 0 {
		return nil, fmt.Errorf("no %q lines found; did the gas_profile_ tests run?", strings.TrimSpace(logPrefix))
	}

	sort.Slice(samples, func(i, j int) bool { return samples[i].key() < samples[j].key() })
	return samples, nil
}

func percent(value, limit uint64) float64 {
	if limit == 0 {
		return 0
	}
	return float64(value) * 100 / float64(limit)
}

// warnings lists every sample that uses more than threshold percent of the
// transaction CPU or memory budget.
func warnings(r Report, threshold float64) []string {
	var out []string
	for _, s := range r.Samples {
		if p := percent(s.Instructions, r.Limits.Instructions); p > threshold {
			out = append(out, fmt.Sprintf("%s uses %.1f%% of the transaction CPU budget", s.key(), p))
		}
		if p := percent(s.MemBytes, r.Limits.MemBytes); p > threshold {
			out = append(out, fmt.Sprintf("%s uses %.1f%% of the transaction memory budget", s.key(), p))
		}
	}
	return out
}

func growth(baseline, current uint64) float64 {
	if baseline == 0 {
		return 0
	}
	return (float64(current) - float64(baseline)) * 100 / float64(baseline)
}

// compare reports every CPU or memory metric that grew by more than
// maxRegression percent relative to the baseline. Functions that are new or
// were removed are not regressions.
func compare(baseline, current Report, maxRegression float64) []Regression {
	base := map[string]Sample{}
	for _, s := range baseline.Samples {
		base[s.key()] = s
	}

	var out []Regression
	for _, cur := range current.Samples {
		old, ok := base[cur.key()]
		if !ok {
			continue
		}
		if g := growth(old.Instructions, cur.Instructions); g > maxRegression {
			out = append(out, Regression{cur.key(), "CPU instructions", old.Instructions, cur.Instructions, g})
		}
		if g := growth(old.MemBytes, cur.MemBytes); g > maxRegression {
			out = append(out, Regression{cur.key(), "memory bytes", old.MemBytes, cur.MemBytes, g})
		}
	}
	return out
}

// markdown renders one comparison table per contract, followed by the budget
// warnings and, when a baseline is given, the CPU and memory change per case.
func markdown(r Report, baseline *Report, warns []string, regressions []Regression, maxRegression float64) string {
	var b strings.Builder
	b.WriteString("# Soroban gas report\n\n")
	fmt.Fprintf(&b, "Limits per transaction: %d CPU instructions, %d memory bytes, %d ledger read bytes, %d ledger write bytes.\n",
		r.Limits.Instructions, r.Limits.MemBytes, r.Limits.ReadBytes, r.Limits.WriteBytes)

	contract := ""
	for _, s := range r.Samples {
		if s.Contract != contract {
			contract = s.Contract
			fmt.Fprintf(&b, "\n## %s\n\n", contract)
			b.WriteString("| Function | Case | CPU instructions | CPU % | Memory bytes | Memory % | Read bytes | Write bytes | Fee (stroops) |\n")
			b.WriteString("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
		}
		fmt.Fprintf(&b, "| `%s` | %s | %d | %.2f%% | %d | %.2f%% | %d | %d | %d |\n",
			s.Function, s.Case, s.Instructions, percent(s.Instructions, r.Limits.Instructions),
			s.MemBytes, percent(s.MemBytes, r.Limits.MemBytes), s.ReadBytes, s.WriteBytes, s.FeeStroops)
	}

	b.WriteString("\n## Budget warnings\n\n")
	if len(warns) == 0 {
		b.WriteString("No function exceeds the warning threshold.\n")
	}
	for _, w := range warns {
		fmt.Fprintf(&b, "- %s\n", w)
	}

	if baseline != nil {
		fmt.Fprintf(&b, "\n## Regressions (more than %.0f%% over baseline)\n\n", maxRegression)
		if len(regressions) == 0 {
			b.WriteString("No regressions against the baseline.\n")
		}
		for _, reg := range regressions {
			fmt.Fprintf(&b, "- %s\n", reg)
		}
	}

	return b.String()
}

func readReport(path string) (Report, error) {
	data, err := os.ReadFile(path) // #nosec G304 -- path is an operator-supplied CLI flag
	if err != nil {
		return Report{}, fmt.Errorf("read report %s: %w", path, err)
	}
	var r Report
	if err := json.Unmarshal(data, &r); err != nil {
		return Report{}, fmt.Errorf("parse report %s: %w", path, err)
	}
	return r, nil
}

// collect returns the cargo test output holding the GAS_PROFILE lines, either
// from a saved log or by running the gas_profile_ tests.
func collect(ctx context.Context, input, manifest string, stderr io.Writer) (io.Reader, error) {
	switch input {
	case "":
		cmd := exec.CommandContext(ctx, "cargo", "test", "--manifest-path", manifest, "--workspace", "gas_profile_", "--", "--nocapture", "--test-threads=1") // #nosec G204 -- manifest is an operator-supplied CLI flag
		var out bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = stderr
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("run gas profile tests: %w", err)
		}
		return &out, nil
	case "-":
		return os.Stdin, nil
	default:
		f, err := os.Open(input) // #nosec G304 -- path is an operator-supplied CLI flag
		if err != nil {
			return nil, fmt.Errorf("open %s: %w", input, err)
		}
		defer f.Close()
		var buf bytes.Buffer
		if _, err := io.Copy(&buf, f); err != nil {
			return nil, fmt.Errorf("read %s: %w", input, err)
		}
		return &buf, nil
	}
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("gas_profiler", flag.ContinueOnError)
	fs.SetOutput(stderr)
	input := fs.String("input", "", "saved cargo test output to parse ('-' for stdin); runs the gas_profile_ tests when empty")
	manifest := fs.String("manifest", "contracts/soroban/Cargo.toml", "workspace manifest used to run the gas_profile_ tests")
	markdownOut := fs.String("markdown", "", "write the Markdown comparison tables to this file (default: stdout)")
	jsonOut := fs.String("json", "", "write the JSON report to this file")
	baselinePath := fs.String("baseline", "", "JSON report to compare against; regressions fail the run")
	maxRegression := fs.Float64("max-regression", 15, "largest allowed CPU or memory growth over the baseline, in percent")
	warnThreshold := fs.Float64("warn-threshold", 75, "warn when a function uses more than this percent of the CPU or memory budget")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	reader, err := collect(context.Background(), *input, *manifest, stderr)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	samples, err := parseSamples(reader)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	report := Report{Limits: NetworkLimits, Samples: samples}

	var baseline *Report
	var regressions []Regression
	if *baselinePath != "" {
		b, err := readReport(*baselinePath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		baseline = &b
		regressions = compare(b, report, *maxRegression)
	}

	warns := warnings(report, *warnThreshold)
	md := markdown(report, baseline, warns, regressions, *maxRegression)
	if *markdownOut != "" {
		if err := os.WriteFile(*markdownOut, []byte(md), 0o600); err != nil {
			fmt.Fprintf(stderr, "write %s: %v\n", *markdownOut, err)
			return 1
		}
	} else {
		fmt.Fprint(stdout, md)
	}

	if *jsonOut != "" {
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			fmt.Fprintf(stderr, "encode report: %v\n", err)
			return 1
		}
		if err := os.WriteFile(*jsonOut, append(data, '\n'), 0o600); err != nil {
			fmt.Fprintf(stderr, "write %s: %v\n", *jsonOut, err)
			return 1
		}
	}

	for _, w := range warns {
		fmt.Fprintf(stderr, "warning: %s\n", w)
	}
	for _, reg := range regressions {
		fmt.Fprintf(stderr, "regression: %s\n", reg)
	}
	if len(regressions) > 0 {
		return 1
	}
	return 0
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}
