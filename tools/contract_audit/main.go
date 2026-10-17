// Command contract_audit scans Soroban Rust contract sources for
// Checks-Effects-Interactions violations: a storage write that happens after
// an external contract call inside the same function. Such a write can be
// observed in its old state by a callee that re-enters the contract.
//
// It is a lexical scanner, not a compiler. It strips comments, string
// literals and #[cfg(test)] modules, splits the source into functions, and
// within each function orders three kinds of events:
//
//   - interactions: calls on a `...Client::new(..)` binding or inline client,
//     and invoke_contract / try_invoke_contract
//   - effects: storage set/remove, directly or through a `let s = ...storage()`
//     binding
//   - helper calls: one level of same-file helper functions that contain
//     either of the above
//
// A function that uses nonreentrant! or ReentrancyGuard (contracts/soroban/
// common/src/guard.rs) is treated as protected and skipped.
//
// Usage:
//
//	contract_audit [path ...]
//
// Paths may be .rs files or directories; the default is contracts/soroban. It
// exits 0 when nothing is found, 1 when violations are found and 2 on error,
// printing one "file:line: message" line per finding.
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const defaultRoot = "contracts/soroban"

// Finding is one Checks-Effects-Interactions violation.
type Finding struct {
	File    string
	Line    int
	Message string
}

func (f Finding) String() string { return fmt.Sprintf("%s:%d: %s", f.File, f.Line, f.Message) }

type eventKind int

const (
	kindInteraction eventKind = iota
	kindEffect
)

type event struct {
	pos  int
	kind eventKind
	desc string
}

type function struct {
	name       string
	start, end int // body offsets, braces included
}

var (
	fnRe          = regexp.MustCompile(`\bfn\s+([A-Za-z_]\w*)`)
	cfgTestModRe  = regexp.MustCompile(`#\[cfg\(test\)\]\s*mod\s+\w+\s*\{`)
	clientDeclRe  = regexp.MustCompile(`\blet\s+(?:mut\s+)?([A-Za-z_]\w*)\s*(?::[^=;]+)?=\s*[\w:]*Client::new\s*\(`)
	storageDeclRe = regexp.MustCompile(`\blet\s+(?:mut\s+)?([A-Za-z_]\w*)\s*(?::[^=;]+)?=[^;]*?\.storage\s*\(\s*\)\s*\.\s*\w+\s*\(\s*\)\s*;`)
	inlineCallRe  = regexp.MustCompile(`Client::new\s*\([^;{}]*\)\s*\.\s*(\w+)\s*\(`)
	invokeRe      = regexp.MustCompile(`\.\s*(?:try_)?invoke_contract\s*(?:::\s*<[^>]*>)?\s*\(`)
	storageSetRe  = regexp.MustCompile(`\.storage\s*\(\s*\)\s*\.\s*\w+\s*\(\s*\)\s*\.\s*(set|remove)\s*\(`)
	guardRe       = regexp.MustCompile(`\bnonreentrant!|\bReentrancyGuard\b`)
)

// readOnlyMethods are token-interface calls that cannot move funds or call
// back into the caller in a way that matters for ordering.
var readOnlyMethods = map[string]bool{
	"balance": true, "allowance": true, "decimals": true,
	"name": true, "symbol": true, "total_supply": true, "spendable_balance": true,
}

func main() {
	roots := os.Args[1:]
	if len(roots) == 0 {
		roots = []string{defaultRoot}
	}

	findings, err := ScanPaths(roots)
	if err != nil {
		fmt.Fprintln(os.Stderr, "contract_audit:", err)
		os.Exit(2)
	}
	for _, f := range findings {
		fmt.Println(f)
	}
	if len(findings) > 0 {
		fmt.Fprintf(os.Stderr, "contract_audit: %d Checks-Effects-Interactions violation(s)\n", len(findings))
		os.Exit(1)
	}
}

// ScanPaths scans every Rust source file under the given files or directories.
func ScanPaths(roots []string) ([]Finding, error) {
	var findings []Finding
	for _, root := range roots {
		err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				switch d.Name() {
				case "target", "test_snapshots", ".git":
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".rs") || d.Name() == "test.rs" || strings.HasSuffix(path, "_test.rs") {
				return nil
			}
			src, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			findings = append(findings, Scan(path, string(src))...)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", root, err)
		}
	}
	return findings, nil
}

// Scan reports the Checks-Effects-Interactions violations in one source file.
func Scan(file, src string) []Finding {
	clean := blankTestModules(stripRust(src))
	funcs := findFunctions(clean)

	// First pass: which helpers write storage or call out, so a call to one
	// counts as that event in its callers.
	writers := map[string]bool{}
	callers := map[string]bool{}
	for _, fn := range funcs {
		for _, e := range directEvents(clean, fn) {
			if e.kind == kindEffect {
				writers[fn.name] = true
			} else {
				callers[fn.name] = true
			}
		}
	}

	var findings []Finding
	for _, fn := range funcs {
		body := clean[fn.start:fn.end]
		if guardRe.MatchString(body) {
			continue
		}

		events := directEvents(clean, fn)
		events = append(events, helperEvents(clean, fn, writers, kindEffect)...)
		events = append(events, helperEvents(clean, fn, callers, kindInteraction)...)
		sort.Slice(events, func(i, j int) bool { return events[i].pos < events[j].pos })

		firstCall := -1
		for _, e := range events {
			switch {
			case e.kind == kindInteraction && firstCall < 0:
				firstCall = e.pos
			case e.kind == kindEffect && firstCall >= 0:
				findings = append(findings, Finding{
					File: file,
					Line: lineOf(clean, e.pos),
					Message: fmt.Sprintf("fn %s: %s after external call at line %d (Checks-Effects-Interactions violation)",
						fn.name, e.desc, lineOf(clean, firstCall)),
				})
			}
		}
	}
	return findings
}

// directEvents finds storage writes and external calls written inline in fn.
func directEvents(clean string, fn function) []event {
	body := clean[fn.start:fn.end]
	var events []event
	add := func(offset int, kind eventKind, desc string) {
		events = append(events, event{pos: fn.start + offset, kind: kind, desc: desc})
	}

	clients := map[string]bool{}
	for _, m := range clientDeclRe.FindAllStringSubmatch(body, -1) {
		clients[m[1]] = true
	}
	storages := map[string]bool{}
	for _, m := range storageDeclRe.FindAllStringSubmatch(body, -1) {
		storages[m[1]] = true
	}

	for _, m := range inlineCallRe.FindAllStringSubmatchIndex(body, -1) {
		if method := body[m[2]:m[3]]; !readOnlyMethods[method] {
			add(m[0], kindInteraction, "external call "+method)
		}
	}
	for _, m := range invokeRe.FindAllStringIndex(body, -1) {
		add(m[0], kindInteraction, "external call invoke_contract")
	}
	for name := range clients {
		re := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\s*\.\s*(\w+)\s*\(`)
		for _, m := range re.FindAllStringSubmatchIndex(body, -1) {
			if method := body[m[2]:m[3]]; !readOnlyMethods[method] {
				add(m[0], kindInteraction, "external call "+method)
			}
		}
	}

	for _, m := range storageSetRe.FindAllStringSubmatchIndex(body, -1) {
		add(m[0], kindEffect, "storage "+body[m[2]:m[3]])
	}
	for name := range storages {
		re := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\s*\.\s*(set|remove)\s*\(`)
		for _, m := range re.FindAllStringSubmatchIndex(body, -1) {
			add(m[0], kindEffect, "storage "+body[m[2]:m[3]])
		}
	}
	return events
}

// helperEvents turns calls to same-file helper functions into events of kind.
func helperEvents(clean string, fn function, helpers map[string]bool, kind eventKind) []event {
	body := clean[fn.start:fn.end]
	var events []event
	for name := range helpers {
		if name == fn.name {
			continue
		}
		re := regexp.MustCompile(`(?:\bSelf::|\b)` + regexp.QuoteMeta(name) + `\s*\(`)
		for _, m := range re.FindAllStringIndex(body, -1) {
			desc := "call to " + name + " (writes storage)"
			if kind == kindInteraction {
				desc = "call to " + name + " (makes an external call)"
			}
			events = append(events, event{pos: fn.start + m[0], kind: kind, desc: desc})
		}
	}
	return events
}

// findFunctions locates every function that has a body.
func findFunctions(clean string) []function {
	var funcs []function
	for _, m := range fnRe.FindAllStringSubmatchIndex(clean, -1) {
		name := clean[m[2]:m[3]]
		open := bodyStart(clean, m[1])
		if open < 0 {
			continue
		}
		end := matchBrace(clean, open)
		if end < 0 {
			continue
		}
		funcs = append(funcs, function{name: name, start: open, end: end + 1})
	}
	return funcs
}

// bodyStart returns the offset of the `{` that opens the function body, or -1
// for a declaration that ends in `;`.
func bodyStart(clean string, from int) int {
	depth := 0
	for i := from; i < len(clean); i++ {
		switch clean[i] {
		case '(', '[':
			depth++
		case ')', ']':
			depth--
		case ';':
			if depth <= 0 {
				return -1
			}
		case '{':
			if depth <= 0 {
				return i
			}
		}
	}
	return -1
}

// matchBrace returns the offset of the `}` matching the `{` at open.
func matchBrace(s string, open int) int {
	depth := 0
	for i := open; i < len(s); i++ {
		switch s[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// blankTestModules blanks out #[cfg(test)] modules, keeping line numbers.
func blankTestModules(clean string) string {
	out := []byte(clean)
	for _, m := range cfgTestModRe.FindAllStringIndex(clean, -1) {
		open := m[1] - 1
		end := matchBrace(clean, open)
		if end < 0 {
			continue
		}
		for i := m[0]; i <= end; i++ {
			if out[i] != '\n' {
				out[i] = ' '
			}
		}
	}
	return string(out)
}

// stripRust replaces comments and the contents of string and char literals
// with spaces, preserving offsets and newlines.
func stripRust(src string) string {
	out := []byte(src)
	blank := func(from, to int) {
		for i := from; i < to && i < len(out); i++ {
			if out[i] != '\n' {
				out[i] = ' '
			}
		}
	}

	for i := 0; i < len(src); {
		switch {
		case strings.HasPrefix(src[i:], "//"):
			end := strings.IndexByte(src[i:], '\n')
			if end < 0 {
				end = len(src) - i
			}
			blank(i, i+end)
			i += end
		case strings.HasPrefix(src[i:], "/*"):
			depth, j := 0, i
			for j < len(src) {
				if strings.HasPrefix(src[j:], "/*") {
					depth++
					j += 2
				} else if strings.HasPrefix(src[j:], "*/") {
					depth--
					j += 2
					if depth == 0 {
						break
					}
				} else {
					j++
				}
			}
			blank(i, j)
			i = j
		case src[i] == '"':
			j := i + 1
			for j < len(src) && src[j] != '"' {
				if src[j] == '\\' {
					j++
				}
				j++
			}
			blank(i+1, j)
			i = j + 1
		case src[i] == '\'':
			// A char literal closes within a few bytes; otherwise this is a
			// lifetime and is left alone.
			if i+1 < len(src) && src[i+1] == '\\' {
				if end := strings.IndexByte(src[i+2:], '\''); end >= 0 {
					blank(i+1, i+2+end)
					i += end + 3
					continue
				}
			} else if i+2 < len(src) && src[i+2] == '\'' {
				blank(i+1, i+2)
				i += 3
				continue
			}
			i++
		default:
			i++
		}
	}
	return string(out)
}

func lineOf(s string, pos int) int {
	return strings.Count(s[:pos], "\n") + 1
}
