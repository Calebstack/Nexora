package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

type fakeLedger struct {
	entries map[string]xdr.LedgerEntryData
	err     error
}

func (f *fakeLedger) GetLedgerEntries(_ context.Context, request protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error) {
	if f.err != nil {
		return protocol.GetLedgerEntriesResponse{}, f.err
	}
	var resp protocol.GetLedgerEntriesResponse
	for _, key := range request.Keys {
		data, ok := f.entries[key]
		if !ok {
			continue
		}
		encoded, err := xdr.MarshalBase64(data)
		if err != nil {
			return resp, err
		}
		resp.Entries = append(resp.Entries, protocol.LedgerEntryResult{KeyXDR: key, DataXDR: encoded})
	}
	return resp, nil
}

var (
	testWasm       = []byte("\x00asm\x01\x00\x00\x00deterministic")
	testContractID = mustContractID([32]byte{9})
)

func mustContractID(raw [32]byte) string {
	id, err := strkey.Encode(strkey.VersionByteContract, raw[:])
	if err != nil {
		panic(err)
	}
	return id
}

// deployed registers a contract instance and its code in a fake ledger, the way
// a deployed contract appears on the network.
func deployed(t *testing.T, contractID string, code []byte) *fakeLedger {
	t.Helper()
	hash := xdr.Hash(sha256.Sum256(code))
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		t.Fatalf("decode contract id: %v", err)
	}
	var contractRef xdr.ContractId
	copy(contractRef[:], raw)

	instanceKey, err := contractInstanceKey(contractID)
	if err != nil {
		t.Fatalf("contractInstanceKey: %v", err)
	}
	codeKey, err := contractCodeKey(hash)
	if err != nil {
		t.Fatalf("contractCodeKey: %v", err)
	}

	return &fakeLedger{entries: map[string]xdr.LedgerEntryData{
		instanceKey: {
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.ContractDataEntry{
				Contract: xdr.ScAddress{
					Type:       xdr.ScAddressTypeScAddressTypeContract,
					ContractId: &contractRef,
				},
				Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
				Durability: xdr.ContractDataDurabilityPersistent,
				Val: xdr.ScVal{
					Type: xdr.ScValTypeScvContractInstance,
					Instance: &xdr.ScContractInstance{
						Executable: xdr.ContractExecutable{
							Type:     xdr.ContractExecutableTypeContractExecutableWasm,
							WasmHash: &hash,
						},
					},
				},
			},
		},
		codeKey: {
			Type:         xdr.LedgerEntryTypeContractCode,
			ContractCode: &xdr.ContractCodeEntry{Hash: hash, Code: code},
		},
	}}
}

func writeWasm(t *testing.T, content []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "contract.wasm")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func runTool(t *testing.T, ledger ledgerReader, args ...string) (int, string, string) {
	t.Helper()
	var stdout, stderr bytes.Buffer
	code := run(context.Background(), args, &stdout, &stderr, func(string) ledgerReader { return ledger })
	return code, stdout.String(), stderr.String()
}

func TestVerifyMatchesDeployedWasm(t *testing.T) {
	ledger := deployed(t, testContractID, testWasm)

	code, stdout, stderr := runTool(t, ledger, "-rpc", "http://rpc", "-contract", testContractID, "-wasm", writeWasm(t, testWasm))

	if code != 0 || !strings.Contains(stdout, "MATCH") {
		t.Fatalf("exit %d, want a match\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
}

func TestVerifyDetectsDifferentWasm(t *testing.T) {
	ledger := deployed(t, testContractID, testWasm)
	other := append(append([]byte(nil), testWasm...), '!')

	code, _, stderr := runTool(t, ledger, "-rpc", "http://rpc", "-contract", testContractID, "-wasm", writeWasm(t, other))

	if code != 1 || !strings.Contains(stderr, "MISMATCH") {
		t.Fatalf("exit %d, want a mismatch\nstderr: %s", code, stderr)
	}
}

func TestVerifyByWasmHashAndExpectedDigest(t *testing.T) {
	ledger := deployed(t, testContractID, testWasm)
	sum := sha256.Sum256(testWasm)
	digest := hex.EncodeToString(sum[:])

	code, stdout, stderr := runTool(t, ledger, "-rpc", "http://rpc", "-wasm-hash", digest, "-expected-sha256", digest)

	if code != 0 || !strings.Contains(stdout, "MATCH") {
		t.Fatalf("exit %d\nstdout: %s\nstderr: %s", code, stdout, stderr)
	}
}

func TestVerifyRejectsCodeThatDoesNotHashToItsKey(t *testing.T) {
	ledger := deployed(t, testContractID, testWasm)
	hash := xdr.Hash(sha256.Sum256(testWasm))
	codeKey, _ := contractCodeKey(hash)
	ledger.entries[codeKey] = xdr.LedgerEntryData{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.ContractCodeEntry{Hash: hash, Code: []byte("tampered")},
	}

	code, _, stderr := runTool(t, ledger, "-rpc", "http://rpc", "-contract", testContractID, "-wasm", writeWasm(t, testWasm))

	if code != 1 || !strings.Contains(stderr, "expected") {
		t.Fatalf("exit %d, want the tampered code to be rejected\nstderr: %s", code, stderr)
	}
}

func TestVerifyReportsMissingContractAndRPCFailure(t *testing.T) {
	wasm := writeWasm(t, testWasm)

	code, _, stderr := runTool(t, &fakeLedger{}, "-rpc", "http://rpc", "-contract", testContractID, "-wasm", wasm)
	if code != 1 || !strings.Contains(stderr, "not found") {
		t.Errorf("missing contract: exit %d, stderr %s", code, stderr)
	}

	code, _, stderr = runTool(t, &fakeLedger{err: errors.New("boom")}, "-rpc", "http://rpc", "-contract", testContractID, "-wasm", wasm)
	if code != 1 || !strings.Contains(stderr, "boom") {
		t.Errorf("rpc failure: exit %d, stderr %s", code, stderr)
	}
}

func TestHashOnlyModePrintsLocalDigest(t *testing.T) {
	code, stdout, _ := runTool(t, &fakeLedger{}, "-wasm", writeWasm(t, testWasm))

	sum := sha256.Sum256(testWasm)
	if code != 0 || !strings.Contains(stdout, hex.EncodeToString(sum[:])) {
		t.Fatalf("exit %d, stdout %q", code, stdout)
	}
}

func TestUsageErrors(t *testing.T) {
	wasm := writeWasm(t, testWasm)
	cases := map[string][]string{
		"nothing to compare": {},
		"both references":    {"-wasm", wasm, "-expected-sha256", strings.Repeat("0", 64)},
		"bad digest":         {"-expected-sha256", "zz"},
		"no rpc":             {"-wasm", wasm, "-contract", testContractID},
		"both targets":       {"-rpc", "u", "-wasm", wasm, "-contract", testContractID, "-wasm-hash", strings.Repeat("0", 64)},
	}
	for name, args := range cases {
		if code, _, _ := runTool(t, &fakeLedger{}, args...); code != 2 {
			t.Errorf("%s: exit %d, want 2", name, code)
		}
	}
}
