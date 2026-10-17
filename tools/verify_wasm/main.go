// Command verify_wasm proves that the WASM deployed on the Stellar network is
// the artifact built from a repository commit.
//
// It downloads the contract code over Soroban RPC, computes its SHA-256 and
// compares it with the SHA-256 of a locally built artifact (or an expected hash
// taken from a release's build provenance). Because the Docker build in
// contracts/soroban/Dockerfile is deterministic, a match means the on-chain
// bytes are bit-for-bit the ones the commit produces.
//
// Usage:
//
//	verify_wasm -wasm out/attestation.wasm
//	verify_wasm -rpc https://mainnet.sorobanrpc.com -contract C... -wasm out/attestation.wasm
//	verify_wasm -rpc https://mainnet.sorobanrpc.com -contract C... -expected-sha256 <hex>
//	verify_wasm -rpc https://mainnet.sorobanrpc.com -wasm-hash <hex> -wasm out/attestation.wasm
//
// With only -wasm it prints the artifact's SHA-256. With -contract or
// -wasm-hash it also fetches the on-chain code and exits 1 unless the hashes
// are equal.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	rpcclient "github.com/stellar/go/clients/rpcclient"
	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/xdr"
)

const rpcTimeout = 30 * time.Second

// ledgerReader is the part of the Soroban RPC client this tool needs.
type ledgerReader interface {
	GetLedgerEntries(ctx context.Context, request protocol.GetLedgerEntriesRequest) (protocol.GetLedgerEntriesResponse, error)
}

func contractInstanceKey(contractID string) (string, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return "", fmt.Errorf("decode contract id %s: %w", contractID, err)
	}
	var id xdr.ContractId
	copy(id[:], raw)

	key := xdr.LedgerKey{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.LedgerKeyContractData{
			Contract: xdr.ScAddress{
				Type:       xdr.ScAddressTypeScAddressTypeContract,
				ContractId: &id,
			},
			Key:        xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance},
			Durability: xdr.ContractDataDurabilityPersistent,
		},
	}
	return xdr.MarshalBase64(key)
}

func contractCodeKey(hash xdr.Hash) (string, error) {
	return xdr.MarshalBase64(xdr.LedgerKey{
		Type:         xdr.LedgerEntryTypeContractCode,
		ContractCode: &xdr.LedgerKeyContractCode{Hash: hash},
	})
}

func readEntry(ctx context.Context, reader ledgerReader, key string) (xdr.LedgerEntryData, error) {
	resp, err := reader.GetLedgerEntries(ctx, protocol.GetLedgerEntriesRequest{Keys: []string{key}})
	if err != nil {
		return xdr.LedgerEntryData{}, fmt.Errorf("get ledger entries: %w", err)
	}
	if len(resp.Entries) == 0 {
		return xdr.LedgerEntryData{}, fmt.Errorf("ledger entry not found (never deployed, or archived and needs restoring)")
	}

	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(resp.Entries[0].DataXDR, &data); err != nil {
		return xdr.LedgerEntryData{}, fmt.Errorf("decode ledger entry: %w", err)
	}
	return data, nil
}

// onChainWasmHash resolves a deployed contract to the hash of its WASM.
func onChainWasmHash(ctx context.Context, reader ledgerReader, contractID string) (xdr.Hash, error) {
	key, err := contractInstanceKey(contractID)
	if err != nil {
		return xdr.Hash{}, err
	}
	entry, err := readEntry(ctx, reader, key)
	if err != nil {
		return xdr.Hash{}, fmt.Errorf("contract %s: %w", contractID, err)
	}

	contractData, ok := entry.GetContractData()
	if !ok || contractData.Val.Instance == nil {
		return xdr.Hash{}, fmt.Errorf("contract %s: ledger entry is not a contract instance", contractID)
	}
	executable := contractData.Val.Instance.Executable
	if executable.Type != xdr.ContractExecutableTypeContractExecutableWasm || executable.WasmHash == nil {
		return xdr.Hash{}, fmt.Errorf("contract %s does not run WASM (it is a Stellar Asset Contract)", contractID)
	}
	return *executable.WasmHash, nil
}

// fetchWasm downloads the code stored under hash and checks that it really
// hashes to it, so a misbehaving RPC node cannot hand back different bytes.
func fetchWasm(ctx context.Context, reader ledgerReader, hash xdr.Hash) ([]byte, error) {
	key, err := contractCodeKey(hash)
	if err != nil {
		return nil, fmt.Errorf("encode contract code key: %w", err)
	}
	entry, err := readEntry(ctx, reader, key)
	if err != nil {
		return nil, fmt.Errorf("wasm %s: %w", hex.EncodeToString(hash[:]), err)
	}

	code, ok := entry.GetContractCode()
	if !ok {
		return nil, fmt.Errorf("wasm %s: ledger entry is not contract code", hex.EncodeToString(hash[:]))
	}
	if sum := sha256.Sum256(code.Code); sum != [32]byte(hash) {
		return nil, fmt.Errorf("rpc returned code hashing to %x, expected %x", sum[:], hash[:])
	}
	return code.Code, nil
}

func parseSHA256(s string) ([32]byte, error) {
	var out [32]byte
	raw, err := hex.DecodeString(strings.TrimSpace(s))
	if err != nil || len(raw) != len(out) {
		return out, fmt.Errorf("%q is not a 64 character hex SHA-256", s)
	}
	copy(out[:], raw)
	return out, nil
}

func localSHA256(path string) ([32]byte, error) {
	f, err := os.Open(path) // #nosec G304 -- path is an operator-supplied CLI flag
	if err != nil {
		return [32]byte{}, fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return [32]byte{}, fmt.Errorf("hash %s: %w", path, err)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer, newReader func(url string) ledgerReader) int {
	fs := flag.NewFlagSet("verify_wasm", flag.ContinueOnError)
	fs.SetOutput(stderr)
	rpcURL := fs.String("rpc", "", "Soroban RPC URL used to download the on-chain WASM")
	contractID := fs.String("contract", "", "deployed contract id (C...) whose WASM is verified")
	wasmHash := fs.String("wasm-hash", "", "hex hash of an uploaded WASM to verify instead of a contract id")
	wasmPath := fs.String("wasm", "", "locally built .wasm artifact to compare against")
	expected := fs.String("expected-sha256", "", "hex SHA-256 to compare against, e.g. from a release's provenance")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	var want [32]byte
	switch {
	case *wasmPath != "" && *expected != "":
		fmt.Fprintln(stderr, "use either -wasm or -expected-sha256, not both")
		return 2
	case *wasmPath != "":
		sum, err := localSHA256(*wasmPath)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		want = sum
		fmt.Fprintf(stdout, "local sha256:    %x\n", want)
	case *expected != "":
		sum, err := parseSHA256(*expected)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		want = sum
		fmt.Fprintf(stdout, "expected sha256: %x\n", want)
	default:
		fmt.Fprintln(stderr, "one of -wasm or -expected-sha256 is required")
		return 2
	}

	if *contractID == "" && *wasmHash == "" {
		return 0
	}
	if *contractID != "" && *wasmHash != "" {
		fmt.Fprintln(stderr, "use either -contract or -wasm-hash, not both")
		return 2
	}
	if *rpcURL == "" {
		fmt.Fprintln(stderr, "-rpc is required to download the on-chain WASM")
		return 2
	}

	reader := newReader(*rpcURL)
	var hash xdr.Hash
	if *contractID != "" {
		h, err := onChainWasmHash(ctx, reader, *contractID)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 1
		}
		hash = h
	} else {
		h, err := parseSHA256(*wasmHash)
		if err != nil {
			fmt.Fprintln(stderr, err)
			return 2
		}
		hash = xdr.Hash(h)
	}

	code, err := fetchWasm(ctx, reader, hash)
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	got := sha256.Sum256(code)
	fmt.Fprintf(stdout, "on-chain sha256: %x\n", got)

	if got != want {
		fmt.Fprintln(stderr, "MISMATCH: the deployed WASM is not the verified artifact")
		return 1
	}
	fmt.Fprintln(stdout, "MATCH: the deployed WASM is bit-for-bit the verified artifact")
	return 0
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*rpcTimeout)
	defer cancel()

	code := run(ctx, os.Args[1:], os.Stdout, os.Stderr, func(url string) ledgerReader {
		return rpcclient.NewClient(url, &http.Client{Timeout: rpcTimeout})
	})
	cancel()
	os.Exit(code)
}
