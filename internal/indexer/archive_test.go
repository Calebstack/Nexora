package indexer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/stellar/go/xdr"
)

func TestCheckpointPath(t *testing.T) {
	// For checkpoint 63 (0x3f): ledger/00/00/00/ledger-0000003f.xdr.gz
	path := CheckpointPath("ledger", 63)
	want := "ledger/00/00/00/ledger-0000003f.xdr.gz"
	if path != want {
		t.Fatalf("CheckpointPath(ledger, 63) = %s, want %s", path, want)
	}

	// For checkpoint 127 (0x7f): transactions/00/00/00/transactions-0000007f.xdr.gz
	path2 := CheckpointPath("transactions", 127)
	want2 := "transactions/00/00/00/transactions-0000007f.xdr.gz"
	if path2 != want2 {
		t.Fatalf("CheckpointPath(transactions, 127) = %s, want %s", path2, want2)
	}
}

func TestCheckpointForLedger(t *testing.T) {
	cases := []struct {
		ledger uint32
		want   uint32
	}{
		{1, 63},
		{63, 63},
		{64, 127},
		{127, 127},
		{128, 191},
	}

	for _, c := range cases {
		got := CheckpointForLedger(c.ledger)
		if got != c.want {
			t.Fatalf("CheckpointForLedger(%d) = %d, want %d", c.ledger, got, c.want)
		}
	}
}

func createTestHeader(seq uint32, prevHash xdr.Hash) xdr.LedgerHeaderHistoryEntry {
	header := xdr.LedgerHeader{
		LedgerSeq:          xdr.Uint32(seq),
		PreviousLedgerHash: prevHash,
		ScpValue: xdr.StellarValue{
			CloseTime: xdr.TimePoint(1700000000 + int64(seq)*5),
		},
	}
	hb, _ := header.MarshalBinary()
	hash := xdr.Hash(sha256.Sum256(hb))
	return xdr.LedgerHeaderHistoryEntry{
		Hash:   hash,
		Header: header,
	}
}

func TestVerifyLedgerHashChain_Valid(t *testing.T) {
	h1 := createTestHeader(1, xdr.Hash{})
	h2 := createTestHeader(2, h1.Hash)
	h3 := createTestHeader(3, h2.Hash)

	headers := []xdr.LedgerHeaderHistoryEntry{h1, h2, h3}
	if err := VerifyLedgerHashChain(headers); err != nil {
		t.Fatalf("expected valid hash chain, got error: %v", err)
	}
}

func TestVerifyLedgerHashChain_BrokenLink(t *testing.T) {
	h1 := createTestHeader(1, xdr.Hash{})
	// Tamper: h2 has incorrect previous hash
	badPrevHash := xdr.Hash{0xde, 0xad, 0xbe, 0xef}
	h2 := createTestHeader(2, badPrevHash)

	headers := []xdr.LedgerHeaderHistoryEntry{h1, h2}
	err := VerifyLedgerHashChain(headers)
	if err == nil {
		t.Fatal("expected error due to broken cryptographic link")
	}
}

func TestExtractAndVerifyWithLocalMockArchive(t *testing.T) {
	tempDir := t.TempDir()

	// Build 2 valid headers for checkpoint 63
	h1 := createTestHeader(1, xdr.Hash{})
	h2 := createTestHeader(2, h1.Hash)

	// Write ledger/00/00/00/ledger-0000003f.xdr.gz
	ledgerPath := filepath.Join(tempDir, CheckpointPath("ledger", 63))
	_ = os.MkdirAll(filepath.Dir(ledgerPath), 0755)

	// Write length-framed XDR stream into gzip file
	outFile, err := os.Create(ledgerPath)
	if err != nil {
		t.Fatalf("create ledger file: %v", err)
	}
	gzOut := gzip.NewWriter(outFile)

	for _, h := range []xdr.LedgerHeaderHistoryEntry{h1, h2} {
		b, err := h.MarshalBinary()
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		// Write 4-byte big-endian length followed by XDR bytes
		lenBuf := []byte{
			byte(len(b) >> 24),
			byte(len(b) >> 16),
			byte(len(b) >> 8),
			byte(len(b)),
		}
		_, _ = gzOut.Write(lenBuf)
		_, _ = gzOut.Write(b)
	}
	_ = gzOut.Close()
	_ = outFile.Close()

	extractor := NewLedgerArchiveExtractor(tempDir, 2, nil)
	headers, err := extractor.ExtractLedgerHeaders(context.Background(), 63)
	if err != nil {
		t.Fatalf("ExtractLedgerHeaders: %v", err)
	}
	if len(headers) != 2 {
		t.Fatalf("got %d headers, want 2", len(headers))
	}

	if err := VerifyLedgerHashChain(headers); err != nil {
		t.Fatalf("VerifyLedgerHashChain: %v", err)
	}

	// Test CSV parallel extraction
	var csvBuf bytes.Buffer
	csvWriter := csv.NewWriter(&csvBuf)
	summary, err := extractor.ParallelExtract(context.Background(), 1, 2, true, csvWriter)
	if err != nil {
		t.Fatalf("ParallelExtract: %v", err)
	}
	if summary.TotalLedgers != 2 {
		t.Fatalf("total ledgers = %d, want 2", summary.TotalLedgers)
	}
}
