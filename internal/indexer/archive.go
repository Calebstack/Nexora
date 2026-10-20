package indexer

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/stellar/go/xdr"
)

// CheckpointFrequency is the number of ledgers per history archive checkpoint in Stellar.
const CheckpointFrequency uint32 = 64

// ParsedTransaction contains extracted analytical attributes from historical ledger archives.
type ParsedTransaction struct {
	LedgerSeq        uint32    `json:"ledger_seq"`
	TransactionIndex uint32    `json:"tx_index"`
	TransactionHash  string    `json:"tx_hash"`
	SourceAccount    string    `json:"source_account"`
	FeeCharged       int64     `json:"fee_charged"`
	OperationCount   int       `json:"op_count"`
	MemoType         string    `json:"memo_type"`
	MemoValue        string    `json:"memo_value"`
	ClosedAt         time.Time `json:"closed_at"`
	Successful       bool      `json:"successful"`
	ResultCode       string    `json:"result_code"`
}

// ExtractedLedger holds raw decoded historical entries for one ledger.
type ExtractedLedger struct {
	Header            xdr.LedgerHeaderHistoryEntry
	Transaction       *xdr.TransactionHistoryEntry
	TransactionResult *xdr.TransactionHistoryResultEntry
}

// CheckpointPath generates the official Stellar history archive relative path for a category.
// Category paths follow: category/xx/yy/zz/category-xxxxxxxx.xdr.gz
func CheckpointPath(category string, checkpoint uint32) string {
	d1 := (checkpoint >> 24) & 0xff
	d2 := (checkpoint >> 16) & 0xff
	d3 := (checkpoint >> 8) & 0xff
	return fmt.Sprintf("%s/%02x/%02x/%02x/%s-%08x.xdr.gz", category, d1, d2, d3, category, checkpoint)
}

// CheckpointForLedger computes the checkpoint sequence number that contains the given ledger.
func CheckpointForLedger(ledgerSeq uint32) uint32 {
	if ledgerSeq == 0 {
		return CheckpointFrequency - 1
	}
	remainder := ledgerSeq % CheckpointFrequency
	if remainder == 0 {
		return ledgerSeq + CheckpointFrequency - 1
	}
	return ((ledgerSeq / CheckpointFrequency) + 1)*CheckpointFrequency - 1
}

// ArchiveStorageReader downloads and opens archive files from HTTP(S), file, or cloud URLs.
type ArchiveStorageReader struct {
	baseURL    string
	httpClient *http.Client
}

// NewArchiveStorageReader creates a reader for public Stellar storage buckets or local mirrors.
func NewArchiveStorageReader(baseURL string, client *http.Client) *ArchiveStorageReader {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	return &ArchiveStorageReader{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: client,
	}
}

// ReadFile streams decompressed data for the requested relative path.
func (r *ArchiveStorageReader) ReadFile(ctx context.Context, relativePath string) (io.ReadCloser, error) {
	if strings.HasPrefix(r.baseURL, "file://") || !strings.Contains(r.baseURL, "://") {
		basePath := strings.TrimPrefix(r.baseURL, "file://")
		fullPath := filepath.Join(basePath, relativePath)
		file, err := os.Open(fullPath)
		if err != nil {
			return nil, fmt.Errorf("open local archive file %s: %w", fullPath, err)
		}
		gz, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, fmt.Errorf("decompress gzip %s: %w", fullPath, err)
		}
		return &gzipFileCloser{gz: gz, file: file}, nil
	}

	u, err := url.Parse(r.baseURL + "/" + relativePath)
	if err != nil {
		return nil, fmt.Errorf("invalid archive url: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create archive request: %w", err)
	}
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err := r.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch archive %s: %w", u.String(), err)
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("archive fetch %s returned HTTP %d", u.String(), resp.StatusCode)
	}

	// Decompress transparently
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		resp.Body.Close()
		return nil, fmt.Errorf("decompress gzip %s: %w", u.String(), err)
	}

	return &gzipFileCloser{gz: gz, file: resp.Body}, nil
}

type gzipFileCloser struct {
	gz   *gzip.Reader
	file io.Closer
}

func (c *gzipFileCloser) Read(p []byte) (int, error) {
	return c.gz.Read(p)
}

func (c *gzipFileCloser) Close() error {
	_ = c.gz.Close()
	return c.file.Close()
}

// LedgerArchiveExtractor orchestrates offline parsing, cryptographic verification, and export.
type LedgerArchiveExtractor struct {
	storage    *ArchiveStorageReader
	numWorkers int
}

// NewLedgerArchiveExtractor constructs an archive extractor with configurable worker pool.
func NewLedgerArchiveExtractor(archiveURL string, workers int, client *http.Client) *LedgerArchiveExtractor {
	if workers <= 0 {
		workers = 8
	}
	return &LedgerArchiveExtractor{
		storage:    NewArchiveStorageReader(archiveURL, client),
		numWorkers: workers,
	}
}

// ExtractLedgerHeaders reads and decodes LedgerHeaderHistoryEntry records from checkpoint.
func (e *LedgerArchiveExtractor) ExtractLedgerHeaders(ctx context.Context, checkpoint uint32) ([]xdr.LedgerHeaderHistoryEntry, error) {
	path := CheckpointPath("ledger", checkpoint)
	reader, err := e.storage.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	stream := xdr.NewStream(reader)
	var entries []xdr.LedgerHeaderHistoryEntry
	for {
		var entry xdr.LedgerHeaderHistoryEntry
		err := stream.ReadOne(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode ledger header entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// ExtractTransactions reads and decodes TransactionHistoryEntry records from checkpoint.
func (e *LedgerArchiveExtractor) ExtractTransactions(ctx context.Context, checkpoint uint32) ([]xdr.TransactionHistoryEntry, error) {
	path := CheckpointPath("transactions", checkpoint)
	reader, err := e.storage.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	stream := xdr.NewStream(reader)
	var entries []xdr.TransactionHistoryEntry
	for {
		var entry xdr.TransactionHistoryEntry
		err := stream.ReadOne(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode transaction entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// ExtractTransactionResults reads and decodes TransactionHistoryResultEntry records from checkpoint.
func (e *LedgerArchiveExtractor) ExtractTransactionResults(ctx context.Context, checkpoint uint32) ([]xdr.TransactionHistoryResultEntry, error) {
	path := CheckpointPath("results", checkpoint)
	reader, err := e.storage.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	stream := xdr.NewStream(reader)
	var entries []xdr.TransactionHistoryResultEntry
	for {
		var entry xdr.TransactionHistoryResultEntry
		err := stream.ReadOne(&entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode transaction result entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// VerifyLedgerHashChain verifies cryptographic hash links connecting sequential ledgers back to genesis.
// Ensures L_{i+1}.Header.PreviousLedgerHash == Hash(L_i.Header) and validates internal header hash.
func VerifyLedgerHashChain(headers []xdr.LedgerHeaderHistoryEntry) error {
	if len(headers) == 0 {
		return nil
	}

	for i := 0; i < len(headers); i++ {
		cur := headers[i]

		// 1. Verify that cur.Hash equals SHA-256 of marshaled LedgerHeader
		headerBytes, err := cur.Header.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal header seq %d: %w", cur.Header.LedgerSeq, err)
		}
		computedHash := sha256.Sum256(headerBytes)
		if !bytes.Equal(cur.Hash[:], computedHash[:]) {
			return fmt.Errorf("ledger %d header hash mismatch: got %x, computed %x",
				cur.Header.LedgerSeq, cur.Hash[:], computedHash[:])
		}

		// 2. Verify sequential linkage
		if i > 0 {
			prev := headers[i-1]
			if cur.Header.LedgerSeq != prev.Header.LedgerSeq+1 {
				return fmt.Errorf("sequence gap: ledger %d followed by %d", prev.Header.LedgerSeq, cur.Header.LedgerSeq)
			}
			if !bytes.Equal(cur.Header.PreviousLedgerHash[:], prev.Hash[:]) {
				return fmt.Errorf("cryptographic chain break between ledger %d and %d: expected previous hash %x, got %x",
					prev.Header.LedgerSeq, cur.Header.LedgerSeq, prev.Hash[:], cur.Header.PreviousLedgerHash[:])
			}
		} else if cur.Header.LedgerSeq == 1 {
			// Genesis ledger: previous hash must be all zeros
			var zeroHash xdr.Hash
			if !bytes.Equal(cur.Header.PreviousLedgerHash[:], zeroHash[:]) {
				return fmt.Errorf("genesis ledger 1 has non-zero previous ledger hash: %x", cur.Header.PreviousLedgerHash[:])
			}
		}
	}

	return nil
}

// ParseTransactionsFromEntries converts raw XDR transactions and results into flat analytical models.
func ParseTransactionsFromEntries(
	txEntries []xdr.TransactionHistoryEntry,
	resEntries []xdr.TransactionHistoryResultEntry,
	headers []xdr.LedgerHeaderHistoryEntry,
) []ParsedTransaction {
	headerTimes := make(map[uint32]time.Time, len(headers))
	for _, h := range headers {
		closedAt := time.Unix(int64(h.Header.ScpValue.CloseTime), 0).UTC()
		headerTimes[uint32(h.Header.LedgerSeq)] = closedAt
	}

	resultMap := make(map[string]xdr.TransactionResultPair)
	for _, resEntry := range resEntries {
		for _, pair := range resEntry.TxResultSet.Results {
			hashHex := hex.EncodeToString(pair.TransactionHash[:])
			resultMap[hashHex] = pair
		}
	}

	var parsed []ParsedTransaction
	for _, txEntry := range txEntries {
		seq := uint32(txEntry.LedgerSeq)
		closedAt := headerTimes[seq]

		var envelopes []xdr.TransactionEnvelope
		envelopes = append(envelopes, txEntry.TxSet.Txs...)
		if txEntry.Ext.V == 1 && txEntry.Ext.GeneralizedTxSet != nil && txEntry.Ext.GeneralizedTxSet.V1TxSet != nil {
			for _, phase := range txEntry.Ext.GeneralizedTxSet.V1TxSet.Phases {
				if phase.V0Components != nil {
					for _, comp := range *phase.V0Components {
						if comp.TxsMaybeDiscountedFee != nil {
							envelopes = append(envelopes, comp.TxsMaybeDiscountedFee.Txs...)
						}
					}
				}
			}
		}

		for idx, env := range envelopes {
			b, _ := env.MarshalBinary()
			hash := sha256.Sum256(b)
			txHashHex := hex.EncodeToString(hash[:])

			srcAcc := env.SourceAccount()
			sourceAccount := srcAcc.Address()
			opCount := int(env.OperationsCount())
			feeCharged := int64(env.Fee())
			memoType := env.Memo().Type.String()
			memoVal := ""

			successful := true
			resultCode := "txSUCCESS"
			if res, ok := resultMap[txHashHex]; ok {
				feeCharged = int64(res.Result.FeeCharged)
				resultCode = res.Result.Result.Code.String()
				successful = res.Result.Successful()
			}

			parsed = append(parsed, ParsedTransaction{
				LedgerSeq:        seq,
				TransactionIndex: uint32(idx),
				TransactionHash:  txHashHex,
				SourceAccount:    sourceAccount,
				FeeCharged:       feeCharged,
				OperationCount:   opCount,
				MemoType:         memoType,
				MemoValue:        memoVal,
				ClosedAt:         closedAt,
				Successful:       successful,
				ResultCode:       resultCode,
			})
		}
	}

	return parsed
}

// ExtractionSummary aggregates parallel throughput metrics.
type ExtractionSummary struct {
	TotalLedgers      int64
	TotalTransactions int64
	Duration          time.Duration
	LedgersPerMinute  float64
}

// ParallelExtract extracts a sequence of ledgers concurrently and streams transactions to CSV.
func (e *LedgerArchiveExtractor) ParallelExtract(
	ctx context.Context,
	startLedger uint32,
	endLedger uint32,
	verifyHashes bool,
	csvWriter *csv.Writer,
) (*ExtractionSummary, error) {
	if startLedger > endLedger {
		return nil, fmt.Errorf("start ledger %d cannot exceed end ledger %d", startLedger, endLedger)
	}

	startTime := time.Now()

	// Write CSV Header
	if err := csvWriter.Write([]string{
		"ledger_sequence", "transaction_index", "transaction_hash",
		"source_account", "fee_charged", "operation_count",
		"memo_type", "memo_value", "closed_at", "successful", "result_code",
	}); err != nil {
		return nil, fmt.Errorf("write csv header: %w", err)
	}

	// Compute checkpoints needed
	startChk := CheckpointForLedger(startLedger)
	endChk := CheckpointForLedger(endLedger)

	var checkpoints []uint32
	for chk := startChk; chk <= endChk; chk += CheckpointFrequency {
		checkpoints = append(checkpoints, chk)
	}

	chkChan := make(chan uint32, len(checkpoints))
	for _, chk := range checkpoints {
		chkChan <- chk
	}
	close(chkChan)

	type workerResult struct {
		checkpoint uint32
		headers    []xdr.LedgerHeaderHistoryEntry
		txs        []ParsedTransaction
		err        error
	}

	resultChan := make(chan workerResult, len(checkpoints))
	var wg sync.WaitGroup

	for i := 0; i < e.numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chk := range chkChan {
				headers, err := e.ExtractLedgerHeaders(ctx, chk)
				if err != nil {
					resultChan <- workerResult{checkpoint: chk, err: fmt.Errorf("extract headers for chk %d: %w", chk, err)}
					return
				}

				txEntries, err := e.ExtractTransactions(ctx, chk)
				if err != nil {
					// Some checkpoints may have no transactions
					txEntries = nil
				}

				resEntries, err := e.ExtractTransactionResults(ctx, chk)
				if err != nil {
					resEntries = nil
				}

				parsedTxs := ParseTransactionsFromEntries(txEntries, resEntries, headers)
				resultChan <- workerResult{
					checkpoint: chk,
					headers:    headers,
					txs:        parsedTxs,
				}
			}
		}()
	}

	wg.Wait()
	close(resultChan)

	// Collect and sort results by checkpoint sequence
	resultsByChk := make(map[uint32]workerResult)
	for res := range resultChan {
		if res.err != nil {
			return nil, res.err
		}
		resultsByChk[res.checkpoint] = res
	}

	var totalLedgers int64
	var totalTxs int64
	var allHeaders []xdr.LedgerHeaderHistoryEntry

	var csvMutex sync.Mutex

	for _, chk := range checkpoints {
		res, ok := resultsByChk[chk]
		if !ok {
			continue
		}

		// Filter ledgers to requested window [startLedger, endLedger]
		for _, h := range res.headers {
			seq := uint32(h.Header.LedgerSeq)
			if seq >= startLedger && seq <= endLedger {
				allHeaders = append(allHeaders, h)
				atomic.AddInt64(&totalLedgers, 1)
			}
		}

		// Write matching parsed transactions
		csvMutex.Lock()
		for _, tx := range res.txs {
			if tx.LedgerSeq >= startLedger && tx.LedgerSeq <= endLedger {
				_ = csvWriter.Write([]string{
					strconv.FormatUint(uint64(tx.LedgerSeq), 10),
					strconv.FormatUint(uint64(tx.TransactionIndex), 10),
					tx.TransactionHash,
					tx.SourceAccount,
					strconv.FormatInt(tx.FeeCharged, 10),
					strconv.Itoa(tx.OperationCount),
					tx.MemoType,
					tx.MemoValue,
					tx.ClosedAt.Format(time.RFC3339),
					strconv.FormatBool(tx.Successful),
					tx.ResultCode,
				})
				atomic.AddInt64(&totalTxs, 1)
			}
		}
		csvWriter.Flush()
		csvMutex.Unlock()
	}

	if verifyHashes && len(allHeaders) > 0 {
		if err := VerifyLedgerHashChain(allHeaders); err != nil {
			return nil, fmt.Errorf("hash chain verification failed: %w", err)
		}
	}

	duration := time.Since(startTime)
	minutes := duration.Minutes()
	var lpm float64
	if minutes > 0 {
		lpm = float64(totalLedgers) / minutes
	}

	return &ExtractionSummary{
		TotalLedgers:      totalLedgers,
		TotalTransactions: totalTxs,
		Duration:          duration,
		LedgersPerMinute:  lpm,
	}, nil
}
