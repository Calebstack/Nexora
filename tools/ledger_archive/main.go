package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/indexer"
)

func main() {
	archiveURL := flag.String("archive-url", "https://history.stellar.org/prd/core-live/core_live_001", "Stellar history archive URL (HTTP, GCS, S3, or local path)")
	startLedger := flag.Uint("start-ledger", 2, "Start ledger sequence number")
	endLedger := flag.Uint("end-ledger", 64, "End ledger sequence number")
	outputFile := flag.String("output", "transactions.csv", "Output analytical CSV file path")
	workers := flag.Int("workers", runtime.NumCPU()*2, "Number of concurrent parallel extraction workers")
	verifyHashes := flag.Bool("verify-hashes", true, "Verify cryptographic hash links back to genesis")

	flag.Parse()

	if *startLedger == 0 {
		*startLedger = 1
	}
	if *endLedger < *startLedger {
		fmt.Fprintf(os.Stderr, "Error: end-ledger (%d) cannot be less than start-ledger (%d)\n", *endLedger, *startLedger)
		os.Exit(1)
	}

	fmt.Println("Nexora Stellar Ledger Archive Historical Audit & Extraction CLI")
	fmt.Printf("Archive URL:    %s\n", *archiveURL)
	fmt.Printf("Ledger Range:   %d -> %d (%d ledgers)\n", *startLedger, *endLedger, *endLedger-*startLedger+1)
	fmt.Printf("Workers:        %d\n", *workers)
	fmt.Printf("Verify Hashes:  %t\n", *verifyHashes)
	fmt.Printf("Output File:    %s\n\n", *outputFile)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	outFile, err := os.Create(*outputFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output file: %v\n", err)
		os.Exit(1)
	}
	defer outFile.Close()

	csvWriter := csv.NewWriter(outFile)
	defer csvWriter.Flush()

	httpClient := &http.Client{
		Timeout: 45 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 50,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	extractor := indexer.NewLedgerArchiveExtractor(*archiveURL, *workers, httpClient)

	startTime := time.Now()
	summary, err := extractor.ParallelExtract(ctx, uint32(*startLedger), uint32(*endLedger), *verifyHashes, csvWriter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Extraction failed: %v\n", err)
		os.Exit(1)
	}

	elapsed := time.Since(startTime)
	fmt.Println("Extraction Complete")
	fmt.Printf("Processed Ledgers:      %d\n", summary.TotalLedgers)
	fmt.Printf("Extracted Transactions: %d\n", summary.TotalTransactions)
	fmt.Printf("Elapsed Time:           %s\n", elapsed)
	if summary.LedgersPerMinute > 0 {
		fmt.Printf("Throughput:             %.1f ledgers/minute\n", summary.LedgersPerMinute)
	}
}
