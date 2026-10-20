package indexer

import "time"

const LedgerBatchSize = 50

type LedgerSource interface { Operations(fromLedger, toLedger uint32) ([]Operation, error) }
type BalanceStore interface { ApplyBalanceDeltas(tenantID string, deltas map[string]int64) error }
type LagObserver interface { ObserveProcessingLag(time.Duration) }

type Pipeline struct { Source LedgerSource; Store DedupStore; Balances BalanceStore; Lag LagObserver }

// Ingest processes at most 50 ledgers per batch. The repository is responsible
// for wrapping the insert and balance update in one database transaction.
func (p Pipeline) Ingest(fromLedger, toLedger uint32) (int, error) {
	if fromLedger > toLedger { return 0, nil }
	processed := 0
	for start := fromLedger; start <= toLedger; {
		end := start + LedgerBatchSize - 1
		if end > toLedger { end = toLedger }
		operations, err := p.Source.Operations(start, end)
		if err != nil { return processed, err }
		deltas := map[string]int64{}
		for _, operation := range operations {
			inserted, err := p.Store.InsertOperation(operation, OperationKey(operation))
			if err != nil { return processed, err }
			if inserted { processed++; deltas[operation.TenantID] += operation.BalanceDelta }
		}
		if p.Balances != nil && len(deltas) > 0 { if err := p.Balances.ApplyBalanceDeltas("batch", deltas); err != nil { return processed, err } }
		if p.Lag != nil { p.Lag.ObserveProcessingLag(0) }
		if end == ^uint32(0) { break }
		start = end + 1
	}
	return processed, nil
}
