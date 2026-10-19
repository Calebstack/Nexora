package indexer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

type Operation struct {
	Hash      string
	Ledger    uint32
	ClosedAt  int64
	TenantID  string
	BalanceDelta int64
}

func OperationKey(op Operation) string {
	if op.Hash == "" { return "" }
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", op.Hash, op.Ledger)))
	return hex.EncodeToString(h[:])
}

// DedupStore is implemented by the SQL repository. Insert must use
// INSERT ... ON CONFLICT (idempotency_key) DO NOTHING.
type DedupStore interface {
	InsertOperation(operation Operation, idempotencyKey string) (inserted bool, err error)
}
