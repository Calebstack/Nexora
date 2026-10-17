package domain

import (
	"encoding/json"
	"time"
)

// ContractEvent is a Soroban contract event with its XDR topics and data
// decoded into JSON.
type ContractEvent struct {
	ID         string          `json:"id"`
	ContractID string          `json:"contract_id"`
	EventType  string          `json:"event_type"`
	Topic      string          `json:"topic"`
	Topics     json.RawMessage `json:"topics"`
	Data       json.RawMessage `json:"data"`
	Ledger     uint32          `json:"ledger"`
	TxHash     string          `json:"tx_hash"`
	CreatedAt  time.Time       `json:"created_at"`
}

// IndexedLedger records that a ledger has been processed by the contract event
// indexer.
type IndexedLedger struct {
	Sequence uint32
	Hash     string
}
