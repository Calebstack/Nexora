package settlement

import "time"

type PendingCredit struct { ID string; CreatedAt time.Time; LedgerCloses int; Reverted bool }

// ConfirmationWindow determines whether an optimistic credit must be held or reverted.
func ConfirmationWindow(credit PendingCredit, currentLedger uint32, createdLedger uint32) bool {
	return currentLedger-createdLedger < 3 && !credit.Reverted
}
