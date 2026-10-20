package fx

import "github.com/shopspring/decimal"

// BalanceHold is an in-memory pre-authorization hold against a wallet's
// spendable balance while a rate lock is outstanding (issue #78).
// Persistence, release-on-expiry and the debit-on-execution wiring are not
// covered here - this is just the atomic hold-amount check.
type BalanceHold struct {
	WalletID string
	AssetCode string
	Amount   decimal.Decimal
}

// CanHold reports whether placing a new hold of amount would exceed the
// wallet's spendable balance given its already-held total.
func CanHold(spendable, alreadyHeld, amount decimal.Decimal) bool {
	return alreadyHeld.Add(amount).LessThanOrEqual(spendable)
}
