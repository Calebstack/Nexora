package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// ChannelAccountStatus is the lifecycle state of a channel account.
type ChannelAccountStatus string

const (
	// ChannelAccountActive means the account is in rotation and may be leased
	// for transaction sequencing.
	ChannelAccountActive ChannelAccountStatus = "active"
	// ChannelAccountDraining means the account is being recycled and should not
	// be leased for new work.
	ChannelAccountDraining ChannelAccountStatus = "draining"
	// ChannelAccountDisabled means the account is taken out of rotation
	// (e.g. under-funded with no funder, or manually quarantined).
	ChannelAccountDisabled ChannelAccountStatus = "disabled"
)

// ChannelAccount is a funded Stellar account dedicated to supplying transaction
// sequence numbers, so Nexora can submit many transactions concurrently without
// serialising on the primary treasury account's single sequence. Each account
// contributes at most one transaction per ledger, so throughput scales with the
// size of the pool.
type ChannelAccount struct {
	ID              string               `json:"id"`
	PublicKey       string               `json:"public_key"`
	EncryptedSecret string               `json:"-"`
	Status          ChannelAccountStatus `json:"status"`
	// NativeBalance is the last observed XLM balance, refreshed by the pool's
	// balance monitor; it drives top-up decisions.
	NativeBalance  decimal.Decimal `json:"native_balance"`
	LastUsedAt     *time.Time      `json:"last_used_at,omitempty"`
	LastToppedUpAt *time.Time      `json:"last_topped_up_at,omitempty"`
	BalanceCheckAt *time.Time      `json:"balance_check_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}
