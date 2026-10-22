package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/shopspring/decimal"
)

// ChannelAccountRepo persists Stellar channel accounts used for concurrent
// transaction sequencing (issue #41). It follows the same pgx-based repository
// pattern as ConversionRepo.
type ChannelAccountRepo struct {
	db DB
}

// NewChannelAccountRepo builds a repository backed by db.
func NewChannelAccountRepo(db DB) *ChannelAccountRepo {
	return &ChannelAccountRepo{db: db}
}

// Create inserts a new channel account. The caller supplies at least
// PublicKey, EncryptedSecret and Status; the database defaults fill the rest.
func (r *ChannelAccountRepo) Create(ctx context.Context, ch *domain.ChannelAccount) error {
	_, err := r.db.Exec(ctx,
		`INSERT INTO channel_accounts (id, public_key, encrypted_secret, status, native_balance, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		ch.ID, ch.PublicKey, ch.EncryptedSecret, string(ch.Status),
		ch.NativeBalance.String(), ch.CreatedAt, ch.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert channel account: %w", err)
	}
	return nil
}

// GetByPublicKey retrieves a channel account by its Stellar public key.
func (r *ChannelAccountRepo) GetByPublicKey(ctx context.Context, pubKey string) (*domain.ChannelAccount, error) {
	row := r.db.QueryRow(ctx,
		`SELECT id, public_key, encrypted_secret, status, native_balance,
		        last_used_at, last_topped_up_at, balance_check_at, created_at, updated_at
		 FROM channel_accounts
		 WHERE public_key = $1`, pubKey,
	)
	return scanChannelAccount(row)
}

// GetByID retrieves a channel account by primary key.
func (r *ChannelAccountRepo) GetByID(ctx context.Context, id string) (*domain.ChannelAccount, error) {
	row := r.db.QueryRow(ctx,
		`SELECT id, public_key, encrypted_secret, status, native_balance,
		        last_used_at, last_topped_up_at, balance_check_at, created_at, updated_at
		 FROM channel_accounts
		 WHERE id = $1`, id,
	)
	return scanChannelAccount(row)
}

// ListActive returns all accounts in the "active" state, ordered by
// least-recently-used first so the pool spreads load evenly.
func (r *ChannelAccountRepo) ListActive(ctx context.Context) ([]*domain.ChannelAccount, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, public_key, encrypted_secret, status, native_balance,
		        last_used_at, last_topped_up_at, balance_check_at, created_at, updated_at
		 FROM channel_accounts
		 WHERE status = 'active'
		 ORDER BY last_used_at NULLS FIRST, created_at ASC`,
	)
	if err != nil {
		return nil, fmt.Errorf("list active channel accounts: %w", err)
	}
	defer rows.Close()
	return collectChannelAccounts(rows)
}

// ListByStatus returns accounts in the given lifecycle state.
func (r *ChannelAccountRepo) ListByStatus(ctx context.Context, status domain.ChannelAccountStatus) ([]*domain.ChannelAccount, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, public_key, encrypted_secret, status, native_balance,
		        last_used_at, last_topped_up_at, balance_check_at, created_at, updated_at
		 FROM channel_accounts
		 WHERE status = $1
		 ORDER BY created_at ASC`, string(status),
	)
	if err != nil {
		return nil, fmt.Errorf("list channel accounts by status %s: %w", status, err)
	}
	defer rows.Close()
	return collectChannelAccounts(rows)
}

// ListNeedingBalanceCheck returns up to limit accounts whose balance has not
// been checked recently, ordered soonest-stale first.
func (r *ChannelAccountRepo) ListNeedingBalanceCheck(ctx context.Context, olderThan time.Time, limit int) ([]*domain.ChannelAccount, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, public_key, encrypted_secret, status, native_balance,
		        last_used_at, last_topped_up_at, balance_check_at, created_at, updated_at
		 FROM channel_accounts
		 WHERE status = 'active'
		   AND (balance_check_at IS NULL OR balance_check_at < $1)
		 ORDER BY balance_check_at NULLS FIRST
		 LIMIT $2`, olderThan, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list accounts needing balance check: %w", err)
	}
	defer rows.Close()
	return collectChannelAccounts(rows)
}

// UpdateBalance records a new XLM balance observed on chain.
func (r *ChannelAccountRepo) UpdateBalance(ctx context.Context, id string, balance decimal.Decimal) error {
	now := time.Now().UTC()
	tag, err := r.db.Exec(ctx,
		`UPDATE channel_accounts
		 SET native_balance = $1, balance_check_at = $2, updated_at = $2
		 WHERE id = $3`, balance.String(), now, id,
	)
	if err != nil {
		return fmt.Errorf("update channel balance: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("channel account %s not found", id)
	}
	return nil
}

// UpdateStatus transitions a channel account's lifecycle state.
func (r *ChannelAccountRepo) UpdateStatus(ctx context.Context, id string, status domain.ChannelAccountStatus) error {
	now := time.Now().UTC()
	tag, err := r.db.Exec(ctx,
		`UPDATE channel_accounts
		 SET status = $1, updated_at = $2
		 WHERE id = $3`, string(status), now, id,
	)
	if err != nil {
		return fmt.Errorf("update channel status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("channel account %s not found", id)
	}
	return nil
}

// MarkUsed timestamps a lease event so the pool's round-robin spread stays
// fair.
func (r *ChannelAccountRepo) MarkUsed(ctx context.Context, id string) error {
	now := time.Now().UTC()
	_, err := r.db.Exec(ctx,
		`UPDATE channel_accounts SET last_used_at = $1, updated_at = $1 WHERE id = $2`,
		now, id,
	)
	if err != nil {
		return fmt.Errorf("mark channel used: %w", err)
	}
	return nil
}

// MarkToppedUp records a funding top-up event.
func (r *ChannelAccountRepo) MarkToppedUp(ctx context.Context, id string, newBalance decimal.Decimal) error {
	now := time.Now().UTC()
	_, err := r.db.Exec(ctx,
		`UPDATE channel_accounts
		 SET native_balance = $1, last_topped_up_at = $2, balance_check_at = $2, updated_at = $2
		 WHERE id = $3`, newBalance.String(), now, id,
	)
	if err != nil {
		return fmt.Errorf("mark channel topped up: %w", err)
	}
	return nil
}

// Delete permanently removes a channel account (e.g. after draining and
// merging it on chain).
func (r *ChannelAccountRepo) Delete(ctx context.Context, id string) error {
	tag, err := r.db.Exec(ctx, `DELETE FROM channel_accounts WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete channel account: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("channel account %s not found", id)
	}
	return nil
}

// Count returns the number of channel accounts by status.
func (r *ChannelAccountRepo) Count(ctx context.Context, status domain.ChannelAccountStatus) (int, error) {
	var n int
	err := r.db.QueryRow(ctx,
		`SELECT COUNT(*) FROM channel_accounts WHERE status = $1`, string(status),
	).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count channel accounts: %w", err)
	}
	return n, nil
}

// scanChannelAccount scans a single row into a ChannelAccount.
func scanChannelAccount(row interface{ Scan(dest ...interface{}) error }) (*domain.ChannelAccount, error) {
	ch := &domain.ChannelAccount{}
	var bal string
	if err := row.Scan(
		&ch.ID, &ch.PublicKey, &ch.EncryptedSecret, &ch.Status,
		&bal, &ch.LastUsedAt, &ch.LastToppedUpAt, &ch.BalanceCheckAt,
		&ch.CreatedAt, &ch.UpdatedAt,
	); err != nil {
		return nil, err
	}
	ch.NativeBalance, _ = decimal.NewFromString(bal)
	return ch, nil
}

// collectChannelAccounts scans all rows in a result set.
func collectChannelAccounts(rows interface {
	Next() bool
	Scan(dest ...interface{}) error
	Err() error
}) ([]*domain.ChannelAccount, error) {
	var out []*domain.ChannelAccount
	for rows.Next() {
		ch := &domain.ChannelAccount{}
		var bal string
		if err := rows.Scan(
			&ch.ID, &ch.PublicKey, &ch.EncryptedSecret, &ch.Status,
			&bal, &ch.LastUsedAt, &ch.LastToppedUpAt, &ch.BalanceCheckAt,
			&ch.CreatedAt, &ch.UpdatedAt,
		); err != nil {
			return nil, err
		}
		ch.NativeBalance, _ = decimal.NewFromString(bal)
		out = append(out, ch)
	}
	return out, rows.Err()
}
