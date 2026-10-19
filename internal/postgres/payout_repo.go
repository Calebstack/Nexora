package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// PayoutRepo persists the anchor payout queue and satisfies anchor.PayoutStore.
type PayoutRepo struct {
	db DB
}

func NewPayoutRepo(db DB) *PayoutRepo {
	return &PayoutRepo{db: db}
}

// payoutColumns excludes the beneficiary ciphertext, which only the
// submitting worker needs.
const payoutColumns = `p.id, p.batch_id, p.tenant_id, p.wallet_id, p.reference, p.partner, p.destination_currency, p.country,
	p.destination_amount, p.source_asset, p.source_issuer, p.source_amount, p.status, p.reservation_state,
	p.provider_ref, p.attempts, p.next_attempt_at, p.last_error, p.created_at, p.updated_at, p.completed_at`

func scanPayout(row pgx.Row, extra ...any) (*anchor.Payout, error) {
	p := &anchor.Payout{}
	dest := append([]any{&p.ID, &p.BatchID, &p.TenantID, &p.WalletID, &p.Reference, &p.Partner, &p.DestinationCurrency, &p.Country,
		&p.DestinationAmount, &p.SourceAsset, &p.SourceIssuer, &p.SourceAmount, &p.Status, &p.Reservation,
		&p.ProviderRef, &p.Attempts, &p.NextAttemptAt, &p.LastError, &p.CreatedAt, &p.UpdatedAt, &p.CompletedAt}, extra...)
	if err := row.Scan(dest...); err != nil {
		return nil, err
	}
	return p, nil
}

// CreateBatch inserts the batch, reserves every hold and inserts all payouts
// in one transaction and a constant number of round trips per hold.
func (r *PayoutRepo) CreateBatch(ctx context.Context, b *anchor.PayoutBatch, holds []anchor.BalanceHold) (*anchor.PayoutBatch, bool, error) {
	created := false
	err := RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)

		// ON CONFLICT waits for a concurrent insert of the same key, so a
		// replay never double-reserves.
		tag, err := db.Exec(ctx, `
			INSERT INTO anchor_payout_batches (id, tenant_id, wallet_id, idempotency_key, request_hash, created_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (tenant_id, idempotency_key) DO NOTHING`,
			b.ID, b.TenantID, b.WalletID, b.IdempotencyKey, b.RequestHash, b.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert payout batch: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return nil
		}
		created = true

		var owned bool
		if err := db.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM wallets WHERE id = $1 AND tenant_id = $2 AND mode = $3)`,
			b.WalletID, b.TenantID, walletMode(ctx),
		).Scan(&owned); err != nil {
			return fmt.Errorf("check wallet ownership: %w", err)
		}
		if !owned {
			return domain.ErrWalletNotFound
		}

		for _, h := range holds {
			if err := reserve(ctx, db, h, b.CreatedAt); err != nil {
				return err
			}
		}
		return insertPayouts(ctx, db, b)
	})
	if err != nil {
		return nil, false, err
	}
	if !created {
		stored, err := r.batchByKey(ctx, b.TenantID, b.IdempotencyKey)
		return stored, false, err
	}
	return b, true, nil
}

// reserve locks the wallet balance row, adds the hold and rejects it when
// the running total exceeds the balance; the caller's rollback undoes it.
func reserve(ctx context.Context, db DB, h anchor.BalanceHold, at time.Time) error {
	var balance decimal.Decimal
	err := db.QueryRow(ctx, `
		SELECT balance FROM balances
		WHERE wallet_id = $1 AND asset_code = $2 AND issuer = $3
		FOR UPDATE`,
		h.WalletID, h.AssetCode, h.Issuer,
	).Scan(&balance)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: no %s balance", domain.ErrInsufficientBalance, h.AssetCode)
	}
	if err != nil {
		return fmt.Errorf("lock balance: %w", err)
	}

	var held decimal.Decimal
	if err := db.QueryRow(ctx, `
		INSERT INTO anchor_payout_balance_holds (wallet_id, asset_code, issuer, held, updated_at)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (wallet_id, asset_code, issuer)
		DO UPDATE SET held = anchor_payout_balance_holds.held + EXCLUDED.held, updated_at = EXCLUDED.updated_at
		RETURNING held`,
		h.WalletID, h.AssetCode, h.Issuer, h.Amount.String(), at,
	).Scan(&held); err != nil {
		return fmt.Errorf("reserve balance: %w", err)
	}
	if held.GreaterThan(balance) {
		return fmt.Errorf("%w: %s available %s, requested %s", domain.ErrInsufficientBalance,
			h.AssetCode, balance.Sub(held).Add(h.Amount), h.Amount)
	}
	return nil
}

// insertPayouts writes the whole batch in a single statement via unnest.
func insertPayouts(ctx context.Context, db DB, b *anchor.PayoutBatch) error {
	n := len(b.Payouts)
	ids, refs, partners, currencies, countries := make([]string, n), make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	destAmounts, assets, issuers, srcAmounts := make([]string, n), make([]string, n), make([]string, n), make([]string, n)
	beneficiaries := make([][]byte, n)
	for i, p := range b.Payouts {
		ids[i], refs[i], partners[i], currencies[i], countries[i] = p.ID, p.Reference, p.Partner, p.DestinationCurrency, p.Country
		destAmounts[i], assets[i], issuers[i], srcAmounts[i] = p.DestinationAmount.String(), p.SourceAsset, p.SourceIssuer, p.SourceAmount.String()
		beneficiaries[i] = p.BeneficiaryCiphertext
	}
	_, err := db.Exec(ctx, `
		INSERT INTO anchor_payouts
		    (id, batch_id, tenant_id, wallet_id, reference, partner, destination_currency, country, destination_amount,
		     source_asset, source_issuer, source_amount, beneficiary_ciphertext, status, reservation_state,
		     next_attempt_at, created_at, updated_at)
		SELECT u.id::uuid, $1, $2, $3, u.reference, u.partner, u.currency, u.country, u.dest_amount::numeric,
		       u.asset, u.issuer, u.src_amount::numeric, u.beneficiary, 'queued', 'held', $4, $4, $4
		FROM unnest($5::text[], $6::text[], $7::text[], $8::text[], $9::text[], $10::text[], $11::text[], $12::text[], $13::text[], $14::bytea[])
		     AS u(id, reference, partner, currency, country, dest_amount, asset, issuer, src_amount, beneficiary)`,
		b.ID, b.TenantID, b.WalletID, b.CreatedAt,
		ids, refs, partners, currencies, countries, destAmounts, assets, issuers, srcAmounts, beneficiaries)
	if err != nil {
		return fmt.Errorf("insert payouts: %w", err)
	}
	return nil
}

func (r *PayoutRepo) batchByKey(ctx context.Context, tenantID, key string) (*anchor.PayoutBatch, error) {
	var id string
	err := r.db.QueryRow(ctx,
		`SELECT id FROM anchor_payout_batches WHERE tenant_id = $1 AND idempotency_key = $2`, tenantID, key,
	).Scan(&id)
	if err != nil {
		return nil, fmt.Errorf("load replayed payout batch: %w", err)
	}
	return r.GetBatch(ctx, tenantID, id)
}

// GetBatch returns the tenant's batch and its payouts.
func (r *PayoutRepo) GetBatch(ctx context.Context, tenantID, batchID string) (*anchor.PayoutBatch, error) {
	if _, err := uuid.Parse(batchID); err != nil {
		return nil, anchor.ErrPayoutBatchNotFound
	}
	b := &anchor.PayoutBatch{}
	err := r.db.QueryRow(ctx, `
		SELECT id, tenant_id, wallet_id, idempotency_key, request_hash, created_at
		FROM anchor_payout_batches WHERE id = $1 AND tenant_id = $2`,
		batchID, tenantID,
	).Scan(&b.ID, &b.TenantID, &b.WalletID, &b.IdempotencyKey, &b.RequestHash, &b.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, anchor.ErrPayoutBatchNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get payout batch: %w", err)
	}

	rows, err := r.db.Query(ctx, `SELECT `+payoutColumns+` FROM anchor_payouts p WHERE p.batch_id = $1 ORDER BY p.reference`, batchID)
	if err != nil {
		return nil, fmt.Errorf("list batch payouts: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		p, err := scanPayout(rows)
		if err != nil {
			return nil, fmt.Errorf("scan payout: %w", err)
		}
		b.Payouts = append(b.Payouts, p)
	}
	return b, rows.Err()
}

// ClaimForPartner serialises claimers of one partner with a transaction
// advisory lock so the in-flight count and the claim are consistent across
// workers. Payouts whose lease expired (a crashed worker) are reclaimed.
func (r *PayoutRepo) ClaimForPartner(ctx context.Context, partner string, maxInFlight int, now, leaseUntil time.Time) ([]*anchor.Payout, error) {
	var claimed []*anchor.Payout
	err := RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)
		if _, err := db.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "anchor_payouts:"+partner); err != nil {
			return fmt.Errorf("lock partner queue: %w", err)
		}

		var inFlight int
		if err := db.QueryRow(ctx, `
			SELECT count(*) FROM anchor_payouts
			WHERE partner = $1 AND status = 'submitting' AND lease_expires_at > $2`,
			partner, now,
		).Scan(&inFlight); err != nil {
			return fmt.Errorf("count in-flight payouts: %w", err)
		}
		slots := maxInFlight - inFlight
		if slots <= 0 {
			return nil
		}

		rows, err := db.Query(ctx, `
			WITH due AS (
			    SELECT id FROM anchor_payouts
			    WHERE partner = $1
			      AND ((status = 'queued' AND next_attempt_at <= $2)
			        OR (status = 'submitting' AND lease_expires_at <= $2))
			    ORDER BY next_attempt_at
			    LIMIT $3
			    FOR UPDATE SKIP LOCKED
			)
			UPDATE anchor_payouts p
			SET status = 'submitting', lease_expires_at = $4, attempts = p.attempts + 1, updated_at = $2
			FROM due
			WHERE p.id = due.id
			RETURNING `+payoutColumns+`, p.beneficiary_ciphertext`,
			partner, now, slots, leaseUntil)
		if err != nil {
			return fmt.Errorf("claim payouts: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var ciphertext []byte
			p, err := scanPayout(rows, &ciphertext)
			if err != nil {
				return fmt.Errorf("scan claimed payout: %w", err)
			}
			p.BeneficiaryCiphertext = ciphertext
			claimed = append(claimed, p)
		}
		return rows.Err()
	})
	return claimed, err
}

// MarkSubmitted stores the partner reference. A callback that already
// settled the payout is never regressed to submitted.
func (r *PayoutRepo) MarkSubmitted(ctx context.Context, payoutID, providerRef string, at time.Time) error {
	_, err := r.db.Exec(ctx, `
		UPDATE anchor_payouts
		SET provider_ref = $2,
		    status = CASE WHEN status = 'submitting' THEN 'submitted' ELSE status END,
		    lease_expires_at = NULL, last_error = '', updated_at = $3
		WHERE id = $1`,
		payoutID, providerRef, at)
	if err != nil {
		return fmt.Errorf("mark payout submitted: %w", err)
	}
	return nil
}

func (r *PayoutRepo) Reschedule(ctx context.Context, payoutID string, next time.Time, lastErr string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE anchor_payouts
		SET status = 'queued', next_attempt_at = $2, last_error = $3, lease_expires_at = NULL, updated_at = NOW()
		WHERE id = $1 AND status = 'submitting'`,
		payoutID, next, lastErr)
	if err != nil {
		return fmt.Errorf("reschedule payout: %w", err)
	}
	return nil
}

// ApplyEvent locks the payout, records the event once, applies the state
// transition and releases the hold in one transaction. An invalid
// transition rolls back the event too, so a later valid delivery applies.
func (r *PayoutRepo) ApplyEvent(ctx context.Context, e anchor.PayoutEvent) (*anchor.Payout, bool, error) {
	if _, err := uuid.Parse(e.PayoutID); err != nil {
		return nil, false, anchor.ErrPayoutNotFound
	}
	var (
		result  *anchor.Payout
		applied bool
	)
	err := RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)
		current, err := scanPayout(db.QueryRow(ctx,
			`SELECT `+payoutColumns+` FROM anchor_payouts p WHERE p.id = $1 AND p.partner = $2 FOR UPDATE`,
			e.PayoutID, e.Partner))
		if errors.Is(err, pgx.ErrNoRows) {
			return anchor.ErrPayoutNotFound
		}
		if err != nil {
			return fmt.Errorf("lock payout: %w", err)
		}

		tag, err := db.Exec(ctx, `
			INSERT INTO anchor_payout_events (partner, event_id, payout_id, outcome, reason, received_at)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (partner, event_id) DO NOTHING`,
			e.Partner, e.EventID, e.PayoutID, string(e.Outcome), e.Reason, e.At)
		if err != nil {
			return fmt.Errorf("record payout event: %w", err)
		}
		if tag.RowsAffected() == 0 {
			result = current
			return nil
		}

		next, err := anchor.NextPayoutState(current.Status, e.Outcome)
		if err != nil {
			return err
		}
		result, err = scanPayout(db.QueryRow(ctx, `
			UPDATE anchor_payouts p
			SET status = $2, reservation_state = $3, lease_expires_at = NULL, updated_at = $4,
			    completed_at = CASE WHEN $2 = 'completed' THEN $4 ELSE p.completed_at END,
			    last_error = CASE WHEN $2 = 'failed' THEN $5 ELSE p.last_error END
			WHERE p.id = $1
			RETURNING `+payoutColumns,
			e.PayoutID, string(next.Status), string(next.Reservation), e.At, e.Reason))
		if err != nil {
			return fmt.Errorf("update payout: %w", err)
		}

		if next.Release {
			if _, err := db.Exec(ctx, `
				UPDATE anchor_payout_balance_holds
				SET held = held - $4, updated_at = $5
				WHERE wallet_id = $1 AND asset_code = $2 AND issuer = $3`,
				current.WalletID, current.SourceAsset, current.SourceIssuer, current.SourceAmount.String(), e.At); err != nil {
				return fmt.Errorf("release balance hold: %w", err)
			}
		}
		applied = true
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return result, applied, nil
}
