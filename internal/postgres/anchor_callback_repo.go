package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// AnchorCallbackRepo is the durable callback outbox and satisfies
// anchor.CallbackStore.
type AnchorCallbackRepo struct {
	db DB
}

func NewAnchorCallbackRepo(db DB) *AnchorCallbackRepo {
	return &AnchorCallbackRepo{db: db}
}

const pgForeignKeyViolation = "23503"

func (r *AnchorCallbackRepo) CreateEndpoint(ctx context.Context, e *anchor.CallbackEndpoint) error {
	err := r.db.QueryRow(ctx, `
		INSERT INTO anchor_callback_endpoints (partner, url, secret_ciphertext, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $4)
		RETURNING id`,
		e.Partner, e.URL, e.SecretCiphertext, e.CreatedAt,
	).Scan(&e.ID)
	if err != nil {
		return fmt.Errorf("insert callback endpoint: %w", err)
	}
	return nil
}

// Enqueue inserts the delivery, taking the quarantined state from the
// endpoint in the same statement.
func (r *AnchorCallbackRepo) Enqueue(ctx context.Context, d *anchor.CallbackDelivery) error {
	err := r.db.QueryRow(ctx, `
		INSERT INTO anchor_callback_deliveries
		    (endpoint_id, sep, transaction_id, transaction_status, payload, state, next_attempt_at, expires_at, created_at)
		SELECT e.id, $2, $3, $4, $5,
		       CASE WHEN e.quarantined_at IS NULL THEN 'pending' ELSE 'quarantined' END,
		       $6, $7, $8
		FROM anchor_callback_endpoints e
		WHERE e.id = $1
		RETURNING id, state`,
		d.EndpointID, d.SEP, d.TransactionID, d.TransactionStatus, d.Payload, d.NextAttemptAt, d.ExpiresAt, d.CreatedAt,
	).Scan(&d.ID, &d.State)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.Is(err, pgx.ErrNoRows) || (errors.As(err, &pgErr) && pgErr.Code == pgForeignKeyViolation) {
			return anchor.ErrCallbackEndpointNotFound
		}
		return fmt.Errorf("enqueue callback: %w", err)
	}
	return nil
}

// ClaimDue leases due deliveries with FOR UPDATE SKIP LOCKED so concurrent
// workers never claim the same row. O(limit · log n) on the partial due index.
func (r *AnchorCallbackRepo) ClaimDue(ctx context.Context, now, leaseUntil time.Time, limit int) ([]anchor.ClaimedCallback, error) {
	rows, err := r.db.Query(ctx, `
		WITH due AS (
		    SELECT id FROM anchor_callback_deliveries
		    WHERE state = 'pending' AND next_attempt_at <= $1
		    ORDER BY next_attempt_at
		    LIMIT $3
		    FOR UPDATE SKIP LOCKED
		)
		UPDATE anchor_callback_deliveries d
		SET next_attempt_at = $2
		FROM due, anchor_callback_endpoints e
		WHERE d.id = due.id AND e.id = d.endpoint_id
		RETURNING d.id, d.endpoint_id, d.sep, d.transaction_id, d.transaction_status, d.payload, d.state,
		          d.attempts, d.next_attempt_at, d.expires_at, d.created_at,
		          e.id, e.partner, e.url, e.secret_ciphertext, e.consecutive_failures, e.quarantined_at`,
		now, leaseUntil, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("claim due callbacks: %w", err)
	}
	defer rows.Close()

	var out []anchor.ClaimedCallback
	for rows.Next() {
		var c anchor.ClaimedCallback
		d, e := &c.Delivery, &c.Endpoint
		if err := rows.Scan(&d.ID, &d.EndpointID, &d.SEP, &d.TransactionID, &d.TransactionStatus, &d.Payload, &d.State,
			&d.Attempts, &d.NextAttemptAt, &d.ExpiresAt, &d.CreatedAt,
			&e.ID, &e.Partner, &e.URL, &e.SecretCiphertext, &e.ConsecutiveFailures, &e.QuarantinedAt); err != nil {
			return nil, fmt.Errorf("scan claimed callback: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// RecordSuccess marks the delivery delivered and, unless the endpoint is
// quarantined (only an operator replay lifts that), resets its failure count.
func (r *AnchorCallbackRepo) RecordSuccess(ctx context.Context, deliveryID, endpointID string, attempts, statusCode int, at time.Time) error {
	return RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)
		if _, err := db.Exec(ctx, `
			UPDATE anchor_callback_deliveries
			SET state = 'delivered', attempts = $2, last_status_code = $3, last_error = '', delivered_at = $4
			WHERE id = $1`,
			deliveryID, attempts, statusCode, at); err != nil {
			return fmt.Errorf("mark callback delivered: %w", err)
		}
		if _, err := db.Exec(ctx, `
			UPDATE anchor_callback_endpoints
			SET consecutive_failures = 0, updated_at = $2
			WHERE id = $1 AND quarantined_at IS NULL AND consecutive_failures <> 0`,
			endpointID, at); err != nil {
			return fmt.Errorf("reset endpoint failures: %w", err)
		}
		return nil
	})
}

// RecordFailure runs in one transaction. The endpoint row lock taken by the
// increment serialises concurrent failures for the same endpoint, so the
// quarantine transition happens exactly once.
func (r *AnchorCallbackRepo) RecordFailure(ctx context.Context, f anchor.CallbackFailure) (bool, error) {
	var justQuarantined bool
	err := RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)

		var failures int
		var quarantined bool
		if err := db.QueryRow(ctx, `
			UPDATE anchor_callback_endpoints
			SET consecutive_failures = consecutive_failures + 1, updated_at = $2
			WHERE id = $1
			RETURNING consecutive_failures, quarantined_at IS NOT NULL`,
			f.EndpointID, f.At,
		).Scan(&failures, &quarantined); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return anchor.ErrCallbackEndpointNotFound
			}
			return fmt.Errorf("increment endpoint failures: %w", err)
		}

		if !quarantined && failures >= f.QuarantineAfter {
			if _, err := db.Exec(ctx, `UPDATE anchor_callback_endpoints SET quarantined_at = $2 WHERE id = $1`, f.EndpointID, f.At); err != nil {
				return fmt.Errorf("quarantine endpoint: %w", err)
			}
			quarantined, justQuarantined = true, true
		}

		state := anchor.CallbackPending
		switch {
		case quarantined:
			state = anchor.CallbackQuarantined
		case f.NextAttemptAt == nil:
			state = anchor.CallbackExpired
		}
		if _, err := db.Exec(ctx, `
			UPDATE anchor_callback_deliveries
			SET state = $2, attempts = $3, last_status_code = $4, last_error = $5,
			    next_attempt_at = COALESCE($6, next_attempt_at)
			WHERE id = $1`,
			f.DeliveryID, string(state), f.Attempts, f.StatusCode, f.Error, f.NextAttemptAt); err != nil {
			return fmt.Errorf("record callback failure: %w", err)
		}

		if justQuarantined {
			if _, err := db.Exec(ctx, `
				UPDATE anchor_callback_deliveries SET state = 'quarantined'
				WHERE endpoint_id = $1 AND state = 'pending'`, f.EndpointID); err != nil {
				return fmt.Errorf("quarantine pending callbacks: %w", err)
			}
		}
		return nil
	})
	return justQuarantined, err
}

func (r *AnchorCallbackRepo) ListQuarantined(ctx context.Context, endpointID string, limit int) ([]anchor.CallbackDelivery, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, endpoint_id, sep, transaction_id, transaction_status, state, attempts,
		       next_attempt_at, expires_at, last_error, last_status_code, created_at
		FROM anchor_callback_deliveries
		WHERE endpoint_id = $1 AND state = 'quarantined'
		ORDER BY created_at
		LIMIT $2`,
		endpointID, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("list quarantined callbacks: %w", err)
	}
	defer rows.Close()

	var out []anchor.CallbackDelivery
	for rows.Next() {
		var d anchor.CallbackDelivery
		if err := rows.Scan(&d.ID, &d.EndpointID, &d.SEP, &d.TransactionID, &d.TransactionStatus, &d.State, &d.Attempts,
			&d.NextAttemptAt, &d.ExpiresAt, &d.LastError, &d.LastStatusCode, &d.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan quarantined callback: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ReleaseQuarantine lifts the quarantine and requeues quarantined
// deliveries atomically.
func (r *AnchorCallbackRepo) ReleaseQuarantine(ctx context.Context, endpointID string, now, expiresAt time.Time) (int, error) {
	var requeued int
	err := RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)
		tag, err := db.Exec(ctx, `
			UPDATE anchor_callback_endpoints
			SET quarantined_at = NULL, consecutive_failures = 0, updated_at = $2
			WHERE id = $1`, endpointID, now)
		if err != nil {
			return fmt.Errorf("release endpoint: %w", err)
		}
		if tag.RowsAffected() == 0 {
			return anchor.ErrCallbackEndpointNotFound
		}
		tag, err = db.Exec(ctx, `
			UPDATE anchor_callback_deliveries
			SET state = 'pending', attempts = 0, next_attempt_at = $2, expires_at = $3, last_error = '', last_status_code = 0
			WHERE endpoint_id = $1 AND state = 'quarantined'`, endpointID, now, expiresAt)
		if err != nil {
			return fmt.Errorf("requeue quarantined callbacks: %w", err)
		}
		requeued = int(tag.RowsAffected())
		return nil
	})
	return requeued, err
}
