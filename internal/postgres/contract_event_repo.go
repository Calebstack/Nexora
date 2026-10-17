package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/jackc/pgx/v5"
)

type ContractEventRepo struct {
	db DB
}

func NewContractEventRepo(db DB) *ContractEventRepo {
	return &ContractEventRepo{db: db}
}

// SaveLedger records a processed ledger and its events in one transaction and
// returns the events that were not already stored. Re-indexing a ledger after a
// re-org therefore never duplicates rows or re-announces events.
func (r *ContractEventRepo) SaveLedger(ctx context.Context, ledger domain.IndexedLedger, events []*domain.ContractEvent) ([]*domain.ContractEvent, error) {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin contract event save: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx,
		`INSERT INTO contract_event_ledgers (sequence, hash) VALUES ($1, $2)
		 ON CONFLICT (sequence) DO UPDATE SET hash = EXCLUDED.hash, indexed_at = NOW()`,
		int64(ledger.Sequence), ledger.Hash,
	); err != nil {
		return nil, fmt.Errorf("upsert indexed ledger %d: %w", ledger.Sequence, err)
	}

	inserted := make([]*domain.ContractEvent, 0, len(events))
	if len(events) > 0 {
		batch := &pgx.Batch{}
		for _, e := range events {
			batch.Queue(
				`INSERT INTO contract_events (id, contract_id, event_type, topic, topics, data, ledger, tx_hash, created_at)
				 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
				 ON CONFLICT (id) DO NOTHING`,
				e.ID, e.ContractID, e.EventType, e.Topic, []byte(e.Topics), []byte(e.Data),
				int64(e.Ledger), e.TxHash, e.CreatedAt,
			)
		}

		results := tx.SendBatch(ctx, batch)
		for _, e := range events {
			tag, err := results.Exec()
			if err != nil {
				results.Close()
				return nil, fmt.Errorf("insert contract event %s: %w", e.ID, err)
			}
			if tag.RowsAffected() > 0 {
				inserted = append(inserted, e)
			}
		}
		if err := results.Close(); err != nil {
			return nil, fmt.Errorf("close contract event batch: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit contract event save: %w", err)
	}
	return inserted, nil
}

// GetLedger returns the indexed ledger with the given sequence, or nil if it
// has not been indexed.
func (r *ContractEventRepo) GetLedger(ctx context.Context, sequence uint32) (*domain.IndexedLedger, error) {
	return r.scanLedger(r.db.QueryRow(ctx,
		`SELECT sequence, hash FROM contract_event_ledgers WHERE sequence = $1`, int64(sequence)))
}

// LastLedger returns the highest indexed ledger, or nil if nothing has been
// indexed yet.
func (r *ContractEventRepo) LastLedger(ctx context.Context) (*domain.IndexedLedger, error) {
	return r.scanLedger(r.db.QueryRow(ctx,
		`SELECT sequence, hash FROM contract_event_ledgers ORDER BY sequence DESC LIMIT 1`))
}

func (r *ContractEventRepo) scanLedger(row pgx.Row) (*domain.IndexedLedger, error) {
	var (
		sequence int64
		ledger   domain.IndexedLedger
	)
	if err := row.Scan(&sequence, &ledger.Hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("scan indexed ledger: %w", err)
	}
	ledger.Sequence = uint32(sequence)
	return &ledger, nil
}

// DeleteFromLedger removes every indexed ledger and event at or above
// sequence so they can be re-indexed after a re-org.
func (r *ContractEventRepo) DeleteFromLedger(ctx context.Context, sequence uint32) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin contract event rollback: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `DELETE FROM contract_events WHERE ledger >= $1`, int64(sequence)); err != nil {
		return fmt.Errorf("delete contract events from ledger %d: %w", sequence, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM contract_event_ledgers WHERE sequence >= $1`, int64(sequence)); err != nil {
		return fmt.Errorf("delete indexed ledgers from %d: %w", sequence, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit contract event rollback: %w", err)
	}
	return nil
}
