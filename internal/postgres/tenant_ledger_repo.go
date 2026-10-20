package postgres

import (
	"context"
	"fmt"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/indexer"
)

// TenantLedgerRepo stores per-tenant ledger checkpoints and satisfies
// indexer.LedgerCheckpointStore.
type TenantLedgerRepo struct {
	db DB
}

func NewTenantLedgerRepo(db DB) *TenantLedgerRepo {
	return &TenantLedgerRepo{db: db}
}

// Tenants lists tenants that own wallets in mode.
func (r *TenantLedgerRepo) Tenants(ctx context.Context, mode domain.Mode) ([]string, error) {
	rows, err := r.db.Query(ctx,
		`SELECT DISTINCT tenant_id FROM wallets WHERE tenant_id IS NOT NULL AND mode = $1 ORDER BY tenant_id`, string(mode))
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// TenantWallets returns the tenant's wallets in mode via idx_wallets_tenant_mode.
func (r *TenantLedgerRepo) TenantWallets(ctx context.Context, tenantID string, mode domain.Mode) ([]*domain.Wallet, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, public_key, tenant_id FROM wallets WHERE tenant_id = $1 AND mode = $2`, tenantID, string(mode))
	if err != nil {
		return nil, fmt.Errorf("list tenant wallets: %w", err)
	}
	defer rows.Close()
	var out []*domain.Wallet
	for rows.Next() {
		w := &domain.Wallet{Mode: mode}
		if err := rows.Scan(&w.ID, &w.PublicKey, &w.TenantID); err != nil {
			return nil, fmt.Errorf("scan wallet: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// Bounds reads min and max from the primary key (O(log n) each) and the
// contiguous watermark, which defaults to min - 1.
func (r *TenantLedgerRepo) Bounds(ctx context.Context, tenantID string, mode domain.Mode) (indexer.LedgerBounds, bool, error) {
	var minSeq, maxSeq, contiguous *int64
	err := r.db.QueryRow(ctx, `
		SELECT l.min_seq, l.max_seq, COALESCE(w.contiguous_through, l.min_seq - 1)
		FROM (SELECT min(sequence) AS min_seq, max(sequence) AS max_seq
		      FROM tenant_indexed_ledgers WHERE tenant_id = $1 AND mode = $2) l
		LEFT JOIN tenant_ledger_watermarks w ON w.tenant_id = $1 AND w.mode = $2`,
		tenantID, string(mode),
	).Scan(&minSeq, &maxSeq, &contiguous)
	if err != nil {
		return indexer.LedgerBounds{}, false, fmt.Errorf("read ledger bounds: %w", err)
	}
	if minSeq == nil {
		return indexer.LedgerBounds{}, false, nil
	}
	return indexer.LedgerBounds{Min: uint32(*minSeq), Max: uint32(*maxSeq), Contiguous: uint32(*contiguous)}, true, nil
}

// Gaps finds missing ranges with a LEAD window over the primary-key range
// scan from `from`: O(k) for the k rows above the watermark.
func (r *TenantLedgerRepo) Gaps(ctx context.Context, tenantID string, mode domain.Mode, from uint32, limit int) ([]indexer.LedgerRange, error) {
	rows, err := r.db.Query(ctx, `
		SELECT sequence + 1, next_seq - 1
		FROM (
		    SELECT sequence, LEAD(sequence) OVER (ORDER BY sequence) AS next_seq
		    FROM tenant_indexed_ledgers
		    WHERE tenant_id = $1 AND mode = $2 AND sequence >= $3
		) t
		WHERE next_seq > sequence + 1
		ORDER BY sequence
		LIMIT $4`,
		tenantID, string(mode), int64(from), limit)
	if err != nil {
		return nil, fmt.Errorf("find ledger gaps: %w", err)
	}
	defer rows.Close()
	var out []indexer.LedgerRange
	for rows.Next() {
		var start, end int64
		if err := rows.Scan(&start, &end); err != nil {
			return nil, fmt.Errorf("scan ledger gap: %w", err)
		}
		out = append(out, indexer.LedgerRange{From: uint32(start), To: uint32(end)})
	}
	return out, rows.Err()
}

func (r *TenantLedgerRepo) MarkIndexed(ctx context.Context, tenantID string, mode domain.Mode, sequence uint32) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO tenant_indexed_ledgers (tenant_id, mode, sequence) VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING`, tenantID, string(mode), int64(sequence))
	if err != nil {
		return fmt.Errorf("mark ledger indexed: %w", err)
	}
	return nil
}

func (r *TenantLedgerRepo) AdvanceContiguous(ctx context.Context, tenantID string, mode domain.Mode, sequence uint32) error {
	_, err := r.db.Exec(ctx, `
		INSERT INTO tenant_ledger_watermarks (tenant_id, mode, contiguous_through) VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, mode) DO UPDATE
		SET contiguous_through = GREATEST(tenant_ledger_watermarks.contiguous_through, EXCLUDED.contiguous_through),
		    updated_at = NOW()`, tenantID, string(mode), int64(sequence))
	if err != nil {
		return fmt.Errorf("advance ledger watermark: %w", err)
	}
	return nil
}
