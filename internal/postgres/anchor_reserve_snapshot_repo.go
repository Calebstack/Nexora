package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
)

// AnchorReserveSnapshotRepo persists append-only anchor reserve snapshots
// and satisfies anchor.ReserveSnapshotStore.
type AnchorReserveSnapshotRepo struct {
	db DB
}

func NewAnchorReserveSnapshotRepo(db DB) *AnchorReserveSnapshotRepo {
	return &AnchorReserveSnapshotRepo{db: db}
}

// InsertHourly writes s unless the hour is already recorded for the asset.
func (r *AnchorReserveSnapshotRepo) InsertHourly(ctx context.Context, s *anchor.ReserveSnapshot) (bool, error) {
	tag, err := r.db.Exec(ctx, `
		INSERT INTO anchor_reserve_snapshots
		    (asset_code, issuer, reserve_currency, on_chain_supply, off_chain_reserves,
		     reserve_ratio, deviation, breached, snapshot_hour, captured_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (asset_code, issuer, snapshot_hour) DO NOTHING`,
		s.AssetCode, s.Issuer, s.ReserveCurrency, s.OnChainSupply.String(), s.OffChainReserves.String(),
		nullableDecimalPtr(s.Ratio), nullableDecimalPtr(s.Deviation), s.Breached, s.SnapshotHour, s.CapturedAt,
	)
	if err != nil {
		return false, fmt.Errorf("insert reserve snapshot: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// ListSince returns the asset's snapshots captured at or after since, oldest
// first, using the (asset_code, issuer, captured_at) index.
func (r *AnchorReserveSnapshotRepo) ListSince(ctx context.Context, assetCode, issuer string, since time.Time) ([]anchor.ReserveSnapshot, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, asset_code, issuer, reserve_currency, on_chain_supply, off_chain_reserves,
		       reserve_ratio, deviation, breached, snapshot_hour, captured_at
		FROM anchor_reserve_snapshots
		WHERE asset_code = $1 AND issuer = $2 AND captured_at >= $3
		ORDER BY captured_at ASC`,
		assetCode, issuer, since,
	)
	if err != nil {
		return nil, fmt.Errorf("list reserve snapshots: %w", err)
	}
	defer rows.Close()

	var out []anchor.ReserveSnapshot
	for rows.Next() {
		var s anchor.ReserveSnapshot
		if err := rows.Scan(&s.ID, &s.AssetCode, &s.Issuer, &s.ReserveCurrency, &s.OnChainSupply, &s.OffChainReserves,
			&s.Ratio, &s.Deviation, &s.Breached, &s.SnapshotHour, &s.CapturedAt); err != nil {
			return nil, fmt.Errorf("scan reserve snapshot: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
