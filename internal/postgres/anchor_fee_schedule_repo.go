package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/fees"
	"github.com/jackc/pgx/v5"
	"github.com/shopspring/decimal"
)

// AnchorFeeScheduleRepo reads anchor_corridor_fee_schedules and satisfies
// anchor.FeeScheduleStore.
type AnchorFeeScheduleRepo struct {
	db DB
}

func NewAnchorFeeScheduleRepo(db DB) *AnchorFeeScheduleRepo {
	return &AnchorFeeScheduleRepo{db: db}
}

type surgeWindowRow struct {
	Weekdays    []time.Weekday  `json:"weekdays"`
	StartMinute int             `json:"start_minute"`
	EndMinute   int             `json:"end_minute"`
	Multiplier  decimal.Decimal `json:"multiplier"`
}

// EffectiveCorridorSchedule returns the tenant override for (corridor, tier)
// when present, otherwise the platform default. One indexed lookup; NULLS
// LAST orders the tenant row ahead of the default.
func (r *AnchorFeeScheduleRepo) EffectiveCorridorSchedule(ctx context.Context, tenantID, corridor string, tier fees.SpeedTier) (*fees.CorridorSchedule, error) {
	var tenantArg *string
	if tenantID != "" {
		tenantArg = &tenantID
	}

	var (
		s         fees.CorridorSchedule
		timeZone  string
		surgeJSON []byte
	)
	err := r.db.QueryRow(ctx, `
		SELECT corridor, speed_tier, rail,
		       processing_flat, processing_bps, processing_min, processing_max,
		       rail_flat, rail_bps, rail_min, rail_max,
		       time_zone, surge_windows, operations
		FROM anchor_corridor_fee_schedules
		WHERE corridor = $1 AND speed_tier = $2
		  AND (tenant_id = $3::uuid OR tenant_id IS NULL)
		ORDER BY tenant_id NULLS LAST
		LIMIT 1`,
		corridor, string(tier), tenantArg,
	).Scan(
		&s.Corridor, &s.SpeedTier, &s.Rail,
		&s.Processing.Flat, &s.Processing.Bps, &s.Processing.Min, &s.Processing.Max,
		&s.RailSurcharge.Flat, &s.RailSurcharge.Bps, &s.RailSurcharge.Min, &s.RailSurcharge.Max,
		&timeZone, &surgeJSON, &s.Operations,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrFeeScheduleNotFound
		}
		return nil, fmt.Errorf("get corridor fee schedule: %w", err)
	}
	if s.Location, err = time.LoadLocation(timeZone); err != nil {
		return nil, fmt.Errorf("corridor %s: invalid time_zone %q: %w", corridor, timeZone, err)
	}

	var windows []surgeWindowRow
	if err := json.Unmarshal(surgeJSON, &windows); err != nil {
		return nil, fmt.Errorf("corridor %s: decode surge_windows: %w", corridor, err)
	}
	s.SurgeWindows = make([]fees.SurgeWindow, len(windows))
	for i, w := range windows {
		s.SurgeWindows[i] = fees.SurgeWindow(w)
	}
	return &s, nil
}
