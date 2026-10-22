package fx

import (
	"context"
	"database/sql"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// AlertRepository handles persistence of FX rate alerts.
type AlertRepository interface {
	Create(ctx context.Context, alert *domain.RateAlert) error
	Get(ctx context.Context, id, tenantID string) (*domain.RateAlert, error)
	List(ctx context.Context, tenantID string, active bool) ([]*domain.RateAlert, error)
	Update(ctx context.Context, alert *domain.RateAlert) error
	Delete(ctx context.Context, id, tenantID string) error
	GetActiveAlertsForPair(ctx context.Context, fromAsset, toAsset string) ([]*domain.RateAlert, error)
	GetAllActiveAlerts(ctx context.Context) ([]*domain.RateAlert, error)
	RecordFiring(ctx context.Context, id string, rate decimal.Decimal) error
	RecordFiringAtomicCAS(ctx context.Context, id string, rate decimal.Decimal, expectedLastFiredAt *time.Time) (bool, error)
}

type alertRepository struct {
	db *sql.DB
}

// NewAlertRepository creates a new FX alert repository.
func NewAlertRepository(db *sql.DB) AlertRepository {
	return &alertRepository{db: db}
}

func (r *alertRepository) Create(ctx context.Context, alert *domain.RateAlert) error {
	alert.ID = uuid.New().String()
	alert.CreatedAt = time.Now()
	alert.UpdatedAt = time.Now()
	alert.Active = true

	_, err := r.db.ExecContext(ctx, `
		INSERT INTO fx_rate_alerts 
		(id, tenant_id, from_asset, to_asset, target_rate, direction, expires_at, active, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, alert.ID, alert.TenantID, alert.FromAsset, alert.ToAsset, alert.TargetRate,
		alert.Direction, alert.ExpiresAt, alert.Active, alert.CreatedAt, alert.UpdatedAt)

	return err
}

func (r *alertRepository) Get(ctx context.Context, id, tenantID string) (*domain.RateAlert, error) {
	alert := &domain.RateAlert{}
	err := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, from_asset, to_asset, target_rate, direction, 
		       expires_at, last_fired_at, last_eval_rate, active, created_at, updated_at
		FROM fx_rate_alerts
		WHERE id = $1 AND tenant_id = $2
	`, id, tenantID).Scan(
		&alert.ID, &alert.TenantID, &alert.FromAsset, &alert.ToAsset, &alert.TargetRate,
		&alert.Direction, &alert.ExpiresAt, &alert.LastFiredAt, &alert.LastEvalRate,
		&alert.Active, &alert.CreatedAt, &alert.UpdatedAt,
	)

	if err == sql.ErrNoRows {
		return nil, domain.ErrWalletNotFound
	}
	return alert, err
}

func (r *alertRepository) List(ctx context.Context, tenantID string, active bool) ([]*domain.RateAlert, error) {
	query := `
		SELECT id, tenant_id, from_asset, to_asset, target_rate, direction,
		       expires_at, last_fired_at, last_eval_rate, active, created_at, updated_at
		FROM fx_rate_alerts
		WHERE tenant_id = $1
	`
	if active {
		query += " AND active = TRUE"
	}
	query += " ORDER BY created_at DESC"

	rows, err := r.db.QueryContext(ctx, query, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []*domain.RateAlert
	for rows.Next() {
		alert := &domain.RateAlert{}
		err := rows.Scan(
			&alert.ID, &alert.TenantID, &alert.FromAsset, &alert.ToAsset, &alert.TargetRate,
			&alert.Direction, &alert.ExpiresAt, &alert.LastFiredAt, &alert.LastEvalRate,
			&alert.Active, &alert.CreatedAt, &alert.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}

	return alerts, rows.Err()
}

func (r *alertRepository) Update(ctx context.Context, alert *domain.RateAlert) error {
	alert.UpdatedAt = time.Now()

	_, err := r.db.ExecContext(ctx, `
		UPDATE fx_rate_alerts
		SET target_rate = $1, direction = $2, expires_at = $3, active = $4, updated_at = $5
		WHERE id = $6 AND tenant_id = $7
	`, alert.TargetRate, alert.Direction, alert.ExpiresAt, alert.Active, alert.UpdatedAt, alert.ID, alert.TenantID)

	return err
}

func (r *alertRepository) Delete(ctx context.Context, id, tenantID string) error {
	_, err := r.db.ExecContext(ctx, `
		DELETE FROM fx_rate_alerts WHERE id = $1 AND tenant_id = $2
	`, id, tenantID)
	return err
}

func (r *alertRepository) GetActiveAlertsForPair(ctx context.Context, fromAsset, toAsset string) ([]*domain.RateAlert, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, from_asset, to_asset, target_rate, direction,
		       expires_at, last_fired_at, last_eval_rate, active, created_at, updated_at
		FROM fx_rate_alerts
		WHERE from_asset = $1 AND to_asset = $2 AND active = TRUE
		  AND (expires_at IS NULL OR expires_at > NOW())
	`, fromAsset, toAsset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []*domain.RateAlert
	for rows.Next() {
		alert := &domain.RateAlert{}
		err := rows.Scan(
			&alert.ID, &alert.TenantID, &alert.FromAsset, &alert.ToAsset, &alert.TargetRate,
			&alert.Direction, &alert.ExpiresAt, &alert.LastFiredAt, &alert.LastEvalRate,
			&alert.Active, &alert.CreatedAt, &alert.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}

	return alerts, rows.Err()
}

func (r *alertRepository) GetAllActiveAlerts(ctx context.Context) ([]*domain.RateAlert, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, from_asset, to_asset, target_rate, direction,
		       expires_at, last_fired_at, last_eval_rate, active, created_at, updated_at
		FROM fx_rate_alerts
		WHERE active = TRUE
		  AND (expires_at IS NULL OR expires_at > NOW())
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var alerts []*domain.RateAlert
	for rows.Next() {
		alert := &domain.RateAlert{}
		err := rows.Scan(
			&alert.ID, &alert.TenantID, &alert.FromAsset, &alert.ToAsset, &alert.TargetRate,
			&alert.Direction, &alert.ExpiresAt, &alert.LastFiredAt, &alert.LastEvalRate,
			&alert.Active, &alert.CreatedAt, &alert.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		alerts = append(alerts, alert)
	}

	return alerts, rows.Err()
}

func (r *alertRepository) RecordFiring(ctx context.Context, id string, rate decimal.Decimal) error {
	now := time.Now()
	_, err := r.db.ExecContext(ctx, `
		UPDATE fx_rate_alerts
		SET last_fired_at = $1, last_eval_rate = $2, updated_at = $3
		WHERE id = $4
	`, now, rate, now, id)
	return err
}

func (r *alertRepository) RecordFiringAtomicCAS(ctx context.Context, id string, rate decimal.Decimal, expectedLastFiredAt *time.Time) (bool, error) {
	now := time.Now().UTC()
	var query string
	var args []interface{}
	if expectedLastFiredAt == nil {
		query = `
			UPDATE fx_rate_alerts
			SET last_fired_at = $1, last_eval_rate = $2, updated_at = $1
			WHERE id = $3 AND active = TRUE AND last_fired_at IS NULL
		`
		args = []interface{}{now, rate, id}
	} else {
		query = `
			UPDATE fx_rate_alerts
			SET last_fired_at = $1, last_eval_rate = $2, updated_at = $1
			WHERE id = $3 AND active = TRUE AND last_fired_at = $4
		`
		args = []interface{}{now, rate, id, *expectedLastFiredAt}
	}
	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return rows > 0, nil
}

