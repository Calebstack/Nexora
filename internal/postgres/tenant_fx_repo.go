package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/shopspring/decimal"
)

const (
	// Platform allowable margin ceilings (basis points)
	// -500 bps = -5.00% max allowable discount
	// 5000 bps = +50.00% max allowable markup
	MinAllowableMarginBps = -500
	MaxAllowableMarginBps = 5000
)

// TenantFXRepo handles persistence, retrieval, tier resolution, and audit tracking
// for multi-tenant custom FX markups.
type TenantFXRepo interface {
	CreateMargin(ctx context.Context, m *domain.TenantFXMargin, actorPrincipal, actorIP string) error
	GetMargin(ctx context.Context, id, tenantID string) (*domain.TenantFXMargin, error)
	FindMargin(ctx context.Context, tenantID, fromAsset, toAsset string, amount decimal.Decimal) (*domain.TenantFXMargin, error)
	ListMargins(ctx context.Context, tenantID, fromAsset, toAsset string, limit, offset int) ([]*domain.TenantFXMargin, int, error)
	UpdateMargin(ctx context.Context, m *domain.TenantFXMargin, actorPrincipal, actorIP string) error
	DeleteMargin(ctx context.Context, id, tenantID, actorPrincipal, actorIP string) error
	ListMarginAudits(ctx context.Context, tenantID, marginID string, limit, offset int) ([]*domain.TenantFXMarginAudit, error)
}

type tenantFXRepo struct {
	db DB
}

// NewTenantFXRepo constructs a PostgreSQL-backed repository for tenant FX margin policies.
func NewTenantFXRepo(db DB) TenantFXRepo {
	return &tenantFXRepo{db: db}
}

func validateCorridorAndCeilings(m *domain.TenantFXMargin) error {
	from := strings.ToUpper(strings.TrimSpace(m.FromAsset))
	to := strings.ToUpper(strings.TrimSpace(m.ToAsset))
	if from == "" || to == "" {
		return domain.ErrInvalidAsset
	}
	if from == to {
		return domain.ErrCorridorConflict
	}
	m.FromAsset = from
	m.ToAsset = to

	if m.TierMinAmount.IsNegative() {
		return fmt.Errorf("%w: tier_min_amount cannot be negative", domain.ErrInvalidAmount)
	}
	if m.TierMaxAmount != nil && m.TierMaxAmount.LessThanOrEqual(m.TierMinAmount) {
		return fmt.Errorf("%w: tier_max_amount must exceed tier_min_amount", domain.ErrInvalidAmount)
	}

	// Default ceilings if unspecified
	if m.MinMarginBps == 0 && m.MaxMarginBps == 0 {
		m.MinMarginBps = 0
		m.MaxMarginBps = MaxAllowableMarginBps
	}
	if m.MinMarginBps < MinAllowableMarginBps || m.MaxMarginBps > MaxAllowableMarginBps || m.MinMarginBps > m.MaxMarginBps {
		return domain.ErrMarginCeilingExceeded
	}
	if m.MarkupBps < m.MinMarginBps || m.MarkupBps > m.MaxMarginBps {
		return domain.ErrMarginCeilingExceeded
	}

	return nil
}

func (r *tenantFXRepo) CreateMargin(ctx context.Context, m *domain.TenantFXMargin, actorPrincipal, actorIP string) error {
	if err := validateCorridorAndCeilings(m); err != nil {
		return err
	}
	if m.ID == "" {
		m.ID = uuid.New().String()
	}
	now := time.Now().UTC()
	m.CreatedAt = now
	m.UpdatedAt = now
	if actorPrincipal == "" {
		actorPrincipal = "admin"
	}
	m.CreatedBy = actorPrincipal
	m.UpdatedBy = actorPrincipal

	var tierMaxStr *string
	if m.TierMaxAmount != nil {
		s := m.TierMaxAmount.String()
		tierMaxStr = &s
	}

	return RunInTx(ctx, r.db, func(txCtx context.Context) error {
		tx := TxFromContext(txCtx, r.db)

		_, err := tx.Exec(txCtx, `
			INSERT INTO tenant_fx_margins (
				id, tenant_id, from_asset, to_asset, tier_min_amount, tier_max_amount,
				markup_bps, min_margin_bps, max_margin_bps, created_by, updated_by, created_at, updated_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		`, m.ID, m.TenantID, m.FromAsset, m.ToAsset, m.TierMinAmount.String(), tierMaxStr,
			m.MarkupBps, m.MinMarginBps, m.MaxMarginBps, m.CreatedBy, m.UpdatedBy, m.CreatedAt, m.UpdatedAt)

		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" { // unique violation
				return domain.ErrDuplicateMarginTier
			}
			return fmt.Errorf("insert tenant_fx_margin: %w", err)
		}

		newValBytes, _ := json.Marshal(m)
		_, err = tx.Exec(txCtx, `
			INSERT INTO tenant_fx_margin_audit (
				id, margin_id, tenant_id, action, new_values, actor_principal, actor_ip, created_at
			) VALUES ($1, $2, $3, 'create', $4, $5, $6, $7)
		`, uuid.New().String(), m.ID, m.TenantID, newValBytes, actorPrincipal, actorIP, now)

		if err != nil {
			return fmt.Errorf("insert tenant_fx_margin_audit: %w", err)
		}

		return nil
	})
}

func (r *tenantFXRepo) GetMargin(ctx context.Context, id, tenantID string) (*domain.TenantFXMargin, error) {
	row := r.db.QueryRow(ctx, `
		SELECT id, tenant_id, from_asset, to_asset, tier_min_amount, tier_max_amount,
		       markup_bps, min_margin_bps, max_margin_bps, created_by, updated_by,
		       created_at, updated_at
		FROM tenant_fx_margins
		WHERE id = $1 AND ($2 = '' OR tenant_id = $2::uuid)
	`, id, tenantID)

	m, err := scanMargin(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, domain.ErrMarginNotFound
		}
		return nil, fmt.Errorf("get tenant_fx_margin: %w", err)
	}
	return m, nil
}

func (r *tenantFXRepo) FindMargin(ctx context.Context, tenantID, fromAsset, toAsset string, amount decimal.Decimal) (*domain.TenantFXMargin, error) {
	from := strings.ToUpper(strings.TrimSpace(fromAsset))
	to := strings.ToUpper(strings.TrimSpace(toAsset))

	row := r.db.QueryRow(ctx, `
		SELECT id, tenant_id, from_asset, to_asset, tier_min_amount, tier_max_amount,
		       markup_bps, min_margin_bps, max_margin_bps, created_by, updated_by,
		       created_at, updated_at
		FROM tenant_fx_margins
		WHERE tenant_id = $1::uuid
		  AND from_asset = $2
		  AND to_asset = $3
		  AND tier_min_amount <= $4
		  AND (tier_max_amount IS NULL OR tier_max_amount >= $4)
		ORDER BY tier_min_amount DESC
		LIMIT 1
	`, tenantID, from, to, amount.String())

	m, err := scanMargin(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil // No custom margin configured; fallback to default
		}
		return nil, fmt.Errorf("find tenant_fx_margin: %w", err)
	}
	return m, nil
}

func (r *tenantFXRepo) ListMargins(ctx context.Context, tenantID, fromAsset, toAsset string, limit, offset int) ([]*domain.TenantFXMargin, int, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	whereParts := []string{"1=1"}
	args := []interface{}{}
	argIdx := 1

	if tenantID != "" {
		whereParts = append(whereParts, fmt.Sprintf("tenant_id = $%d::uuid", argIdx))
		args = append(args, tenantID)
		argIdx++
	}
	if fromAsset != "" {
		whereParts = append(whereParts, fmt.Sprintf("from_asset = $%d", argIdx))
		args = append(args, strings.ToUpper(strings.TrimSpace(fromAsset)))
		argIdx++
	}
	if toAsset != "" {
		whereParts = append(whereParts, fmt.Sprintf("to_asset = $%d", argIdx))
		args = append(args, strings.ToUpper(strings.TrimSpace(toAsset)))
		argIdx++
	}

	whereClause := strings.Join(whereParts, " AND ")

	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM tenant_fx_margins WHERE %s", whereClause)
	if err := r.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count tenant_fx_margins: %w", err)
	}

	query := fmt.Sprintf(`
		SELECT id, tenant_id, from_asset, to_asset, tier_min_amount, tier_max_amount,
		       markup_bps, min_margin_bps, max_margin_bps, created_by, updated_by,
		       created_at, updated_at
		FROM tenant_fx_margins
		WHERE %s
		ORDER BY tenant_id ASC, from_asset ASC, to_asset ASC, tier_min_amount ASC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("list tenant_fx_margins: %w", err)
	}
	defer rows.Close()

	margins := make([]*domain.TenantFXMargin, 0)
	for rows.Next() {
		m, err := scanMargin(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("scan tenant_fx_margin: %w", err)
		}
		margins = append(margins, m)
	}
	return margins, total, rows.Err()
}

func (r *tenantFXRepo) UpdateMargin(ctx context.Context, m *domain.TenantFXMargin, actorPrincipal, actorIP string) error {
	if err := validateCorridorAndCeilings(m); err != nil {
		return err
	}
	now := time.Now().UTC()
	m.UpdatedAt = now
	if actorPrincipal == "" {
		actorPrincipal = "admin"
	}
	m.UpdatedBy = actorPrincipal

	return RunInTx(ctx, r.db, func(txCtx context.Context) error {
		tx := TxFromContext(txCtx, r.db)

		// 1. Fetch existing row for audit log
		existingRow := tx.QueryRow(txCtx, `
			SELECT id, tenant_id, from_asset, to_asset, tier_min_amount, tier_max_amount,
			       markup_bps, min_margin_bps, max_margin_bps, created_by, updated_by,
			       created_at, updated_at
			FROM tenant_fx_margins
			WHERE id = $1 AND ($2 = '' OR tenant_id = $2::uuid)
			FOR UPDATE
		`, m.ID, m.TenantID)

		oldMargin, err := scanMargin(existingRow)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrMarginNotFound
			}
			return fmt.Errorf("fetch existing margin for update: %w", err)
		}

		var tierMaxStr *string
		if m.TierMaxAmount != nil {
			s := m.TierMaxAmount.String()
			tierMaxStr = &s
		}

		// 2. Perform update
		res, err := tx.Exec(txCtx, `
			UPDATE tenant_fx_margins
			SET from_asset = $1, to_asset = $2, tier_min_amount = $3, tier_max_amount = $4,
			    markup_bps = $5, min_margin_bps = $6, max_margin_bps = $7,
			    updated_by = $8, updated_at = $9
			WHERE id = $10 AND tenant_id = $11
		`, m.FromAsset, m.ToAsset, m.TierMinAmount.String(), tierMaxStr,
			m.MarkupBps, m.MinMarginBps, m.MaxMarginBps, m.UpdatedBy, m.UpdatedAt,
			m.ID, m.TenantID)

		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return domain.ErrDuplicateMarginTier
			}
			return fmt.Errorf("update tenant_fx_margin: %w", err)
		}

		if res.RowsAffected() == 0 {
			return domain.ErrMarginNotFound
		}

		oldValBytes, _ := json.Marshal(oldMargin)
		newValBytes, _ := json.Marshal(m)

		_, err = tx.Exec(txCtx, `
			INSERT INTO tenant_fx_margin_audit (
				id, margin_id, tenant_id, action, old_values, new_values, actor_principal, actor_ip, created_at
			) VALUES ($1, $2, $3, 'update', $4, $5, $6, $7, $8)
		`, uuid.New().String(), m.ID, m.TenantID, oldValBytes, newValBytes, actorPrincipal, actorIP, now)

		if err != nil {
			return fmt.Errorf("insert tenant_fx_margin_audit on update: %w", err)
		}

		return nil
	})
}

func (r *tenantFXRepo) DeleteMargin(ctx context.Context, id, tenantID, actorPrincipal, actorIP string) error {
	now := time.Now().UTC()
	if actorPrincipal == "" {
		actorPrincipal = "admin"
	}

	return RunInTx(ctx, r.db, func(txCtx context.Context) error {
		tx := TxFromContext(txCtx, r.db)

		// Fetch existing row for audit before deletion
		existingRow := tx.QueryRow(txCtx, `
			SELECT id, tenant_id, from_asset, to_asset, tier_min_amount, tier_max_amount,
			       markup_bps, min_margin_bps, max_margin_bps, created_by, updated_by,
			       created_at, updated_at
			FROM tenant_fx_margins
			WHERE id = $1 AND ($2 = '' OR tenant_id = $2::uuid)
			FOR UPDATE
		`, id, tenantID)

		oldMargin, err := scanMargin(existingRow)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return domain.ErrMarginNotFound
			}
			return fmt.Errorf("fetch existing margin for delete: %w", err)
		}

		res, err := tx.Exec(txCtx, `
			DELETE FROM tenant_fx_margins
			WHERE id = $1 AND ($2 = '' OR tenant_id = $2::uuid)
		`, id, tenantID)

		if err != nil {
			return fmt.Errorf("delete tenant_fx_margin: %w", err)
		}

		if res.RowsAffected() == 0 {
			return domain.ErrMarginNotFound
		}

		oldValBytes, _ := json.Marshal(oldMargin)
		_, err = tx.Exec(txCtx, `
			INSERT INTO tenant_fx_margin_audit (
				id, margin_id, tenant_id, action, old_values, actor_principal, actor_ip, created_at
			) VALUES ($1, $2, $3, 'delete', $4, $5, $6, $7)
		`, uuid.New().String(), id, oldMargin.TenantID, oldValBytes, actorPrincipal, actorIP, now)

		if err != nil {
			return fmt.Errorf("insert tenant_fx_margin_audit on delete: %w", err)
		}

		return nil
	})
}

func (r *tenantFXRepo) ListMarginAudits(ctx context.Context, tenantID, marginID string, limit, offset int) ([]*domain.TenantFXMarginAudit, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	if offset < 0 {
		offset = 0
	}

	whereParts := []string{"1=1"}
	args := []interface{}{}
	argIdx := 1

	if tenantID != "" {
		whereParts = append(whereParts, fmt.Sprintf("tenant_id = $%d::uuid", argIdx))
		args = append(args, tenantID)
		argIdx++
	}
	if marginID != "" {
		whereParts = append(whereParts, fmt.Sprintf("margin_id = $%d::uuid", argIdx))
		args = append(args, marginID)
		argIdx++
	}

	whereClause := strings.Join(whereParts, " AND ")
	query := fmt.Sprintf(`
		SELECT id, margin_id, tenant_id, action, old_values, new_values, actor_principal, actor_ip, created_at
		FROM tenant_fx_margin_audit
		WHERE %s
		ORDER BY created_at DESC
		LIMIT $%d OFFSET $%d
	`, whereClause, argIdx, argIdx+1)

	args = append(args, limit, offset)
	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query tenant_fx_margin_audit: %w", err)
	}
	defer rows.Close()

	audits := make([]*domain.TenantFXMarginAudit, 0)
	for rows.Next() {
		var a domain.TenantFXMarginAudit
		var oldBytes, newBytes []byte
		var actorIP *string
		if err := rows.Scan(
			&a.ID, &a.MarginID, &a.TenantID, &a.Action, &oldBytes, &newBytes,
			&a.ActorPrincipal, &actorIP, &a.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan tenant_fx_margin_audit: %w", err)
		}
		if actorIP != nil {
			a.ActorIP = *actorIP
		}
		if len(oldBytes) > 0 {
			_ = json.Unmarshal(oldBytes, &a.OldValues)
		}
		if len(newBytes) > 0 {
			_ = json.Unmarshal(newBytes, &a.NewValues)
		}
		audits = append(audits, &a)
	}

	return audits, rows.Err()
}

func scanMargin(row pgx.Row) (*domain.TenantFXMargin, error) {
	var m domain.TenantFXMargin
	var tierMin decimal.Decimal
	var tierMax *decimal.Decimal

	err := row.Scan(
		&m.ID, &m.TenantID, &m.FromAsset, &m.ToAsset, &tierMin, &tierMax,
		&m.MarkupBps, &m.MinMarginBps, &m.MaxMarginBps, &m.CreatedBy, &m.UpdatedBy,
		&m.CreatedAt, &m.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	m.TierMinAmount = tierMin
	m.TierMaxAmount = tierMax
	return &m, nil
}
