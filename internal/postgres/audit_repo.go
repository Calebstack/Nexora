package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
)

type AuditRepo struct {
	db DB
}

func NewAuditRepo(db DB) *AuditRepo {
	return &AuditRepo{db: db}
}

// Create inserts a new immutable audit record into tenant_audit_events.
// Note: This store is strictly append-only.
func (r *AuditRepo) Create(ctx context.Context, event *domain.AuditEvent) error {
	if event.ID == "" {
		event.ID = uuid.New().String()
	}
	if !event.Mode.Valid() {
		event.Mode = domain.ModeLive
	}
	metadataJSON, err := json.Marshal(domain.RedactMetadata(event.Metadata))
	if err != nil {
		metadataJSON = []byte("{}")
	}

	db := TxFromContext(ctx, r.db)
	_, err = db.Exec(ctx,
		`INSERT INTO tenant_audit_events (
			id, tenant_id, mode, actor_type, actor_id, action,
			resource_type, resource_id, metadata, ip_address, user_agent, created_at
		 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)`,
		event.ID, event.TenantID, string(event.Mode), event.ActorType, event.ActorID, event.Action,
		event.ResourceType, event.ResourceID, metadataJSON, event.IPAddress, event.UserAgent, event.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert tenant audit event: %w", err)
	}
	return nil
}

func (r *AuditRepo) List(ctx context.Context, tenantID string, mode domain.Mode, filter domain.AuditFilter) ([]*domain.AuditEvent, int, error) {
	where := ` WHERE tenant_id = $1 AND mode = $2`
	args := []interface{}{tenantID, string(mode)}
	idx := 3

	if filter.Action != "" {
		where += fmt.Sprintf(" AND action = $%d", idx)
		args = append(args, filter.Action)
		idx++
	}
	if filter.ActorID != "" {
		where += fmt.Sprintf(" AND actor_id = $%d", idx)
		args = append(args, filter.ActorID)
		idx++
	}
	if filter.ResourceType != "" {
		where += fmt.Sprintf(" AND resource_type = $%d", idx)
		args = append(args, filter.ResourceType)
		idx++
	}
	if filter.ResourceID != "" {
		where += fmt.Sprintf(" AND resource_id = $%d", idx)
		args = append(args, filter.ResourceID)
		idx++
	}
	if filter.From != nil {
		where += fmt.Sprintf(" AND created_at >= $%d", idx)
		args = append(args, *filter.From)
		idx++
	}
	if filter.To != nil {
		where += fmt.Sprintf(" AND created_at <= $%d", idx)
		args = append(args, *filter.To)
		idx++
	}

	countQuery := `SELECT COUNT(*) FROM tenant_audit_events` + where
	var total int
	if err := r.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count audit events: %w", err)
	}

	limit := filter.Limit
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	offset := filter.Offset
	if offset < 0 {
		offset = 0
	}

	query := `SELECT id, tenant_id, mode, actor_type, actor_id, action,
	                 resource_type, resource_id, metadata, ip_address, user_agent, created_at
	          FROM tenant_audit_events` + where +
		fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", idx, idx+1)
	args = append(args, limit, offset)

	rows, err := r.db.Query(ctx, query, args...)
	if err != nil {
		return nil, 0, fmt.Errorf("query audit events: %w", err)
	}
	defer rows.Close()

	var events []*domain.AuditEvent
	for rows.Next() {
		e := &domain.AuditEvent{}
		var metaBytes []byte
		var modeStr string
		if err := rows.Scan(
			&e.ID, &e.TenantID, &modeStr, &e.ActorType, &e.ActorID, &e.Action,
			&e.ResourceType, &e.ResourceID, &metaBytes, &e.IPAddress, &e.UserAgent, &e.CreatedAt,
		); err != nil {
			return nil, 0, err
		}
		e.Mode = domain.Mode(modeStr)
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &e.Metadata)
		}
		events = append(events, e)
	}
	return events, total, rows.Err()
}

// ListBetween returns every audit event created in [from, to) across all
// tenants and modes, oldest first. The attestation daemon uses it to build the
// daily Merkle root, so the order is fixed by created_at and id.
func (r *AuditRepo) ListBetween(ctx context.Context, from, to time.Time) ([]*domain.AuditEvent, error) {
	rows, err := r.db.Query(ctx,
		`SELECT id, tenant_id, mode, actor_type, actor_id, action,
		        resource_type, resource_id, metadata, ip_address, user_agent, created_at
		 FROM tenant_audit_events
		 WHERE created_at >= $1 AND created_at < $2
		 ORDER BY created_at ASC, id ASC`,
		from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("query audit events between %s and %s: %w", from, to, err)
	}
	defer rows.Close()

	var events []*domain.AuditEvent
	for rows.Next() {
		e := &domain.AuditEvent{}
		var metaBytes []byte
		var modeStr string
		if err := rows.Scan(
			&e.ID, &e.TenantID, &modeStr, &e.ActorType, &e.ActorID, &e.Action,
			&e.ResourceType, &e.ResourceID, &metaBytes, &e.IPAddress, &e.UserAgent, &e.CreatedAt,
		); err != nil {
			return nil, err
		}
		e.Mode = domain.Mode(modeStr)
		if len(metaBytes) > 0 {
			_ = json.Unmarshal(metaBytes, &e.Metadata)
		}
		events = append(events, e)
	}
	return events, rows.Err()
}
