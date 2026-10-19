package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/jackc/pgx/v5"
)

// CorridorSchemaRepo stores corridor field schema versions and satisfies
// anchor.CorridorSchemaStore.
type CorridorSchemaRepo struct {
	db DB
}

func NewCorridorSchemaRepo(db DB) *CorridorSchemaRepo {
	return &CorridorSchemaRepo{db: db}
}

const corridorSchemaColumns = `id, tenant_id, corridor, customer_type, version, schema, active, created_by, created_at`

func scanCorridorSchema(row pgx.Row) (*anchor.CorridorSchema, error) {
	s := &anchor.CorridorSchema{}
	var schema []byte
	if err := row.Scan(&s.ID, &s.TenantID, &s.Corridor, &s.CustomerType, &s.Version, &schema, &s.Active, &s.CreatedBy, &s.CreatedAt); err != nil {
		return nil, err
	}
	s.Schema = schema
	return s, nil
}

// CreateVersion serialises writers of one key with a transaction advisory
// lock, assigns max(version)+1, deactivates the previous version and inserts
// the new one as active.
func (r *CorridorSchemaRepo) CreateVersion(ctx context.Context, s *anchor.CorridorSchema) error {
	return RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)
		if _, err := db.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`,
			"corridor_field_schemas:"+s.TenantID+":"+s.Corridor+":"+s.CustomerType); err != nil {
			return fmt.Errorf("lock corridor schema key: %w", err)
		}
		if _, err := db.Exec(ctx, `
			UPDATE corridor_field_schemas SET active = FALSE
			WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3 AND active`,
			s.TenantID, s.Corridor, s.CustomerType); err != nil {
			return fmt.Errorf("deactivate corridor schema: %w", err)
		}
		row := db.QueryRow(ctx, `
			INSERT INTO corridor_field_schemas (tenant_id, corridor, customer_type, version, schema, active, created_by, created_at)
			SELECT $1, $2, $3, COALESCE(MAX(version), 0) + 1, $4, TRUE, $5, $6
			FROM corridor_field_schemas
			WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3
			RETURNING `+corridorSchemaColumns,
			s.TenantID, s.Corridor, s.CustomerType, []byte(s.Schema), s.CreatedBy, s.CreatedAt)
		stored, err := scanCorridorSchema(row)
		if err != nil {
			return fmt.Errorf("insert corridor schema: %w", err)
		}
		*s = *stored
		return nil
	})
}

func (r *CorridorSchemaRepo) Get(ctx context.Context, tenantID, corridor, customerType string, version int) (*anchor.CorridorSchema, error) {
	query := `SELECT ` + corridorSchemaColumns + ` FROM corridor_field_schemas
		WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3 AND `
	args := []any{tenantID, corridor, customerType}
	if version == 0 {
		query += `active`
	} else {
		query += `version = $4`
		args = append(args, version)
	}
	s, err := scanCorridorSchema(r.db.QueryRow(ctx, query, args...))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, anchor.ErrCorridorSchemaNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get corridor schema: %w", err)
	}
	return s, nil
}

func (r *CorridorSchemaRepo) History(ctx context.Context, tenantID, corridor, customerType string) ([]*anchor.CorridorSchema, error) {
	rows, err := r.db.Query(ctx, `SELECT `+corridorSchemaColumns+` FROM corridor_field_schemas
		WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3
		ORDER BY version DESC`, tenantID, corridor, customerType)
	if err != nil {
		return nil, fmt.Errorf("list corridor schemas: %w", err)
	}
	defer rows.Close()
	var out []*anchor.CorridorSchema
	for rows.Next() {
		s, err := scanCorridorSchema(rows)
		if err != nil {
			return nil, fmt.Errorf("scan corridor schema: %w", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, anchor.ErrCorridorSchemaNotFound
	}
	return out, nil
}

// Activate switches the active version in one transaction; the partial
// unique index guarantees a single active row.
func (r *CorridorSchemaRepo) Activate(ctx context.Context, tenantID, corridor, customerType string, version int) error {
	return RunInTx(ctx, r.db, func(ctx context.Context) error {
		db := TxFromContext(ctx, r.db)
		var exists bool
		if err := db.QueryRow(ctx, `
			SELECT EXISTS (SELECT 1 FROM corridor_field_schemas
			WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3 AND version = $4)`,
			tenantID, corridor, customerType, version).Scan(&exists); err != nil {
			return fmt.Errorf("find corridor schema version: %w", err)
		}
		if !exists {
			return anchor.ErrCorridorSchemaNotFound
		}
		if _, err := db.Exec(ctx, `
			UPDATE corridor_field_schemas SET active = FALSE
			WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3 AND active`,
			tenantID, corridor, customerType); err != nil {
			return fmt.Errorf("deactivate corridor schema: %w", err)
		}
		if _, err := db.Exec(ctx, `
			UPDATE corridor_field_schemas SET active = TRUE
			WHERE tenant_id = $1 AND corridor = $2 AND customer_type = $3 AND version = $4`,
			tenantID, corridor, customerType, version); err != nil {
			return fmt.Errorf("activate corridor schema: %w", err)
		}
		return nil
	})
}
