package fx

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/postgres"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
)

// mockTenantFXRepo provides an in-memory implementation of TenantFXRepo for unit testing.
type mockTenantFXRepo struct {
	margins []*domain.TenantFXMargin
	audits  []*domain.TenantFXMarginAudit
}

func newMockTenantFXRepo() *mockTenantFXRepo {
	return &mockTenantFXRepo{
		margins: make([]*domain.TenantFXMargin, 0),
		audits:  make([]*domain.TenantFXMarginAudit, 0),
	}
}

func (m *mockTenantFXRepo) CreateMargin(_ context.Context, margin *domain.TenantFXMargin, actorPrincipal, actorIP string) error {
	if margin.MarkupBps < margin.MinMarginBps || margin.MarkupBps > margin.MaxMarginBps ||
		margin.MarkupBps < postgres.MinAllowableMarginBps || margin.MarkupBps > postgres.MaxAllowableMarginBps {
		return domain.ErrMarginCeilingExceeded
	}
	if margin.FromAsset == margin.ToAsset {
		return domain.ErrCorridorConflict
	}
	for _, existing := range m.margins {
		if existing.TenantID == margin.TenantID &&
			existing.FromAsset == margin.FromAsset &&
			existing.ToAsset == margin.ToAsset &&
			existing.TierMinAmount.Equal(margin.TierMinAmount) {
			return domain.ErrDuplicateMarginTier
		}
	}
	if margin.ID == "" {
		margin.ID = "margin-" + decimal.NewFromInt(int64(len(m.margins)+1)).String()
	}
	now := time.Now().UTC()
	margin.CreatedAt = now
	margin.UpdatedAt = now
	margin.CreatedBy = actorPrincipal
	margin.UpdatedBy = actorPrincipal
	m.margins = append(m.margins, margin)

	m.audits = append(m.audits, &domain.TenantFXMarginAudit{
		ID:             "audit-" + decimal.NewFromInt(int64(len(m.audits)+1)).String(),
		MarginID:       margin.ID,
		TenantID:       margin.TenantID,
		Action:         "create",
		ActorPrincipal: actorPrincipal,
		ActorIP:        actorIP,
		CreatedAt:      now,
	})
	return nil
}

func (m *mockTenantFXRepo) GetMargin(_ context.Context, id, tenantID string) (*domain.TenantFXMargin, error) {
	for _, margin := range m.margins {
		if margin.ID == id && (tenantID == "" || margin.TenantID == tenantID) {
			return margin, nil
		}
	}
	return nil, domain.ErrMarginNotFound
}

func (m *mockTenantFXRepo) FindMargin(_ context.Context, tenantID, fromAsset, toAsset string, amount decimal.Decimal) (*domain.TenantFXMargin, error) {
	var bestMatch *domain.TenantFXMargin
	for _, margin := range m.margins {
		if margin.TenantID == tenantID && margin.FromAsset == fromAsset && margin.ToAsset == toAsset {
			if margin.TierMinAmount.LessThanOrEqual(amount) {
				if margin.TierMaxAmount == nil || margin.TierMaxAmount.GreaterThanOrEqual(amount) {
					if bestMatch == nil || margin.TierMinAmount.GreaterThan(bestMatch.TierMinAmount) {
						bestMatch = margin
					}
				}
			}
		}
	}
	return bestMatch, nil
}

func (m *mockTenantFXRepo) ListMargins(_ context.Context, tenantID, fromAsset, toAsset string, limit, offset int) ([]*domain.TenantFXMargin, int, error) {
	filtered := make([]*domain.TenantFXMargin, 0)
	for _, margin := range m.margins {
		if tenantID != "" && margin.TenantID != tenantID {
			continue
		}
		if fromAsset != "" && margin.FromAsset != fromAsset {
			continue
		}
		if toAsset != "" && margin.ToAsset != toAsset {
			continue
		}
		filtered = append(filtered, margin)
	}
	total := len(filtered)
	if offset > total {
		return []*domain.TenantFXMargin{}, total, nil
	}
	end := offset + limit
	if end > total || limit <= 0 {
		end = total
	}
	return filtered[offset:end], total, nil
}

func (m *mockTenantFXRepo) UpdateMargin(_ context.Context, margin *domain.TenantFXMargin, actorPrincipal, actorIP string) error {
	if margin.MarkupBps < margin.MinMarginBps || margin.MarkupBps > margin.MaxMarginBps ||
		margin.MarkupBps < postgres.MinAllowableMarginBps || margin.MarkupBps > postgres.MaxAllowableMarginBps {
		return domain.ErrMarginCeilingExceeded
	}
	for i, existing := range m.margins {
		if existing.ID == margin.ID {
			now := time.Now().UTC()
			margin.UpdatedAt = now
			margin.UpdatedBy = actorPrincipal
			m.margins[i] = margin

			m.audits = append(m.audits, &domain.TenantFXMarginAudit{
				ID:             "audit-" + decimal.NewFromInt(int64(len(m.audits)+1)).String(),
				MarginID:       margin.ID,
				TenantID:       margin.TenantID,
				Action:         "update",
				ActorPrincipal: actorPrincipal,
				ActorIP:        actorIP,
				CreatedAt:      now,
			})
			return nil
		}
	}
	return domain.ErrMarginNotFound
}

func (m *mockTenantFXRepo) DeleteMargin(_ context.Context, id, tenantID, actorPrincipal, actorIP string) error {
	for i, existing := range m.margins {
		if existing.ID == id && (tenantID == "" || existing.TenantID == tenantID) {
			m.margins = append(m.margins[:i], m.margins[i+1:]...)
			m.audits = append(m.audits, &domain.TenantFXMarginAudit{
				ID:             "audit-" + decimal.NewFromInt(int64(len(m.audits)+1)).String(),
				MarginID:       id,
				TenantID:       existing.TenantID,
				Action:         "delete",
				ActorPrincipal: actorPrincipal,
				ActorIP:        actorIP,
				CreatedAt:      time.Now().UTC(),
			})
			return nil
		}
	}
	return domain.ErrMarginNotFound
}

func (m *mockTenantFXRepo) ListMarginAudits(_ context.Context, tenantID, marginID string, _, _ int) ([]*domain.TenantFXMarginAudit, error) {
	res := make([]*domain.TenantFXMarginAudit, 0)
	for _, a := range m.audits {
		if tenantID != "" && a.TenantID != tenantID {
			continue
		}
		if marginID != "" && a.MarginID != marginID {
			continue
		}
		res = append(res, a)
	}
	return res, nil
}

func TestMarginEngine_CalculateCustomerQuote(t *testing.T) {
	repo := newMockTenantFXRepo()
	engine := NewMarginEngine(repo)

	ctx := context.Background()
	tenantID := "tenant-alpha"
	fromAsset := "USDC"
	toAsset := "EURC"

	// Configure Tier 1: 0 - 1000 => 50 bps (+0.5%)
	tier1Max := decimal.NewFromInt(1000)
	err := repo.CreateMargin(ctx, &domain.TenantFXMargin{
		TenantID:      tenantID,
		FromAsset:     fromAsset,
		ToAsset:       toAsset,
		TierMinAmount: decimal.Zero,
		TierMaxAmount: &tier1Max,
		MarkupBps:     50,
		MinMarginBps:  0,
		MaxMarginBps:  2000,
	}, "admin@nexora.io", "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to create tier 1: %v", err)
	}

	// Configure Tier 2: 1000+ => 20 bps (+0.2%)
	err = repo.CreateMargin(ctx, &domain.TenantFXMargin{
		TenantID:      tenantID,
		FromAsset:     fromAsset,
		ToAsset:       toAsset,
		TierMinAmount: decimal.NewFromInt(1000),
		TierMaxAmount: nil,
		MarkupBps:     20,
		MinMarginBps:  0,
		MaxMarginBps:  2000,
	}, "admin@nexora.io", "127.0.0.1")
	if err != nil {
		t.Fatalf("failed to create tier 2: %v", err)
	}

	baseRate := decimal.NewFromFloat(0.9200)

	// Test 1: $500 transfer (Tier 1, 50 bps)
	// Final quote = 0.9200 * (1 + 50 / 10000) = 0.9200 * 1.005 = 0.9246
	quote1, applied1, err := engine.CalculateCustomerQuote(ctx, tenantID, fromAsset, toAsset, decimal.NewFromInt(500), baseRate)
	if err != nil {
		t.Fatalf("unexpected calculation error: %v", err)
	}
	if applied1 == nil || applied1.MarkupBps != 50 {
		t.Fatalf("expected 50 bps markup applied, got: %v", applied1)
	}
	expected1 := baseRate.Mul(decimal.NewFromFloat(1.005))
	if !quote1.Equal(expected1) {
		t.Errorf("expected quote %s, got %s", expected1.String(), quote1.String())
	}

	// Test 2: $5,000 transfer (Tier 2, 20 bps)
	// Final quote = 0.9200 * (1 + 20 / 10000) = 0.9200 * 1.002 = 0.92184
	quote2, applied2, err := engine.CalculateCustomerQuote(ctx, tenantID, fromAsset, toAsset, decimal.NewFromInt(5000), baseRate)
	if err != nil {
		t.Fatalf("unexpected calculation error: %v", err)
	}
	if applied2 == nil || applied2.MarkupBps != 20 {
		t.Fatalf("expected 20 bps markup applied, got: %v", applied2)
	}
	expected2 := baseRate.Mul(decimal.NewFromFloat(1.002))
	if !quote2.Equal(expected2) {
		t.Errorf("expected quote %s, got %s", expected2.String(), quote2.String())
	}

	// Test 3: Unconfigured tenant gets base rate unchanged
	quote3, applied3, err := engine.CalculateCustomerQuote(ctx, "unknown-tenant", fromAsset, toAsset, decimal.NewFromInt(500), baseRate)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if applied3 != nil {
		t.Errorf("expected no margin applied for unknown tenant, got: %v", applied3)
	}
	if !quote3.Equal(baseRate) {
		t.Errorf("expected base rate %s, got %s", baseRate.String(), quote3.String())
	}
}

func TestMarginEngine_CeilingEnforcement(t *testing.T) {
	repo := newMockTenantFXRepo()
	engine := NewMarginEngine(repo)

	ctx := context.Background()

	// 1. Attempt markup above max ceiling (5000 bps)
	err := repo.CreateMargin(ctx, &domain.TenantFXMargin{
		TenantID:      "tenant-beta",
		FromAsset:     "USDC",
		ToAsset:       "EURC",
		TierMinAmount: decimal.Zero,
		MarkupBps:     5001, // exceeds ceiling
		MinMarginBps:  0,
		MaxMarginBps:  5000,
	}, "admin", "127.0.0.1")
	if err != domain.ErrMarginCeilingExceeded {
		t.Errorf("expected ErrMarginCeilingExceeded for markup > 5000, got: %v", err)
	}

	// 2. Attempt markup below min ceiling (-500 bps)
	err = repo.CreateMargin(ctx, &domain.TenantFXMargin{
		TenantID:      "tenant-beta",
		FromAsset:     "USDC",
		ToAsset:       "EURC",
		TierMinAmount: decimal.Zero,
		MarkupBps:     -501, // exceeds discount ceiling
		MinMarginBps:  -500,
		MaxMarginBps:  5000,
	}, "admin", "127.0.0.1")
	if err != domain.ErrMarginCeilingExceeded {
		t.Errorf("expected ErrMarginCeilingExceeded for markup < -500, got: %v", err)
	}

	// 3. Attempt identical corridor assets
	err = repo.CreateMargin(ctx, &domain.TenantFXMargin{
		TenantID:      "tenant-beta",
		FromAsset:     "USDC",
		ToAsset:       "USDC",
		TierMinAmount: decimal.Zero,
		MarkupBps:     50,
		MinMarginBps:  0,
		MaxMarginBps:  2000,
	}, "admin", "127.0.0.1")
	if err != domain.ErrCorridorConflict {
		t.Errorf("expected ErrCorridorConflict for identical assets, got: %v", err)
	}

	_ = engine
}

func TestMarginHandler_CRUDAndAudit(t *testing.T) {
	repo := newMockTenantFXRepo()
	engine := NewMarginEngine(repo)
	handler := NewMarginHandler(engine)

	r := chi.NewRouter()
	r.Route("/admin/fx/margins", handler.AdminRoutes())

	// 1. POST /admin/fx/margins
	body := map[string]interface{}{
		"tenant_id":       "4f9b884d-e962-4aa7-9273-0ff7855b4124",
		"from_asset":      "USDC",
		"to_asset":        "EURC",
		"tier_min_amount": "0",
		"markup_bps":      75,
	}
	bodyBytes, _ := json.Marshal(body)

	req := httptest.NewRequest(http.MethodPost, "/admin/fx/margins", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Actor-ID", "admin@nexora.io")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d: %s", rec.Code, rec.Body.String())
	}

	var created domain.TenantFXMargin
	if err := json.NewDecoder(rec.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if created.MarkupBps != 75 {
		t.Errorf("expected markup 75, got %d", created.MarkupBps)
	}

	// 2. GET /admin/fx/margins/{id}
	reqGet := httptest.NewRequest(http.MethodGet, "/admin/fx/margins/"+created.ID, nil)
	recGet := httptest.NewRecorder()
	r.ServeHTTP(recGet, reqGet)
	if recGet.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", recGet.Code, recGet.Body.String())
	}

	// 3. PUT /admin/fx/margins/{id}
	updateBody := map[string]interface{}{
		"from_asset":      "USDC",
		"to_asset":        "EURC",
		"tier_min_amount": "0",
		"markup_bps":      85,
	}
	updateBytes, _ := json.Marshal(updateBody)
	reqPut := httptest.NewRequest(http.MethodPut, "/admin/fx/margins/"+created.ID, bytes.NewReader(updateBytes))
	reqPut.Header.Set("Content-Type", "application/json")
	reqPut.Header.Set("X-Actor-ID", "admin-updater@nexora.io")
	recPut := httptest.NewRecorder()
	r.ServeHTTP(recPut, reqPut)
	if recPut.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on update, got %d: %s", recPut.Code, recPut.Body.String())
	}

	// 4. GET /admin/fx/margins/{id}/audit
	reqAudit := httptest.NewRequest(http.MethodGet, "/admin/fx/margins/"+created.ID+"/audit", nil)
	recAudit := httptest.NewRecorder()
	r.ServeHTTP(recAudit, reqAudit)
	if recAudit.Code != http.StatusOK {
		t.Fatalf("expected 200 OK on audit list, got %d: %s", recAudit.Code, recAudit.Body.String())
	}

	var audits []*domain.TenantFXMarginAudit
	if err := json.NewDecoder(recAudit.Body).Decode(&audits); err != nil {
		t.Fatalf("failed to decode audits: %v", err)
	}
	if len(audits) < 2 {
		t.Errorf("expected at least 2 audit entries (create, update), got %d", len(audits))
	}
	if audits[0].ActorPrincipal != "admin@nexora.io" {
		t.Errorf("expected first audit actor 'admin@nexora.io', got %s", audits[0].ActorPrincipal)
	}

	// 5. DELETE /admin/fx/margins/{id}
	reqDel := httptest.NewRequest(http.MethodDelete, "/admin/fx/margins/"+created.ID, nil)
	recDel := httptest.NewRecorder()
	r.ServeHTTP(recDel, reqDel)
	if recDel.Code != http.StatusNoContent {
		t.Fatalf("expected 204 No Content, got %d", recDel.Code)
	}
}
