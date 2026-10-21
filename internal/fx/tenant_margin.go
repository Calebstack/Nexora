package fx

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/audit"
	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/postgres"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
)

// MarginEngine calculates customer quotes with custom tenant markups and enforces margin ceilings.
type MarginEngine interface {
	CalculateCustomerQuote(ctx context.Context, tenantID, fromAsset, toAsset string, amount decimal.Decimal, baseRate decimal.Decimal) (decimal.Decimal, *domain.TenantFXMargin, error)
	Repo() postgres.TenantFXRepo
}

type marginEngine struct {
	repo postgres.TenantFXRepo
}

// NewMarginEngine constructs a margin engine with persistence and tier resolution.
func NewMarginEngine(repo postgres.TenantFXRepo) MarginEngine {
	return &marginEngine{repo: repo}
}

func (e *marginEngine) Repo() postgres.TenantFXRepo {
	return e.repo
}

// CalculateCustomerQuote calculates the final customer quote:
// CustomerRate = BaseRate * (1 + TenantMarkupBps / 10000)
// It enforces that TenantMarkupBps does not violate allowable margin ceilings.
func (e *marginEngine) CalculateCustomerQuote(
	ctx context.Context,
	tenantID, fromAsset, toAsset string,
	amount decimal.Decimal,
	baseRate decimal.Decimal,
) (decimal.Decimal, *domain.TenantFXMargin, error) {
	if baseRate.IsZero() || baseRate.IsNegative() {
		return decimal.Zero, nil, domain.ErrInvalidQuoteAmount
	}

	if tenantID == "" || e.repo == nil {
		return baseRate, nil, nil
	}

	margin, err := e.repo.FindMargin(ctx, tenantID, fromAsset, toAsset, amount)
	if err != nil {
		return decimal.Zero, nil, err
	}
	if margin == nil {
		// No custom margin configured for this tenant/corridor/tier; base rate applies
		return baseRate, nil, nil
	}

	// Enforce margin ceilings
	if margin.MarkupBps < margin.MinMarginBps || margin.MarkupBps > margin.MaxMarginBps ||
		margin.MarkupBps < postgres.MinAllowableMarginBps || margin.MarkupBps > postgres.MaxAllowableMarginBps {
		return decimal.Zero, nil, domain.ErrMarginCeilingExceeded
	}

	// Formula: BaseRate * (1 + TenantMarkupBps / 10000)
	// Precision preserved by computing: BaseRate * (10000 + MarkupBps) / 10000
	factor := decimal.NewFromInt(10000 + int64(margin.MarkupBps))
	divisor := decimal.NewFromInt(10000)
	finalRate := baseRate.Mul(factor).Div(divisor)

	return finalRate, margin, nil
}

// MarginHandler exposes administrative CRUD endpoints for tenant FX margins.
type MarginHandler struct {
	engine   MarginEngine
	auditSvc audit.Service
}

func NewMarginHandler(engine MarginEngine) *MarginHandler {
	return &MarginHandler{engine: engine}
}

func (h *MarginHandler) WithAuditLogger(svc audit.Service) *MarginHandler {
	h.auditSvc = svc
	return h
}

// AdminRoutes returns the Chi router mounting administrative CRUD endpoints.
func (h *MarginHandler) AdminRoutes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Post("/", h.createMargin)
		r.Get("/", h.listMargins)
		r.Get("/audit", h.listAllAudits)
		r.Route("/{id}", func(r chi.Router) {
			r.Get("/", h.getMargin)
			r.Put("/", h.updateMargin)
			r.Delete("/", h.deleteMargin)
			r.Get("/audit", h.listMarginAudits)
		})
	}
}

type createMarginRequest struct {
	TenantID      string  `json:"tenant_id" validate:"required,uuid"`
	FromAsset     string  `json:"from_asset" validate:"required"`
	ToAsset       string  `json:"to_asset" validate:"required"`
	TierMinAmount string  `json:"tier_min_amount"`
	TierMaxAmount *string `json:"tier_max_amount,omitempty"`
	MarkupBps     int     `json:"markup_bps"`
	MinMarginBps  *int    `json:"min_margin_bps,omitempty"`
	MaxMarginBps  *int    `json:"max_margin_bps,omitempty"`
}

type updateMarginRequest struct {
	FromAsset     string  `json:"from_asset" validate:"required"`
	ToAsset       string  `json:"to_asset" validate:"required"`
	TierMinAmount string  `json:"tier_min_amount"`
	TierMaxAmount *string `json:"tier_max_amount,omitempty"`
	MarkupBps     int     `json:"markup_bps"`
	MinMarginBps  *int    `json:"min_margin_bps,omitempty"`
	MaxMarginBps  *int    `json:"max_margin_bps,omitempty"`
}

func getActorPrincipal(r *http.Request) string {
	ctx := r.Context()
	if uID := tenant.UserIDFromContext(ctx); uID != "" {
		return "user:" + uID
	}
	if kID := tenant.APIKeyIDFromContext(ctx); kID != "" {
		return "apikey:" + kID
	}
	if tID := tenant.IDFromContext(ctx); tID != "" {
		return "tenant:" + tID
	}
	if actor := r.Header.Get("X-Actor-ID"); actor != "" {
		return actor
	}
	return "admin"
}

func getClientIP(r *http.Request) string {
	if fwd := r.Header.Get("X-Forwarded-For"); fwd != "" {
		parts := strings.Split(fwd, ",")
		return strings.TrimSpace(parts[0])
	}
	return r.RemoteAddr
}

func (h *MarginHandler) createMargin(w http.ResponseWriter, r *http.Request) {
	var req createMarginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	if err := api.Validate(req); err != nil {
		api.BadRequest(w, err.Error())
		return
	}

	tierMin := decimal.Zero
	if req.TierMinAmount != "" {
		val, err := decimal.NewFromString(req.TierMinAmount)
		if err != nil || val.IsNegative() {
			api.BadRequest(w, "invalid tier_min_amount")
			return
		}
		tierMin = val
	}

	var tierMax *decimal.Decimal
	if req.TierMaxAmount != nil && *req.TierMaxAmount != "" {
		val, err := decimal.NewFromString(*req.TierMaxAmount)
		if err != nil || val.LessThanOrEqual(tierMin) {
			api.BadRequest(w, "tier_max_amount must be greater than tier_min_amount")
			return
		}
		tierMax = &val
	}

	minMargin := 0
	if req.MinMarginBps != nil {
		minMargin = *req.MinMarginBps
	}
	maxMargin := postgres.MaxAllowableMarginBps
	if req.MaxMarginBps != nil {
		maxMargin = *req.MaxMarginBps
	}

	margin := &domain.TenantFXMargin{
		TenantID:      req.TenantID,
		FromAsset:     strings.ToUpper(strings.TrimSpace(req.FromAsset)),
		ToAsset:       strings.ToUpper(strings.TrimSpace(req.ToAsset)),
		TierMinAmount: tierMin,
		TierMaxAmount: tierMax,
		MarkupBps:     req.MarkupBps,
		MinMarginBps:  minMargin,
		MaxMarginBps:  maxMargin,
	}

	actor := getActorPrincipal(r)
	ip := getClientIP(r)

	if err := h.engine.Repo().CreateMargin(r.Context(), margin, actor, ip); err != nil {
		api.HandleDomainError(w, err)
		return
	}

	if h.auditSvc != nil {
		h.auditSvc.Log(r, "fx_margin.create", "tenant_fx_margin", margin.ID, map[string]interface{}{
			"tenant_id":  margin.TenantID,
			"corridor":   margin.FromAsset + "-" + margin.ToAsset,
			"markup_bps": margin.MarkupBps,
		})
	}

	api.JSON(w, http.StatusCreated, margin)
}

func (h *MarginHandler) getMargin(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	tenantID := r.URL.Query().Get("tenant_id")

	margin, err := h.engine.Repo().GetMargin(r.Context(), id, tenantID)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, margin)
}

func (h *MarginHandler) listMargins(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	tenantID := q.Get("tenant_id")
	fromAsset := q.Get("from_asset")
	toAsset := q.Get("to_asset")
	limit, _ := strconv.Atoi(q.Get("limit"))
	offset, _ := strconv.Atoi(q.Get("offset"))

	margins, total, err := h.engine.Repo().ListMargins(r.Context(), tenantID, fromAsset, toAsset, limit, offset)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, map[string]interface{}{
		"data":   margins,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	})
}

func (h *MarginHandler) updateMargin(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	var req updateMarginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	if err := api.Validate(req); err != nil {
		api.BadRequest(w, err.Error())
		return
	}

	tenantID := r.URL.Query().Get("tenant_id")
	existing, err := h.engine.Repo().GetMargin(r.Context(), id, tenantID)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	tierMin := decimal.Zero
	if req.TierMinAmount != "" {
		val, err := decimal.NewFromString(req.TierMinAmount)
		if err != nil || val.IsNegative() {
			api.BadRequest(w, "invalid tier_min_amount")
			return
		}
		tierMin = val
	}

	var tierMax *decimal.Decimal
	if req.TierMaxAmount != nil && *req.TierMaxAmount != "" {
		val, err := decimal.NewFromString(*req.TierMaxAmount)
		if err != nil || val.LessThanOrEqual(tierMin) {
			api.BadRequest(w, "tier_max_amount must be greater than tier_min_amount")
			return
		}
		tierMax = &val
	}

	minMargin := existing.MinMarginBps
	if req.MinMarginBps != nil {
		minMargin = *req.MinMarginBps
	}
	maxMargin := existing.MaxMarginBps
	if req.MaxMarginBps != nil {
		maxMargin = *req.MaxMarginBps
	}

	existing.FromAsset = strings.ToUpper(strings.TrimSpace(req.FromAsset))
	existing.ToAsset = strings.ToUpper(strings.TrimSpace(req.ToAsset))
	existing.TierMinAmount = tierMin
	existing.TierMaxAmount = tierMax
	existing.MarkupBps = req.MarkupBps
	existing.MinMarginBps = minMargin
	existing.MaxMarginBps = maxMargin

	actor := getActorPrincipal(r)
	ip := getClientIP(r)

	if err := h.engine.Repo().UpdateMargin(r.Context(), existing, actor, ip); err != nil {
		api.HandleDomainError(w, err)
		return
	}

	if h.auditSvc != nil {
		h.auditSvc.Log(r, "fx_margin.update", "tenant_fx_margin", existing.ID, map[string]interface{}{
			"tenant_id":  existing.TenantID,
			"corridor":   existing.FromAsset + "-" + existing.ToAsset,
			"markup_bps": existing.MarkupBps,
		})
	}

	api.JSON(w, http.StatusOK, existing)
}

func (h *MarginHandler) deleteMargin(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	tenantID := r.URL.Query().Get("tenant_id")
	actor := getActorPrincipal(r)
	ip := getClientIP(r)

	if err := h.engine.Repo().DeleteMargin(r.Context(), id, tenantID, actor, ip); err != nil {
		api.HandleDomainError(w, err)
		return
	}

	if h.auditSvc != nil {
		h.auditSvc.Log(r, "fx_margin.delete", "tenant_fx_margin", id, map[string]interface{}{
			"tenant_id": tenantID,
		})
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *MarginHandler) listMarginAudits(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	tenantID := r.URL.Query().Get("tenant_id")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	audits, err := h.engine.Repo().ListMarginAudits(r.Context(), tenantID, id, limit, offset)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, audits)
}

func (h *MarginHandler) listAllAudits(w http.ResponseWriter, r *http.Request) {
	tenantID := r.URL.Query().Get("tenant_id")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))

	audits, err := h.engine.Repo().ListMarginAudits(r.Context(), tenantID, "", limit, offset)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, audits)
}
