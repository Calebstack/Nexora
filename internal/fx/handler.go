package fx

import (
	"encoding/json"
	"net/http"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
)

type Handler struct {
	svc           Service
	stream        *QuoteStream
	idem          func(http.Handler) http.Handler
	marginHandler *MarginHandler
}

func NewHandler(svc Service) *Handler {
	return &Handler{svc: svc, stream: NewQuoteStream(svc)}
}

// WithIdempotency attaches the idempotency-key middleware to the
// state-mutating route (POST /convert) only; quoting is not mutating.
func (h *Handler) WithIdempotency(mw func(http.Handler) http.Handler) *Handler {
	h.idem = mw
	return h
}

// WithMarginHandler attaches the administrative margin handler.
func (h *Handler) WithMarginHandler(mh *MarginHandler) *Handler {
	h.marginHandler = mh
	return h
}

// MarginAdminRoutes returns administrative margin routes.
func (h *Handler) MarginAdminRoutes() func(r chi.Router) {
	if h.marginHandler != nil {
		return h.marginHandler.AdminRoutes()
	}
	return func(r chi.Router) {}
}

func (h *Handler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Post("/quote", h.getQuote)
		convert := r.Post
		if h.idem != nil {
			convert = r.With(h.idem).Post
		}
		convert("/convert", h.convert)
		r.Get("/rates", h.getRates)
		r.Get("/quotes/stream", h.streamQuotes)
		r.Get("/catalog", h.getCatalog)
	}
}

type quoteRequest struct {
	FromAsset string `json:"from_asset" validate:"required"`
	ToAsset   string `json:"to_asset"   validate:"required"`
	Amount    string `json:"amount"     validate:"required"`
}

type convertRequest struct {
	WalletID       string  `json:"wallet_id" validate:"required,uuid"`
	QuoteID        string  `json:"quote_id"  validate:"required,uuid"`
	MinAmountOut   *string `json:"min_amount_out,omitempty"`
	MaxSlippageBps *int    `json:"max_slippage_bps,omitempty"`
}

func (h *Handler) getQuote(w http.ResponseWriter, r *http.Request) {
	var req quoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	if err := api.Validate(req); err != nil {
		api.BadRequest(w, err.Error())
		return
	}

	quote, err := h.svc.GetQuote(r.Context(), req.FromAsset, req.ToAsset, req.Amount)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, quote)
}

func (h *Handler) convert(w http.ResponseWriter, r *http.Request) {
	var req convertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.BadRequest(w, "invalid request body")
		return
	}
	if err := api.Validate(req); err != nil {
		api.BadRequest(w, err.Error())
		return
	}

	var minAmtOut *decimal.Decimal
	if req.MinAmountOut != nil {
		val, err := decimal.NewFromString(*req.MinAmountOut)
		if err != nil {
			api.BadRequest(w, "invalid min_amount_out")
			return
		}
		minAmtOut = &val
	}

	conv, err := h.svc.ExecuteConversion(r.Context(), req.WalletID, req.QuoteID, minAmtOut, req.MaxSlippageBps)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, conv)
}

func (h *Handler) getRates(w http.ResponseWriter, r *http.Request) {
	from := r.URL.Query().Get("from")
	to := r.URL.Query().Get("to")
	if from == "" || to == "" {
		api.BadRequest(w, "from and to query params are required")
		return
	}

	resp, err := h.svc.GetRates(r.Context(), from, to)
	if err != nil {
		api.HandleDomainError(w, err)
		return
	}

	api.JSON(w, http.StatusOK, resp)
}

func (h *Handler) getCatalog(w http.ResponseWriter, r *http.Request) {
	// Dummy implementation for now to satisfy routing
	api.JSON(w, http.StatusOK, map[string]interface{}{"assets": []interface{}{}, "supported_pairs": []interface{}{}})
}
