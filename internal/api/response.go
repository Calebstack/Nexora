package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
)

type errorResponse struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Status    int    `json:"status"`
	RequestID string `json:"request_id"`
}

// ValidationErrorDetail describes a single invalid field in a request.
type ValidationErrorDetail struct {
	Row    int    `json:"row"`
	Field  string `json:"field"`
	Value  string `json:"value"`
	Reason string `json:"reason"`
}

// validationErrorResponse includes per-row errors alongside the top-level error.
type validationErrorResponse struct {
	Error            errorDetail             `json:"error"`
	ValidationErrors []ValidationErrorDetail `json:"validation_errors"`
}

func JSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteJSON is an alias for JSON for compatibility.
func WriteJSON(w http.ResponseWriter, status int, v interface{}) {
	JSON(w, status, v)
}

func effectiveRequestID(w http.ResponseWriter) string {
	requestID := strings.TrimSpace(w.Header().Get("X-Request-ID"))
	if requestID == "" {
		requestID = uuid.New().String()
	}
	w.Header().Set("X-Request-ID", requestID)
	return requestID
}

func Error(w http.ResponseWriter, status int, code, message string) {
	JSON(w, status, errorResponse{
		Error: errorDetail{Code: code, Message: message, Status: status, RequestID: effectiveRequestID(w)},
	})
}

// WriteError writes an error response using HandleDomainError for domain errors
// or InternalError for unexpected errors.
func WriteError(w http.ResponseWriter, r *http.Request, err error) {
	if strings.TrimSpace(w.Header().Get("X-Request-ID")) == "" && r != nil {
		if requestID := strings.TrimSpace(r.Header.Get("X-Request-ID")); requestID != "" {
			w.Header().Set("X-Request-ID", requestID)
		}
	}
	HandleDomainError(w, err)
}

func BadRequest(w http.ResponseWriter, message string) {
	Error(w, http.StatusBadRequest, "BAD_REQUEST", message)
}

// BadRequestWithValidationErrors returns a 400 with per-row error details.
func BadRequestWithValidationErrors(w http.ResponseWriter, message string, errs []ValidationErrorDetail) {
	JSON(w, http.StatusBadRequest, validationErrorResponse{
		Error:            errorDetail{Code: "BAD_REQUEST", Message: message, Status: http.StatusBadRequest, RequestID: effectiveRequestID(w)},
		ValidationErrors: errs,
	})
}

func NotFound(w http.ResponseWriter, message string) {
	Error(w, http.StatusNotFound, "NOT_FOUND", message)
}

func UnprocessableEntity(w http.ResponseWriter, code, message string) {
	Error(w, http.StatusUnprocessableEntity, code, message)
}

func InternalError(w http.ResponseWriter, err error) {
	// The client deliberately gets an opaque message, but the cause has to go
	// somewhere — without this an unexpected failure leaves no trace at all.
	log.Error().Err(err).Msg("api: unhandled internal error")
	Error(w, http.StatusInternalServerError, "INTERNAL_ERROR", "an unexpected error occurred")
}

func HandleDomainError(w http.ResponseWriter, err error) {
	var noTrustlineErr *domain.ErrNoTrustline
	if errors.As(err, &noTrustlineErr) {
		UnprocessableEntity(w, "MISSING_TRUSTLINE", err.Error())
		return
	}

	switch {
	case errors.Is(err, domain.ErrWalletNotFound), errors.Is(err, domain.ErrTransactionNotFound),

		errors.Is(err, domain.ErrWebhookNotFound), errors.Is(err, domain.ErrWebhookDeliveryNotFound),
		errors.Is(err, domain.ErrWebhookConfigNotFound),
		errors.Is(err, domain.ErrTransferApprovalNotFound),
		errors.Is(err, domain.ErrIncidentNotFound),
		errors.Is(err, domain.ErrBatchNotFound), errors.Is(err, domain.ErrScheduleNotFound),
		errors.Is(err, domain.ErrUserNotFound), errors.Is(err, domain.ErrOrgNotFound), errors.Is(err, domain.ErrOrgMemberNotFound),
		errors.Is(err, domain.ErrInviteNotFound), errors.Is(err, domain.ErrClaimableBalanceNotFound),
		errors.Is(err, domain.ErrMarginNotFound):
		NotFound(w, err.Error())
	case errors.Is(err, domain.ErrSelfTransfer), errors.Is(err, domain.ErrInvalidAsset),
		errors.Is(err, domain.ErrBeneficiaryNotAllowed),
		errors.Is(err, domain.ErrInsufficientBalance), errors.Is(err, domain.ErrSlippageExceeded),
		errors.Is(err, domain.ErrFeeScheduleNotFound), errors.Is(err, domain.ErrBatchTooLarge),
		errors.Is(err, domain.ErrBatchEmpty), errors.Is(err, domain.ErrWalletLimitReached),
		errors.Is(err, domain.ErrTransferLimitReached), errors.Is(err, domain.ErrWebhookLimitReached),
		errors.Is(err, domain.ErrInvalidQuoteAmount), errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, domain.ErrNoClaimants), errors.Is(err, domain.ErrClaimantNotFound),
		errors.Is(err, domain.ErrClaimantNotCustodied), errors.Is(err, domain.ErrSourceWalletRequired),
		errors.Is(err, domain.ErrSponsorNotCustodied), errors.Is(err, domain.ErrCorridorConflict),
		errors.Is(err, domain.ErrMarginCeilingExceeded):
		BadRequest(w, err.Error())
	// An unsupported fiat currency is a 400 with its own code so a caller can
	// tell "this rail does not serve that currency" from a malformed request.
	case errors.Is(err, domain.ErrUnsupportedFiatCurrency):
		Error(w, http.StatusBadRequest, "UNSUPPORTED_FIAT_CURRENCY", err.Error())
	// An unsatisfiable predicate is a 400 with its own code, not a generic bad
	// request: the caller has to be able to tell "you cannot claim this yet"
	// (retryable, once the predicate holds) from "this balance is unusable".
	case errors.Is(err, domain.ErrPredicateNotSatisfiable):
		Error(w, http.StatusBadRequest, "PREDICATE_NOT_SATISFIABLE", err.Error())
	case errors.Is(err, domain.ErrInvalidPredicate):
		Error(w, http.StatusBadRequest, "INVALID_PREDICATE", err.Error())
	// A balance that is already claimed, expired or revoked is a conflict, not
	// a malformed request: the request was fine, the resource moved on.
	case errors.Is(err, domain.ErrClaimableBalanceNotPending):
		Error(w, http.StatusConflict, "CLAIMABLE_BALANCE_NOT_PENDING", err.Error())
	case errors.Is(err, domain.ErrUserAlreadyExists), errors.Is(err, domain.ErrDuplicateMarginTier):
		Error(w, http.StatusConflict, "CONFLICT", err.Error())
	case errors.Is(err, domain.ErrLastOrgOwner):
		Error(w, http.StatusConflict, "LAST_ORG_OWNER", err.Error())
	case errors.Is(err, domain.ErrInvalidCredentials):
		Error(w, http.StatusUnauthorized, "UNAUTHORIZED", err.Error())
	case errors.Is(err, domain.ErrForbidden), errors.Is(err, domain.ErrQuoteOwnershipMismatch):
		Error(w, http.StatusForbidden, "FORBIDDEN", err.Error())
	case errors.Is(err, domain.ErrQuoteExpired):
		UnprocessableEntity(w, "QUOTE_EXPIRED", err.Error())
	case errors.Is(err, domain.ErrQuoteAlreadyUsed):
		UnprocessableEntity(w, "QUOTE_ALREADY_USED", err.Error())
	case errors.Is(err, domain.ErrInsufficientSweepableBalance):
		Error(w, http.StatusBadRequest, "INSUFFICIENT_SWEEPABLE_BALANCE", err.Error())
	case errors.Is(err, domain.ErrTreasuryConfigNotFound):
		NotFound(w, err.Error())
	case errors.Is(err, domain.ErrComplianceReviewNotFound):
		NotFound(w, err.Error())
	case errors.Is(err, domain.ErrReviewNotPending):
		Error(w, http.StatusConflict, "REVIEW_ALREADY_DECIDED", err.Error())
	// Sanctions blocks get their own 403 code rather than the generic
	// FORBIDDEN above: callers must be able to tell "you may not do this"
	// from "this counterparty is sanctioned", and the latter is terminal —
	// retrying with the same destination will always fail.
	case errors.Is(err, domain.ErrTransferBlockedSanctions):
		Error(w, http.StatusForbidden, "TRANSFER_BLOCKED_SANCTIONS", err.Error())
	case func() bool {
		var target *domain.ErrTransferNotCancellable
		return errors.As(err, &target)
	}():
		var e *domain.ErrTransferNotCancellable
		_ = errors.As(err, &e)
		msg := "transfer cannot be cancelled: status " + e.Status
		if e.TxHash != "" {
			msg += ", tx_hash " + e.TxHash
		}
		Error(w, http.StatusConflict, "TRANSFER_NOT_CANCELLABLE", msg)
	default:
		InternalError(w, err)
	}
}
