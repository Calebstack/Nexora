package anchor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/api"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/Lumen-Nexora/Nexora/internal/wallet"
	"github.com/go-chi/chi/v5"
)

// SEP-30 Request / Response Structs

type Sep30RegisterRequest struct {
	Identities []wallet.RecoveryIdentity `json:"identities"`
}

type Sep30SignerInfo struct {
	Key string `json:"key"`
}

type Sep30AccountResponse struct {
	Address    string                    `json:"address"`
	Identities []wallet.RecoveryIdentity `json:"identities"`
	Signers    []Sep30SignerInfo         `json:"signers"`
}

type Sep30ChallengeRequest struct {
	AuthType  string `json:"auth_type"`            // phone_number, email, kyc_attestation
	AuthValue string `json:"auth_value,omitempty"` // phone number, email address, or KYC customer ID
}

type Sep30ChallengeResponse struct {
	ChallengeID string `json:"challenge_id"`
	AuthType    string `json:"auth_type"`
	AuthValue   string `json:"auth_value"`
	ExpiresAt   string `json:"expires_at"`
}

type Sep30SignatureRequest struct {
	Transaction      string `json:"transaction"` // Base64 transaction XDR
	ChallengeID      string `json:"challenge_id,omitempty"`
	Code             string `json:"code,omitempty"`
	KYCAttestationID string `json:"kyc_attestation_id,omitempty"`
}

type Sep30SignatureResponse struct {
	NetworkPassphrase string `json:"network_passphrase"`
	Signature         string `json:"signature"` // Base64 decorated signature
	Signer            string `json:"signer"`
	Transaction       string `json:"transaction"` // Signed transaction XDR
}

// ----------------------------------------------------------------------
// SEP-30 HTTP Handler
// ----------------------------------------------------------------------

type Sep30Handler struct {
	recoverySvc wallet.RecoveryService
}

func NewSep30Handler(recoverySvc wallet.RecoveryService) *Sep30Handler {
	return &Sep30Handler{recoverySvc: recoverySvc}
}

// Routes returns a sub-router for SEP-30 recovery server endpoints.
func (h *Sep30Handler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Route("/accounts/{address}", func(r chi.Router) {
			r.Post("/", h.RegisterAccount)
			r.Get("/", h.GetAccount)
			r.Delete("/", h.DeleteAccount)
			r.Post("/challenge", h.CreateChallenge)
			r.Post("/signatures", h.SignTransaction)
			r.Post("/sign", h.SignTransaction) // Alias per standard variants
			r.Post("/sign/{signing_address}", h.SignTransaction) // Alias with signing key in path
		})
	}
}

// RegisterAccount handles POST /accounts/{address}.
func (h *Sep30Handler) RegisterAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)
	address := chi.URLParam(r, "address")

	var req Sep30RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.Error(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON request body")
		return
	}

	acct, err := h.recoverySvc.RegisterAccount(ctx, tenantID, address, req.Identities)
	if err != nil {
		if errors.Is(err, wallet.ErrAccountAlreadyRegistered) {
			api.Error(w, http.StatusConflict, "ALREADY_REGISTERED", "account already registered for recovery")
			return
		}
		if errors.Is(err, wallet.ErrInvalidStellarAddress) {
			api.Error(w, http.StatusBadRequest, "INVALID_ADDRESS", "invalid Stellar address format")
			return
		}
		api.Error(w, http.StatusInternalServerError, "REGISTRATION_FAILED", err.Error())
		return
	}

	api.JSON(w, http.StatusCreated, Sep30AccountResponse{
		Address:    acct.Address,
		Identities: acct.Identities,
		Signers: []Sep30SignerInfo{
			{Key: acct.SignerKey},
		},
	})
}

// GetAccount handles GET /accounts/{address}.
func (h *Sep30Handler) GetAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)
	address := chi.URLParam(r, "address")

	acct, err := h.recoverySvc.GetAccount(ctx, tenantID, address)
	if err != nil {
		if errors.Is(err, wallet.ErrAccountNotRegistered) {
			api.Error(w, http.StatusNotFound, "NOT_FOUND", "account not registered for recovery")
			return
		}
		api.Error(w, http.StatusInternalServerError, "GET_FAILED", err.Error())
		return
	}

	api.JSON(w, http.StatusOK, Sep30AccountResponse{
		Address:    acct.Address,
		Identities: acct.Identities,
		Signers: []Sep30SignerInfo{
			{Key: acct.SignerKey},
		},
	})
}

// DeleteAccount handles DELETE /accounts/{address}.
func (h *Sep30Handler) DeleteAccount(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)
	address := chi.URLParam(r, "address")

	if err := h.recoverySvc.DeleteAccount(ctx, tenantID, address); err != nil {
		if errors.Is(err, wallet.ErrAccountNotRegistered) {
			api.Error(w, http.StatusNotFound, "NOT_FOUND", "account not registered for recovery")
			return
		}
		api.Error(w, http.StatusInternalServerError, "DELETE_FAILED", err.Error())
		return
	}

	api.JSON(w, http.StatusOK, map[string]string{
		"status": "deleted",
	})
}

// CreateChallenge handles POST /accounts/{address}/challenge.
func (h *Sep30Handler) CreateChallenge(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)
	address := chi.URLParam(r, "address")

	var req Sep30ChallengeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.Error(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON request body")
		return
	}

	ch, err := h.recoverySvc.CreateChallenge(ctx, tenantID, address, req.AuthType, req.AuthValue)
	if err != nil {
		if errors.Is(err, wallet.ErrRateLimited) {
			api.Error(w, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", err.Error())
			return
		}
		if errors.Is(err, wallet.ErrAccountNotRegistered) {
			api.Error(w, http.StatusNotFound, "NOT_FOUND", err.Error())
			return
		}
		if errors.Is(err, wallet.ErrNoMatchingIdentity) {
			api.Error(w, http.StatusForbidden, "NO_MATCHING_IDENTITY", err.Error())
			return
		}
		if errors.Is(err, wallet.ErrInvalidKYCAttestation) {
			api.Error(w, http.StatusForbidden, "KYC_VERIFICATION_FAILED", err.Error())
			return
		}
		api.Error(w, http.StatusBadRequest, "CHALLENGE_FAILED", err.Error())
		return
	}

	// Mask phone / email in response for security
	maskedValue := maskIdentifier(ch.AuthValue)

	api.JSON(w, http.StatusOK, Sep30ChallengeResponse{
		ChallengeID: ch.ID,
		AuthType:    ch.AuthType,
		AuthValue:   maskedValue,
		ExpiresAt:   ch.ExpiresAt.Format(time.RFC3339),
	})
}

// SignTransaction handles POST /accounts/{address}/signatures and POST /accounts/{address}/sign.
func (h *Sep30Handler) SignTransaction(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	tenantID := tenant.IDFromContext(ctx)
	address := chi.URLParam(r, "address")

	var req Sep30SignatureRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		api.Error(w, http.StatusBadRequest, "INVALID_REQUEST", "invalid JSON request body")
		return
	}

	if req.Transaction == "" {
		api.Error(w, http.StatusBadRequest, "MISSING_TRANSACTION", "transaction XDR is required")
		return
	}

	proof := wallet.SignatureAuthProof{
		ChallengeID:      req.ChallengeID,
		Code:             req.Code,
		KYCAttestationID: req.KYCAttestationID,
	}

	sigRes, err := h.recoverySvc.SignTransaction(ctx, tenantID, address, req.Transaction, proof)
	if err != nil {
		if errors.Is(err, wallet.ErrRateLimited) {
			api.Error(w, http.StatusTooManyRequests, "RATE_LIMIT_EXCEEDED", err.Error())
			return
		}
		if errors.Is(err, wallet.ErrAccountNotRegistered) {
			api.Error(w, http.StatusNotFound, "NOT_FOUND", err.Error())
			return
		}
		if errors.Is(err, wallet.ErrUnauthorizedTransaction) {
			api.Error(w, http.StatusForbidden, "UNAUTHORIZED_TRANSACTION", err.Error())
			return
		}
		if errors.Is(err, wallet.ErrInvalidOTPCode) || errors.Is(err, wallet.ErrMissingAuthProof) || errors.Is(err, wallet.ErrInvalidKYCAttestation) {
			api.Error(w, http.StatusUnauthorized, "AUTH_VERIFICATION_FAILED", err.Error())
			return
		}
		api.Error(w, http.StatusBadRequest, "SIGNING_FAILED", err.Error())
		return
	}

	api.JSON(w, http.StatusOK, Sep30SignatureResponse{
		NetworkPassphrase: sigRes.NetworkPassphrase,
		Signature:         sigRes.Signature,
		Signer:            sigRes.SignerPublicKey,
		Transaction:       sigRes.SignedTransaction,
	})
}

func maskIdentifier(id string) string {
	if strings.Contains(id, "@") {
		parts := strings.Split(id, "@")
		if len(parts[0]) <= 2 {
			return "*@" + parts[1]
		}
		return parts[0][:2] + "***@" + parts[1]
	}
	if len(id) > 4 {
		return id[:2] + "****" + id[len(id)-2:]
	}
	return id
}

// ----------------------------------------------------------------------
// SEP-30 Client Implementation
// ----------------------------------------------------------------------

type Sep30Client struct {
	httpClient *http.Client
}

func NewSep30Client(httpClient *http.Client) *Sep30Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 30 * time.Second}
	}
	return &Sep30Client{httpClient: httpClient}
}

// RegisterAccount registers a user account with an external SEP-30 recovery server.
func (c *Sep30Client) RegisterAccount(ctx context.Context, serverURL, jwt, address string, identities []wallet.RecoveryIdentity) (*Sep30AccountResponse, error) {
	targetURL := strings.TrimSuffix(serverURL, "/") + "/accounts/" + url.PathEscape(address)

	bodyBytes, err := json.Marshal(Sep30RegisterRequest{Identities: identities})
	if err != nil {
		return nil, fmt.Errorf("marshal register request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build register request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute register request: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read register response: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("register account failed with status %d: %s", resp.StatusCode, string(respBytes))
	}

	var out Sep30AccountResponse
	if err := json.Unmarshal(respBytes, &out); err != nil {
		return nil, fmt.Errorf("decode register response: %w", err)
	}
	return &out, nil
}

// RequestChallenge requests an OTP challenge from the recovery server.
func (c *Sep30Client) RequestChallenge(ctx context.Context, serverURL, jwt, address, authType, authValue string) (*Sep30ChallengeResponse, error) {
	targetURL := strings.TrimSuffix(serverURL, "/") + "/accounts/" + url.PathEscape(address) + "/challenge"

	bodyBytes, err := json.Marshal(Sep30ChallengeRequest{
		AuthType:  authType,
		AuthValue: authValue,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal challenge request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build challenge request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute challenge request: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read challenge response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request challenge failed with status %d: %s", resp.StatusCode, string(respBytes))
	}

	var out Sep30ChallengeResponse
	if err := json.Unmarshal(respBytes, &out); err != nil {
		return nil, fmt.Errorf("decode challenge response: %w", err)
	}
	return &out, nil
}

// RequestSignatures requests a transaction signature from the recovery server.
func (c *Sep30Client) RequestSignatures(ctx context.Context, serverURL, jwt, address, txXDR string, proof wallet.SignatureAuthProof) (*Sep30SignatureResponse, error) {
	targetURL := strings.TrimSuffix(serverURL, "/") + "/accounts/" + url.PathEscape(address) + "/signatures"

	bodyBytes, err := json.Marshal(Sep30SignatureRequest{
		Transaction:      txXDR,
		ChallengeID:      proof.ChallengeID,
		Code:             proof.Code,
		KYCAttestationID: proof.KYCAttestationID,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal signature request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("build signature request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("execute signature request: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read signature response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("request signatures failed with status %d: %s", resp.StatusCode, string(respBytes))
	}

	var out Sep30SignatureResponse
	if err := json.Unmarshal(respBytes, &out); err != nil {
		return nil, fmt.Errorf("decode signature response: %w", err)
	}
	return &out, nil
}
