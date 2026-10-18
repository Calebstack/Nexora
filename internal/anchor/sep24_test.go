package anchor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/stellar/go/keypair"
)

func TestSep24Client_GetInteractiveUrl_ReturnsValidHTTPSUrl(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/transactions/deposit/interactive", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST per SEP-24, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-jwt" {
			t.Errorf("expected bearer jwt, got %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"type": "interactive_customer_info_needed",
			"url":  "https://anchor.example.com/kycflow?token=abc",
			"id":   "sep24-tx-1",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep24Client(nil)
	interactiveURL, txID, err := client.GetInteractiveUrl(context.Background(), srv.URL, "test-jwt", "USDC", "GACCOUNT", "50", domain.AnchorTxTypeDeposit)
	if err != nil {
		t.Fatalf("GetInteractiveUrl returned unexpected error: %v", err)
	}
	if txID != "sep24-tx-1" {
		t.Fatalf("expected transaction id sep24-tx-1, got %q", txID)
	}
	parsed, err := url.Parse(interactiveURL)
	if err != nil {
		t.Fatalf("interactive url did not parse: %v", err)
	}
	if parsed.Scheme != "https" {
		t.Fatalf("expected an https interactive url, got %q", interactiveURL)
	}
}

func TestSep24Client_GetInteractiveUrl_RejectsNonHTTPS(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/transactions/withdraw/interactive", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"type": "interactive_customer_info_needed",
			"url":  "http://insecure.example.com/kycflow", // not https
			"id":   "sep24-tx-2",
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep24Client(nil)
	_, _, err := client.GetInteractiveUrl(context.Background(), srv.URL, "test-jwt", "USDC", "GACCOUNT", "", domain.AnchorTxTypeWithdrawal)
	if err == nil {
		t.Fatalf("expected an error for a non-https interactive url")
	}
}

func TestSep24Client_PollTransaction(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/transaction", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"transaction": map[string]interface{}{
				"id":     "sep24-tx-1",
				"status": "pending_user_transfer_start",
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep24Client(nil)
	tx, err := client.PollTransaction(context.Background(), srv.URL, "test-jwt", "sep24-tx-1")
	if err != nil {
		t.Fatalf("PollTransaction returned unexpected error: %v", err)
	}
	if tx.Status != "pending_user_transfer_start" {
		t.Fatalf("unexpected status %q", tx.Status)
	}
}

// -------------------------------------------------------------------------
// SEP-24 Coordinator, State Machine & Webhook Tests (Issue #22)
// -------------------------------------------------------------------------

func setupTestSep24Coordinator(t *testing.T) (*Sep24Coordinator, []byte) {
	t.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	webhookSecret := []byte("webhook-secret-key-32bytes-long!")

	coord, err := NewSep24Coordinator(Sep24CoordinatorConfig{
		EncryptionKey:      key,
		InteractiveBaseURL: "https://anchor.example.com/webview",
		WebhookSecret:      webhookSecret,
		SessionTTL:         10 * time.Minute,
	})
	if err != nil {
		t.Fatalf("NewSep24Coordinator error: %v", err)
	}
	return coord, webhookSecret
}

func TestSep24Coordinator_SessionTokenEncryptionAndValidation(t *testing.T) {
	coord, _ := setupTestSep24Coordinator(t)
	account := keypair.MustRandom().Address()

	token, err := coord.GenerateSessionToken("tx-123", account, "USDC", "deposit")
	if err != nil {
		t.Fatalf("GenerateSessionToken error: %v", err)
	}
	if token == "" {
		t.Fatalf("expected non-empty session token")
	}

	payload, err := coord.ValidateSessionToken(token)
	if err != nil {
		t.Fatalf("ValidateSessionToken error: %v", err)
	}
	if payload.TransactionID != "tx-123" {
		t.Fatalf("expected tx_id tx-123, got %s", payload.TransactionID)
	}
	if payload.Account != account {
		t.Fatalf("expected account %s, got %s", account, payload.Account)
	}
	if payload.AssetCode != "USDC" {
		t.Fatalf("expected asset USDC, got %s", payload.AssetCode)
	}
	if payload.Operation != "deposit" {
		t.Fatalf("expected operation deposit, got %s", payload.Operation)
	}

	// Tampered token must fail
	_, err = coord.ValidateSessionToken(token + "tampered")
	if err == nil {
		t.Fatalf("expected tampered token to fail validation")
	}
}

func TestSep24Coordinator_EndpointsAndStateTransitions(t *testing.T) {
	coord, webhookSecret := setupTestSep24Coordinator(t)
	handler := NewSep24Handler(coord)

	mux := http.NewServeMux()
	mux.HandleFunc("/transactions/deposit/interactive", handler.DepositInteractive)
	mux.HandleFunc("/transactions/withdraw/interactive", handler.WithdrawInteractive)
	mux.HandleFunc("/webhook", handler.Webhook)
	mux.HandleFunc("/transaction", handler.GetTransaction)

	srv := httptest.NewServer(mux)
	defer srv.Close()

	clientKP := keypair.MustRandom()

	// 1. Initiate Interactive Deposit
	depositReq := interactiveRequest{
		AssetCode: "USDC",
		Account:   clientKP.Address(),
		Amount:    "100.50",
	}
	reqBytes, _ := json.Marshal(depositReq)
	resp, err := http.Post(srv.URL+"/transactions/deposit/interactive", "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatalf("POST /transactions/deposit/interactive error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	}

	var initResp interactiveResponse
	if err := json.NewDecoder(resp.Body).Decode(&initResp); err != nil {
		t.Fatalf("decode init response error: %v", err)
	}
	if initResp.Type != "interactive_customer_info_needed" {
		t.Fatalf("expected type interactive_customer_info_needed, got %s", initResp.Type)
	}
	if initResp.ID == "" {
		t.Fatalf("expected non-empty transaction id")
	}

	parsedURL, err := url.Parse(initResp.URL)
	if err != nil || parsedURL.Scheme != "https" {
		t.Fatalf("expected valid https interactive url, got: %s", initResp.URL)
	}

	// 2. Initial state is 'incomplete'
	session, err := coord.GetSession(context.Background(), initResp.ID)
	if err != nil {
		t.Fatalf("GetSession error: %v", err)
	}
	if session.Status != Sep24StatusIncomplete {
		t.Fatalf("expected initial status incomplete, got %s", session.Status)
	}

	// Helper to send signed webhook callback
	sendWebhook := func(payload Sep24WebhookPayload) int {
		body, _ := json.Marshal(payload)
		mac := hmac.New(sha256.New, webhookSecret)
		mac.Write(body)
		sig := hex.EncodeToString(mac.Sum(nil))

		req, _ := http.NewRequest(http.MethodPost, srv.URL+"/webhook", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Anchor-Signature", sig)
		wResp, wErr := http.DefaultClient.Do(req)
		if wErr != nil {
			t.Fatalf("webhook request failed: %v", wErr)
		}
		defer wResp.Body.Close()
		return wResp.StatusCode
	}

	// 3. Invalid transition test: cannot jump directly from 'incomplete' to 'completed'
	status := sendWebhook(Sep24WebhookPayload{
		TransactionID: initResp.ID,
		Status:        Sep24StatusCompleted,
	})
	if status != http.StatusConflict {
		t.Fatalf("expected 409 Conflict for invalid transition incomplete->completed, got %d", status)
	}

	// 4. Valid transition: incomplete -> pending_user_transfer_start (KYC completed)
	status = sendWebhook(Sep24WebhookPayload{
		TransactionID: initResp.ID,
		Status:        Sep24StatusPendingUserTransferStart,
		ExternalTxID:  "ext-kyc-999",
		Message:       "KYC verified",
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK for transition incomplete->pending_user_transfer_start, got %d", status)
	}

	// Verify state in store
	session, _ = coord.GetSession(context.Background(), initResp.ID)
	if session.Status != Sep24StatusPendingUserTransferStart {
		t.Fatalf("expected status pending_user_transfer_start, got %s", session.Status)
	}
	if session.ExternalTxID != "ext-kyc-999" {
		t.Fatalf("expected external_tx_id ext-kyc-999, got %s", session.ExternalTxID)
	}

	// 5. Valid transition: pending_user_transfer_start -> pending_anchor
	status = sendWebhook(Sep24WebhookPayload{
		TransactionID: initResp.ID,
		Status:        Sep24StatusPendingAnchor,
		Message:       "Funds detected, anchor processing",
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK for pending_user_transfer_start->pending_anchor, got %d", status)
	}

	// 6. Valid transition: pending_anchor -> completed
	status = sendWebhook(Sep24WebhookPayload{
		TransactionID: initResp.ID,
		Status:        Sep24StatusCompleted,
		Message:       "Transfer complete",
	})
	if status != http.StatusOK {
		t.Fatalf("expected 200 OK for pending_anchor->completed, got %d", status)
	}

	session, _ = coord.GetSession(context.Background(), initResp.ID)
	if session.Status != Sep24StatusCompleted {
		t.Fatalf("expected final status completed, got %s", session.Status)
	}
	if session.CompletedAt == nil {
		t.Fatalf("expected non-nil completed_at timestamp")
	}

	// 7. Verify GET /transaction?id=...
	pollResp, err := http.Get(srv.URL + "/transaction?id=" + initResp.ID)
	if err != nil {
		t.Fatalf("GET /transaction error: %v", err)
	}
	defer pollResp.Body.Close()
	if pollResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from GET /transaction, got %d", pollResp.StatusCode)
	}
}

func TestSep24Coordinator_WithdrawFlowAndErrorTransition(t *testing.T) {
	coord, webhookSecret := setupTestSep24Coordinator(t)
	handler := NewSep24Handler(coord)

	mux := http.NewServeMux()
	mux.HandleFunc("/transactions/withdraw/interactive", handler.WithdrawInteractive)
	mux.HandleFunc("/webhook", handler.Webhook)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	clientKP := keypair.MustRandom()

	// Initiate Interactive Withdrawal
	withdrawReq := interactiveRequest{
		AssetCode: "XLM",
		Account:   clientKP.Address(),
		Amount:    "500",
	}
	reqBytes, _ := json.Marshal(withdrawReq)
	resp, err := http.Post(srv.URL+"/transactions/withdraw/interactive", "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		t.Fatalf("POST /transactions/withdraw/interactive error: %v", err)
	}
	defer resp.Body.Close()

	var initResp interactiveResponse
	_ = json.NewDecoder(resp.Body).Decode(&initResp)

	// Webhook transitions session to error
	payload := Sep24WebhookPayload{
		TransactionID: initResp.ID,
		Status:        Sep24StatusError,
		Message:       "Compliance sanctions check failed",
	}
	body, _ := json.Marshal(payload)
	mac := hmac.New(sha256.New, webhookSecret)
	mac.Write(body)
	sig := hex.EncodeToString(mac.Sum(nil))

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/webhook", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Anchor-Signature", sig)
	wResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("webhook error: %v", err)
	}
	defer wResp.Body.Close()

	if wResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for transition to error, got %d", wResp.StatusCode)
	}

	session, _ := coord.GetSession(context.Background(), initResp.ID)
	if session.Status != Sep24StatusError {
		t.Fatalf("expected status error, got %s", session.Status)
	}
	if session.Message != "Compliance sanctions check failed" {
		t.Fatalf("expected error message recorded, got: %s", session.Message)
	}
}
