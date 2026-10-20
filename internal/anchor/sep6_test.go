package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/base"
	"github.com/stellar/go/txnbuild"
)

func TestSep6Client_InitiateDeposit_ReturnsBankInstructions(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/deposit", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("expected GET per SEP-6, got %s", r.Method)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-jwt" {
			t.Errorf("expected bearer jwt, got %q", got)
		}
		q := r.URL.Query()
		if q.Get("asset_code") != "USDC" {
			t.Errorf("expected asset_code=USDC, got %q", q.Get("asset_code"))
		}
		if q.Get("account") == "" {
			t.Errorf("expected account to be set")
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"id":  "deposit-tx-1",
			"how": "Make a payment to Bank: 121122676 Account: 13719713158835300",
			"eta": 1800,
			"instructions": map[string]interface{}{
				"organization.bank_number": map[string]string{
					"value":       "121122676",
					"description": "US bank routing number",
				},
				"organization.bank_account_number": map[string]string{
					"value":       "13719713158835300",
					"description": "US bank account number",
				},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep6Client(nil)
	instr, err := client.InitiateDeposit(context.Background(), srv.URL, "test-jwt", "USDC", "GACCOUNT", "100", "user@example.com")
	if err != nil {
		t.Fatalf("InitiateDeposit returned unexpected error: %v", err)
	}
	if instr.ID != "deposit-tx-1" {
		t.Fatalf("expected id deposit-tx-1, got %q", instr.ID)
	}
	if len(instr.Instructions) != 2 {
		t.Fatalf("expected 2 instruction fields, got %d", len(instr.Instructions))
	}
	if instr.Instructions["organization.bank_number"].Value != "121122676" {
		t.Fatalf("unexpected bank number instruction: %+v", instr.Instructions["organization.bank_number"])
	}
}

func TestSep6Client_GetInfo_ParsesDepositWithdrawSupport(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/info", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"deposit": map[string]interface{}{
				"USDC": map[string]interface{}{"enabled": true, "fee_fixed": 0.5, "min_amount": 1},
			},
			"withdraw": map[string]interface{}{
				"USDC": map[string]interface{}{"enabled": true, "fee_percent": 1.5},
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep6Client(nil)
	info, err := client.GetInfo(context.Background(), srv.URL)
	if err != nil {
		t.Fatalf("GetInfo returned unexpected error: %v", err)
	}
	if !info.Deposit["USDC"].Enabled {
		t.Fatalf("expected USDC deposit enabled")
	}
	if !info.Withdraw["USDC"].Enabled {
		t.Fatalf("expected USDC withdraw enabled")
	}
}

func TestSep6Client_GetTransaction_ReflectsCompletedStatus(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/transaction", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("id") != "deposit-tx-1" {
			t.Errorf("expected id=deposit-tx-1, got %q", r.URL.Query().Get("id"))
		}
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"transaction": map[string]interface{}{
				"id":                     "deposit-tx-1",
				"kind":                   "deposit",
				"status":                 "completed",
				"amount_in":              "100.00",
				"stellar_transaction_id": "abc123",
			},
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep6Client(nil)
	tx, err := client.GetTransaction(context.Background(), srv.URL, "test-jwt", "deposit-tx-1")
	if err != nil {
		t.Fatalf("GetTransaction returned unexpected error: %v", err)
	}
	if tx.Status != "completed" {
		t.Fatalf("expected status completed, got %q", tx.Status)
	}
}

// ============================================================================
// SEP-6 Programmatic Deposit and Withdrawal Engine Tests (Issue #25)
// ============================================================================

type mockSep6AccountInspector struct {
	accounts map[string]horizon.Account
	loadErr  error
}

func (m *mockSep6AccountInspector) LoadAccount(accountID string) (horizon.Account, error) {
	if m.loadErr != nil {
		return horizon.Account{}, m.loadErr
	}
	acct, ok := m.accounts[accountID]
	if !ok {
		return horizon.Account{}, errors.New("account not found on horizon")
	}
	return acct, nil
}

func setupSep6TestEngine(t *testing.T, inspector Sep6AccountInspector) (*Sep6Engine, *Sep10Server, *httptest.Server) {
	t.Helper()
	serverKP := keypair.MustRandom()
	sep10Srv, err := NewSep10Server(Sep10ServerConfig{
		ServerSeed:        serverKP.Seed(),
		HomeDomain:        "anchor.nexora.io",
		WebAuthDomain:     "anchor.nexora.io",
		NetworkPassphrase: network.TestNetworkPassphrase,
		JWTSecret:         []byte("sep6-test-jwt-secret-32-bytes-key"),
		JWTDuration:       1 * time.Hour,
	})
	if err != nil {
		t.Fatalf("NewSep10Server error: %v", err)
	}

	engine := NewSep6Engine(Sep6EngineConfig{
		AccountInspector:     inspector,
		TokenVerifier:        sep10Srv,
		DefaultAnchorAccount: serverKP.Address(),
	})

	handler := NewSep6Handler(engine)
	r := chi.NewRouter()
	r.Group(handler.Routes())
	srv := httptest.NewServer(r)
	t.Cleanup(srv.Close)

	return engine, sep10Srv, srv
}

func issueSep10JWTForTest(t *testing.T, sep10Srv *Sep10Server, clientKP *keypair.Full) string {
	t.Helper()
	ctx := context.Background()
	chalXDR, err := sep10Srv.GenerateChallenge(ctx, clientKP.Address(), "anchor.nexora.io", "", nil)
	if err != nil {
		t.Fatalf("GenerateChallenge error: %v", err)
	}
	genericTx, err := txnbuild.TransactionFromXDR(chalXDR)
	if err != nil {
		t.Fatalf("TransactionFromXDR error: %v", err)
	}
	tx, _ := genericTx.Transaction()
	signedTx, err := tx.Sign(network.TestNetworkPassphrase, clientKP)
	if err != nil {
		t.Fatalf("Sign challenge error: %v", err)
	}
	signedXDR, err := signedTx.Base64()
	if err != nil {
		t.Fatalf("Base64 error: %v", err)
	}
	tok, err := sep10Srv.VerifyAndIssueToken(ctx, signedXDR, "")
	if err != nil {
		t.Fatalf("VerifyAndIssueToken error: %v", err)
	}
	return tok.Token
}

func TestSep6Engine_StrictAuthentication_EnforcedOnAllEndpoints(t *testing.T) {
	clientKP := keypair.MustRandom()
	otherKP := keypair.MustRandom()
	inspector := &mockSep6AccountInspector{
		accounts: map[string]horizon.Account{
			clientKP.Address(): {
				AccountID:     clientKP.Address(),
				SubentryCount: 1,
				Balances: []horizon.Balance{
					{Asset: base.Asset{Type: "native"}, Balance: "10.0000000"},
					{Asset: base.Asset{Type: "credit_alphanum4", Code: "USDC"}, Balance: "0.0000000", Limit: "100000.0000000"},
				},
			},
		},
	}

	_, sep10Srv, srv := setupSep6TestEngine(t, inspector)
	validJWT := issueSep10JWTForTest(t, sep10Srv, clientKP)
	otherJWT := issueSep10JWTForTest(t, sep10Srv, otherKP)

	endpoints := []string{
		"/deposit?asset_code=USDC&account=" + clientKP.Address(),
		"/withdraw?asset_code=USDC&type=bank_account&dest=123456789",
		"/transaction?id=nonexistent",
		"/transactions?asset_code=USDC",
	}

	// 1. Missing Authorization header -> 403 Forbidden
	for _, ep := range endpoints {
		resp, err := http.Get(srv.URL + ep)
		if err != nil {
			t.Fatalf("GET %s error: %v", ep, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden without JWT on %s, got %d", ep, resp.StatusCode)
		}
	}

	// 2. Malformed / forged JWT -> 403 Forbidden
	for _, ep := range endpoints {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+ep, nil)
		req.Header.Set("Authorization", "Bearer forged.jwt.token")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s error: %v", ep, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("expected 403 Forbidden with forged JWT on %s, got %d", ep, resp.StatusCode)
		}
	}

	// 3. Valid JWT for another account attempting to deposit to clientKP -> 403 Forbidden
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/deposit?asset_code=USDC&account="+clientKP.Address(), nil)
	req.Header.Set("Authorization", "Bearer "+otherJWT)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /deposit mismatch error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 Forbidden when JWT subject != deposit account, got %d", resp.StatusCode)
	}

	// 4. Valid JWT for clientKP succeeds -> 200 OK
	req, _ = http.NewRequest(http.MethodGet, srv.URL+"/deposit?asset_code=USDC&account="+clientKP.Address()+"&amount=250", nil)
	req.Header.Set("Authorization", "Bearer "+validJWT)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /deposit valid error: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK with valid SEP-10 JWT, got %d", resp.StatusCode)
	}
}

func TestSep6Engine_TrustlineAndReserveValidation(t *testing.T) {
	noTrustlineKP := keypair.MustRandom()
	lowReserveKP := keypair.MustRandom()
	readyKP := keypair.MustRandom()

	inspector := &mockSep6AccountInspector{
		accounts: map[string]horizon.Account{
			// Has enough XLM reserve (10 XLM >= (2+0)*0.5 = 1.0 XLM), but no USDC trustline
			noTrustlineKP.Address(): {
				AccountID:     noTrustlineKP.Address(),
				SubentryCount: 0,
				Balances: []horizon.Balance{
					{Asset: base.Asset{Type: "native"}, Balance: "10.0000000"},
				},
			},
			// Has USDC trustline (subentries=2 -> requires (2+2)*0.5 = 2.0 XLM), but only has 1.2 XLM
			lowReserveKP.Address(): {
				AccountID:     lowReserveKP.Address(),
				SubentryCount: 2,
				Balances: []horizon.Balance{
					{Asset: base.Asset{Type: "native"}, Balance: "1.2000000"},
					{Asset: base.Asset{Type: "credit_alphanum4", Code: "USDC"}, Balance: "5.0000000", Limit: "10000.0000000"},
				},
			},
			// Has both sufficient XLM reserve (5.0 XLM >= (2+1)*0.5 = 1.5 XLM) and USDC trustline
			readyKP.Address(): {
				AccountID:     readyKP.Address(),
				SubentryCount: 1,
				Balances: []horizon.Balance{
					{Asset: base.Asset{Type: "native"}, Balance: "5.0000000"},
					{Asset: base.Asset{Type: "credit_alphanum4", Code: "USDC"}, Balance: "10.0000000", Limit: "10000.0000000"},
				},
			},
		},
	}

	engine, sep10Srv, srv := setupSep6TestEngine(t, inspector)
	_ = engine

	// Case 1: Missing trustline rejected before accepting deposit
	jwtNoTL := issueSep10JWTForTest(t, sep10Srv, noTrustlineKP)
	req1, _ := http.NewRequest(http.MethodGet, srv.URL+"/deposit?asset_code=USDC&account="+noTrustlineKP.Address()+"&amount=100", nil)
	req1.Header.Set("Authorization", "Bearer "+jwtNoTL)
	resp1, err := http.DefaultClient.Do(req1)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for account without USDC trustline, got %d", resp1.StatusCode)
	}

	// Native XLM deposit on noTrustlineKP should succeed since XLM requires no trustline and reserve is met
	reqXLM, _ := http.NewRequest(http.MethodGet, srv.URL+"/deposit?asset_code=XLM&account="+noTrustlineKP.Address()+"&amount=50", nil)
	reqXLM.Header.Set("Authorization", "Bearer "+jwtNoTL)
	respXLM, err := http.DefaultClient.Do(reqXLM)
	if err != nil {
		t.Fatalf("XLM deposit request failed: %v", err)
	}
	defer respXLM.Body.Close()
	if respXLM.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for native XLM deposit with sufficient reserve, got %d", respXLM.StatusCode)
	}

	// Case 2: Insufficient XLM reserve rejected before accepting deposit
	jwtLowRes := issueSep10JWTForTest(t, sep10Srv, lowReserveKP)
	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/deposit?asset_code=USDC&account="+lowReserveKP.Address()+"&amount=100", nil)
	req2.Header.Set("Authorization", "Bearer "+jwtLowRes)
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for account below Stellar reserve requirement, got %d", resp2.StatusCode)
	}

	// Case 3: Ready account with trustline and reserve succeeds
	jwtReady := issueSep10JWTForTest(t, sep10Srv, readyKP)
	req3, _ := http.NewRequest(http.MethodGet, srv.URL+"/deposit?asset_code=USDC&account="+readyKP.Address()+"&amount=100", nil)
	req3.Header.Set("Authorization", "Bearer "+jwtReady)
	resp3, err := http.DefaultClient.Do(req3)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp3.Body.Close()
	if resp3.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for account meeting trustline and reserve requirements, got %d", resp3.StatusCode)
	}
}

func TestSep6Engine_DeterministicInstructionsAndPollingFlow(t *testing.T) {
	clientKP := keypair.MustRandom()
	inspector := &mockSep6AccountInspector{
		accounts: map[string]horizon.Account{
			clientKP.Address(): {
				AccountID:     clientKP.Address(),
				SubentryCount: 2,
				Balances: []horizon.Balance{
					{Asset: base.Asset{Type: "native"}, Balance: "25.0000000"},
					{Asset: base.Asset{Type: "credit_alphanum4", Code: "USDC"}, Balance: "100.0000000", Limit: "1000000.0000000"},
					{Asset: base.Asset{Type: "credit_alphanum4", Code: "EURC"}, Balance: "50.0000000", Limit: "1000000.0000000"},
				},
			},
		},
	}

	engine, sep10Srv, srv := setupSep6TestEngine(t, inspector)
	jwt := issueSep10JWTForTest(t, sep10Srv, clientKP)
	sep6Client := NewSep6Client(srv.Client())

	// 1. Initiate two deposits with identical parameters and verify deterministic payment instructions
	dep1, err := sep6Client.InitiateDeposit(context.Background(), srv.URL, jwt, "USDC", clientKP.Address(), "500.00", "treasury@partner.io")
	if err != nil {
		t.Fatalf("first InitiateDeposit error: %v", err)
	}
	dep2, err := sep6Client.InitiateDeposit(context.Background(), srv.URL, jwt, "USDC", clientKP.Address(), "500.00", "treasury@partner.io")
	if err != nil {
		t.Fatalf("second InitiateDeposit error: %v", err)
	}

	for _, key := range []string{
		"organization.bank_number",
		"organization.bank_account_number",
		"organization.iban",
		"organization.crypto_address",
		"organization.reference",
	} {
		v1 := dep1.Instructions[key].Value
		v2 := dep2.Instructions[key].Value
		if v1 == "" {
			t.Fatalf("expected non-empty deterministic instruction for %s", key)
		}
		if v1 != v2 {
			t.Fatalf("expected deterministic instruction for %s to match across calls, got %q vs %q", key, v1, v2)
		}
	}

	// Verify SEPA / IBAN type and crypto type instructions via direct query
	sepaParams, err := ParseSep6DepositParams(map[string][]string{
		"asset_code": {"EURC"},
		"account":    {clientKP.Address()},
		"type":       {"SEPA"},
		"amount":     {"250.00"},
	})
	if err != nil {
		t.Fatalf("ParseSep6DepositParams SEPA error: %v", err)
	}
	sepaInstr, err := engine.Deposit(context.Background(), &Sep10Claims{Sub: clientKP.Address()}, sepaParams)
	if err != nil {
		t.Fatalf("SEPA Deposit error: %v", err)
	}
	if sepaInstr.Instructions["organization.iban"].Value == "" || sepaInstr.Instructions["organization.bic"].Value == "" {
		t.Fatalf("expected IBAN and BIC instructions for SEPA deposit, got %+v", sepaInstr.Instructions)
	}

	cryptoParams, err := ParseSep6DepositParams(map[string][]string{
		"asset_code": {"USDC"},
		"account":    {clientKP.Address()},
		"type":       {"crypto"},
	})
	if err != nil {
		t.Fatalf("ParseSep6DepositParams crypto error: %v", err)
	}
	cryptoInstr, err := engine.Deposit(context.Background(), &Sep10Claims{Sub: clientKP.Address()}, cryptoParams)
	if err != nil {
		t.Fatalf("crypto Deposit error: %v", err)
	}
	if cryptoInstr.Instructions["organization.crypto_address"].Value == "" {
		t.Fatalf("expected crypto_address instruction for crypto deposit")
	}

	// 2. Initiate programmatic withdrawal via GET /withdraw
	withdrawRes, err := sep6Client.InitiateWithdrawal(context.Background(), srv.URL, jwt, "USDC", "300.00", "DE89370400440532013000")
	if err != nil {
		t.Fatalf("InitiateWithdrawal error: %v", err)
	}
	if withdrawRes.ID == "" || withdrawRes.AccountID == "" || withdrawRes.Memo == "" {
		t.Fatalf("expected withdrawal id, receiving account_id, and memo, got %+v", withdrawRes)
	}

	// 3. Poll single transaction status via GET /transaction
	polledDep, err := sep6Client.GetTransaction(context.Background(), srv.URL, jwt, dep1.ID)
	if err != nil {
		t.Fatalf("GetTransaction error: %v", err)
	}
	if polledDep.ID != dep1.ID || polledDep.Status != "pending_user_transfer_start" {
		t.Fatalf("unexpected polled deposit transaction: %+v", polledDep)
	}

	// Advance status to completed and poll again by stellar_transaction_id
	_, err = engine.UpdateTransactionStatus(context.Background(), dep1.ID, "completed", "stellar-tx-hash-999", "bank-wire-777", "Deposit settled on-chain")
	if err != nil {
		t.Fatalf("UpdateTransactionStatus error: %v", err)
	}

	reqByStellarID, _ := http.NewRequest(http.MethodGet, srv.URL+"/transaction?stellar_transaction_id=stellar-tx-hash-999", nil)
	reqByStellarID.Header.Set("Authorization", "Bearer "+jwt)
	respTx, err := http.DefaultClient.Do(reqByStellarID)
	if err != nil {
		t.Fatalf("GET /transaction by stellar_transaction_id error: %v", err)
	}
	defer respTx.Body.Close()
	if respTx.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from GET /transaction?stellar_transaction_id=..., got %d", respTx.StatusCode)
	}
	var singleWrap getTransactionResponse
	if err := json.NewDecoder(respTx.Body).Decode(&singleWrap); err != nil {
		t.Fatalf("decode single transaction error: %v", err)
	}
	if singleWrap.Transaction.Status != "completed" || singleWrap.Transaction.CompletedAt == "" {
		t.Fatalf("expected completed status and timestamp, got %+v", singleWrap.Transaction)
	}

	// 4. Poll transaction list via GET /transactions
	txList, err := sep6Client.GetTransactions(context.Background(), srv.URL, jwt, "USDC", clientKP.Address(), "", 10)
	if err != nil {
		t.Fatalf("GetTransactions error: %v", err)
	}
	// We created 3 USDC transactions (dep1, dep2, cryptoInstr) + 1 USDC withdrawal = 4 USDC transactions
	if len(txList) != 4 {
		t.Fatalf("expected 4 USDC transactions for account, got %d", len(txList))
	}

	// Filter by kind=withdrawal
	withdrawList, err := sep6Client.GetTransactions(context.Background(), srv.URL, jwt, "USDC", clientKP.Address(), "withdrawal", 10)
	if err != nil {
		t.Fatalf("GetTransactions kind=withdrawal error: %v", err)
	}
	if len(withdrawList) != 1 || withdrawList[0].ID != withdrawRes.ID {
		t.Fatalf("expected 1 withdrawal transaction matching %s, got %+v", withdrawRes.ID, withdrawList)
	}

	// Ensuredecimal package used directly in test for precision check
	expectedNet := decimal.RequireFromString("500.00").Sub(decimal.RequireFromString("0.50")).Sub(decimal.RequireFromString("0.50"))
	if polledDep.AmountOut != expectedNet.StringFixed(2) {
		t.Fatalf("expected net amount_out %s, got %s", expectedNet.StringFixed(2), polledDep.AmountOut)
	}
}

