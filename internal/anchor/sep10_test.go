package anchor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/txnbuild"
)

func buildTestChallenge(t *testing.T, serverKP, clientKP *keypair.Full, webAuthDomain, homeDomain string) string {
	t.Helper()
	tx, err := txnbuild.BuildChallengeTx(
		serverKP.Seed(),
		clientKP.Address(),
		webAuthDomain,
		homeDomain,
		network.TestNetworkPassphrase,
		300*time.Second,
		nil,
	)
	if err != nil {
		t.Fatalf("build challenge tx: %v", err)
	}
	xdrStr, err := tx.Base64()
	if err != nil {
		t.Fatalf("encode challenge tx: %v", err)
	}
	return xdrStr
}

func TestSep10Client_Challenge_RejectsWrongSourceAccount(t *testing.T) {
	realServerKP := keypair.MustRandom()
	imposterKP := keypair.MustRandom() // signs/sources the challenge but is NOT the anchor's declared signing key
	clientKP := keypair.MustRandom()

	mux := http.NewServeMux()
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		// The anchor's stellar.toml claims SIGNING_KEY = realServerKP, but the
		// challenge it actually returns is sourced from a different account.
		challengeXDR := buildTestChallenge(t, imposterKP, clientKP, r.Host, "example.com")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"transaction":        challengeXDR,
			"network_passphrase": network.TestNetworkPassphrase,
		})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep10Client(srv.URL+"/auth", "example.com", network.TestNetworkPassphrase, nil)

	jwt, err := client.Challenge(context.Background(), realServerKP.Address(), clientKP.Address())
	if err == nil {
		t.Fatalf("expected Challenge to reject a challenge not sourced from the anchor's signing key, got challenge %q with no error", jwt)
	}
	if jwt != "" {
		t.Fatalf("expected no challenge to be returned on validation failure, got %q", jwt)
	}
}

func TestSep10Client_ChallengeAndAuthenticate_Success(t *testing.T) {
	serverKP := keypair.MustRandom()
	clientKP := keypair.MustRandom()

	var challengeXDR string
	var authenticatedToken = "sep10-jwt-token"

	mux := http.NewServeMux()
	mux.HandleFunc("/auth", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			webAuthDomain := r.Host
			challengeXDR = buildTestChallenge(t, serverKP, clientKP, webAuthDomain, "example.com")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"transaction":        challengeXDR,
				"network_passphrase": network.TestNetworkPassphrase,
			})
		case http.MethodPost:
			var body struct {
				Transaction string `json:"transaction"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body.Transaction == "" {
				t.Errorf("expected a signed transaction in the auth request body")
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"token": authenticatedToken})
		}
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := NewSep10Client(srv.URL+"/auth", "example.com", network.TestNetworkPassphrase, nil)

	challenge, err := client.Challenge(context.Background(), serverKP.Address(), clientKP.Address())
	if err != nil {
		t.Fatalf("Challenge returned unexpected error: %v", err)
	}
	if challenge == "" {
		t.Fatalf("expected a non-empty challenge")
	}

	token, err := client.Authenticate(context.Background(), challenge, clientKP.Seed())
	if err != nil {
		t.Fatalf("Authenticate returned unexpected error: %v", err)
	}
	if token != authenticatedToken {
		t.Fatalf("expected token %q, got %q", authenticatedToken, token)
	}
}

// -------------------------------------------------------------------------
// SEP-10 Server Verification & Security Tests (Issue #21)
// -------------------------------------------------------------------------

func setupTestSep10Server(t *testing.T) (*Sep10Server, *keypair.Full, []byte) {
	t.Helper()
	serverKP := keypair.MustRandom()
	jwtSecret := []byte("test-super-secret-jwt-key-32bytes!")

	srv, err := NewSep10Server(Sep10ServerConfig{
		ServerSeed:        serverKP.Seed(),
		HomeDomain:        "nexora.example.com",
		WebAuthDomain:     "auth.nexora.example.com",
		NetworkPassphrase: network.TestNetworkPassphrase,
		JWTSecret:         jwtSecret,
		JWTDuration:       2 * time.Hour,
		ChallengeTimeout:  300 * time.Second,
	})
	if err != nil {
		t.Fatalf("NewSep10Server failed: %v", err)
	}
	return srv, serverKP, jwtSecret
}

func TestSep10Server_GenerateChallenge_ValidSequenceZeroAndRandomNonce(t *testing.T) {
	srv, serverKP, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()

	challengeXDR, err := srv.GenerateChallenge(context.Background(), clientKP.Address(), "nexora.example.com", "", nil)
	if err != nil {
		t.Fatalf("GenerateChallenge error: %v", err)
	}

	genericTx, err := txnbuild.TransactionFromXDR(challengeXDR)
	if err != nil {
		t.Fatalf("parse challenge xdr: %v", err)
	}
	tx, ok := genericTx.Transaction()
	if !ok {
		t.Fatalf("expected standard transaction envelope")
	}

	// 1. Verify sequence number is zero
	if tx.SequenceNumber() != 0 {
		t.Fatalf("expected sequence number 0, got %d", tx.SequenceNumber())
	}

	// 2. Verify source account is server
	if tx.SourceAccount().AccountID != serverKP.Address() {
		t.Fatalf("expected source account %s, got %s", serverKP.Address(), tx.SourceAccount().AccountID)
	}

	// 3. Verify operation: ManageData '<homeDomain> auth' with 64 random bytes
	ops := tx.Operations()
	if len(ops) < 1 {
		t.Fatalf("expected at least 1 operation, got %d", len(ops))
	}
	op1, ok := ops[0].(*txnbuild.ManageData)
	if !ok {
		t.Fatalf("first operation is not ManageData")
	}
	if op1.Name != "nexora.example.com auth" {
		t.Fatalf("unexpected op name: %s", op1.Name)
	}
	if len(op1.Value) != 64 {
		t.Fatalf("expected 64 random bytes for auth value, got %d", len(op1.Value))
	}
	if op1.SourceAccount != clientKP.Address() {
		t.Fatalf("expected client source account %s, got %s", clientKP.Address(), op1.SourceAccount)
	}

	// 4. Verify timebounds are bounded to 300s
	tb := tx.Timebounds()
	if tb.MaxTime-tb.MinTime != 300 {
		t.Fatalf("expected 300s challenge window, got %d", tb.MaxTime-tb.MinTime)
	}
}

func TestSep10Server_VerifyAndIssueToken_Success(t *testing.T) {
	srv, _, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()

	// 1. Server generates challenge
	challengeXDR, err := srv.GenerateChallenge(context.Background(), clientKP.Address(), "", "", nil)
	if err != nil {
		t.Fatalf("GenerateChallenge error: %v", err)
	}

	// 2. Client signs the challenge
	genericTx, err := txnbuild.TransactionFromXDR(challengeXDR)
	if err != nil {
		t.Fatalf("parse xdr: %v", err)
	}
	tx, _ := genericTx.Transaction()
	signedTx, err := tx.Sign(network.TestNetworkPassphrase, clientKP)
	if err != nil {
		t.Fatalf("client sign error: %v", err)
	}
	signedXDR, err := signedTx.Base64()
	if err != nil {
		t.Fatalf("base64 encode: %v", err)
	}

	// 3. Server validates challenge and issues signed JWT bearer token
	sep10Token, err := srv.VerifyAndIssueToken(context.Background(), signedXDR, "")
	if err != nil {
		t.Fatalf("VerifyAndIssueToken unexpected error: %v", err)
	}
	if sep10Token.Account != clientKP.Address() {
		t.Fatalf("expected account %s, got %s", clientKP.Address(), sep10Token.Account)
	}
	if sep10Token.Token == "" {
		t.Fatalf("expected non-empty token")
	}

	// 4. Validate JWT claims
	claims, err := srv.VerifyJWT(sep10Token.Token)
	if err != nil {
		t.Fatalf("VerifyJWT error: %v", err)
	}
	if claims.Sub != clientKP.Address() {
		t.Fatalf("expected sub %s, got %s", clientKP.Address(), claims.Sub)
	}
	if claims.HomeDomain != "nexora.example.com" {
		t.Fatalf("expected home_domain nexora.example.com, got %s", claims.HomeDomain)
	}
	if claims.Iss != "https://auth.nexora.example.com" {
		t.Fatalf("expected iss https://auth.nexora.example.com, got %s", claims.Iss)
	}
}

func TestSep10Server_ReplayProtection_RejectsReplayedTransaction(t *testing.T) {
	srv, _, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()

	challengeXDR, err := srv.GenerateChallenge(context.Background(), clientKP.Address(), "", "", nil)
	if err != nil {
		t.Fatalf("GenerateChallenge error: %v", err)
	}

	genericTx, _ := txnbuild.TransactionFromXDR(challengeXDR)
	tx, _ := genericTx.Transaction()
	signedTx, _ := tx.Sign(network.TestNetworkPassphrase, clientKP)
	signedXDR, _ := signedTx.Base64()

	// First verification succeeds
	token, err := srv.VerifyAndIssueToken(context.Background(), signedXDR, "")
	if err != nil || token == nil {
		t.Fatalf("first verification failed: %v", err)
	}

	// Replay attempt with the exact same transaction hash MUST be rejected
	_, err = srv.VerifyAndIssueToken(context.Background(), signedXDR, "")
	if err != ErrChallengeReplayed {
		t.Fatalf("expected ErrChallengeReplayed, got %v", err)
	}
}

func TestSep10Server_RejectsMissingClientSignature(t *testing.T) {
	srv, _, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()

	// Challenge only signed by server, not by client
	challengeXDR, err := srv.GenerateChallenge(context.Background(), clientKP.Address(), "", "", nil)
	if err != nil {
		t.Fatalf("GenerateChallenge error: %v", err)
	}

	_, err = srv.VerifyAndIssueToken(context.Background(), challengeXDR, "")
	if err == nil {
		t.Fatalf("expected error verifying challenge without client signature, got nil")
	}
}

func TestSep10Server_EnforcesClientDomainSigning(t *testing.T) {
	srv, _, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()
	clientDomainKP := keypair.MustRandom()
	clientDomain := "client.wallet.com"

	// Challenge requested with client_domain
	challengeXDR, err := srv.GenerateChallenge(context.Background(), clientKP.Address(), "", clientDomain, nil)
	if err != nil {
		t.Fatalf("GenerateChallenge error: %v", err)
	}

	// 1. Client signs, but client_domain does NOT sign
	genericTx, _ := txnbuild.TransactionFromXDR(challengeXDR)
	tx, _ := genericTx.Transaction()
	onlyClientSignedTx, _ := tx.Sign(network.TestNetworkPassphrase, clientKP)
	onlyClientSignedXDR, _ := onlyClientSignedTx.Base64()

	// Verification fails because client_domain key did not sign
	_, err = srv.VerifyAndIssueToken(context.Background(), onlyClientSignedXDR, clientDomainKP.Address())
	if err == nil {
		t.Fatalf("expected failure when client_domain signature is missing")
	}

	// 2. Both client and client_domain sign the transaction envelope
	fullySignedTx, err := onlyClientSignedTx.Sign(network.TestNetworkPassphrase, clientDomainKP)
	if err != nil {
		t.Fatalf("client_domain sign error: %v", err)
	}
	fullySignedXDR, _ := fullySignedTx.Base64()

	// Verification succeeds when client_domain signing key matches
	token, err := srv.VerifyAndIssueToken(context.Background(), fullySignedXDR, clientDomainKP.Address())
	if err != nil {
		t.Fatalf("expected success with client_domain signature, got error: %v", err)
	}
	if token == nil || token.Token == "" {
		t.Fatalf("expected valid token returned")
	}

	claims, err := srv.VerifyJWT(token.Token)
	if err != nil {
		t.Fatalf("VerifyJWT error: %v", err)
	}
	if claims.ClientDomain != clientDomain {
		t.Fatalf("expected claims.ClientDomain %s, got %s", clientDomain, claims.ClientDomain)
	}
}

func TestSep10Server_RejectsMismatchedHomeDomain(t *testing.T) {
	srv, _, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()

	// Requesting with wrong home_domain must be rejected
	_, err := srv.GenerateChallenge(context.Background(), clientKP.Address(), "malicious.com", "", nil)
	if err != ErrInvalidHomeDomain {
		t.Fatalf("expected ErrInvalidHomeDomain, got %v", err)
	}
}

func TestSep10Server_RejectsExpiredChallenge_OlderThan300Seconds(t *testing.T) {
	srv, serverKP, _ := setupTestSep10Server(t)
	clientKP := keypair.MustRandom()

	// Build a challenge manually with expired timebounds (>300 seconds in the past)
	past := time.Now().UTC().Add(-400 * time.Second)
	tb := txnbuild.NewTimebounds(past.Unix(), past.Add(300*time.Second).Unix())

	rawBytes := make([]byte, 48)
	randomBytes := make([]byte, 64)
	base64.StdEncoding.Encode(randomBytes, rawBytes)

	account := txnbuild.SimpleAccount{
		AccountID: serverKP.Address(),
		Sequence:  0,
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations: []txnbuild.Operation{
			&txnbuild.ManageData{
				SourceAccount: clientKP.Address(),
				Name:          "nexora.example.com auth",
				Value:         randomBytes,
			},
		},
		BaseFee: txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: tb,
		},
	})
	if err != nil {
		t.Fatalf("build tx error: %v", err)
	}

	serverSignedTx, err := tx.Sign(network.TestNetworkPassphrase, serverKP)
	if err != nil {
		t.Fatalf("server sign error: %v", err)
	}
	clientSignedTx, err := serverSignedTx.Sign(network.TestNetworkPassphrase, clientKP)
	if err != nil {
		t.Fatalf("client sign error: %v", err)
	}
	signedXDR, err := clientSignedTx.Base64()
	if err != nil {
		t.Fatalf("encode error: %v", err)
	}

	_, err = srv.VerifyAndIssueToken(context.Background(), signedXDR, "")
	if err != ErrChallengeExpired {
		t.Fatalf("expected ErrChallengeExpired for challenge older than 300s, got: %v", err)
	}
}

