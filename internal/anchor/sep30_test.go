package anchor_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/anchor"
	"github.com/Lumen-Nexora/Nexora/internal/wallet"
	"github.com/go-chi/chi/v5"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

func setupSep30TestServer(t *testing.T) (*httptest.Server, wallet.RecoveryService) {
	t.Helper()
	masterKey := make([]byte, 32)
	_, _ = rand.Read(masterKey)

	recoverySvc, err := wallet.NewRecoveryService(nil, masterKey, "Test SDF Network ; September 2015", nil)
	if err != nil {
		t.Fatalf("failed to create recovery service: %v", err)
	}

	h := anchor.NewSep30Handler(recoverySvc)
	r := chi.NewRouter()
	r.Group(h.Routes())

	srv := httptest.NewServer(r)
	return srv, recoverySvc
}

func buildRecoveryTx(t *testing.T, sourceAccount, newSignerKey string) string {
	t.Helper()
	kp, _ := keypair.ParseAddress(sourceAccount)
	acct := &horizon.Account{
		AccountID: kp.Address(),
		Sequence:  10,
	}

	weight := txnbuild.Threshold(1)
	op := &txnbuild.SetOptions{
		Signer: &txnbuild.Signer{
			Address: newSignerKey,
			Weight:  weight,
		},
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        acct,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	if err != nil {
		t.Fatalf("failed to build test transaction: %v", err)
	}

	b64, err := tx.Base64()
	if err != nil {
		t.Fatalf("failed to encode transaction: %v", err)
	}
	return b64
}

func TestSep30_CompleteRecoveryFlow(t *testing.T) {
	srv, recoverySvc := setupSep30TestServer(t)
	defer srv.Close()

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	// 1. Register Account via POST /accounts/{address}
	regPayload, _ := json.Marshal(anchor.Sep30RegisterRequest{
		Identities: []wallet.RecoveryIdentity{
			{
				Role: "owner",
				AuthMethods: []wallet.RecoveryAuthMethod{
					{Type: "email", Value: "recovery-user@example.com"},
				},
			},
		},
	})

	resp, err := http.Post(srv.URL+"/accounts/"+userAddress, "application/json", bytes.NewReader(regPayload))
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 Created, got %d", resp.StatusCode)
	}

	var regResp anchor.Sep30AccountResponse
	_ = json.NewDecoder(resp.Body).Decode(&regResp)
	if len(regResp.Signers) == 0 || regResp.Signers[0].Key == "" {
		t.Fatalf("expected signer public key in registration response")
	}
	recoverySignerKey := regResp.Signers[0].Key

	// 2. GET /accounts/{address}
	getResp, err := http.Get(srv.URL + "/accounts/" + userAddress)
	if err != nil {
		t.Fatalf("GET /accounts failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", getResp.StatusCode)
	}

	// 3. Request challenge via POST /accounts/{address}/challenge
	chalPayload, _ := json.Marshal(anchor.Sep30ChallengeRequest{
		AuthType:  "email",
		AuthValue: "recovery-user@example.com",
	})
	chalResp, err := http.Post(srv.URL+"/accounts/"+userAddress+"/challenge", "application/json", bytes.NewReader(chalPayload))
	if err != nil {
		t.Fatalf("challenge request failed: %v", err)
	}
	defer chalResp.Body.Close()
	if chalResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for challenge, got %d", chalResp.StatusCode)
	}

	var chalData anchor.Sep30ChallengeResponse
	_ = json.NewDecoder(chalResp.Body).Decode(&chalData)
	if chalData.ChallengeID == "" {
		t.Fatalf("expected challenge_id in response")
	}

	// Retrieve challenge from service to get raw test OTP
	rawCh, err := recoverySvc.CreateChallenge(context.Background(), "", userAddress, "email", "recovery-user@example.com")
	if err != nil {
		t.Fatalf("failed to retrieve challenge: %v", err)
	}

	// 4. Request signatures via POST /accounts/{address}/signatures
	newDeviceKP, _ := keypair.Random()
	txXDR := buildRecoveryTx(t, userAddress, newDeviceKP.Address())

	sigPayload, _ := json.Marshal(anchor.Sep30SignatureRequest{
		Transaction: txXDR,
		ChallengeID: rawCh.ID,
		Code:        rawCh.RawCode,
	})

	sigResp, err := http.Post(srv.URL+"/accounts/"+userAddress+"/signatures", "application/json", bytes.NewReader(sigPayload))
	if err != nil {
		t.Fatalf("signatures request failed: %v", err)
	}
	defer sigResp.Body.Close()

	if sigResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK for signatures, got %d", sigResp.StatusCode)
	}

	var sigData anchor.Sep30SignatureResponse
	_ = json.NewDecoder(sigResp.Body).Decode(&sigData)

	if sigData.Signature == "" {
		t.Errorf("expected non-empty signature")
	}
	if sigData.Signer != recoverySignerKey {
		t.Errorf("expected signer %s, got %s", recoverySignerKey, sigData.Signer)
	}
	if sigData.Transaction == "" {
		t.Errorf("expected signed transaction XDR")
	}
}

func TestSep30_UnauthorizedTransactionRejected(t *testing.T) {
	srv, recoverySvc := setupSep30TestServer(t)
	defer srv.Close()

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	_, _ = recoverySvc.RegisterAccount(context.Background(), "", userAddress, []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: "phone_number", Value: "+15550001111"},
			},
		},
	})

	ch, _ := recoverySvc.CreateChallenge(context.Background(), "", userAddress, "phone_number", "+15550001111")

	// Create transaction for completely different account
	otherKP, _ := keypair.Random()
	unrelatedTxXDR := buildRecoveryTx(t, otherKP.Address(), userAddress)

	sigPayload, _ := json.Marshal(anchor.Sep30SignatureRequest{
		Transaction: unrelatedTxXDR,
		ChallengeID: ch.ID,
		Code:        ch.RawCode,
	})

	resp, err := http.Post(srv.URL+"/accounts/"+userAddress+"/signatures", "application/json", bytes.NewReader(sigPayload))
	if err != nil {
		t.Fatalf("post signatures failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("expected 403 Forbidden for unauthorized transaction, got %d", resp.StatusCode)
	}
}

func TestSep30_ClientIntegration(t *testing.T) {
	srv, recoverySvc := setupSep30TestServer(t)
	defer srv.Close()

	client := anchor.NewSep30Client(nil)
	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	// Test client.RegisterAccount
	acctResp, err := client.RegisterAccount(context.Background(), srv.URL, "jwt-token", userAddress, []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: "email", Value: "client-test@example.com"},
			},
		},
	})
	if err != nil {
		t.Fatalf("client.RegisterAccount failed: %v", err)
	}
	if len(acctResp.Signers) == 0 {
		t.Fatalf("expected signers in client response")
	}

	// Test client.RequestChallenge
	chalResp, err := client.RequestChallenge(context.Background(), srv.URL, "jwt-token", userAddress, "email", "client-test@example.com")
	if err != nil {
		t.Fatalf("client.RequestChallenge failed: %v", err)
	}
	if chalResp.ChallengeID == "" {
		t.Fatalf("expected challenge_id from client")
	}

	// Verify via service challenge
	ch, _ := recoverySvc.CreateChallenge(context.Background(), "", userAddress, "email", "client-test@example.com")

	// Test client.RequestSignatures
	newKP, _ := keypair.Random()
	txXDR := buildRecoveryTx(t, userAddress, newKP.Address())

	sigRes, err := client.RequestSignatures(context.Background(), srv.URL, "jwt-token", userAddress, txXDR, wallet.SignatureAuthProof{
		ChallengeID: ch.ID,
		Code:        ch.RawCode,
	})
	if err != nil {
		t.Fatalf("client.RequestSignatures failed: %v", err)
	}
	if sigRes.Signature == "" || sigRes.Transaction == "" {
		t.Errorf("expected valid signature response from client")
	}
}

func TestSep30_SignSubpathAlias(t *testing.T) {
	srv, recoverySvc := setupSep30TestServer(t)
	defer srv.Close()

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	acct, err := recoverySvc.RegisterAccount(context.Background(), "", userAddress, []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: "email", Value: "alias@example.com"},
			},
		},
	})
	if err != nil {
		t.Fatalf("RegisterAccount failed: %v", err)
	}

	ch, _ := recoverySvc.CreateChallenge(context.Background(), "", userAddress, "email", "alias@example.com")
	newDeviceKP, _ := keypair.Random()
	txXDR := buildRecoveryTx(t, userAddress, newDeviceKP.Address())

	sigPayload, _ := json.Marshal(anchor.Sep30SignatureRequest{
		Transaction: txXDR,
		ChallengeID: ch.ID,
		Code:        ch.RawCode,
	})

	// 1. Test POST /accounts/{address}/sign
	resp1, err := http.Post(srv.URL+"/accounts/"+userAddress+"/sign", "application/json", bytes.NewReader(sigPayload))
	if err != nil {
		t.Fatalf("request to /sign failed: %v", err)
	}
	defer resp1.Body.Close()
	if resp1.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /sign, got %d", resp1.StatusCode)
	}

	// 2. Test POST /accounts/{address}/sign/{signing_address}
	resp2, err := http.Post(srv.URL+"/accounts/"+userAddress+"/sign/"+acct.SignerKey, "application/json", bytes.NewReader(sigPayload))
	if err != nil {
		t.Fatalf("request to /sign/{signing_address} failed: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK from /sign/{signing_address}, got %d", resp2.StatusCode)
	}
}

