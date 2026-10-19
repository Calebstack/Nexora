package wallet_test

import (
	"context"
	"crypto/rand"
	"testing"

	"github.com/Lumen-Nexora/Nexora/internal/compliance"
	"github.com/Lumen-Nexora/Nexora/internal/wallet"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

func setupRecoveryTest(t *testing.T) (wallet.RecoveryService, compliance.CustomerService, []byte) {
	t.Helper()
	masterKey := make([]byte, 32)
	_, _ = rand.Read(masterKey)

	// Compliance customer service for KYC attestation testing
	repo := compliance.NewMemoryCustomerRepository()
	custSvc, err := compliance.NewCustomerService(repo, masterKey, nil)
	if err != nil {
		t.Fatalf("failed to create compliance service: %v", err)
	}

	recoverySvc, err := wallet.NewRecoveryService(nil, masterKey, "Test SDF Network ; September 2015", custSvc)
	if err != nil {
		t.Fatalf("failed to create recovery service: %v", err)
	}

	return recoverySvc, custSvc, masterKey
}

func buildTestTransaction(t *testing.T, sourceAccount, newSignerKey string) string {
	t.Helper()
	kp, err := keypair.ParseAddress(sourceAccount)
	if err != nil {
		t.Fatalf("invalid test source account: %v", err)
	}

	acct := &horizon.Account{
		AccountID: kp.Address(),
		Sequence:  100,
	}

	// SetOptions adds new signer or changes thresholds to recover account
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

func TestRecoveryService_RegistrationAndSigningWithOTP(t *testing.T) {
	ctx := context.Background()
	recoverySvc, _, _ := setupRecoveryTest(t)

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	identities := []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: wallet.AuthTypeEmail, Value: "user@example.com"},
				{Type: wallet.AuthTypePhone, Value: "+15551234567"},
			},
		},
	}

	// 1. Register Account
	acct, err := recoverySvc.RegisterAccount(ctx, "tenant-1", userAddress, identities)
	if err != nil {
		t.Fatalf("RegisterAccount failed: %v", err)
	}
	if acct.SignerKey == "" {
		t.Fatalf("expected non-empty recovery signer key")
	}

	// 2. Request OTP Challenge
	ch, err := recoverySvc.CreateChallenge(ctx, "tenant-1", userAddress, wallet.AuthTypeEmail, "user@example.com")
	if err != nil {
		t.Fatalf("CreateChallenge failed: %v", err)
	}
	if ch.RawCode == "" {
		t.Fatalf("expected raw code in test harness")
	}

	// 3. Verify OTP Challenge with incorrect code -> Error
	_, err = recoverySvc.VerifyChallenge(ctx, "tenant-1", ch.ID, "000000")
	if err != wallet.ErrInvalidOTPCode {
		t.Errorf("expected ErrInvalidOTPCode, got %v", err)
	}

	// 4. Verify OTP Challenge with correct code -> Success
	verifiedCh, err := recoverySvc.VerifyChallenge(ctx, "tenant-1", ch.ID, ch.RawCode)
	if err != nil {
		t.Fatalf("VerifyChallenge failed: %v", err)
	}
	if !verifiedCh.Verified {
		t.Errorf("expected challenge to be verified")
	}

	// 5. Build transaction and generate recovery signature
	newDeviceKP, _ := keypair.Random()
	txXDR := buildTestTransaction(t, userAddress, newDeviceKP.Address())

	sigResult, err := recoverySvc.SignTransaction(ctx, "tenant-1", userAddress, txXDR, wallet.SignatureAuthProof{
		ChallengeID: ch.ID,
	})
	if err != nil {
		t.Fatalf("SignTransaction failed: %v", err)
	}

	if sigResult.Signature == "" {
		t.Errorf("expected signature to be non-empty")
	}
	if sigResult.SignedTransaction == "" {
		t.Errorf("expected signed transaction to be non-empty")
	}
	if sigResult.SignerPublicKey != acct.SignerKey {
		t.Errorf("expected signer %s, got %s", acct.SignerKey, sigResult.SignerPublicKey)
	}
}

func TestRecoveryService_SigningWithKYCAttestation(t *testing.T) {
	ctx := context.Background()
	recoverySvc, custSvc, _ := setupRecoveryTest(t)

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	// Register KYC customer and approve status
	cust, err := custSvc.PutCustomer(ctx, compliance.PutCustomerInput{
		TenantID: "tenant-1",
		Account:  userAddress,
		Fields: map[string]string{
			"first_name":    "Grace",
			"last_name":     "Hopper",
			"email_address": "grace@navy.mil",
		},
		Documents: map[string]*compliance.CustomerDocument{
			compliance.DocFieldPassport: {
				Field:    compliance.DocFieldPassport,
				Filename: "passport.jpg",
				Data:     []byte("PASSPORT_DATA"),
			},
		},
	})
	if err != nil {
		t.Fatalf("PutCustomer failed: %v", err)
	}

	// Approve KYC status
	_, err = custSvc.UpdateStatus(ctx, "tenant-1", cust.ID, compliance.CustomerStatusAccepted, "Approved")
	if err != nil {
		t.Fatalf("UpdateStatus failed: %v", err)
	}

	// Register recovery with KYC attestation identity
	identities := []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: wallet.AuthTypeKYC, Value: cust.ID},
			},
		},
	}
	_, err = recoverySvc.RegisterAccount(ctx, "tenant-1", userAddress, identities)
	if err != nil {
		t.Fatalf("RegisterAccount failed: %v", err)
	}

	// Sign transaction using KYC attestation proof
	newKP, _ := keypair.Random()
	txXDR := buildTestTransaction(t, userAddress, newKP.Address())

	sigResult, err := recoverySvc.SignTransaction(ctx, "tenant-1", userAddress, txXDR, wallet.SignatureAuthProof{
		KYCAttestationID: cust.ID,
	})
	if err != nil {
		t.Fatalf("SignTransaction with KYC failed: %v", err)
	}

	if sigResult.Signature == "" || sigResult.SignedTransaction == "" {
		t.Errorf("expected valid signature and signed transaction")
	}
}

func TestRecoveryService_StrictAuthorizationConstraints(t *testing.T) {
	ctx := context.Background()
	recoverySvc, _, _ := setupRecoveryTest(t)

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	identities := []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: wallet.AuthTypeEmail, Value: "alice@example.com"},
			},
		},
	}
	_, err := recoverySvc.RegisterAccount(ctx, "tenant-1", userAddress, identities)
	if err != nil {
		t.Fatalf("RegisterAccount failed: %v", err)
	}

	ch, _ := recoverySvc.CreateChallenge(ctx, "tenant-1", userAddress, wallet.AuthTypeEmail, "alice@example.com")
	_, _ = recoverySvc.VerifyChallenge(ctx, "tenant-1", ch.ID, ch.RawCode)

	// Build a transaction for a COMPLETELY DIFFERENT account
	otherKP, _ := keypair.Random()
	attackerKP, _ := keypair.Random()
	unrelatedTxXDR := buildTestTransaction(t, otherKP.Address(), attackerKP.Address())

	// Attempt to get signature for unrelated account
	_, err = recoverySvc.SignTransaction(ctx, "tenant-1", userAddress, unrelatedTxXDR, wallet.SignatureAuthProof{
		ChallengeID: ch.ID,
	})

	if err != wallet.ErrUnauthorizedTransaction {
		t.Errorf("expected ErrUnauthorizedTransaction, got %v", err)
	}
}

func TestRecoveryService_BruteForceRateLimiting(t *testing.T) {
	ctx := context.Background()
	recoverySvc, _, _ := setupRecoveryTest(t)

	userKP, _ := keypair.Random()
	userAddress := userKP.Address()

	identities := []wallet.RecoveryIdentity{
		{
			Role: "owner",
			AuthMethods: []wallet.RecoveryAuthMethod{
				{Type: wallet.AuthTypePhone, Value: "+15559876543"},
			},
		},
	}
	_, _ = recoverySvc.RegisterAccount(ctx, "tenant-1", userAddress, identities)

	// Rapidly fire challenges to trip rate limiter
	for i := 0; i < 5; i++ {
		_, err := recoverySvc.CreateChallenge(ctx, "tenant-1", userAddress, wallet.AuthTypePhone, "+15559876543")
		if err != nil {
			t.Fatalf("challenge %d failed: %v", i, err)
		}
	}

	// 6th attempt must be rate limited
	_, err := recoverySvc.CreateChallenge(ctx, "tenant-1", userAddress, wallet.AuthTypePhone, "+15559876543")
	if err != wallet.ErrRateLimited {
		t.Errorf("expected ErrRateLimited on 6th attempt, got %v", err)
	}
}
