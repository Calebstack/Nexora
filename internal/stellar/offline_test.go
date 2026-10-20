package stellar

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stellar/go/keypair"
	"github.com/stellar/go/network"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/protocols/horizon/operations"
	"github.com/stellar/go/txnbuild"
)

// mockAirGappedSigningDevice simulates an isolated cold-storage hardware/air-gapped
// signing appliance that holds private keys offline, inspects unsigned XDR envelopes,
// and produces signed XDR envelopes without ever exposing secret seeds to Nexora.
type mockAirGappedSigningDevice struct {
	networkPassphrase string
	coldKeys          []*keypair.Full
}

func newMockAirGappedSigningDevice(networkPassphrase string, keys ...*keypair.Full) *mockAirGappedSigningDevice {
	return &mockAirGappedSigningDevice{
		networkPassphrase: networkPassphrase,
		coldKeys:          keys,
	}
}

func (d *mockAirGappedSigningDevice) SignEnvelopeXDR(unsignedXDR string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(unsignedXDR)
	if err != nil {
		return "", err
	}
	tx, ok := genericTx.Transaction()
	if !ok || tx == nil {
		return "", errors.New("air-gapped device only signs standard Stellar transactions")
	}
	signedTx, err := tx.Sign(d.networkPassphrase, d.coldKeys...)
	if err != nil {
		return "", err
	}
	return signedTx.Base64()
}

type mockOfflineHorizonClient struct {
	submitCount atomic.Int32
	lastHash    string
	submitErr   error
}

func (m *mockOfflineHorizonClient) LoadAccount(accountID string) (horizon.Account, error) {
	return horizon.Account{Sequence: 500}, nil
}

func (m *mockOfflineHorizonClient) SubmitTransaction(tx *txnbuild.Transaction) (horizon.Transaction, error) {
	if m.submitErr != nil {
		return horizon.Transaction{}, m.submitErr
	}
	m.submitCount.Add(1)
	h, _ := tx.HashHex(network.TestNetworkPassphrase)
	m.lastHash = h
	return horizon.Transaction{
		Hash:       h,
		Ledger:     1042,
		Successful: true,
	}, nil
}

func (m *mockOfflineHorizonClient) FindPathsStrict(sourceAccount, destAccount, destAsset, destIssuer, destAmount string) ([]horizon.Path, error) {
	return nil, nil
}

func (m *mockOfflineHorizonClient) TransactionDetail(hash string) (horizon.Transaction, error) {
	return horizon.Transaction{}, nil
}

func (m *mockOfflineHorizonClient) OperationsForTransaction(hash string) ([]operations.Operation, error) {
	return nil, nil
}

func (m *mockOfflineHorizonClient) Payments(accountID, cursor string, limit uint) ([]operations.Operation, error) {
	return nil, nil
}

func (m *mockOfflineHorizonClient) StreamPayments(ctx context.Context, accountID, cursor string, handler func(operations.Operation) error) error {
	return nil
}

func (m *mockOfflineHorizonClient) Offers(accountID string, limit uint) ([]horizon.Offer, error) {
	return nil, nil
}

func TestOfflineSigningEngine_EndToEndWithMockAirGappedDevice(t *testing.T) {
	coldKey1, _ := keypair.Random()
	coldKey2, _ := keypair.Random()
	coldKey3, _ := keypair.Random()
	destKP, _ := keypair.Random()

	txRepo := NewInMemoryEnvelopeRepository()
	horizonMock := &mockOfflineHorizonClient{}
	ledgerProvider := NewStaticLedgerSequenceProvider(1000)

	// Configure 2-of-3 multi-sig cold-storage authorization
	authorizedSigners := []string{coldKey1.Address(), coldKey2.Address(), coldKey3.Address()}
	engine := NewOfflineSigningEngine(
		txRepo,
		horizonMock,
		ledgerProvider,
		"testnet",
		WithDefaultAuthorizedSigners(authorizedSigners, 2),
	)
	handler := NewOfflineSigningHandler(engine)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	// Step 1: Generate unsigned transaction XDR envelope and store in transaction_repo
	srcAccount := txnbuild.NewSimpleAccount(coldKey1.Address(), 100)
	genRecord, err := engine.GenerateUnsignedEnvelope(context.Background(), GenerateEnvelopeRequest{
		TransactionID: "tx-cold-001",
		SourceAccount: &srcAccount,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: destKP.Address(),
				Asset:       txnbuild.NativeAsset{},
				Amount:      "250000.0000000",
			},
		},
		ExpirationLedgers:  80,
		AuthorizedSigners:  authorizedSigners,
		RequiredSignatures: 2,
	})
	if err != nil {
		t.Fatalf("GenerateUnsignedEnvelope failed: %v", err)
	}
	if genRecord.GeneratedAtLedger != 1000 || genRecord.ExpiresAtLedger != 1080 {
		t.Fatalf("expected ledger bounds [1000, 1080], got [%d, %d]", genRecord.GeneratedAtLedger, genRecord.ExpiresAtLedger)
	}
	if genRecord.Status != EnvelopeStatusPendingSignature {
		t.Fatalf("expected status %s, got %s", EnvelopeStatusPendingSignature, genRecord.Status)
	}

	// Step 2: Air-gapped system retrieves unsigned XDR via GET /offline/transactions/{id}/xdr
	getResp, err := http.Get(srv.URL + "/offline/transactions/tx-cold-001/xdr")
	if err != nil {
		t.Fatalf("GET unsigned XDR failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from GET unsigned XDR, got %d", getResp.StatusCode)
	}

	var fetchedRecord OfflineEnvelopeRecord
	if err := json.NewDecoder(getResp.Body).Decode(&fetchedRecord); err != nil {
		t.Fatalf("decode fetched envelope: %v", err)
	}
	if fetchedRecord.UnsignedXDR == "" || fetchedRecord.UnsignedXDR != genRecord.UnsignedXDR {
		t.Fatal("fetched unsigned XDR did not match stored record")
	}

	// Step 3: Mock air-gapped signing device signs the XDR offline using 2 of the 3 authorized cold keys
	airGappedDevice := newMockAirGappedSigningDevice(network.TestNetworkPassphrase, coldKey1, coldKey2)
	signedXDR, err := airGappedDevice.SignEnvelopeXDR(fetchedRecord.UnsignedXDR)
	if err != nil {
		t.Fatalf("air-gapped device signing failed: %v", err)
	}

	// Advance ledger to 1040 (within the 1080 deadline)
	ledgerProvider.SetLedger(1040)

	// Step 4: Air-gapped bridge uploads signed envelope via POST /offline/transactions/{id}/sign
	uploadPayload, _ := json.Marshal(SubmitSignedEnvelopeRequest{
		SignedXDR: signedXDR,
	})
	postResp, err := http.Post(
		srv.URL+"/offline/transactions/tx-cold-001/sign",
		"application/json",
		bytes.NewReader(uploadPayload),
	)
	if err != nil {
		t.Fatalf("POST signed XDR failed: %v", err)
	}
	defer postResp.Body.Close()
	if postResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from POST signed XDR, got %d", postResp.StatusCode)
	}

	var finalRecord OfflineEnvelopeRecord
	if err := json.NewDecoder(postResp.Body).Decode(&finalRecord); err != nil {
		t.Fatalf("decode final record: %v", err)
	}

	if finalRecord.Status != EnvelopeStatusBroadcasted {
		t.Fatalf("expected status %s, got %s", EnvelopeStatusBroadcasted, finalRecord.Status)
	}
	if len(finalRecord.VerifiedSigners) != 2 {
		t.Fatalf("expected 2 verified signers, got %d", len(finalRecord.VerifiedSigners))
	}
	if horizonMock.submitCount.Load() != 1 {
		t.Fatalf("expected exactly 1 broadcast to Horizon, got %d", horizonMock.submitCount.Load())
	}
	if horizonMock.lastHash != genRecord.TxHash {
		t.Fatalf("expected broadcasted tx hash %s, got %s", genRecord.TxHash, horizonMock.lastHash)
	}
}

func TestOfflineSigningEngine_RejectsUnauthorizedAndInsufficientSignatures(t *testing.T) {
	authKey1, _ := keypair.Random()
	authKey2, _ := keypair.Random()
	rogueKey, _ := keypair.Random()
	destKP, _ := keypair.Random()

	txRepo := NewInMemoryEnvelopeRepository()
	horizonMock := &mockOfflineHorizonClient{}
	ledgerProvider := NewStaticLedgerSequenceProvider(2000)

	engine := NewOfflineSigningEngine(txRepo, horizonMock, ledgerProvider, "testnet")

	srcAccount := txnbuild.NewSimpleAccount(authKey1.Address(), 200)
	rec, err := engine.GenerateUnsignedEnvelope(context.Background(), GenerateEnvelopeRequest{
		TransactionID: "tx-sec-001",
		SourceAccount: &srcAccount,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: destKP.Address(),
				Asset:       txnbuild.NativeAsset{},
				Amount:      "500.0000000",
			},
		},
		AuthorizedSigners:  []string{authKey1.Address(), authKey2.Address()},
		RequiredSignatures: 2,
	})
	if err != nil {
		t.Fatalf("GenerateUnsignedEnvelope failed: %v", err)
	}

	// 1. Rogue air-gapped device signs with an unauthorized key -> must be rejected before broadcast
	rogueDevice := newMockAirGappedSigningDevice(network.TestNetworkPassphrase, rogueKey)
	rogueXDR, err := rogueDevice.SignEnvelopeXDR(rec.UnsignedXDR)
	if err != nil {
		t.Fatalf("rogue sign failed: %v", err)
	}

	_, err = engine.SubmitSignedEnvelope(context.Background(), SubmitSignedEnvelopeRequest{
		TransactionID: "tx-sec-001",
		SignedXDR:     rogueXDR,
	})
	if !errors.Is(err, ErrUnauthorizedSignature) {
		t.Fatalf("expected ErrUnauthorizedSignature for rogue key, got %v", err)
	}
	if horizonMock.submitCount.Load() != 0 {
		t.Fatalf("expected 0 broadcasts for unauthorized signature, got %d", horizonMock.submitCount.Load())
	}

	// 2. Device signs with only 1 authorized key when 2 are required -> must be rejected
	partialDevice := newMockAirGappedSigningDevice(network.TestNetworkPassphrase, authKey1)
	partialXDR, err := partialDevice.SignEnvelopeXDR(rec.UnsignedXDR)
	if err != nil {
		t.Fatalf("partial sign failed: %v", err)
	}

	_, err = engine.SubmitSignedEnvelope(context.Background(), SubmitSignedEnvelopeRequest{
		TransactionID: "tx-sec-001",
		SignedXDR:     partialXDR,
	})
	if !errors.Is(err, ErrInsufficientSignatures) {
		t.Fatalf("expected ErrInsufficientSignatures for 1/2 signatures, got %v", err)
	}

	// 3. Compromised device tampers with payment amount/destination before signing -> must be rejected
	tamperedSrc := txnbuild.NewSimpleAccount(authKey1.Address(), 200)
	tamperedTx, _ := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &tamperedSrc,
		IncrementSequenceNum: true,
		BaseFee:              int64(txnbuild.MinBaseFee),
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: rogueKey.Address(),
				Asset:       txnbuild.NativeAsset{},
				Amount:      "999999.0000000",
			},
		},
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewInfiniteTimeout(),
			LedgerBounds: &txnbuild.LedgerBounds{
				MinLedger: 2000,
				MaxLedger: 2100,
			},
		},
	})
	tamperedSigned, _ := tamperedTx.Sign(network.TestNetworkPassphrase, authKey1, authKey2)
	tamperedXDR, _ := tamperedSigned.Base64()

	_, err = engine.SubmitSignedEnvelope(context.Background(), SubmitSignedEnvelopeRequest{
		TransactionID: "tx-sec-001",
		SignedXDR:     tamperedXDR,
	})
	if !errors.Is(err, ErrEnvelopeTampered) {
		t.Fatalf("expected ErrEnvelopeTampered for modified transaction payload, got %v", err)
	}
}

func TestOfflineSigningEngine_EnforcesStrict100LedgerExpirationDeadline(t *testing.T) {
	coldKey, _ := keypair.Random()
	destKP, _ := keypair.Random()

	txRepo := NewInMemoryEnvelopeRepository()
	horizonMock := &mockOfflineHorizonClient{}
	ledgerProvider := NewStaticLedgerSequenceProvider(5000)

	engine := NewOfflineSigningEngine(txRepo, horizonMock, ledgerProvider, "testnet")

	// 1. Requesting an expiration deadline > 100 ledgers from generation must fail immediately
	srcAccount := txnbuild.NewSimpleAccount(coldKey.Address(), 10)
	_, err := engine.GenerateUnsignedEnvelope(context.Background(), GenerateEnvelopeRequest{
		TransactionID: "tx-over-100",
		SourceAccount: &srcAccount,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: destKP.Address(),
				Asset:       txnbuild.NativeAsset{},
				Amount:      "100.0000000",
			},
		},
		ExpirationLedgers:  101,
		AuthorizedSigners:  []string{coldKey.Address()},
		RequiredSignatures: 1,
	})
	if !errors.Is(err, ErrMaxLedgerWindowExceeded) {
		t.Fatalf("expected ErrMaxLedgerWindowExceeded when requesting 101 ledgers, got %v", err)
	}

	// 2. Generate a valid envelope with the maximum 100-ledger deadline (ledgers 5000 -> 5100)
	srcAccount2 := txnbuild.NewSimpleAccount(coldKey.Address(), 10)
	rec, err := engine.GenerateUnsignedEnvelope(context.Background(), GenerateEnvelopeRequest{
		TransactionID: "tx-expire-100",
		SourceAccount: &srcAccount2,
		Operations: []txnbuild.Operation{
			&txnbuild.Payment{
				Destination: destKP.Address(),
				Asset:       txnbuild.NativeAsset{},
				Amount:      "100.0000000",
			},
		},
		ExpirationLedgers:  100,
		AuthorizedSigners:  []string{coldKey.Address()},
		RequiredSignatures: 1,
	})
	if err != nil {
		t.Fatalf("expected 100-ledger envelope generation to succeed, got %v", err)
	}
	if rec.ExpiresAtLedger != 5100 {
		t.Fatalf("expected ExpiresAtLedger=5100, got %d", rec.ExpiresAtLedger)
	}

	// Sign the envelope on the air-gapped device while still valid
	device := newMockAirGappedSigningDevice(network.TestNetworkPassphrase, coldKey)
	signedXDR, err := device.SignEnvelopeXDR(rec.UnsignedXDR)
	if err != nil {
		t.Fatalf("SignEnvelopeXDR failed: %v", err)
	}

	// Advance ledger to 5101 (101 ledgers after generation -> past the 100-ledger deadline)
	ledgerProvider.SetLedger(5101)

	// Both retrieval and upload must now fail with ErrEnvelopeExpired
	_, err = engine.GetUnsignedEnvelope(context.Background(), "tx-expire-100")
	if !errors.Is(err, ErrEnvelopeExpired) {
		t.Fatalf("expected ErrEnvelopeExpired on GET at ledger 5101, got %v", err)
	}

	_, err = engine.SubmitSignedEnvelope(context.Background(), SubmitSignedEnvelopeRequest{
		TransactionID: "tx-expire-100",
		SignedXDR:     signedXDR,
	})
	if !errors.Is(err, ErrEnvelopeExpired) {
		t.Fatalf("expected ErrEnvelopeExpired on POST at ledger 5101, got %v", err)
	}
	if horizonMock.submitCount.Load() != 0 {
		t.Fatalf("expected 0 Horizon broadcasts for expired envelope, got %d", horizonMock.submitCount.Load())
	}
}
