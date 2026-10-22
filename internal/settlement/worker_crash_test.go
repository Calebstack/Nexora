package settlement

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/queue"
	"github.com/hibiken/asynq"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

const (
	crashHelperEnv  = "NEXORA_SETTLEMENT_CRASH_HELPER"
	crashStateEnv   = "NEXORA_SETTLEMENT_CRASH_STATE"
	crashStartedEnv = "NEXORA_SETTLEMENT_CRASH_STARTED"
)

type crashTxState struct {
	Transaction             domain.Transaction `json:"transaction"`
	BroadcastOutcomeUnknown bool               `json:"broadcast_outcome_unknown"`
}

type crashTxRepo struct {
	*fakeTxRepo
	path string
}

func newCrashTxRepo(path string, tx *domain.Transaction) *crashTxRepo {
	return &crashTxRepo{fakeTxRepo: newFakeTxRepo(tx), path: path}
}

func (r *crashTxRepo) GetByID(_ context.Context, id string) (*domain.Transaction, error) {
	tx, err := r.read()
	if err != nil {
		return nil, err
	}
	if tx.ID != id {
		return nil, domain.ErrTransactionNotFound
	}
	return tx, nil
}

func (r *crashTxRepo) ClaimForSubmission(_ context.Context, id string) error {
	tx, err := r.read()
	if err != nil {
		return err
	}
	if tx.ID != id {
		return domain.ErrTransactionNotFound
	}
	if tx.Status != domain.StatusPending || tx.TxHash != "" || tx.BroadcastOutcomeUnknown {
		return domain.ErrConcurrentUpdate
	}
	tx.Status = domain.StatusSubmitted
	tx.BroadcastOutcomeUnknown = false
	return r.write(tx)
}

func (r *crashTxRepo) UpdateStatus(_ context.Context, id string, status domain.TransactionStatus, txHash string) error {
	tx, err := r.read()
	if err != nil {
		return err
	}
	if tx.ID != id {
		return domain.ErrTransactionNotFound
	}
	tx.Status = status
	if txHash != "" {
		tx.TxHash = txHash
		tx.BroadcastOutcomeUnknown = true
	}
	return r.write(tx)
}

func (r *crashTxRepo) read() (*domain.Transaction, error) {
	return readCrashTransaction(r.path)
}

func readCrashTransaction(path string) (*domain.Transaction, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read crash-test transaction: %w", err)
	}
	var state crashTxState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("decode crash-test transaction: %w", err)
	}
	state.Transaction.BroadcastOutcomeUnknown = state.BroadcastOutcomeUnknown
	return &state.Transaction, nil
}

func (r *crashTxRepo) write(tx *domain.Transaction) error {
	data, err := json.Marshal(crashTxState{
		Transaction:             *tx,
		BroadcastOutcomeUnknown: tx.BroadcastOutcomeUnknown,
	})
	if err != nil {
		return fmt.Errorf("encode crash-test transaction: %w", err)
	}
	if err := os.WriteFile(r.path, data, 0600); err != nil {
		return fmt.Errorf("persist crash-test transaction: %w", err)
	}
	return nil
}

type blockingCrashStellar struct {
	*fakeStellarClient
	startedPath string
}

func (s *blockingCrashStellar) SubmitTransaction(_ *txnbuild.Transaction) (horizon.Transaction, error) {
	if err := os.WriteFile(s.startedPath, []byte("broadcast started"), 0600); err != nil {
		panic(err)
	}
	time.Sleep(time.Hour)
	return horizon.Transaction{}, nil
}

func TestSettlementWorkerCrashHelper(t *testing.T) {
	if os.Getenv(crashHelperEnv) != "1" {
		return
	}

	statePath := os.Getenv(crashStateEnv)
	startedPath := os.Getenv(crashStartedEnv)
	tx, err := readCrashTransaction(statePath)
	if err != nil {
		t.Fatal(err)
	}

	src, dst := testWallets(t)
	src.ID = "src-wallet"
	dst.ID = "dst-wallet"
	wallets := &fakeWalletRepo{wallets: map[string]*domain.Wallet{
		src.ID: src,
		dst.ID: dst,
	}}
	stellarClient := &blockingCrashStellar{
		fakeStellarClient: &fakeStellarClient{},
		startedPath:       startedPath,
	}
	engine := NewEngine(newCrashTxRepo(statePath, tx), wallets, &fakeFeesService{}, stellarClient, identitySigner{}, "testnet", nil, testFeeWallet)
	task, err := json.Marshal(queue.ProcessTransferPayload{TransactionID: tx.ID})
	if err != nil {
		t.Fatal(err)
	}

	_ = NewWorker(engine).HandleProcessTransfer(
		context.Background(),
		asynq.NewTask(queue.TypeProcessTransfer, task),
	)
	t.Fatal("worker returned instead of blocking inside the broadcast")
}

func TestWorkerRecoveryAfterKillDuringBroadcastDoesNotResubmit(t *testing.T) {
	tempDir := t.TempDir()
	statePath := filepath.Join(tempDir, "transaction.json")
	startedPath := filepath.Join(tempDir, "broadcast-started")
	tx := &domain.Transaction{
		ID: "crash-tx", Status: domain.StatusPending,
		FromWallet: "src-wallet", ToWallet: "dst-wallet",
		Asset: "XLM", Amount: decimal.NewFromInt(10), Fee: decimal.Zero,
		CreatedAt: time.Now().UTC().Add(-time.Hour),
	}
	data, err := json.Marshal(crashTxState{Transaction: *tx})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, data, 0600); err != nil {
		t.Fatal(err)
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestSettlementWorkerCrashHelper$")
	cmd.Env = append(os.Environ(),
		crashHelperEnv+"=1",
		crashStateEnv+"="+statePath,
		crashStartedEnv+"="+startedPath,
	)
	if err := cmd.Start(); err != nil {
		t.Fatalf("start crash-test worker: %v", err)
	}
	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		select {
		case err := <-waitCh:
			t.Fatalf("worker exited before reaching the broadcast: %v", err)
		default:
		}
		if _, err := os.Stat(startedPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = cmd.Process.Kill()
			<-waitCh
			t.Fatal("worker did not reach the broadcast before timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := cmd.Process.Kill(); err != nil {
		<-waitCh
		t.Fatalf("kill worker during broadcast: %v", err)
	}
	if err := <-waitCh; err == nil {
		t.Fatal("worker exited successfully instead of being killed")
	}

	repo, err := readCrashTransaction(statePath)
	if err != nil {
		t.Fatalf("read transaction after worker kill: %v", err)
	}
	if repo.Status != domain.StatusSubmitted || repo.TxHash == "" || !repo.BroadcastOutcomeUnknown {
		t.Fatalf("broadcast intent was not persisted before kill: %+v", repo)
	}

	stl := &fakeStellarClient{submitFunc: alwaysSucceeds()}
	engine := NewEngine(
		newCrashTxRepo(statePath, repo),
		&fakeWalletRepo{},
		&fakeFeesService{},
		stl,
		identitySigner{},
		"testnet",
		nil,
		testFeeWallet,
	)
	payload, err := json.Marshal(queue.ProcessTransferPayload{TransactionID: tx.ID})
	if err != nil {
		t.Fatal(err)
	}
	if err := NewWorker(engine).HandleProcessTransfer(
		context.Background(),
		asynq.NewTask(queue.TypeProcessTransfer, payload),
	); err != nil {
		t.Fatalf("redeliver transfer after worker kill: %v", err)
	}
	if got := stl.calls(); got != 0 {
		t.Fatalf("redelivered hashed transaction was broadcast %d times", got)
	}
}
