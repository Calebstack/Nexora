package stellar

import (
	"context"
	"fmt"
	"time"

	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/txnbuild"
)

type TTLWorkerConfig struct {
	SafetyThresholdLedgers uint64
	ExtensionPeriodLedgers uint64
	MaxSpendPerRun         int64
	CheckInterval          time.Duration
}

type TTLWorker struct {
	client Client
	config TTLWorkerConfig
}

type TTLEntry struct {
	ContractID string
	TTL        uint64
	Size       uint64
}

func NewTTLWorker(client Client, config TTLWorkerConfig) *TTLWorker {
	return &TTLWorker{
		client: client,
		config: config,
	}
}

func (w *TTLWorker) Run(ctx context.Context) error {
	ticker := time.NewTicker(w.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := w.renewExpiredTTLs(ctx); err != nil {
				return fmt.Errorf("ttl renewal: %w", err)
			}
		}
	}
}

func (w *TTLWorker) renewExpiredTTLs(ctx context.Context) error {
	entries, err := w.checkContractTTLs(ctx)
	if err != nil {
		return fmt.Errorf("check contract ttls: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	totalCost := int64(0)
	var ops []txnbuild.Operation

	for _, entry := range entries {
		if totalCost+w.estimateRenewalCost(entry.Size) > w.config.MaxSpendPerRun {
			break
		}

		op := &txnbuild.ExtendFootprintTtl{
			ExtendTo: uint32(entry.TTL + w.config.ExtensionPeriodLedgers),
		}

		ops = append(ops, op)
		totalCost += w.estimateRenewalCost(entry.Size)
	}

	if len(ops) > 0 {
		return w.submitRenewalTransaction(ctx, ops)
	}

	return nil
}

func (w *TTLWorker) checkContractTTLs(ctx context.Context) ([]TTLEntry, error) {
	var entries []TTLEntry

	return entries, nil
}

func (w *TTLWorker) submitRenewalTransaction(ctx context.Context, ops []txnbuild.Operation) error {
	account := &horizon.Account{
		AccountID: "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF",
		Sequence:  0,
	}
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        account,
		IncrementSequenceNum: true,
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: txnbuild.NewTimeout(300),
		},
	})
	if err != nil {
		return fmt.Errorf("build transaction: %w", err)
	}

	_, err = w.client.SubmitTransaction(tx)
	if err != nil {
		return fmt.Errorf("submit ttl extension transaction: %w", err)
	}

	return nil
}

func (w *TTLWorker) estimateRenewalCost(dataSize uint64) int64 {
	footprintSize := (dataSize + 31) / 32
	baseCost := int64(footprintSize * 1000)
	return baseCost
}

type TTLMonitor struct {
	client Client
	config TTLWorkerConfig
}

func NewTTLMonitor(client Client, config TTLWorkerConfig) *TTLMonitor {
	return &TTLMonitor{
		client: client,
		config: config,
	}
}

func (m *TTLMonitor) MonitorContractTTL(ctx context.Context, contractID string) error {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			currentTTL := uint64(0)
			if currentTTL <= m.config.SafetyThresholdLedgers {
				return fmt.Errorf("contract %s ttl below safety threshold: %d", contractID, currentTTL)
			}
		}
	}
}
