package stellar

import (
	"context"
	"fmt"

	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

type StreamConfig struct {
	Asset       string
	Sender      string
	Recipient   string
	TotalAmount int64
	StartLedger uint64
	EndLedger   uint64
}

type PaymentStreamService struct {
	client     SorobanClient
	contractID string
	account    string
}

func NewPaymentStreamService(client SorobanClient, contractID, account string) *PaymentStreamService {
	return &PaymentStreamService{
		client:     client,
		contractID: contractID,
		account:    account,
	}
}

func (s *PaymentStreamService) CreateStream(ctx context.Context, config StreamConfig) error {
	binding := NewPaymentStreamContractBinding(s.client, s.contractID, s.account)
	return binding.Initialize(ctx, config.Asset, config.Sender, config.Recipient, config.TotalAmount, config.StartLedger, config.EndLedger)
}

func (s *PaymentStreamService) WithdrawFromStream(ctx context.Context, amount int64) error {
	binding := NewPaymentStreamContractBinding(s.client, s.contractID, s.account)
	return binding.Withdraw(ctx, amount)
}

func (s *PaymentStreamService) ClaimableBalance(ctx context.Context) (int64, error) {
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{},
	}

	result, err := s.client.SimulateInvocation(ctx, s.account, op)
	if err != nil {
		return 0, fmt.Errorf("simulate claimable balance query: %w", err)
	}

	return 0, nil
}

func (s *PaymentStreamService) Cancel(ctx context.Context) error {
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{},
	}

	tx, err := s.client.PrepareInvocation(ctx, s.account, op)
	if err != nil {
		return fmt.Errorf("prepare stream cancellation: %w", err)
	}

	_, err = s.client.SubmitTransaction(ctx, tx)
	return err
}

type StreamMetrics struct {
	VestedAmount   int64
	ClaimedAmount  int64
	RemainingVest  int64
	StreamProgress float64
}

func (s *PaymentStreamService) GetMetrics(ctx context.Context) (*StreamMetrics, error) {
	return &StreamMetrics{
		VestedAmount:  0,
		ClaimedAmount: 0,
		RemainingVest: 0,
		StreamProgress: 0.0,
	}, nil
}
