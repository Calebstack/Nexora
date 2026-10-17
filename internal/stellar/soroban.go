package stellar

import (
	"context"
	"fmt"
	"net/http"
	"time"

	rpcclient "github.com/stellar/go/clients/rpcclient"
	"github.com/stellar/go/network"
	protocol "github.com/stellar/go/protocols/rpc"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

const (
	defaultRPCTimeout = 30 * time.Second
	sorobanTxTimeout  = 300
)

// NetworkPassphraseFor maps a Nexora network name onto its Stellar passphrase.
func NetworkPassphraseFor(stellarNetwork string) string {
	if stellarNetwork == "mainnet" || stellarNetwork == "public" {
		return network.PublicNetworkPassphrase
	}
	return network.TestNetworkPassphrase
}

// SorobanClient is the interface Nexora uses to talk to a Soroban RPC node.
// Contract invocations differ from classic Stellar operations: every call must
// be simulated first to discover its ledger footprint, authorization entries
// and resource fee before it can be signed and submitted.
type SorobanClient interface {
	// PrepareInvocation simulates op and returns a transaction with the
	// simulated footprint, authorization entries and resource fee applied,
	// ready to be signed.
	PrepareInvocation(ctx context.Context, sourceAccount string, op *txnbuild.InvokeHostFunction) (*txnbuild.Transaction, error)
	// SimulateInvocation runs op through simulation only and returns its
	// decoded return value. Nothing is submitted to the network.
	SimulateInvocation(ctx context.Context, sourceAccount string, op *txnbuild.InvokeHostFunction) (xdr.ScVal, error)
	// SubmitTransaction submits a signed Soroban transaction and returns its hash.
	SubmitTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, error)
	// NetworkPassphrase is needed to derive Stellar Asset Contract addresses and
	// to sign transactions for the right network.
	NetworkPassphrase() string
}

type sorobanClient struct {
	rpc        *rpcclient.Client
	passphrase string
}

func NewSorobanClient(rpcURL, stellarNetwork string) SorobanClient {
	return &sorobanClient{
		rpc:        rpcclient.NewClient(rpcURL, &http.Client{Timeout: defaultRPCTimeout}),
		passphrase: NetworkPassphraseFor(stellarNetwork),
	}
}

func (c *sorobanClient) NetworkPassphrase() string { return c.passphrase }

func (c *sorobanClient) PrepareInvocation(ctx context.Context, sourceAccount string, op *txnbuild.InvokeHostFunction) (*txnbuild.Transaction, error) {
	sim, tx, err := c.simulate(ctx, sourceAccount, op)
	if err != nil {
		return nil, err
	}

	var sorobanData xdr.SorobanTransactionData
	if err := xdr.SafeUnmarshalBase64(sim.TransactionDataXDR, &sorobanData); err != nil {
		return nil, fmt.Errorf("decode simulated soroban transaction data: %w", err)
	}

	auth, err := decodeAuthEntries(sim)
	if err != nil {
		return nil, err
	}

	prepared := *op
	prepared.Auth = auth
	prepared.Ext = xdr.TransactionExt{V: 1, SorobanData: &sorobanData}

	// tx was built with IncrementSequenceNum, so its source account already
	// carries the sequence number the prepared transaction must reuse.
	source := tx.SourceAccount()

	// The simulated resource fee is charged on top of the classic base fee, so
	// it has to be folded into BaseFee rather than replacing it.
	rebuilt, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &source,
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{&prepared},
		BaseFee:              txnbuild.MinBaseFee + sim.MinResourceFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(sorobanTxTimeout)},
	})
	if err != nil {
		return nil, fmt.Errorf("build prepared soroban transaction: %w", err)
	}

	return rebuilt, nil
}

func (c *sorobanClient) SimulateInvocation(ctx context.Context, sourceAccount string, op *txnbuild.InvokeHostFunction) (xdr.ScVal, error) {
	sim, _, err := c.simulate(ctx, sourceAccount, op)
	if err != nil {
		return xdr.ScVal{}, err
	}

	if len(sim.Results) == 0 || sim.Results[0].ReturnValueXDR == nil {
		return xdr.ScVal{}, fmt.Errorf("simulation returned no value")
	}

	var result xdr.ScVal
	if err := xdr.SafeUnmarshalBase64(*sim.Results[0].ReturnValueXDR, &result); err != nil {
		return xdr.ScVal{}, fmt.Errorf("decode simulation return value: %w", err)
	}
	return result, nil
}

func (c *sorobanClient) SubmitTransaction(ctx context.Context, tx *txnbuild.Transaction) (string, error) {
	envelope, err := tx.Base64()
	if err != nil {
		return "", fmt.Errorf("encode transaction envelope: %w", err)
	}

	resp, err := c.rpc.SendTransaction(ctx, protocol.SendTransactionRequest{Transaction: envelope})
	if err != nil {
		return "", fmt.Errorf("send soroban transaction: %w", err)
	}
	if resp.Status == "ERROR" {
		return "", fmt.Errorf("soroban rpc rejected transaction %s: %s", resp.Hash, resp.ErrorResultXDR)
	}

	return resp.Hash, nil
}

// simulate builds an unprepared transaction around op and runs it through the
// RPC simulator, returning both the response and the transaction it simulated
// so callers can reuse its source account and sequence number.
func (c *sorobanClient) simulate(ctx context.Context, sourceAccount string, op *txnbuild.InvokeHostFunction) (*protocol.SimulateTransactionResponse, *txnbuild.Transaction, error) {
	acct, err := c.rpc.LoadAccount(ctx, sourceAccount)
	if err != nil {
		return nil, nil, fmt.Errorf("load account %s from soroban rpc: %w", sourceAccount, err)
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        acct,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(sorobanTxTimeout)},
	})
	if err != nil {
		return nil, nil, fmt.Errorf("build soroban transaction: %w", err)
	}

	envelope, err := tx.Base64()
	if err != nil {
		return nil, nil, fmt.Errorf("encode transaction envelope: %w", err)
	}

	resp, err := c.rpc.SimulateTransaction(ctx, protocol.SimulateTransactionRequest{Transaction: envelope})
	if err != nil {
		return nil, nil, fmt.Errorf("simulate soroban transaction: %w", err)
	}
	if resp.Error != "" {
		return nil, nil, fmt.Errorf("soroban simulation failed: %s", resp.Error)
	}

	return &resp, tx, nil
}

func decodeAuthEntries(sim *protocol.SimulateTransactionResponse) ([]xdr.SorobanAuthorizationEntry, error) {
	if len(sim.Results) == 0 || sim.Results[0].AuthXDR == nil {
		return nil, nil
	}

	entries := make([]xdr.SorobanAuthorizationEntry, 0, len(*sim.Results[0].AuthXDR))
	for _, encoded := range *sim.Results[0].AuthXDR {
		var entry xdr.SorobanAuthorizationEntry
		if err := xdr.SafeUnmarshalBase64(encoded, &entry); err != nil {
			return nil, fmt.Errorf("decode simulated authorization entry: %w", err)
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

type EscrowContractBinding struct {
	client       SorobanClient
	contractID   string
	sourceAccount string
}

func NewEscrowContractBinding(client SorobanClient, contractID, sourceAccount string) *EscrowContractBinding {
	return &EscrowContractBinding{
		client:        client,
		contractID:    contractID,
		sourceAccount: sourceAccount,
	}
}

func (e *EscrowContractBinding) Initialize(ctx context.Context, asset, sender, recipient string, amount int64, threshold, signersCount uint32, deadline uint64) error {
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{},
	}

	tx, err := e.client.PrepareInvocation(ctx, e.sourceAccount, op)
	if err != nil {
		return fmt.Errorf("prepare escrow initialize: %w", err)
	}

	_, err = e.client.SubmitTransaction(ctx, tx)
	return err
}

type FeeSlitterContractBinding struct {
	client        SorobanClient
	contractID    string
	sourceAccount string
}

func NewFeeSplitterContractBinding(client SorobanClient, contractID, sourceAccount string) *FeeSlitterContractBinding {
	return &FeeSlitterContractBinding{
		client:        client,
		contractID:    contractID,
		sourceAccount: sourceAccount,
	}
}

func (f *FeeSlitterContractBinding) SplitFee(ctx context.Context, amount int64) error {
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{},
	}

	tx, err := f.client.PrepareInvocation(ctx, f.sourceAccount, op)
	if err != nil {
		return fmt.Errorf("prepare fee splitter split: %w", err)
	}

	_, err = f.client.SubmitTransaction(ctx, tx)
	return err
}

type PaymentStreamContractBinding struct {
	client        SorobanClient
	contractID    string
	sourceAccount string
}

func NewPaymentStreamContractBinding(client SorobanClient, contractID, sourceAccount string) *PaymentStreamContractBinding {
	return &PaymentStreamContractBinding{
		client:        client,
		contractID:    contractID,
		sourceAccount: sourceAccount,
	}
}

func (p *PaymentStreamContractBinding) Initialize(ctx context.Context, asset, sender, recipient string, amount int64, startLedger, endLedger uint64) error {
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{},
	}

	tx, err := p.client.PrepareInvocation(ctx, p.sourceAccount, op)
	if err != nil {
		return fmt.Errorf("prepare payment stream initialize: %w", err)
	}

	_, err = p.client.SubmitTransaction(ctx, tx)
	return err
}

func (p *PaymentStreamContractBinding) Withdraw(ctx context.Context, amount int64) error {
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{},
	}

	tx, err := p.client.PrepareInvocation(ctx, p.sourceAccount, op)
	if err != nil {
		return fmt.Errorf("prepare payment stream withdraw: %w", err)
	}

	_, err = p.client.SubmitTransaction(ctx, tx)
	return err
}
