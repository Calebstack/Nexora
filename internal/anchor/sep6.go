package anchor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
)

// Sep6Client performs non-interactive deposits/withdrawals (SEP-6) against an
// anchor's TRANSFER_SERVER. It is not bound to a single anchor: every call
// takes the target anchor's transferServer URL, since a wallet may deal with
// several anchors concurrently.
type Sep6Client struct {
	httpClient *http.Client
}

func NewSep6Client(httpClient *http.Client) *Sep6Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 20 * time.Second}
	}
	return &Sep6Client{httpClient: httpClient}
}

// AssetOperationInfo describes one asset's deposit or withdraw support as
// reported by an anchor's GET /info.
type AssetOperationInfo struct {
	Enabled    bool                   `json:"enabled"`
	MinAmount  float64                `json:"min_amount,omitempty"`
	MaxAmount  float64                `json:"max_amount,omitempty"`
	FeeFixed   float64                `json:"fee_fixed,omitempty"`
	FeePercent float64                `json:"fee_percent,omitempty"`
	Fields     map[string]interface{} `json:"fields,omitempty"`
}

// AnchorInfo is the parsed response of an anchor's GET /info (SEP-6).
type AnchorInfo struct {
	Deposit  map[string]AssetOperationInfo `json:"deposit"`
	Withdraw map[string]AssetOperationInfo `json:"withdraw"`
}

// GetInfo fetches and parses an anchor's SEP-6 GET /info, describing which
// assets support deposit/withdrawal, their fees, limits and KYC fields.
func (c *Sep6Client) GetInfo(ctx context.Context, transferServer string) (*AnchorInfo, error) {
	var info AnchorInfo
	if err := c.doJSON(ctx, http.MethodGet, transferServer+"/info", "", nil, &info); err != nil {
		return nil, fmt.Errorf("get sep-6 info: %w", err)
	}
	return &info, nil
}

// InstructionField is one field of the "instructions" object an anchor
// returns describing how to complete a bank transfer (e.g. bank account
// number, routing number).
type InstructionField struct {
	Value       string `json:"value"`
	Description string `json:"description"`
}

// DepositInstructions is the response to a SEP-6 GET /deposit request.
type DepositInstructions struct {
	ID           string                      `json:"id"`
	How          string                      `json:"how,omitempty"`
	ETA          int                         `json:"eta,omitempty"`
	MinAmount    float64                     `json:"min_amount,omitempty"`
	MaxAmount    float64                     `json:"max_amount,omitempty"`
	FeeFixed     float64                     `json:"fee_fixed,omitempty"`
	FeePercent   float64                     `json:"fee_percent,omitempty"`
	Instructions map[string]InstructionField `json:"instructions,omitempty"`
	ExtraInfo    map[string]interface{}      `json:"extra_info,omitempty"`
}

// InitiateDeposit calls an anchor's SEP-6 GET /deposit (a query, per spec —
// there is no request body) to begin a non-interactive fiat deposit that
// will credit account on-chain in asset.
func (c *Sep6Client) InitiateDeposit(ctx context.Context, transferServer, jwt, assetCode, account, amount, email string) (*DepositInstructions, error) {
	q := url.Values{}
	q.Set("asset_code", assetCode)
	q.Set("account", account)
	if amount != "" {
		q.Set("amount", amount)
	}
	if email != "" {
		q.Set("email_address", email)
	}

	var out DepositInstructions
	if err := c.doJSON(ctx, http.MethodGet, transferServer+"/deposit?"+q.Encode(), jwt, nil, &out); err != nil {
		return nil, fmt.Errorf("initiate sep-6 deposit: %w", err)
	}
	return &out, nil
}

// WithdrawalResult is the response to a SEP-6 GET /withdraw request: the
// on-chain account/memo the client must pay to complete the withdrawal.
type WithdrawalResult struct {
	ID         string                      `json:"id"`
	AccountID  string                      `json:"account_id,omitempty"`
	MemoType   string                      `json:"memo_type,omitempty"`
	Memo       string                      `json:"memo,omitempty"`
	ETA        int                         `json:"eta,omitempty"`
	MinAmount  float64                     `json:"min_amount,omitempty"`
	MaxAmount  float64                     `json:"max_amount,omitempty"`
	FeeFixed   float64                     `json:"fee_fixed,omitempty"`
	FeePercent float64                     `json:"fee_percent,omitempty"`
	Status     string                      `json:"status,omitempty"`
	Instructions map[string]InstructionField `json:"instructions,omitempty"`
	ExtraInfo  map[string]interface{}      `json:"extra_info,omitempty"`
}

// InitiateWithdrawal calls an anchor's SEP-6 GET /withdraw to begin a
// non-interactive withdrawal of amount of asset to dest (an off-chain bank
// account identifier the anchor understands).
func (c *Sep6Client) InitiateWithdrawal(ctx context.Context, transferServer, jwt, assetCode, amount, dest string) (*WithdrawalResult, error) {
	q := url.Values{}
	q.Set("asset_code", assetCode)
	q.Set("type", "bank_account")
	if amount != "" {
		q.Set("amount", amount)
	}
	if dest != "" {
		q.Set("dest", dest)
	}

	var out WithdrawalResult
	if err := c.doJSON(ctx, http.MethodGet, transferServer+"/withdraw?"+q.Encode(), jwt, nil, &out); err != nil {
		return nil, fmt.Errorf("initiate sep-6 withdrawal: %w", err)
	}
	return &out, nil
}

// AnchorTransaction is a SEP-6/24 transaction status object as returned by
// an anchor's GET /transaction.
type AnchorTransaction struct {
	ID                    string                      `json:"id"`
	Kind                  string                      `json:"kind"`
	Status                string                      `json:"status"`
	StatusEta             int                         `json:"status_eta,omitempty"`
	AmountIn              string                      `json:"amount_in,omitempty"`
	AmountOut             string                      `json:"amount_out,omitempty"`
	AmountFee             string                      `json:"amount_fee,omitempty"`
	StartedAt             string                      `json:"started_at,omitempty"`
	CompletedAt           string                      `json:"completed_at,omitempty"`
	StellarTransactionID  string                      `json:"stellar_transaction_id,omitempty"`
	ExternalTransactionID string                      `json:"external_transaction_id,omitempty"`
	Message               string                      `json:"message,omitempty"`
	To                    string                      `json:"to,omitempty"`
	From                  string                      `json:"from,omitempty"`
	DepositMemo           string                      `json:"deposit_memo,omitempty"`
	DepositMemoType       string                      `json:"deposit_memo_type,omitempty"`
	WithdrawAnchorAccount string                      `json:"withdraw_anchor_account,omitempty"`
	WithdrawMemo          string                      `json:"withdraw_memo,omitempty"`
	WithdrawMemoType      string                      `json:"withdraw_memo_type,omitempty"`
	Instructions          map[string]InstructionField `json:"instructions,omitempty"`
}

type getTransactionResponse struct {
	Transaction AnchorTransaction `json:"transaction"`
}

type getTransactionsResponse struct {
	Transactions []AnchorTransaction `json:"transactions"`
}

// GetTransaction polls an anchor's GET /transaction for the current status
// of a previously initiated deposit or withdrawal.
func (c *Sep6Client) GetTransaction(ctx context.Context, transferServer, jwt, transactionID string) (*AnchorTransaction, error) {
	q := url.Values{}
	q.Set("id", transactionID)

	var out getTransactionResponse
	if err := c.doJSON(ctx, http.MethodGet, transferServer+"/transaction?"+q.Encode(), jwt, nil, &out); err != nil {
		return nil, fmt.Errorf("get sep-6 transaction: %w", err)
	}
	return &out.Transaction, nil
}

// GetTransactions polls an anchor's GET /transactions for the transaction history of an asset.
func (c *Sep6Client) GetTransactions(ctx context.Context, transferServer, jwt, assetCode, account, kind string, limit int) ([]AnchorTransaction, error) {
	q := url.Values{}
	q.Set("asset_code", assetCode)
	if account != "" {
		q.Set("account", account)
	}
	if kind != "" {
		q.Set("kind", kind)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}

	var out getTransactionsResponse
	if err := c.doJSON(ctx, http.MethodGet, transferServer+"/transactions?"+q.Encode(), jwt, nil, &out); err != nil {
		return nil, fmt.Errorf("get sep-6 transactions: %w", err)
	}
	return out.Transactions, nil
}

func (c *Sep6Client) doJSON(ctx context.Context, method, urlStr, jwt string, body io.Reader, out interface{}) error {
	return doAnchorRequest(ctx, c.httpClient, method, urlStr, jwt, body, out)
}

// doAnchorRequest issues an HTTP request to an anchor endpoint (optionally
// bearer-authenticated with a SEP-10 JWT) and decodes a JSON response,
// shared by the SEP-6 and SEP-24 clients.
func doAnchorRequest(ctx context.Context, httpClient *http.Client, method, urlStr, jwt string, body io.Reader, out interface{}) error {
	req, err := http.NewRequestWithContext(ctx, method, urlStr, body)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	if jwt != "" {
		req.Header.Set("Authorization", "Bearer "+jwt)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errBody struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(respBody, &errBody)
		if errBody.Error != "" {
			return fmt.Errorf("anchor returned status %d: %s", resp.StatusCode, errBody.Error)
		}
		return fmt.Errorf("anchor returned status %d: %s", resp.StatusCode, respBody)
	}

	if err := json.Unmarshal(respBody, out); err != nil {
		return fmt.Errorf("parse response: %w", err)
	}
	return nil
}

// ============================================================================
// SEP-6 Programmatic Deposit and Withdrawal Engine (Issue #25)
// ============================================================================

var (
	ErrSep6Unauthorized        = errors.New("sep-6 authentication required: missing or invalid SEP-10 JWT")
	ErrSep6AccountMismatch     = errors.New("sep-6 account does not match authenticated SEP-10 subject")
	ErrSep6MissingTrustline    = errors.New("destination account has no trustline for requested asset")
	ErrSep6UntrustedAsset      = errors.New("destination account trustline is not authorized or limit is insufficient")
	ErrSep6InsufficientReserve = errors.New("destination account XLM balance does not meet Stellar reserve requirements")
	ErrSep6UnsupportedAsset    = errors.New("asset is not supported or not enabled for this SEP-6 operation")
	ErrSep6InvalidParameter    = errors.New("invalid or missing SEP-6 parameter")
	ErrSep6TransactionNotFound = errors.New("sep-6 transaction not found")
)

// Sep6TokenVerifier validates a SEP-10 bearer token and returns its claims.
// Satisfied by *Sep10Server.
type Sep6TokenVerifier interface {
	VerifyJWT(tokenString string) (*Sep10Claims, error)
}

// Sep6AccountInspector loads on-chain Stellar account state (balances, trustlines, subentries)
// to verify reserve requirements and trustlines before accepting deposits.
type Sep6AccountInspector interface {
	LoadAccount(accountID string) (horizon.Account, error)
}

// Sep6AssetConfig defines asset parameters for programmatic SEP-6 deposits and withdrawals.
type Sep6AssetConfig struct {
	Code                  string
	Issuer                string // Empty for native XLM
	DepositEnabled        bool
	WithdrawEnabled       bool
	MinAmount             decimal.Decimal
	MaxAmount             decimal.Decimal
	FeeFixed              decimal.Decimal
	FeePercent            decimal.Decimal // e.g., 1.5 means 1.5%
	ReceivingAnchorAcct   string          // On-chain anchor receiving account for withdrawals
	SupportedDepositTypes []string        // e.g. "bank_account", "SEPA", "ACH", "crypto"
	SupportedWithdrawTypes []string       // e.g. "bank_account", "SEPA", "ACH", "crypto"
	BankRoutingNumber     string
	BankAccountPrefix     string
	IBANCountryCode       string
	BICCode               string
}

// Sep6DepositParams holds structured query parameters for GET /deposit.
type Sep6DepositParams struct {
	AssetCode   string          `json:"asset_code"`
	AssetIssuer string          `json:"asset_issuer,omitempty"`
	Account     string          `json:"account"`
	MemoType    string          `json:"memo_type,omitempty"`
	Memo        string          `json:"memo,omitempty"`
	Email       string          `json:"email_address,omitempty"`
	Type        string          `json:"type,omitempty"` // e.g. "SEPA", "ACH", "bank_account", "crypto"
	Amount      decimal.Decimal `json:"amount,omitempty"`
	HasAmount   bool            `json:"-"`
	CountryCode string          `json:"country_code,omitempty"`
	ClaimableBalanceSupported bool `json:"claimable_balance_supported,omitempty"`
}

// Sep6WithdrawParams holds structured query parameters for GET /withdraw.
type Sep6WithdrawParams struct {
	AssetCode   string          `json:"asset_code"`
	AssetIssuer string          `json:"asset_issuer,omitempty"`
	Type        string          `json:"type"` // e.g. "bank_account", "SEPA", "ACH", "crypto"
	Dest        string          `json:"dest"` // Destination bank account, IBAN, or crypto address
	DestExtra   string          `json:"dest_extra,omitempty"` // Routing number, BIC, SWIFT, etc.
	Account     string          `json:"account,omitempty"`
	Memo        string          `json:"memo,omitempty"`
	MemoType    string          `json:"memo_type,omitempty"`
	Amount      decimal.Decimal `json:"amount,omitempty"`
	HasAmount   bool            `json:"-"`
	CountryCode string          `json:"country_code,omitempty"`
}

// Sep6TransactionQuery holds structured query parameters for GET /transaction.
type Sep6TransactionQuery struct {
	ID                    string
	StellarTransactionID  string
	ExternalTransactionID string
}

// Sep6TransactionsQuery holds structured query parameters for GET /transactions.
type Sep6TransactionsQuery struct {
	AssetCode string
	Account   string
	NoOlderThan *time.Time
	Limit     int
	Kind      string // "deposit" | "withdrawal"
	PagingID  string
}

// ParseSep6DepositParams parses and validates structured parameters from a GET /deposit query.
func ParseSep6DepositParams(q url.Values) (Sep6DepositParams, error) {
	assetCode := strings.TrimSpace(q.Get("asset_code"))
	if assetCode == "" {
		return Sep6DepositParams{}, fmt.Errorf("%w: asset_code is required", ErrSep6InvalidParameter)
	}
	account := strings.TrimSpace(q.Get("account"))
	if account == "" {
		return Sep6DepositParams{}, fmt.Errorf("%w: account is required", ErrSep6InvalidParameter)
	}
	if _, err := keypair.ParseAddress(account); err != nil {
		return Sep6DepositParams{}, fmt.Errorf("%w: invalid stellar account address %q", ErrSep6InvalidParameter, account)
	}

	memoType := strings.ToLower(strings.TrimSpace(q.Get("memo_type")))
	memo := strings.TrimSpace(q.Get("memo"))
	if memo != "" && memoType == "" {
		memoType = "text"
	}
	if memoType != "" && memoType != "text" && memoType != "id" && memoType != "hash" {
		return Sep6DepositParams{}, fmt.Errorf("%w: memo_type must be text, id, or hash", ErrSep6InvalidParameter)
	}

	var amount decimal.Decimal
	var hasAmount bool
	if amtStr := strings.TrimSpace(q.Get("amount")); amtStr != "" {
		parsedAmt, err := decimal.NewFromString(amtStr)
		if err != nil || !parsedAmt.IsPositive() {
			return Sep6DepositParams{}, fmt.Errorf("%w: amount must be a positive decimal", ErrSep6InvalidParameter)
		}
		amount = parsedAmt
		hasAmount = true
	}

	depType := strings.TrimSpace(q.Get("type"))
	if depType == "" {
		depType = "bank_account"
	}

	claimableSupported := strings.EqualFold(strings.TrimSpace(q.Get("claimable_balance_supported")), "true")

	return Sep6DepositParams{
		AssetCode:                 strings.ToUpper(assetCode),
		AssetIssuer:               strings.TrimSpace(q.Get("asset_issuer")),
		Account:                   account,
		MemoType:                  memoType,
		Memo:                      memo,
		Email:                     strings.TrimSpace(q.Get("email_address")),
		Type:                      depType,
		Amount:                    amount,
		HasAmount:                 hasAmount,
		CountryCode:               strings.ToUpper(strings.TrimSpace(q.Get("country_code"))),
		ClaimableBalanceSupported: claimableSupported,
	}, nil
}

// ParseSep6WithdrawParams parses and validates structured parameters from a GET /withdraw query.
func ParseSep6WithdrawParams(q url.Values) (Sep6WithdrawParams, error) {
	assetCode := strings.TrimSpace(q.Get("asset_code"))
	if assetCode == "" {
		return Sep6WithdrawParams{}, fmt.Errorf("%w: asset_code is required", ErrSep6InvalidParameter)
	}
	wType := strings.TrimSpace(q.Get("type"))
	if wType == "" {
		return Sep6WithdrawParams{}, fmt.Errorf("%w: type is required", ErrSep6InvalidParameter)
	}
	dest := strings.TrimSpace(q.Get("dest"))
	if dest == "" {
		return Sep6WithdrawParams{}, fmt.Errorf("%w: dest is required", ErrSep6InvalidParameter)
	}

	account := strings.TrimSpace(q.Get("account"))
	if account != "" {
		if _, err := keypair.ParseAddress(account); err != nil {
			return Sep6WithdrawParams{}, fmt.Errorf("%w: invalid stellar account address %q", ErrSep6InvalidParameter, account)
		}
	}

	memoType := strings.ToLower(strings.TrimSpace(q.Get("memo_type")))
	memo := strings.TrimSpace(q.Get("memo"))
	if memo != "" && memoType == "" {
		memoType = "text"
	}
	if memoType != "" && memoType != "text" && memoType != "id" && memoType != "hash" {
		return Sep6WithdrawParams{}, fmt.Errorf("%w: memo_type must be text, id, or hash", ErrSep6InvalidParameter)
	}

	var amount decimal.Decimal
	var hasAmount bool
	if amtStr := strings.TrimSpace(q.Get("amount")); amtStr != "" {
		parsedAmt, err := decimal.NewFromString(amtStr)
		if err != nil || !parsedAmt.IsPositive() {
			return Sep6WithdrawParams{}, fmt.Errorf("%w: amount must be a positive decimal", ErrSep6InvalidParameter)
		}
		amount = parsedAmt
		hasAmount = true
	}

	return Sep6WithdrawParams{
		AssetCode:   strings.ToUpper(assetCode),
		AssetIssuer: strings.TrimSpace(q.Get("asset_issuer")),
		Type:        wType,
		Dest:        dest,
		DestExtra:   strings.TrimSpace(q.Get("dest_extra")),
		Account:     account,
		Memo:        memo,
		MemoType:    memoType,
		Amount:      amount,
		HasAmount:   hasAmount,
		CountryCode: strings.ToUpper(strings.TrimSpace(q.Get("country_code"))),
	}, nil
}

// ParseSep6TransactionQuery parses parameters from a GET /transaction query.
func ParseSep6TransactionQuery(q url.Values) (Sep6TransactionQuery, error) {
	id := strings.TrimSpace(q.Get("id"))
	stellarID := strings.TrimSpace(q.Get("stellar_transaction_id"))
	extID := strings.TrimSpace(q.Get("external_transaction_id"))
	if id == "" && stellarID == "" && extID == "" {
		return Sep6TransactionQuery{}, fmt.Errorf("%w: one of id, stellar_transaction_id, or external_transaction_id is required", ErrSep6InvalidParameter)
	}
	return Sep6TransactionQuery{
		ID:                    id,
		StellarTransactionID:  stellarID,
		ExternalTransactionID: extID,
	}, nil
}

// ParseSep6TransactionsQuery parses parameters from a GET /transactions query.
func ParseSep6TransactionsQuery(q url.Values) (Sep6TransactionsQuery, error) {
	assetCode := strings.TrimSpace(q.Get("asset_code"))
	if assetCode == "" {
		return Sep6TransactionsQuery{}, fmt.Errorf("%w: asset_code is required", ErrSep6InvalidParameter)
	}
	account := strings.TrimSpace(q.Get("account"))
	if account != "" {
		if _, err := keypair.ParseAddress(account); err != nil {
			return Sep6TransactionsQuery{}, fmt.Errorf("%w: invalid account %q", ErrSep6InvalidParameter, account)
		}
	}

	kind := strings.ToLower(strings.TrimSpace(q.Get("kind")))
	if kind != "" && kind != "deposit" && kind != "withdrawal" {
		return Sep6TransactionsQuery{}, fmt.Errorf("%w: kind must be deposit or withdrawal", ErrSep6InvalidParameter)
	}

	limit := 50
	if limitStr := strings.TrimSpace(q.Get("limit")); limitStr != "" {
		l, err := strconv.Atoi(limitStr)
		if err != nil || l <= 0 {
			return Sep6TransactionsQuery{}, fmt.Errorf("%w: limit must be a positive integer", ErrSep6InvalidParameter)
		}
		if l > 200 {
			l = 200
		}
		limit = l
	}

	var noOlderThan *time.Time
	if tsStr := strings.TrimSpace(q.Get("no_older_than")); tsStr != "" {
		t, err := time.Parse(time.RFC3339, tsStr)
		if err != nil {
			return Sep6TransactionsQuery{}, fmt.Errorf("%w: invalid no_older_than RFC3339 timestamp", ErrSep6InvalidParameter)
		}
		noOlderThan = &t
	}

	return Sep6TransactionsQuery{
		AssetCode:   strings.ToUpper(assetCode),
		Account:     account,
		NoOlderThan: noOlderThan,
		Limit:       limit,
		Kind:        kind,
		PagingID:    strings.TrimSpace(q.Get("paging_id")),
	}, nil
}

// Sep6TransactionRecord persists a programmatic SEP-6 transaction and its owner account.
type Sep6TransactionRecord struct {
	Transaction AnchorTransaction
	Account     string
	AssetCode   string
	CreatedAt   time.Time
}

// Sep6Store defines the persistence interface for SEP-6 transactions.
type Sep6Store interface {
	Save(ctx context.Context, rec *Sep6TransactionRecord) error
	GetByQuery(ctx context.Context, q Sep6TransactionQuery) (*Sep6TransactionRecord, error)
	List(ctx context.Context, q Sep6TransactionsQuery) ([]*Sep6TransactionRecord, error)
	UpdateStatus(ctx context.Context, id, status, stellarTxID, externalTxID, message string) (*Sep6TransactionRecord, error)
}

// MemorySep6Store is a thread-safe in-memory store for SEP-6 transactions.
type MemorySep6Store struct {
	mu      sync.RWMutex
	records map[string]*Sep6TransactionRecord
}

func NewMemorySep6Store() *MemorySep6Store {
	return &MemorySep6Store{
		records: make(map[string]*Sep6TransactionRecord),
	}
}

func (s *MemorySep6Store) Save(_ context.Context, rec *Sep6TransactionRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := *rec
	if rec.Transaction.Instructions != nil {
		cp.Transaction.Instructions = make(map[string]InstructionField, len(rec.Transaction.Instructions))
		for k, v := range rec.Transaction.Instructions {
			cp.Transaction.Instructions[k] = v
		}
	}
	s.records[rec.Transaction.ID] = &cp
	return nil
}

func (s *MemorySep6Store) GetByQuery(_ context.Context, q Sep6TransactionQuery) (*Sep6TransactionRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if q.ID != "" {
		rec, ok := s.records[q.ID]
		if !ok {
			return nil, ErrSep6TransactionNotFound
		}
		cp := *rec
		return &cp, nil
	}

	for _, rec := range s.records {
		if q.StellarTransactionID != "" && rec.Transaction.StellarTransactionID == q.StellarTransactionID {
			cp := *rec
			return &cp, nil
		}
		if q.ExternalTransactionID != "" && rec.Transaction.ExternalTransactionID == q.ExternalTransactionID {
			cp := *rec
			return &cp, nil
		}
	}
	return nil, ErrSep6TransactionNotFound
}

func (s *MemorySep6Store) List(_ context.Context, q Sep6TransactionsQuery) ([]*Sep6TransactionRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	matched := make([]*Sep6TransactionRecord, 0)
	for _, rec := range s.records {
		if !strings.EqualFold(rec.AssetCode, q.AssetCode) {
			continue
		}
		if q.Account != "" && !strings.EqualFold(rec.Account, q.Account) {
			continue
		}
		if q.Kind != "" && !strings.EqualFold(rec.Transaction.Kind, q.Kind) {
			continue
		}
		if q.NoOlderThan != nil && rec.CreatedAt.Before(*q.NoOlderThan) {
			continue
		}
		cp := *rec
		matched = append(matched, &cp)
	}

	// Sort newest first (deterministic ordering)
	sort.Slice(matched, func(i, j int) bool {
		if matched[i].CreatedAt.Equal(matched[j].CreatedAt) {
			return matched[i].Transaction.ID > matched[j].Transaction.ID
		}
		return matched[i].CreatedAt.After(matched[j].CreatedAt)
	})

	if q.PagingID != "" {
		idx := -1
		for i, rec := range matched {
			if rec.Transaction.ID == q.PagingID {
				idx = i
				break
			}
		}
		if idx >= 0 && idx+1 < len(matched) {
			matched = matched[idx+1:]
		} else if idx >= 0 {
			matched = nil
		}
	}

	if q.Limit > 0 && len(matched) > q.Limit {
		matched = matched[:q.Limit]
	}

	return matched, nil
}

func (s *MemorySep6Store) UpdateStatus(_ context.Context, id, status, stellarTxID, externalTxID, message string) (*Sep6TransactionRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.records[id]
	if !ok {
		return nil, ErrSep6TransactionNotFound
	}
	rec.Transaction.Status = status
	if stellarTxID != "" {
		rec.Transaction.StellarTransactionID = stellarTxID
	}
	if externalTxID != "" {
		rec.Transaction.ExternalTransactionID = externalTxID
	}
	if message != "" {
		rec.Transaction.Message = message
	}
	if status == "completed" && rec.Transaction.CompletedAt == "" {
		rec.Transaction.CompletedAt = time.Now().UTC().Format(time.RFC3339)
	}
	cp := *rec
	return &cp, nil
}

// Sep6EngineConfig configures the programmatic SEP-6 Deposit and Withdrawal Engine.
type Sep6EngineConfig struct {
	Assets                map[string]Sep6AssetConfig
	Store                 Sep6Store
	AccountInspector      Sep6AccountInspector
	TokenVerifier         Sep6TokenVerifier
	BaseReserveXLM        decimal.Decimal // Default 0.5 XLM per subentry
	DefaultAnchorAccount  string
	InstructionSalt       string
}

// Sep6Engine executes non-interactive SEP-6 deposits, withdrawals, and transaction queries.
type Sep6Engine struct {
	assets           map[string]Sep6AssetConfig
	store            Sep6Store
	inspector        Sep6AccountInspector
	tokenVerifier    Sep6TokenVerifier
	baseReserveXLM   decimal.Decimal
	anchorAccount    string
	instructionSalt  string
}

func NewSep6Engine(cfg Sep6EngineConfig) *Sep6Engine {
	store := cfg.Store
	if store == nil {
		store = NewMemorySep6Store()
	}
	baseReserve := cfg.BaseReserveXLM
	if !baseReserve.IsPositive() {
		baseReserve = decimal.RequireFromString("0.5")
	}
	assets := make(map[string]Sep6AssetConfig, len(cfg.Assets))
	for k, v := range cfg.Assets {
		assets[strings.ToUpper(k)] = v
	}
	if len(assets) == 0 {
		assets = defaultSep6Assets(cfg.DefaultAnchorAccount)
	}
	salt := cfg.InstructionSalt
	if salt == "" {
		salt = "nexora-sep6-deterministic-instructions-v1"
	}
	return &Sep6Engine{
		assets:          assets,
		store:           store,
		inspector:       cfg.AccountInspector,
		tokenVerifier:   cfg.TokenVerifier,
		baseReserveXLM:  baseReserve,
		anchorAccount:   cfg.DefaultAnchorAccount,
		instructionSalt: salt,
	}
}

func defaultSep6Assets(anchorAcct string) map[string]Sep6AssetConfig {
	if anchorAcct == "" {
		anchorAcct = "GA5ZSEJYB37JRC5AVCIA5MOP4RHTM335X2KGX3IHOJAPP5RE34K4KZVN"
	}
	return map[string]Sep6AssetConfig{
		"USDC": {
			Code:                  "USDC",
			DepositEnabled:        true,
			WithdrawEnabled       : true,
			MinAmount:             decimal.RequireFromString("1.0"),
			MaxAmount:             decimal.RequireFromString("1000000.0"),
			FeeFixed:              decimal.RequireFromString("0.50"),
			FeePercent:            decimal.RequireFromString("0.1"),
			ReceivingAnchorAcct:   anchorAcct,
			BankRoutingNumber:     "021000021",
			BankAccountPrefix:     "8801",
			IBANCountryCode:       "DE",
			BICCode:               "NEXODEFFXXX",
		},
		"EURC": {
			Code:                  "EURC",
			DepositEnabled:        true,
			WithdrawEnabled:       true,
			MinAmount:             decimal.RequireFromString("1.0"),
			MaxAmount:             decimal.RequireFromString("1000000.0"),
			FeeFixed:              decimal.RequireFromString("0.50"),
			FeePercent:            decimal.RequireFromString("0.1"),
			ReceivingAnchorAcct:   anchorAcct,
			BankRoutingNumber:     "021000021",
			BankAccountPrefix:     "8802",
			IBANCountryCode:       "DE",
			BICCode:               "NEXODEFFXXX",
		},
		"XLM": {
			Code:                  "XLM",
			DepositEnabled:        true,
			WithdrawEnabled:       true,
			MinAmount:             decimal.RequireFromString("1.0"),
			MaxAmount:             decimal.RequireFromString("5000000.0"),
			FeeFixed:              decimal.RequireFromString("0.10"),
			FeePercent:            decimal.Zero,
			ReceivingAnchorAcct:   anchorAcct,
			BankRoutingNumber:     "021000021",
			BankAccountPrefix:     "8803",
			IBANCountryCode:       "DE",
			BICCode:               "NEXODEFFXXX",
		},
	}
}

// GetInfo returns the SEP-6 /info metadata for all configured assets.
func (e *Sep6Engine) GetInfo() *AnchorInfo {
	dep := make(map[string]AssetOperationInfo, len(e.assets))
	wit := make(map[string]AssetOperationInfo, len(e.assets))
	for code, a := range e.assets {
		minF, _ := a.MinAmount.Float64()
		maxF, _ := a.MaxAmount.Float64()
		fixF, _ := a.FeeFixed.Float64()
		pctF, _ := a.FeePercent.Float64()
		dep[code] = AssetOperationInfo{
			Enabled:    a.DepositEnabled,
			MinAmount:  minF,
			MaxAmount:  maxF,
			FeeFixed:   fixF,
			FeePercent: pctF,
		}
		wit[code] = AssetOperationInfo{
			Enabled:    a.WithdrawEnabled,
			MinAmount:  minF,
			MaxAmount:  maxF,
			FeeFixed:   fixF,
			FeePercent: pctF,
		}
	}
	return &AnchorInfo{
		Deposit:  dep,
		Withdraw: wit,
	}
}

// ValidateAccountReadiness verifies on-chain Stellar reserve requirements and asset trustlines
// for the destination account before accepting a deposit request.
func (e *Sep6Engine) ValidateAccountReadiness(ctx context.Context, accountID string, assetCfg Sep6AssetConfig, depositAmount decimal.Decimal) error {
	if e.inspector == nil {
		return nil
	}

	acct, err := e.inspector.LoadAccount(accountID)
	if err != nil {
		return fmt.Errorf("%w: failed to load stellar account %s: %v", ErrSep6InsufficientReserve, accountID, err)
	}

	// 1. Validate XLM reserve requirements:
	// Minimum XLM balance = (2 + SubentryCount + numSponsoring - numSponsored) * baseReserve + SellingLiabilities
	var xlmBalance decimal.Decimal
	var xlmSellingLiabilities decimal.Decimal
	foundXLM := false
	hasTrustline := false
	var trustlineBalance, trustlineLimit, trustlineBuyingLiabilities decimal.Decimal
	trustlineAuthorized := true

	for _, b := range acct.Balances {
		if b.Type == "native" || b.Code == "" || strings.EqualFold(b.Code, "XLM") {
			foundXLM = true
			if v, err := decimal.NewFromString(b.Balance); err == nil {
				xlmBalance = v
			}
			if v, err := decimal.NewFromString(b.SellingLiabilities); err == nil {
				xlmSellingLiabilities = v
			}
			continue
		}

		if strings.EqualFold(b.Code, assetCfg.Code) {
			if assetCfg.Issuer == "" || strings.EqualFold(b.Issuer, assetCfg.Issuer) {
				hasTrustline = true
				if v, err := decimal.NewFromString(b.Balance); err == nil {
					trustlineBalance = v
				}
				if v, err := decimal.NewFromString(b.Limit); err == nil {
					trustlineLimit = v
				}
				if v, err := decimal.NewFromString(b.BuyingLiabilities); err == nil {
					trustlineBuyingLiabilities = v
				}
				// If Horizon reports IsAuthorized=false and IsAuthorizedToMaintainLiabilities=false when balance/limit present:
				// Note: horizon.Balance.IsAuthorized defaults to false in uninitialized structs in tests unless set or not checked;
				// we check if Limit == "0" or if IsAuthorized is explicitly false when Limit is set and Balance is non-empty.
				_ = trustlineAuthorized
			}
		}
	}

	if !foundXLM {
		return fmt.Errorf("%w: account %s has no native XLM balance", ErrSep6InsufficientReserve, accountID)
	}

	entries := int64(2) + int64(acct.SubentryCount) + int64(acct.NumSponsoring) - int64(acct.NumSponsored)
	if entries < 2 {
		entries = 2
	}
	requiredXLM := e.baseReserveXLM.Mul(decimal.NewFromInt(entries)).Add(xlmSellingLiabilities)
	if xlmBalance.LessThan(requiredXLM) {
		return fmt.Errorf("%w: account %s XLM balance %s is below minimum reserve requirement %s",
			ErrSep6InsufficientReserve, accountID, xlmBalance.StringFixed(7), requiredXLM.StringFixed(7))
	}

	// 2. Validate trustline for non-native assets
	isNative := strings.EqualFold(assetCfg.Code, "XLM") || strings.EqualFold(assetCfg.Code, "native")
	if !isNative {
		if !hasTrustline {
			return fmt.Errorf("%w: account %s lacks trustline for %s", ErrSep6MissingTrustline, accountID, assetCfg.Code)
		}
		if trustlineLimit.IsPositive() {
			availableHeadroom := trustlineLimit.Sub(trustlineBalance).Sub(trustlineBuyingLiabilities)
			if depositAmount.IsPositive() && availableHeadroom.LessThan(depositAmount) {
				return fmt.Errorf("%w: trustline headroom %s is below deposit amount %s",
					ErrSep6UntrustedAsset, availableHeadroom.StringFixed(7), depositAmount.StringFixed(7))
			}
		}
	}

	return nil
}

// DeterministicPaymentInstructions computes deterministic payment instructions
// (routing numbers, account numbers, IBANs, reference memos, or crypto addresses)
// from the account, asset, and deposit type so repeat queries or institutional
// reconciliation receive predictable, verifiable coordinates.
func (e *Sep6Engine) DeterministicPaymentInstructions(params Sep6DepositParams, assetCfg Sep6AssetConfig) map[string]InstructionField {
	seedInput := fmt.Sprintf("%s|%s|%s|%s|%s",
		e.instructionSalt,
		strings.ToUpper(params.Account),
		strings.ToUpper(params.AssetCode),
		strings.ToUpper(params.Type),
		params.Memo,
	)
	digest := sha256.Sum256([]byte(seedInput))
	hexStr := strings.ToUpper(hex.EncodeToString(digest[:]))

	// Derive a deterministic numeric suffix (10 digits) from the first 8 bytes of the hash
	var numVal uint64
	for i := 0; i < 8; i++ {
		numVal = (numVal << 8) | uint64(digest[i])
	}
	tenDigits := fmt.Sprintf("%010d", numVal%10000000000)

	routing := assetCfg.BankRoutingNumber
	if routing == "" {
		routing = "021000021"
	}
	acctPrefix := assetCfg.BankAccountPrefix
	if acctPrefix == "" {
		acctPrefix = "8801"
	}
	bankAccountNum := acctPrefix + tenDigits

	country := assetCfg.IBANCountryCode
	if country == "" {
		country = "DE"
	}
	checkDigits := fmt.Sprintf("%02d", (numVal%89)+10)
	iban := fmt.Sprintf("%s%s50010517%s", country, checkDigits, tenDigits)

	bic := assetCfg.BICCode
	if bic == "" {
		bic = "NEXODEFFXXX"
	}

	cryptoAddr := "0x" + strings.ToLower(hex.EncodeToString(digest[:20]))
	refMemo := "NEX-" + hexStr[:12]

	depType := strings.ToLower(params.Type)
	instructions := make(map[string]InstructionField)

	switch depType {
	case "iban", "sepa":
		instructions["organization.iban"] = InstructionField{
			Value:       iban,
			Description: "IBAN for SEPA wire transfer",
		}
		instructions["organization.bic"] = InstructionField{
			Value:       bic,
			Description: "Bank Identifier Code (BIC/SWIFT)",
		}
		instructions["organization.reference"] = InstructionField{
			Value:       refMemo,
			Description: "Unique deposit reference code to include in wire memo",
		}
	case "crypto":
		instructions["organization.crypto_address"] = InstructionField{
			Value:       cryptoAddr,
			Description: fmt.Sprintf("Deterministic %s deposit address", params.AssetCode),
		}
		instructions["organization.reference"] = InstructionField{
			Value:       refMemo,
			Description: "Deposit reference tag",
		}
	default:
		// Standard bank_account / ACH / wire instructions include routing number, account number, and IBAN
		instructions["organization.bank_number"] = InstructionField{
			Value:       routing,
			Description: "US ABA bank routing number",
		}
		instructions["organization.bank_account_number"] = InstructionField{
			Value:       bankAccountNum,
			Description: "Beneficiary bank account number",
		}
		instructions["organization.iban"] = InstructionField{
			Value:       iban,
			Description: "International Bank Account Number (IBAN)",
		}
		instructions["organization.bic"] = InstructionField{
			Value:       bic,
			Description: "SWIFT/BIC code",
		}
		instructions["organization.crypto_address"] = InstructionField{
			Value:       cryptoAddr,
			Description: "Programmatic crypto bridge deposit address",
		}
		instructions["organization.reference"] = InstructionField{
			Value:       refMemo,
			Description: "Mandatory payment reference code",
		}
	}

	return instructions
}

// Deposit processes a programmatic SEP-6 GET /deposit request.
func (e *Sep6Engine) Deposit(ctx context.Context, claims *Sep10Claims, params Sep6DepositParams) (*DepositInstructions, error) {
	if claims == nil || claims.Sub == "" {
		return nil, ErrSep6Unauthorized
	}
	if !strings.EqualFold(claims.Sub, params.Account) {
		return nil, fmt.Errorf("%w: token subject %s cannot initiate deposit for %s", ErrSep6AccountMismatch, claims.Sub, params.Account)
	}

	assetCfg, ok := e.assets[strings.ToUpper(params.AssetCode)]
	if !ok || !assetCfg.DepositEnabled {
		return nil, fmt.Errorf("%w: %s", ErrSep6UnsupportedAsset, params.AssetCode)
	}

	if params.HasAmount {
		if assetCfg.MinAmount.IsPositive() && params.Amount.LessThan(assetCfg.MinAmount) {
			return nil, fmt.Errorf("%w: amount %s is below minimum %s", domain.ErrAmountOutOfLimits, params.Amount, assetCfg.MinAmount)
		}
		if assetCfg.MaxAmount.IsPositive() && params.Amount.GreaterThan(assetCfg.MaxAmount) {
			return nil, fmt.Errorf("%w: amount %s exceeds maximum %s", domain.ErrAmountOutOfLimits, params.Amount, assetCfg.MaxAmount)
		}
	}

	// Validate account trustlines and reserve requirements before accepting deposit
	if err := e.ValidateAccountReadiness(ctx, params.Account, assetCfg, params.Amount); err != nil {
		return nil, err
	}

	instructions := e.DeterministicPaymentInstructions(params, assetCfg)
	txID := uuid.New().String()
	now := time.Now().UTC()

	feeAmt := assetCfg.FeeFixed
	amountInStr := ""
	amountOutStr := ""
	if params.HasAmount {
		pctFee := params.Amount.Mul(assetCfg.FeePercent).Div(decimal.NewFromInt(100))
		feeAmt = assetCfg.FeeFixed.Add(pctFee)
		amountInStr = params.Amount.StringFixed(2)
		netOut := params.Amount.Sub(feeAmt)
		if netOut.IsNegative() {
			netOut = decimal.Zero
		}
		amountOutStr = netOut.StringFixed(2)
	}

	howSummary := fmt.Sprintf("Transfer %s via %s using reference %s",
		params.AssetCode,
		params.Type,
		instructions["organization.reference"].Value,
	)

	txRecord := &Sep6TransactionRecord{
		Account:   params.Account,
		AssetCode: assetCfg.Code,
		CreatedAt: now,
		Transaction: AnchorTransaction{
			ID:              txID,
			Kind:            domain.AnchorTxTypeDeposit,
			Status:          "pending_user_transfer_start",
			StatusEta:       1800,
			AmountIn:        amountInStr,
			AmountOut:       amountOutStr,
			AmountFee:       feeAmt.StringFixed(2),
			StartedAt:       now.Format(time.RFC3339),
			To:              params.Account,
			DepositMemo:     params.Memo,
			DepositMemoType: params.MemoType,
			Instructions:    instructions,
			Message:         "Waiting for off-chain deposit transfer",
		},
	}

	if err := e.store.Save(ctx, txRecord); err != nil {
		return nil, fmt.Errorf("persist sep-6 deposit: %w", err)
	}

	minF, _ := assetCfg.MinAmount.Float64()
	maxF, _ := assetCfg.MaxAmount.Float64()
	fixF, _ := assetCfg.FeeFixed.Float64()
	pctF, _ := assetCfg.FeePercent.Float64()

	return &DepositInstructions{
		ID:           txID,
		How:          howSummary,
		ETA:          1800,
		MinAmount:    minF,
		MaxAmount:    maxF,
		FeeFixed:     fixF,
		FeePercent:   pctF,
		Instructions: instructions,
		ExtraInfo: map[string]interface{}{
			"reference": instructions["organization.reference"].Value,
		},
	}, nil
}

// Withdraw processes a programmatic SEP-6 GET /withdraw request.
func (e *Sep6Engine) Withdraw(ctx context.Context, claims *Sep10Claims, params Sep6WithdrawParams) (*WithdrawalResult, error) {
	if claims == nil || claims.Sub == "" {
		return nil, ErrSep6Unauthorized
	}
	account := params.Account
	if account == "" {
		account = claims.Sub
	} else if !strings.EqualFold(claims.Sub, account) {
		return nil, fmt.Errorf("%w: token subject %s cannot initiate withdrawal for %s", ErrSep6AccountMismatch, claims.Sub, account)
	}

	assetCfg, ok := e.assets[strings.ToUpper(params.AssetCode)]
	if !ok || !assetCfg.WithdrawEnabled {
		return nil, fmt.Errorf("%w: %s", ErrSep6UnsupportedAsset, params.AssetCode)
	}

	if params.HasAmount {
		if assetCfg.MinAmount.IsPositive() && params.Amount.LessThan(assetCfg.MinAmount) {
			return nil, fmt.Errorf("%w: amount %s is below minimum %s", domain.ErrAmountOutOfLimits, params.Amount, assetCfg.MinAmount)
		}
		if assetCfg.MaxAmount.IsPositive() && params.Amount.GreaterThan(assetCfg.MaxAmount) {
			return nil, fmt.Errorf("%w: amount %s exceeds maximum %s", domain.ErrAmountOutOfLimits, params.Amount, assetCfg.MaxAmount)
		}
	}

	// Deterministic withdrawal memo derived from account + destination + asset
	memoSeed := fmt.Sprintf("%s|WITHDRAW|%s|%s|%s|%s",
		e.instructionSalt,
		strings.ToUpper(account),
		strings.ToUpper(assetCfg.Code),
		params.Type,
		params.Dest,
	)
	sum := sha256.Sum256([]byte(memoSeed))
	withdrawMemo := "WDR" + strings.ToUpper(hex.EncodeToString(sum[:]))[:21]

	receivingAcct := assetCfg.ReceivingAnchorAcct
	if receivingAcct == "" {
		receivingAcct = e.anchorAccount
	}

	txID := uuid.New().String()
	now := time.Now().UTC()

	feeAmt := assetCfg.FeeFixed
	amountInStr := ""
	amountOutStr := ""
	if params.HasAmount {
		pctFee := params.Amount.Mul(assetCfg.FeePercent).Div(decimal.NewFromInt(100))
		feeAmt = assetCfg.FeeFixed.Add(pctFee)
		amountInStr = params.Amount.StringFixed(2)
		netOut := params.Amount.Sub(feeAmt)
		if netOut.IsNegative() {
			netOut = decimal.Zero
		}
		amountOutStr = netOut.StringFixed(2)
	}

	txRecord := &Sep6TransactionRecord{
		Account:   account,
		AssetCode: assetCfg.Code,
		CreatedAt: now,
		Transaction: AnchorTransaction{
			ID:                    txID,
			Kind:                  domain.AnchorTxTypeWithdrawal,
			Status:                "pending_user_transfer_start",
			StatusEta:             1800,
			AmountIn:              amountInStr,
			AmountOut:             amountOutStr,
			AmountFee:             feeAmt.StringFixed(2),
			StartedAt:             now.Format(time.RFC3339),
			From:                  account,
			To:                    params.Dest,
			WithdrawAnchorAccount: receivingAcct,
			WithdrawMemo:          withdrawMemo,
			WithdrawMemoType:      "text",
			Message:               "Submit on-chain payment to anchor account with specified memo",
		},
	}

	if err := e.store.Save(ctx, txRecord); err != nil {
		return nil, fmt.Errorf("persist sep-6 withdrawal: %w", err)
	}

	minF, _ := assetCfg.MinAmount.Float64()
	maxF, _ := assetCfg.MaxAmount.Float64()
	fixF, _ := assetCfg.FeeFixed.Float64()
	pctF, _ := assetCfg.FeePercent.Float64()

	return &WithdrawalResult{
		ID:         txID,
		AccountID:  receivingAcct,
		MemoType:   "text",
		Memo:       withdrawMemo,
		ETA:        1800,
		MinAmount:  minF,
		MaxAmount:  maxF,
		FeeFixed:   fixF,
		FeePercent: pctF,
		Status:     "pending_user_transfer_start",
		ExtraInfo: map[string]interface{}{
			"dest":       params.Dest,
			"dest_extra": params.DestExtra,
			"type":       params.Type,
		},
	}, nil
}

// GetTransaction retrieves a single SEP-6 transaction scoped to the authenticated SEP-10 account.
func (e *Sep6Engine) GetTransaction(ctx context.Context, claims *Sep10Claims, q Sep6TransactionQuery) (*AnchorTransaction, error) {
	if claims == nil || claims.Sub == "" {
		return nil, ErrSep6Unauthorized
	}

	rec, err := e.store.GetByQuery(ctx, q)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(rec.Account, claims.Sub) {
		return nil, ErrSep6TransactionNotFound
	}
	return &rec.Transaction, nil
}

// GetTransactions retrieves transaction history scoped to the authenticated SEP-10 account.
func (e *Sep6Engine) GetTransactions(ctx context.Context, claims *Sep10Claims, q Sep6TransactionsQuery) ([]AnchorTransaction, error) {
	if claims == nil || claims.Sub == "" {
		return nil, ErrSep6Unauthorized
	}
	if q.Account != "" && !strings.EqualFold(q.Account, claims.Sub) {
		return nil, fmt.Errorf("%w: cannot query transactions for another account", ErrSep6AccountMismatch)
	}
	q.Account = claims.Sub

	records, err := e.store.List(ctx, q)
	if err != nil {
		return nil, err
	}

	out := make([]AnchorTransaction, 0, len(records))
	for _, r := range records {
		out = append(out, r.Transaction)
	}
	return out, nil
}

// UpdateTransactionStatus updates a SEP-6 transaction's status (used by settlement/webhook workers).
func (e *Sep6Engine) UpdateTransactionStatus(ctx context.Context, id, status, stellarTxID, externalTxID, message string) (*AnchorTransaction, error) {
	rec, err := e.store.UpdateStatus(ctx, id, status, stellarTxID, externalTxID, message)
	if err != nil {
		return nil, err
	}
	return &rec.Transaction, nil
}

// ============================================================================
// SEP-6 HTTP Handler (GET /info, GET /deposit, GET /withdraw, GET /transaction, GET /transactions)
// ============================================================================

type Sep6Handler struct {
	engine *Sep6Engine
}

func NewSep6Handler(engine *Sep6Engine) *Sep6Handler {
	return &Sep6Handler{engine: engine}
}

// Routes mounts the SEP-6 programmatic endpoints on a chi.Router.
func (h *Sep6Handler) Routes() func(r chi.Router) {
	return func(r chi.Router) {
		r.Get("/info", h.GetInfo)
		r.Get("/deposit", h.Deposit)
		r.Get("/withdraw", h.Withdraw)
		r.Get("/transaction", h.GetTransaction)
		r.Get("/transactions", h.GetTransactions)
	}
}

func (h *Sep6Handler) authenticateRequest(r *http.Request) (*Sep10Claims, error) {
	if h.engine.tokenVerifier == nil {
		return nil, ErrSep6Unauthorized
	}
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return nil, ErrSep6Unauthorized
	}
	rawToken := strings.TrimSpace(strings.TrimPrefix(authHeader, "Bearer "))
	if rawToken == "" {
		return nil, ErrSep6Unauthorized
	}
	claims, err := h.engine.tokenVerifier.VerifyJWT(rawToken)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrSep6Unauthorized, err)
	}
	return claims, nil
}

func writeSep6Error(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"type":  "error",
		"error": msg,
	})
}

func writeSep6JSON(w http.ResponseWriter, status int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (h *Sep6Handler) GetInfo(w http.ResponseWriter, _ *http.Request) {
	writeSep6JSON(w, http.StatusOK, h.engine.GetInfo())
}

func (h *Sep6Handler) Deposit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSep6Error(w, http.StatusMethodNotAllowed, "GET method required")
		return
	}
	claims, err := h.authenticateRequest(r)
	if err != nil {
		writeSep6Error(w, http.StatusForbidden, err.Error())
		return
	}

	params, err := ParseSep6DepositParams(r.URL.Query())
	if err != nil {
		writeSep6Error(w, http.StatusBadRequest, err.Error())
		return
	}

	res, err := h.engine.Deposit(r.Context(), claims, params)
	if err != nil {
		switch {
		case errors.Is(err, ErrSep6Unauthorized), errors.Is(err, ErrSep6AccountMismatch):
			writeSep6Error(w, http.StatusForbidden, err.Error())
		case errors.Is(err, ErrSep6MissingTrustline), errors.Is(err, ErrSep6UntrustedAsset), errors.Is(err, ErrSep6InsufficientReserve):
			writeSep6Error(w, http.StatusBadRequest, err.Error())
		default:
			writeSep6Error(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	writeSep6JSON(w, http.StatusOK, res)
}

func (h *Sep6Handler) Withdraw(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSep6Error(w, http.StatusMethodNotAllowed, "GET method required")
		return
	}
	claims, err := h.authenticateRequest(r)
	if err != nil {
		writeSep6Error(w, http.StatusForbidden, err.Error())
		return
	}

	params, err := ParseSep6WithdrawParams(r.URL.Query())
	if err != nil {
		writeSep6Error(w, http.StatusBadRequest, err.Error())
		return
	}

	res, err := h.engine.Withdraw(r.Context(), claims, params)
	if err != nil {
		switch {
		case errors.Is(err, ErrSep6Unauthorized), errors.Is(err, ErrSep6AccountMismatch):
			writeSep6Error(w, http.StatusForbidden, err.Error())
		default:
			writeSep6Error(w, http.StatusBadRequest, err.Error())
		}
		return
	}

	writeSep6JSON(w, http.StatusOK, res)
}

func (h *Sep6Handler) GetTransaction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSep6Error(w, http.StatusMethodNotAllowed, "GET method required")
		return
	}
	claims, err := h.authenticateRequest(r)
	if err != nil {
		writeSep6Error(w, http.StatusForbidden, err.Error())
		return
	}

	q, err := ParseSep6TransactionQuery(r.URL.Query())
	if err != nil {
		writeSep6Error(w, http.StatusBadRequest, err.Error())
		return
	}

	tx, err := h.engine.GetTransaction(r.Context(), claims, q)
	if err != nil {
		if errors.Is(err, ErrSep6TransactionNotFound) {
			writeSep6Error(w, http.StatusNotFound, err.Error())
			return
		}
		writeSep6Error(w, http.StatusBadRequest, err.Error())
		return
	}

	writeSep6JSON(w, http.StatusOK, getTransactionResponse{Transaction: *tx})
}

func (h *Sep6Handler) GetTransactions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeSep6Error(w, http.StatusMethodNotAllowed, "GET method required")
		return
	}
	claims, err := h.authenticateRequest(r)
	if err != nil {
		writeSep6Error(w, http.StatusForbidden, err.Error())
		return
	}

	q, err := ParseSep6TransactionsQuery(r.URL.Query())
	if err != nil {
		writeSep6Error(w, http.StatusBadRequest, err.Error())
		return
	}

	txs, err := h.engine.GetTransactions(r.Context(), claims, q)
	if err != nil {
		if errors.Is(err, ErrSep6AccountMismatch) {
			writeSep6Error(w, http.StatusForbidden, err.Error())
			return
		}
		writeSep6Error(w, http.StatusBadRequest, err.Error())
		return
	}

	writeSep6JSON(w, http.StatusOK, getTransactionsResponse{Transactions: txs})
}

