package wallet

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/compliance"
	"github.com/Lumen-Nexora/Nexora/internal/crypto"
	"github.com/google/uuid"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

var (
	ErrAccountNotRegistered     = errors.New("recovery account not registered")
	ErrAccountAlreadyRegistered  = errors.New("recovery account already registered")
	ErrInvalidStellarAddress    = errors.New("invalid stellar account address")
	ErrNoMatchingIdentity       = errors.New("no matching identity or authentication method found")
	ErrChallengeNotFound        = errors.New("recovery challenge not found")
	ErrChallengeExpired         = errors.New("recovery challenge expired")
	ErrInvalidOTPCode           = errors.New("invalid otp verification code")
	ErrMaxAttemptsExceeded      = errors.New("maximum verification attempts exceeded")
	ErrRateLimited              = errors.New("rate limit exceeded: too many recovery attempts")
	ErrUnauthorizedTransaction  = errors.New("unauthorized transaction: account is not a source or target in envelope")
	ErrInvalidKYCAttestation    = errors.New("kyc attestation verification failed or status not accepted")
	ErrMissingAuthProof         = errors.New("missing verification challenge or kyc attestation proof")
)

// SEP-30 Recovery Identity & Authentication Method Types
const (
	AuthTypePhone   = "phone_number"
	AuthTypeEmail   = "email"
	AuthTypeAddress = "stellar_address"
	AuthTypeKYC     = "kyc_attestation"
)

type RecoveryAuthMethod struct {
	Type  string `json:"type"`  // phone_number, email, stellar_address, kyc_attestation
	Value string `json:"value"` // e.g. "+15551234567", "alice@example.com", "G...", customer-id
}

type RecoveryIdentity struct {
	Role        string               `json:"role"` // owner, sender, receiver
	AuthMethods []RecoveryAuthMethod `json:"auth_methods"`
}

type RecoveryAccount struct {
	Address            string             `json:"address"` // Account to recover (G...)
	TenantID           string             `json:"tenant_id"`
	SignerKey          string             `json:"signer_key"` // Recovery service signer public key (G...)
	EncryptedSignerKey string             `json:"-"`          // Encrypted secret seed (S...)
	Identities         []RecoveryIdentity `json:"identities"`
	CreatedAt          time.Time          `json:"created_at"`
	UpdatedAt          time.Time          `json:"updated_at"`
}

type RecoveryChallenge struct {
	ID             string    `json:"id"`
	TenantID       string    `json:"tenant_id"`
	AccountAddress string    `json:"account_address"`
	AuthType       string    `json:"auth_type"`
	AuthValue      string    `json:"auth_value"`
	CodeHash       string    `json:"-"`
	RawCode        string    `json:"-"` // Exposed only in mock/test delivery
	ExpiresAt      time.Time `json:"expires_at"`
	Attempts       int       `json:"attempts"`
	MaxAttempts    int       `json:"max_attempts"`
	Verified       bool      `json:"verified"`
	VerifiedAt     time.Time `json:"verified_at,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type SignatureAuthProof struct {
	ChallengeID      string `json:"challenge_id,omitempty"`
	Code             string `json:"code,omitempty"`
	KYCAttestationID string `json:"kyc_attestation_id,omitempty"`
}

type SignatureResult struct {
	NetworkPassphrase string `json:"network_passphrase"`
	Signature         string `json:"signature"` // Base64 encoded raw signature
	SignerPublicKey   string `json:"signer"`
	SignedTransaction string `json:"transaction"` // Signed transaction XDR
}

// ----------------------------------------------------------------------
// Rate Limiter
// ----------------------------------------------------------------------

type rateLimitEntry struct {
	attempts int
	window   time.Time
}

type RecoveryRateLimiter struct {
	mu          sync.Mutex
	limits      map[string]*rateLimitEntry
	maxAttempts int
	windowSize  time.Duration
}

func NewRecoveryRateLimiter(maxAttempts int, windowSize time.Duration) *RecoveryRateLimiter {
	return &RecoveryRateLimiter{
		limits:      make(map[string]*rateLimitEntry),
		maxAttempts: maxAttempts,
		windowSize:  windowSize,
	}
}

func (r *RecoveryRateLimiter) CheckAndIncrement(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	entry, exists := r.limits[key]
	if !exists || now.After(entry.window) {
		r.limits[key] = &rateLimitEntry{
			attempts: 1,
			window:   now.Add(r.windowSize),
		}
		return true
	}

	if entry.attempts >= r.maxAttempts {
		return false
	}

	entry.attempts++
	return true
}

func (r *RecoveryRateLimiter) RecordFailure(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	entry, exists := r.limits[key]
	if !exists || now.After(entry.window) {
		r.limits[key] = &rateLimitEntry{
			attempts: 1,
			window:   now.Add(r.windowSize),
		}
		return
	}
	entry.attempts++
}

// ----------------------------------------------------------------------
// Repository
// ----------------------------------------------------------------------

type RecoveryRepository interface {
	SaveAccount(ctx context.Context, acct *RecoveryAccount) error
	GetAccount(ctx context.Context, tenantID, address string) (*RecoveryAccount, error)
	DeleteAccount(ctx context.Context, tenantID, address string) error
	SaveChallenge(ctx context.Context, ch *RecoveryChallenge) error
	GetChallenge(ctx context.Context, challengeID string) (*RecoveryChallenge, error)
	UpdateChallenge(ctx context.Context, ch *RecoveryChallenge) error
}

type MemoryRecoveryRepository struct {
	mu         sync.RWMutex
	accounts   map[string]*RecoveryAccount   // "tenant:address" -> account
	challenges map[string]*RecoveryChallenge // challengeID -> challenge
}

func NewMemoryRecoveryRepository() *MemoryRecoveryRepository {
	return &MemoryRecoveryRepository{
		accounts:   make(map[string]*RecoveryAccount),
		challenges: make(map[string]*RecoveryChallenge),
	}
}

func (m *MemoryRecoveryRepository) acctKey(tenantID, address string) string {
	return fmt.Sprintf("%s:%s", tenantID, address)
}

func (m *MemoryRecoveryRepository) SaveAccount(ctx context.Context, acct *RecoveryAccount) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *acct
	m.accounts[m.acctKey(acct.TenantID, acct.Address)] = &cp
	return nil
}

func (m *MemoryRecoveryRepository) GetAccount(ctx context.Context, tenantID, address string) (*RecoveryAccount, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Try with tenantID first, then search any tenant if tenantID is blank
	if tenantID != "" {
		if acct, ok := m.accounts[m.acctKey(tenantID, address)]; ok {
			cp := *acct
			return &cp, nil
		}
	}
	for _, acct := range m.accounts {
		if acct.Address == address {
			cp := *acct
			return &cp, nil
		}
	}
	return nil, ErrAccountNotRegistered
}

func (m *MemoryRecoveryRepository) DeleteAccount(ctx context.Context, tenantID, address string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.acctKey(tenantID, address)
	if _, ok := m.accounts[key]; !ok {
		// check any tenant
		found := false
		for k, acct := range m.accounts {
			if acct.Address == address {
				delete(m.accounts, k)
				found = true
				break
			}
		}
		if !found {
			return ErrAccountNotRegistered
		}
		return nil
	}
	delete(m.accounts, key)
	return nil
}

func (m *MemoryRecoveryRepository) SaveChallenge(ctx context.Context, ch *RecoveryChallenge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *ch
	m.challenges[ch.ID] = &cp
	return nil
}

func (m *MemoryRecoveryRepository) GetChallenge(ctx context.Context, challengeID string) (*RecoveryChallenge, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ch, ok := m.challenges[challengeID]
	if !ok {
		return nil, ErrChallengeNotFound
	}
	cp := *ch
	return &cp, nil
}

func (m *MemoryRecoveryRepository) UpdateChallenge(ctx context.Context, ch *RecoveryChallenge) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := *ch
	m.challenges[ch.ID] = &cp
	return nil
}

// ----------------------------------------------------------------------
// Recovery Service
// ----------------------------------------------------------------------

type RecoveryService interface {
	RegisterAccount(ctx context.Context, tenantID, address string, identities []RecoveryIdentity) (*RecoveryAccount, error)
	GetAccount(ctx context.Context, tenantID, address string) (*RecoveryAccount, error)
	DeleteAccount(ctx context.Context, tenantID, address string) error
	CreateChallenge(ctx context.Context, tenantID, address, authType, authValue string) (*RecoveryChallenge, error)
	VerifyChallenge(ctx context.Context, tenantID, challengeID, code string) (*RecoveryChallenge, error)
	SignTransaction(ctx context.Context, tenantID, address, txXDR string, proof SignatureAuthProof) (*SignatureResult, error)
	GetRateLimiter() *RecoveryRateLimiter
}

type recoveryService struct {
	repo              RecoveryRepository
	masterKey         []byte
	networkPassphrase string
	rateLimiter       *RecoveryRateLimiter
	customerSvc       compliance.CustomerService
}

func NewRecoveryService(
	repo RecoveryRepository,
	masterKey []byte,
	networkPassphrase string,
	customerSvc compliance.CustomerService,
) (RecoveryService, error) {
	if len(masterKey) != 32 {
		return nil, errors.New("master encryption key must be 32 bytes")
	}
	if repo == nil {
		repo = NewMemoryRecoveryRepository()
	}
	if networkPassphrase == "" {
		networkPassphrase = "Test SDF Network ; September 2015"
	}

	return &recoveryService{
		repo:              repo,
		masterKey:         masterKey,
		networkPassphrase: networkPassphrase,
		rateLimiter:       NewRecoveryRateLimiter(5, 15*time.Minute),
		customerSvc:       customerSvc,
	}, nil
}

func (s *recoveryService) GetRateLimiter() *RecoveryRateLimiter {
	return s.rateLimiter
}

func (s *recoveryService) RegisterAccount(ctx context.Context, tenantID, address string, identities []RecoveryIdentity) (*RecoveryAccount, error) {
	// Validate address format
	if _, err := keypair.ParseAddress(address); err != nil {
		return nil, ErrInvalidStellarAddress
	}

	// Check if already registered
	if existing, _ := s.repo.GetAccount(ctx, tenantID, address); existing != nil {
		return nil, ErrAccountAlreadyRegistered
	}

	// Generate a unique Ed25519 recovery signing keypair
	kp, err := keypair.Random()
	if err != nil {
		return nil, fmt.Errorf("generate recovery signer keypair: %w", err)
	}

	// Encrypt raw seed at rest with AES-256-GCM
	encSeed, err := crypto.Encrypt([]byte(kp.Seed()), s.masterKey)
	if err != nil {
		return nil, fmt.Errorf("encrypt recovery signing key: %w", err)
	}

	now := time.Now().UTC()
	acct := &RecoveryAccount{
		Address:            address,
		TenantID:           tenantID,
		SignerKey:          kp.Address(),
		EncryptedSignerKey: string(encSeed),
		Identities:         identities,
		CreatedAt:          now,
		UpdatedAt:          now,
	}

	if err := s.repo.SaveAccount(ctx, acct); err != nil {
		return nil, fmt.Errorf("save recovery account: %w", err)
	}

	return acct, nil
}

func (s *recoveryService) GetAccount(ctx context.Context, tenantID, address string) (*RecoveryAccount, error) {
	return s.repo.GetAccount(ctx, tenantID, address)
}

func (s *recoveryService) DeleteAccount(ctx context.Context, tenantID, address string) error {
	return s.repo.DeleteAccount(ctx, tenantID, address)
}

func (s *recoveryService) CreateChallenge(ctx context.Context, tenantID, address, authType, authValue string) (*RecoveryChallenge, error) {
	// Rate limit check
	rateKey := fmt.Sprintf("challenge:%s", address)
	if !s.rateLimiter.CheckAndIncrement(rateKey) {
		return nil, ErrRateLimited
	}

	acct, err := s.repo.GetAccount(ctx, tenantID, address)
	if err != nil {
		return nil, err
	}

	// Verify that the requested auth method exists on the registered account
	matched := false
	for _, id := range acct.Identities {
		for _, method := range id.AuthMethods {
			if method.Type == authType && (authValue == "" || method.Value == authValue) {
				authValue = method.Value
				matched = true
				break
			}
		}
		if matched {
			break
		}
	}
	if !matched {
		return nil, ErrNoMatchingIdentity
	}

	now := time.Now().UTC()

	// If KYC attestation: verify KYC customer status directly
	if authType == AuthTypeKYC {
		if s.customerSvc == nil {
			return nil, errors.New("compliance customer service not configured for kyc attestation")
		}
		cust, err := s.customerSvc.GetCustomer(ctx, tenantID, authValue, "", "", "")
		if err != nil || cust.Status != compliance.CustomerStatusAccepted {
			return nil, ErrInvalidKYCAttestation
		}

		ch := &RecoveryChallenge{
			ID:             uuid.New().String(),
			TenantID:       tenantID,
			AccountAddress: address,
			AuthType:       authType,
			AuthValue:      authValue,
			ExpiresAt:      now.Add(15 * time.Minute),
			MaxAttempts:    5,
			Verified:       true,
			VerifiedAt:     now,
			CreatedAt:      now,
		}
		if err := s.repo.SaveChallenge(ctx, ch); err != nil {
			return nil, err
		}
		return ch, nil
	}

	// Generate secure 6-digit random OTP code
	n, err := rand.Int(rand.Reader, big.NewInt(900000))
	if err != nil {
		return nil, fmt.Errorf("generate random otp: %w", err)
	}
	rawCode := fmt.Sprintf("%06d", 100000+n.Int64())

	h := sha256.Sum256([]byte(rawCode))
	codeHash := hex.EncodeToString(h[:])

	ch := &RecoveryChallenge{
		ID:             uuid.New().String(),
		TenantID:       tenantID,
		AccountAddress: address,
		AuthType:       authType,
		AuthValue:      authValue,
		CodeHash:       codeHash,
		RawCode:        rawCode, // Available in test harness
		ExpiresAt:      now.Add(10 * time.Minute),
		MaxAttempts:    5,
		Verified:       false,
		CreatedAt:      now,
	}

	if err := s.repo.SaveChallenge(ctx, ch); err != nil {
		return nil, fmt.Errorf("save challenge: %w", err)
	}

	return ch, nil
}

func (s *recoveryService) VerifyChallenge(ctx context.Context, tenantID, challengeID, code string) (*RecoveryChallenge, error) {
	ch, err := s.repo.GetChallenge(ctx, challengeID)
	if err != nil {
		return nil, err
	}

	rateKey := fmt.Sprintf("verify:%s", ch.AccountAddress)
	if !s.rateLimiter.CheckAndIncrement(rateKey) {
		return nil, ErrRateLimited
	}

	now := time.Now().UTC()
	if now.After(ch.ExpiresAt) {
		return nil, ErrChallengeExpired
	}

	if ch.Attempts >= ch.MaxAttempts {
		return nil, ErrMaxAttemptsExceeded
	}

	ch.Attempts++

	h := sha256.Sum256([]byte(strings.TrimSpace(code)))
	codeHash := hex.EncodeToString(h[:])

	if codeHash != ch.CodeHash {
		_ = s.repo.UpdateChallenge(ctx, ch)
		return nil, ErrInvalidOTPCode
	}

	ch.Verified = true
	ch.VerifiedAt = now
	if err := s.repo.UpdateChallenge(ctx, ch); err != nil {
		return nil, err
	}

	return ch, nil
}

func (s *recoveryService) SignTransaction(ctx context.Context, tenantID, address, txXDR string, proof SignatureAuthProof) (*SignatureResult, error) {
	// Enforce brute-force rate limit per account
	rateKey := fmt.Sprintf("sign:%s", address)
	if !s.rateLimiter.CheckAndIncrement(rateKey) {
		return nil, ErrRateLimited
	}

	// Verify Account Registration
	acct, err := s.repo.GetAccount(ctx, tenantID, address)
	if err != nil {
		return nil, err
	}

	// 1. Verify Authentication Proof (Phone/Email OTP or KYC Attestation)
	if proof.KYCAttestationID != "" {
		if s.customerSvc == nil {
			return nil, errors.New("compliance customer service not configured for kyc verification")
		}
		cust, err := s.customerSvc.GetCustomer(ctx, tenantID, proof.KYCAttestationID, "", "", "")
		if err != nil || cust.Status != compliance.CustomerStatusAccepted {
			s.rateLimiter.RecordFailure(rateKey)
			return nil, ErrInvalidKYCAttestation
		}
	} else if proof.ChallengeID != "" {
		ch, err := s.repo.GetChallenge(ctx, proof.ChallengeID)
		if err != nil {
			return nil, err
		}
		if ch.AccountAddress != address {
			return nil, errors.New("challenge does not match target account")
		}

		if !ch.Verified {
			if proof.Code == "" {
				return nil, ErrMissingAuthProof
			}
			if _, err := s.VerifyChallenge(ctx, tenantID, proof.ChallengeID, proof.Code); err != nil {
				return nil, err
			}
		}
	} else {
		return nil, ErrMissingAuthProof
	}

	// 2. Strict Authorization Constraints on Transaction
	// Decode transaction envelope from base64 XDR
	genericTx, err := txnbuild.TransactionFromXDR(txXDR)
	if err != nil {
		return nil, fmt.Errorf("decode transaction xdr: %w", err)
	}
	tx, ok := genericTx.Transaction()
	if !ok {
		return nil, errors.New("xdr does not represent a standard transaction")
	}

	// Verify that the transaction touches the account being recovered
	accountMatched := false
	if tx.SourceAccount().AccountID == address {
		accountMatched = true
	} else {
		// Check operations
		for _, op := range tx.Operations() {
			if op == nil {
				continue
			}
			// Check operation source account
			if op.GetSourceAccount() != "" && op.GetSourceAccount() == address {
				accountMatched = true
				break
			}
			// Check SetOptions operation targeting address
			if setOpt, isSet := op.(*txnbuild.SetOptions); isSet {
				if setOpt.SourceAccount == address || tx.SourceAccount().AccountID == address {
					accountMatched = true
					break
				}
			}
			// Check Payment destination or source
			if pay, isPay := op.(*txnbuild.Payment); isPay {
				if pay.Destination == address || pay.SourceAccount == address {
					accountMatched = true
					break
				}
			}
		}
	}

	if !accountMatched {
		return nil, ErrUnauthorizedTransaction
	}

	// 3. Decrypt Recovery Signer Secret Key
	decSeed, err := crypto.Decrypt([]byte(acct.EncryptedSignerKey), s.masterKey)
	if err != nil {
		return nil, fmt.Errorf("decrypt recovery signer key: %w", err)
	}

	fullKP, err := keypair.ParseFull(string(decSeed))
	if err != nil {
		return nil, fmt.Errorf("parse recovery keypair: %w", err)
	}

	// 4. Sign Transaction
	signedTx, err := tx.Sign(s.networkPassphrase, fullKP)
	if err != nil {
		return nil, fmt.Errorf("sign transaction: %w", err)
	}

	signedXDR, err := signedTx.Base64()
	if err != nil {
		return nil, fmt.Errorf("encode signed transaction: %w", err)
	}

	// Extract the signature matching our public key
	var rawSig string
	for _, s := range signedTx.Signatures() {
		hint := fullKP.Hint()
		if s.Hint == xdr.SignatureHint(hint) {
			rawSig = base64.StdEncoding.EncodeToString(s.Signature)
			break
		}
	}
	if rawSig == "" && len(signedTx.Signatures()) > 0 {
		rawSig = base64.StdEncoding.EncodeToString(signedTx.Signatures()[len(signedTx.Signatures())-1].Signature)
	}

	return &SignatureResult{
		NetworkPassphrase: s.networkPassphrase,
		Signature:         rawSig,
		SignerPublicKey:   fullKP.Address(),
		SignedTransaction: signedXDR,
	}, nil
}
