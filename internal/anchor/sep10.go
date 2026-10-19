package anchor

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/txnbuild"
)

// Sep10Client performs SEP-10 web authentication against a single anchor's
// WEB_AUTH_ENDPOINT, proving control of a Stellar account to obtain a JWT.
type Sep10Client struct {
	webAuthEndpoint   string
	homeDomain        string
	networkPassphrase string
	httpClient        *http.Client
}

func NewSep10Client(webAuthEndpoint, homeDomain, networkPassphrase string, httpClient *http.Client) *Sep10Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 15 * time.Second}
	}
	return &Sep10Client{
		webAuthEndpoint:   webAuthEndpoint,
		homeDomain:        homeDomain,
		networkPassphrase: networkPassphrase,
		httpClient:        httpClient,
	}
}

type challengeResponse struct {
	Transaction       string `json:"transaction"`
	NetworkPassphrase string `json:"network_passphrase"`
	Error             string `json:"error"`
}

// Challenge requests a SEP-10 challenge transaction from the anchor and
// validates it before returning: the transaction must be signed by the
// anchor's serverSigningKey (the account named in SIGNING_KEY on its
// stellar.toml), have a zero sequence number, and its first ManageData
// operation must be sourced from clientPublicKey and named
// "<home domain> auth". Any anchor that returns a challenge sourced from a
// different account, or otherwise malformed, is rejected here and no JWT is
// ever requested for it.
func (c *Sep10Client) Challenge(ctx context.Context, serverSigningKey, clientPublicKey string) (string, error) {
	endpoint, err := url.Parse(c.webAuthEndpoint)
	if err != nil {
		return "", fmt.Errorf("parse web auth endpoint: %w", err)
	}
	q := endpoint.Query()
	q.Set("account", clientPublicKey)
	if c.homeDomain != "" {
		q.Set("home_domain", c.homeDomain)
	}
	endpoint.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return "", fmt.Errorf("build challenge request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("request challenge: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read challenge response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("web_auth challenge request failed with status %d: %s", resp.StatusCode, body)
	}

	var chal challengeResponse
	if err := json.Unmarshal(body, &chal); err != nil {
		return "", fmt.Errorf("parse challenge response: %w", err)
	}
	if chal.Error != "" {
		return "", fmt.Errorf("anchor returned error: %s", chal.Error)
	}
	if chal.Transaction == "" {
		return "", fmt.Errorf("anchor challenge response has no transaction")
	}

	webAuthDomain := endpoint.Host

	// ReadChallengeTx verifies the transaction is sourced from and signed by
	// serverSigningKey, has sequence number 0, and that the first ManageData
	// operation is sourced from clientPublicKey and named "<home domain> auth".
	// A challenge whose source account is not the anchor's signing key fails
	// here with an error, and Authenticate is never called.
	_, txClientAccountID, _, _, err := txnbuild.ReadChallengeTx(
		chal.Transaction,
		serverSigningKey,
		c.networkPassphrase,
		webAuthDomain,
		[]string{c.homeDomain},
	)
	if err != nil {
		return "", fmt.Errorf("invalid SEP-10 challenge: %w", err)
	}
	if txClientAccountID != clientPublicKey {
		return "", fmt.Errorf("challenge client account %s does not match requested account %s", txClientAccountID, clientPublicKey)
	}

	return chal.Transaction, nil
}

type authRequest struct {
	Transaction string `json:"transaction"`
}

type authResponse struct {
	Token string `json:"token"`
	Error string `json:"error"`
}

// Authenticate signs a challenge previously returned by Challenge with the
// client's secret key and submits it back to the anchor, returning the JWT
// the anchor issues in exchange.
func (c *Sep10Client) Authenticate(ctx context.Context, challenge, clientSecretKey string) (string, error) {
	genericTx, err := txnbuild.TransactionFromXDR(challenge)
	if err != nil {
		return "", fmt.Errorf("parse challenge xdr: %w", err)
	}
	tx, ok := genericTx.Transaction()
	if !ok {
		return "", fmt.Errorf("challenge is not a valid transaction envelope")
	}

	signedTx, err := tx.SignWithKeyString(c.networkPassphrase, clientSecretKey)
	if err != nil {
		return "", fmt.Errorf("sign challenge: %w", err)
	}

	signedXDR, err := signedTx.Base64()
	if err != nil {
		return "", fmt.Errorf("encode signed challenge: %w", err)
	}

	reqBody, err := json.Marshal(authRequest{Transaction: signedXDR})
	if err != nil {
		return "", fmt.Errorf("marshal auth request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.webAuthEndpoint, bytes.NewReader(reqBody))
	if err != nil {
		return "", fmt.Errorf("build auth request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("submit signed challenge: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", fmt.Errorf("read auth response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("web_auth token request failed with status %d: %s", resp.StatusCode, body)
	}

	var auth authResponse
	if err := json.Unmarshal(body, &auth); err != nil {
		return "", fmt.Errorf("parse auth response: %w", err)
	}
	if auth.Error != "" {
		return "", fmt.Errorf("anchor returned error: %s", auth.Error)
	}
	if auth.Token == "" {
		return "", fmt.Errorf("anchor did not return a token")
	}

	return auth.Token, nil
}

// -------------------------------------------------------------------------
// SEP-10 Server & Verification Engine (Issue #21)
// -------------------------------------------------------------------------

var (
	ErrChallengeExpired         = errors.New("challenge transaction expired or older than 300 seconds")
	ErrChallengeInvalidTime     = errors.New("challenge transaction timestamp bounds invalid")
	ErrChallengeReplayed        = errors.New("challenge transaction has already been used")
	ErrChallengeInvalidSequence = errors.New("challenge transaction sequence number must be 0")
	ErrChallengeInvalidSource   = errors.New("challenge transaction source account mismatch")
	ErrChallengeMissingClient   = errors.New("challenge transaction missing client signature")
	ErrChallengeMissingClientDomain = errors.New("challenge transaction missing client domain signature")
	ErrChallengeInvalidOp       = errors.New("challenge transaction first operation must be ManageData auth key")
	ErrInvalidHomeDomain        = errors.New("home_domain does not match server home domain")
	ErrInvalidClientAccount     = errors.New("client account address is invalid")
)

// ReplayGuard prevents replay attacks by tracking transaction hashes of consumed challenges.
type ReplayGuard interface {
	HasBeenUsed(ctx context.Context, txHash string) (bool, error)
	MarkUsed(ctx context.Context, txHash string, ttl time.Duration) error
}

// MemoryReplayGuard provides a thread-safe, in-memory replay guard with automatic TTL eviction.
type MemoryReplayGuard struct {
	mu     sync.RWMutex
	used   map[string]time.Time
	stopCh chan struct{}
}

func NewMemoryReplayGuard() *MemoryReplayGuard {
	g := &MemoryReplayGuard{
		used:   make(map[string]time.Time),
		stopCh: make(chan struct{}),
	}
	go g.evictionLoop()
	return g
}

func (g *MemoryReplayGuard) evictionLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			g.evict()
		case <-g.stopCh:
			return
		}
	}
}

func (g *MemoryReplayGuard) evict() {
	now := time.Now().UTC()
	g.mu.Lock()
	defer g.mu.Unlock()
	for hash, exp := range g.used {
		if now.After(exp) {
			delete(g.used, hash)
		}
	}
}

func (g *MemoryReplayGuard) Close() {
	select {
	case <-g.stopCh:
	default:
		close(g.stopCh)
	}
}

func (g *MemoryReplayGuard) HasBeenUsed(_ context.Context, txHash string) (bool, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	exp, exists := g.used[txHash]
	if !exists {
		return false, nil
	}
	if time.Now().UTC().After(exp) {
		return false, nil
	}
	return true, nil
}

func (g *MemoryReplayGuard) MarkUsed(_ context.Context, txHash string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = 300 * time.Second
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used[txHash] = time.Now().UTC().Add(ttl)
	return nil
}

// Sep10Token represents an issued SEP-10 JWT bearer token.
type Sep10Token struct {
	Token     string `json:"token"`
	Account   string `json:"account"`
	ExpiresAt int64  `json:"expires_at"`
}

// Sep10Claims holds the payload claims of a SEP-10 JWT.
type Sep10Claims struct {
	Iss          string `json:"iss"`
	Sub          string `json:"sub"`
	Iat          int64  `json:"iat"`
	Exp          int64  `json:"exp"`
	Jti          string `json:"jti"`
	ClientDomain string `json:"client_domain,omitempty"`
	HomeDomain   string `json:"home_domain,omitempty"`
}

// Sep10ServerConfig configures the SEP-10 authentication server.
type Sep10ServerConfig struct {
	ServerSeed        string
	HomeDomain        string
	WebAuthDomain     string
	NetworkPassphrase string
	JWTSecret         []byte
	JWTDuration       time.Duration
	ChallengeTimeout  time.Duration
	ReplayGuard       ReplayGuard
}

// Sep10Server issues and verifies SEP-10 challenge transactions.
type Sep10Server struct {
	serverKP          *keypair.Full
	serverAccountID   string
	homeDomain        string
	webAuthDomain     string
	networkPassphrase string
	jwtSecret         []byte
	jwtDuration       time.Duration
	challengeTimeout  time.Duration
	replayGuard       ReplayGuard
}

// NewSep10Server constructs an initialized SEP-10 authentication server.
func NewSep10Server(cfg Sep10ServerConfig) (*Sep10Server, error) {
	kp, err := keypair.ParseFull(cfg.ServerSeed)
	if err != nil {
		return nil, fmt.Errorf("invalid server seed: %w", err)
	}

	jwtDuration := cfg.JWTDuration
	if jwtDuration <= 0 {
		jwtDuration = 24 * time.Hour
	}

	challengeTimeout := cfg.ChallengeTimeout
	if challengeTimeout <= 0 {
		challengeTimeout = 300 * time.Second
	}

	replayGuard := cfg.ReplayGuard
	if replayGuard == nil {
		replayGuard = NewMemoryReplayGuard()
	}

	webAuthDomain := cfg.WebAuthDomain
	if webAuthDomain == "" {
		webAuthDomain = cfg.HomeDomain
	}

	return &Sep10Server{
		serverKP:          kp,
		serverAccountID:   kp.Address(),
		homeDomain:        cfg.HomeDomain,
		webAuthDomain:     webAuthDomain,
		networkPassphrase: cfg.NetworkPassphrase,
		jwtSecret:         cfg.JWTSecret,
		jwtDuration:       jwtDuration,
		challengeTimeout:  challengeTimeout,
		replayGuard:       replayGuard,
	}, nil
}

// GenerateChallenge creates a cryptographically secure SEP-10 challenge transaction envelope.
func (s *Sep10Server) GenerateChallenge(ctx context.Context, clientAccountID, clientHomeDomain, clientDomain string, memo *uint64) (string, error) {
	if _, err := keypair.ParseAddress(clientAccountID); err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidClientAccount, err)
	}

	if clientHomeDomain != "" && !strings.EqualFold(clientHomeDomain, s.homeDomain) {
		return "", ErrInvalidHomeDomain
	}

	now := time.Now().UTC()
	// Sequence zero per SEP-10 specification
	var seqZero int64 = 0

	// 48 cryptographically random bytes base64-encoded to exactly 64 bytes per SEP-10
	rawBytes := make([]byte, 48)
	if _, err := io.ReadFull(rand.Reader, rawBytes); err != nil {
		return "", fmt.Errorf("generate cryptographic nonce: %w", err)
	}
	randomBytes := make([]byte, 64)
	base64.StdEncoding.Encode(randomBytes, rawBytes)

	ops := make([]txnbuild.Operation, 0, 3)

	// Op 1: ManageData operation sourced from client account with '<home_domain> auth'
	ops = append(ops, &txnbuild.ManageData{
		SourceAccount: clientAccountID,
		Name:          fmt.Sprintf("%s auth", s.homeDomain),
		Value:         randomBytes,
	})

	// Op 2: If webAuthDomain is distinct from homeDomain, record web_auth_domain
	if s.webAuthDomain != "" && !strings.EqualFold(s.webAuthDomain, s.homeDomain) {
		ops = append(ops, &txnbuild.ManageData{
			SourceAccount: s.serverAccountID,
			Name:          "web_auth_domain",
			Value:         []byte(s.webAuthDomain),
		})
	}

	// Op 3: If clientDomain is provided, record client_domain ManageData operation sourced from server
	if clientDomain != "" {
		ops = append(ops, &txnbuild.ManageData{
			SourceAccount: s.serverAccountID,
			Name:          "client_domain",
			Value:         []byte(clientDomain),
		})
	}

	var txMemo txnbuild.Memo
	if memo != nil {
		txMemo = txnbuild.MemoID(*memo)
	}

	tb := txnbuild.NewTimebounds(now.Unix(), now.Add(s.challengeTimeout).Unix())

	account := txnbuild.SimpleAccount{
		AccountID: s.serverAccountID,
		Sequence:  seqZero,
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: false,
		Operations:           ops,
		BaseFee:              txnbuild.MinBaseFee,
		Memo:                 txMemo,
		Preconditions: txnbuild.Preconditions{
			TimeBounds: tb,
		},
	})
	if err != nil {
		return "", fmt.Errorf("build challenge transaction: %w", err)
	}

	signedTx, err := tx.Sign(s.networkPassphrase, s.serverKP)
	if err != nil {
		return "", fmt.Errorf("sign challenge with server key: %w", err)
	}

	return signedTx.Base64()
}

// VerifyAndIssueToken strictly validates the challenge transaction and returns a signed JWT.
func (s *Sep10Server) VerifyAndIssueToken(ctx context.Context, signedChallengeXDR, clientDomainSigningKey string) (*Sep10Token, error) {
	genericTx, err := txnbuild.TransactionFromXDR(signedChallengeXDR)
	if err != nil {
		return nil, fmt.Errorf("decode challenge transaction XDR: %w", err)
	}

	tx, ok := genericTx.Transaction()
	if !ok {
		return nil, errors.New("challenge is not a standard transaction envelope")
	}

	// 1. Strict sequence zero verification
	if tx.SequenceNumber() != 0 {
		return nil, ErrChallengeInvalidSequence
	}

	// 2. Strict source account check: must match server signing key
	if tx.SourceAccount().AccountID != s.serverAccountID {
		return nil, ErrChallengeInvalidSource
	}

	// 3. Cryptographic timestamp skew and age bounds (reject > 300s or expired)
	now := time.Now().UTC()
	tb := tx.Timebounds()
	if tb.MinTime == 0 && tb.MaxTime == 0 {
		return nil, ErrChallengeInvalidTime
	}

	// Reject if not yet valid (with 60s clock skew tolerance)
	if now.Unix() < tb.MinTime-60 {
		return nil, ErrChallengeInvalidTime
	}

	// Reject if expired past max_time
	if now.Unix() > tb.MaxTime {
		return nil, ErrChallengeExpired
	}

	// Reject if challenge duration exceeds 300 seconds
	if tb.MaxTime-tb.MinTime > 300 {
		return nil, ErrChallengeExpired
	}

	// Reject if elapsed time since min_time is greater than 300 seconds
	if now.Sub(time.Unix(tb.MinTime, 0)) > 300*time.Second {
		return nil, ErrChallengeExpired
	}

	// 4. Replay attack protection: hash must not have been previously presented
	txHashBytes, err := tx.Hash(s.networkPassphrase)
	if err != nil {
		return nil, fmt.Errorf("calculate transaction hash: %w", err)
	}
	txHash := hex.EncodeToString(txHashBytes[:])

	used, err := s.replayGuard.HasBeenUsed(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("check replay guard: %w", err)
	}
	if used {
		return nil, ErrChallengeReplayed
	}

	// 5. Inspect operations & extract client account and client domain
	ops := tx.Operations()
	if len(ops) == 0 {
		return nil, ErrChallengeInvalidOp
	}

	firstOp, ok := ops[0].(*txnbuild.ManageData)
	if !ok || firstOp == nil {
		return nil, ErrChallengeInvalidOp
	}

	expectedAuthKey := fmt.Sprintf("%s auth", s.homeDomain)
	if firstOp.Name != expectedAuthKey {
		return nil, fmt.Errorf("%w: expected key %q, got %q", ErrChallengeInvalidOp, expectedAuthKey, firstOp.Name)
	}

	clientAccountID := firstOp.SourceAccount
	if clientAccountID == "" {
		return nil, ErrChallengeInvalidSource
	}
	if _, err := keypair.ParseAddress(clientAccountID); err != nil {
		return nil, ErrInvalidClientAccount
	}

	// Extract optional client_domain
	var clientDomain string
	for _, op := range ops[1:] {
		if md, ok := op.(*txnbuild.ManageData); ok && md.Name == "client_domain" {
			clientDomain = string(md.Value)
			break
		}
	}

	// 6. Enforce strict home_domain and client_domain signing requirements
	signers := []string{clientAccountID}
	if clientDomain != "" {
		if clientDomainSigningKey == "" {
			return nil, ErrChallengeMissingClientDomain
		}
		signers = append(signers, clientDomainSigningKey)
	}

	matchedSigners, err := txnbuild.VerifyChallengeTxSigners(
		signedChallengeXDR,
		s.serverAccountID,
		s.networkPassphrase,
		s.webAuthDomain,
		[]string{s.homeDomain},
		signers...,
	)
	if err != nil {
		return nil, fmt.Errorf("verify challenge signatures: %w", err)
	}

	// Verify client signature is present
	clientSigned := false
	for _, signer := range matchedSigners {
		if signer == clientAccountID {
			clientSigned = true
			break
		}
	}
	if !clientSigned {
		return nil, ErrChallengeMissingClient
	}

	// If clientDomain was required, verify client_domain signer was matched
	if clientDomain != "" {
		clientDomainSigned := false
		for _, signer := range matchedSigners {
			if signer == clientDomainSigningKey {
				clientDomainSigned = true
				break
			}
		}
		if !clientDomainSigned {
			return nil, ErrChallengeMissingClientDomain
		}
	}

	// Mark hash as used in replay guard with TTL
	ttl := time.Duration(tb.MaxTime-now.Unix()) * time.Second
	if ttl <= 0 {
		ttl = 300 * time.Second
	}
	if err := s.replayGuard.MarkUsed(ctx, txHash, ttl); err != nil {
		return nil, fmt.Errorf("record consumed challenge in replay guard: %w", err)
	}

	// 7. Issue signed JWT bearer token with scoped claims
	exp := now.Add(s.jwtDuration).Unix()
	claims := Sep10Claims{
		Iss:          fmt.Sprintf("https://%s", s.webAuthDomain),
		Sub:          clientAccountID,
		Iat:          now.Unix(),
		Exp:          exp,
		Jti:          uuid.New().String(),
		ClientDomain: clientDomain,
		HomeDomain:   s.homeDomain,
	}

	tokenStr, err := s.signJWT(claims)
	if err != nil {
		return nil, fmt.Errorf("mint SEP-10 bearer JWT: %w", err)
	}

	return &Sep10Token{
		Token:     tokenStr,
		Account:   clientAccountID,
		ExpiresAt: exp,
	}, nil
}

func (s *Sep10Server) signJWT(claims Sep10Claims) (string, error) {
	headerJSON := `{"alg":"HS256","typ":"JWT"}`
	headerB64 := base64.RawURLEncoding.EncodeToString([]byte(headerJSON))

	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)

	signedContent := headerB64 + "." + claimsB64
	mac := hmac.New(sha256.New, s.jwtSecret)
	mac.Write([]byte(signedContent))
	sig := mac.Sum(nil)
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signedContent + "." + sigB64, nil
}

// VerifyJWT validates a SEP-10 JWT bearer token issued by this server.
func (s *Sep10Server) VerifyJWT(tokenString string) (*Sep10Claims, error) {
	parts := strings.Split(tokenString, ".")
	if len(parts) != 3 {
		return nil, errors.New("invalid jwt format")
	}

	signedContent := parts[0] + "." + parts[1]
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, errors.New("invalid jwt signature encoding")
	}

	mac := hmac.New(sha256.New, s.jwtSecret)
	mac.Write([]byte(signedContent))
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(sig, expectedSig) {
		return nil, errors.New("invalid jwt signature")
	}

	claimsJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("invalid jwt payload encoding")
	}

	var claims Sep10Claims
	if err := json.Unmarshal(claimsJSON, &claims); err != nil {
		return nil, fmt.Errorf("unmarshal claims: %w", err)
	}

	if time.Now().UTC().Unix() > claims.Exp {
		return nil, errors.New("jwt token has expired")
	}

	return &claims, nil
}
