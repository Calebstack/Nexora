package anchor

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/strkey"
)

// ============================================================================
// SEP-1 Specification Data Types
// Reference: https://github.com/stellar/stellar-protocol/blob/master/ecosystem/sep-0001.md
// ============================================================================

// StellarToml represents the complete, RFC-compliant SEP-1 stellar.toml configuration.
type StellarToml struct {
	Version              string         `toml:"VERSION,omitempty"`
	NetworkPassphrase    string         `toml:"NETWORK_PASSPHRASE,omitempty"`
	FederationServer     string         `toml:"FEDERATION_SERVER,omitempty"`
	AuthServer           string         `toml:"AUTH_SERVER,omitempty"`
	TransferServer       string         `toml:"TRANSFER_SERVER,omitempty"`
	TransferServerSep24  string         `toml:"TRANSFER_SERVER_SEP0024,omitempty"`
	KYCServer            string         `toml:"KYC_SERVER,omitempty"`
	WebAuthEndpoint      string         `toml:"WEB_AUTH_ENDPOINT,omitempty"`
	SigningKey           string         `toml:"SIGNING_KEY,omitempty"`
	HorizonURL           string         `toml:"HORIZON_URL,omitempty"`
	Accounts             []string       `toml:"ACCOUNTS,omitempty"`
	URIRequestSigningKey string         `toml:"URI_REQUEST_SIGNING_KEY,omitempty"`
	DirectPaymentServer  string         `toml:"DIRECT_PAYMENT_SERVER,omitempty"`
	Documentation        *Documentation `toml:"DOCUMENTATION,omitempty"`
	Principals           []Principal    `toml:"PRINCIPALS,omitempty"`
	Currencies           []Currency     `toml:"CURRENCIES,omitempty"`
	Validators           []Validator    `toml:"VALIDATORS,omitempty"`
}

// Documentation describes the organization publishing the stellar.toml.
type Documentation struct {
	OrgName                       string `toml:"ORG_NAME,omitempty"`
	OrgDBA                        string `toml:"ORG_DBA,omitempty"`
	OrgURL                        string `toml:"ORG_URL,omitempty"`
	OrgLogo                       string `toml:"ORG_LOGO,omitempty"`
	OrgDescription                string `toml:"ORG_DESCRIPTION,omitempty"`
	OrgPhysicalAddress            string `toml:"ORG_PHYSICAL_ADDRESS,omitempty"`
	OrgPhysicalAddressAttestation string `toml:"ORG_PHYSICAL_ADDRESS_ATTESTATION,omitempty"`
	OrgPhoneNumber                string `toml:"ORG_PHONE_NUMBER,omitempty"`
	OrgPhoneNumberAttestation     string `toml:"ORG_PHONE_NUMBER_ATTESTATION,omitempty"`
	OrgKeybase                    string `toml:"ORG_KEYBASE,omitempty"`
	OrgTwitter                    string `toml:"ORG_TWITTER,omitempty"`
	OrgGithub                     string `toml:"ORG_GITHUB,omitempty"`
	OrgOfficialEmail              string `toml:"ORG_OFFICIAL_EMAIL,omitempty"`
	OrgSupportEmail               string `toml:"ORG_SUPPORT_EMAIL,omitempty"`
	OrgLicensingAuthority         string `toml:"ORG_LICENSING_AUTHORITY,omitempty"`
	OrgLicenseType                string `toml:"ORG_LICENSE_TYPE,omitempty"`
	OrgLicenseNumber              string `toml:"ORG_LICENSE_NUMBER,omitempty"`
}

// Principal defines a primary individual associated with the organization.
type Principal struct {
	Name                  string `toml:"name,omitempty"`
	Email                 string `toml:"email,omitempty"`
	Keybase               string `toml:"keybase,omitempty"`
	Telegram              string `toml:"telegram,omitempty"`
	Twitter               string `toml:"twitter,omitempty"`
	Github                string `toml:"github,omitempty"`
	IDPhotoHash           string `toml:"id_photo_hash,omitempty"`
	VerificationPhotoHash string `toml:"verification_photo_hash,omitempty"`
}

// Currency defines an asset anchored, issued, or supported by the organization.
type Currency struct {
	Code                        string   `toml:"code"`
	CodeTemplate                string   `toml:"code_template,omitempty"`
	Issuer                      string   `toml:"issuer,omitempty"`
	Status                      string   `toml:"status,omitempty"` // "live", "dead", "test"
	DisplayDecimals             int      `toml:"display_decimals,omitempty"`
	Name                        string   `toml:"name,omitempty"`
	Desc                        string   `toml:"desc,omitempty"`
	Conditions                  string   `toml:"conditions,omitempty"`
	Image                       string   `toml:"image,omitempty"`
	FixedNumber                 int64    `toml:"fixed_number,omitempty"`
	MaxNumber                   int64    `toml:"max_number,omitempty"`
	IsUnlimited                 bool     `toml:"is_unlimited,omitempty"`
	IsAssetAnchored             bool     `toml:"is_asset_anchored,omitempty"`
	AnchorAssetType             string   `toml:"anchor_asset_type,omitempty"`
	AnchorAsset                 string   `toml:"anchor_asset,omitempty"`
	AttestationOfReserve        string   `toml:"attestation_of_reserve,omitempty"`
	RedemptionInstructions      string   `toml:"redemption_instructions,omitempty"`
	CollateralAddresses         []string `toml:"collateral_addresses,omitempty"`
	CollateralAddressSignatures []string `toml:"collateral_address_signatures,omitempty"`
	Regulated                   bool     `toml:"regulated,omitempty"`
	ApprovalServer              string   `toml:"approval_server,omitempty"`
	ApprovalCriteria            string   `toml:"approval_criteria,omitempty"`
}

// Validator defines a Stellar Core consensus validator node.
type Validator struct {
	Alias       string `toml:"ALIAS,omitempty"`
	DisplayName string `toml:"DISPLAY_NAME,omitempty"`
	PublicKey   string `toml:"PUBLIC_KEY,omitempty"`
	Host        string `toml:"HOST,omitempty"`
	History     string `toml:"HISTORY,omitempty"`
}

// ============================================================================
// Legacy compatibility structures (preserved for existing callers)
// ============================================================================

type tomlDoc struct {
	SigningKey          string         `toml:"SIGNING_KEY"`
	NetworkPassphrase   string         `toml:"NETWORK_PASSPHRASE"`
	WebAuthEndpoint     string         `toml:"WEB_AUTH_ENDPOINT"`
	TransferServer      string         `toml:"TRANSFER_SERVER"`
	TransferServerSep24 string         `toml:"TRANSFER_SERVER_SEP0024"`
	KYCServer           string         `toml:"KYC_SERVER"`
	Currencies          []tomlCurrency `toml:"CURRENCIES"`
}

type tomlCurrency struct {
	Code   string `toml:"code"`
	Issuer string `toml:"issuer"`
	Status string `toml:"status"`
}

// NormalizeHomeDomain strips a scheme and trailing slash from a home domain.
func NormalizeHomeDomain(homeDomain string) string {
	homeDomain = strings.TrimSpace(homeDomain)
	homeDomain = strings.TrimPrefix(homeDomain, "https://")
	homeDomain = strings.TrimPrefix(homeDomain, "http://")
	homeDomain = strings.TrimSuffix(homeDomain, "/")
	return homeDomain
}

// TomlInfo is the parsed, validated result of fetching an anchor's stellar.toml.
type TomlInfo struct {
	SigningKey          string
	NetworkPassphrase   string
	WebAuthEndpoint     string
	TransferServer      string
	TransferServerSep24 string
	KYCServer           string
	SupportedAssets     []string
}

// FetchStellarToml retrieves and parses the stellar.toml hosted at
// https://<homeDomain>/.well-known/stellar.toml (SEP-1).
func FetchStellarToml(ctx context.Context, homeDomain string, httpClient *http.Client) (*TomlInfo, error) {
	homeDomain = NormalizeHomeDomain(homeDomain)
	if homeDomain == "" {
		return nil, fmt.Errorf("home domain is required")
	}

	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}

	url := "https://" + homeDomain + "/.well-known/stellar.toml"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build stellar.toml request: %w", err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch stellar.toml from %s: %w", homeDomain, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch stellar.toml from %s: unexpected status %d", homeDomain, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read stellar.toml body: %w", err)
	}

	var doc tomlDoc
	if err := toml.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("parse stellar.toml: %w", err)
	}

	if doc.SigningKey == "" {
		return nil, fmt.Errorf("stellar.toml at %s has no SIGNING_KEY", homeDomain)
	}
	if doc.WebAuthEndpoint == "" {
		return nil, fmt.Errorf("stellar.toml at %s has no WEB_AUTH_ENDPOINT", homeDomain)
	}
	if doc.TransferServer == "" && doc.TransferServerSep24 == "" {
		return nil, fmt.Errorf("stellar.toml at %s declares no SEP-6 or SEP-24 transfer server", homeDomain)
	}

	assets := make([]string, 0, len(doc.Currencies))
	for _, c := range doc.Currencies {
		if c.Code != "" {
			assets = append(assets, c.Code)
		}
	}

	return &TomlInfo{
		SigningKey:          doc.SigningKey,
		NetworkPassphrase:   doc.NetworkPassphrase,
		WebAuthEndpoint:     doc.WebAuthEndpoint,
		TransferServer:      doc.TransferServer,
		TransferServerSep24: doc.TransferServerSep24,
		KYCServer:           doc.KYCServer,
		SupportedAssets:     assets,
	}, nil
}

// ============================================================================
// Generator & Validator Implementation (Issue #28)
// ============================================================================

var (
	assetCodeRegex = regexp.MustCompile(`^[a-zA-Z0-9]{1,12}$`)
)

// GenerateToml encodes a StellarToml struct into RFC-compliant TOML bytes and validates it.
func GenerateToml(doc *StellarToml) ([]byte, error) {
	if doc == nil {
		return nil, errors.New("stellar toml document is nil")
	}

	if doc.Version == "" {
		doc.Version = "2.0.0"
	}

	if err := ValidateToml(doc); err != nil {
		return nil, fmt.Errorf("toml validation failed: %w", err)
	}

	data, err := toml.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("marshal toml: %w", err)
	}

	return data, nil
}

// ParseToml decodes raw TOML bytes into a StellarToml struct and validates compliance.
func ParseToml(data []byte) (*StellarToml, error) {
	var doc StellarToml
	if err := toml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("unmarshal toml: %w", err)
	}

	if err := ValidateToml(&doc); err != nil {
		return nil, fmt.Errorf("validate toml: %w", err)
	}

	return &doc, nil
}

// ValidateToml validates a StellarToml struct against official Stellar SEP-1 requirements.
func ValidateToml(doc *StellarToml) error {
	if doc == nil {
		return errors.New("nil toml document")
	}

	// 1. Version check
	if doc.Version == "" {
		return errors.New("VERSION is required in stellar.toml")
	}

	// 2. Signing Key validation if provided
	if doc.SigningKey != "" {
		if !strkey.IsValidEd25519PublicKey(doc.SigningKey) {
			return fmt.Errorf("invalid SIGNING_KEY: %s is not a valid Stellar Ed25519 public key", doc.SigningKey)
		}
	}

	// 3. Accounts validation if provided
	for _, acc := range doc.Accounts {
		if !strkey.IsValidEd25519PublicKey(acc) {
			return fmt.Errorf("invalid ACCOUNTS entry: %s is not a valid Stellar public key", acc)
		}
	}

	// 4. Currencies validation
	for i, c := range doc.Currencies {
		if c.Code == "" {
			return fmt.Errorf("currency[%d]: code is required", i)
		}
		if !assetCodeRegex.MatchString(c.Code) {
			return fmt.Errorf("currency[%d]: invalid asset code %q (must be 1-12 alphanumeric characters)", i, c.Code)
		}

		if c.Code != "XLM" && c.Issuer != "" {
			if !strkey.IsValidEd25519PublicKey(c.Issuer) {
				return fmt.Errorf("currency[%d]: invalid issuer %s for asset %s", i, c.Issuer, c.Code)
			}
		}

		if c.Status != "" {
			switch c.Status {
			case "live", "dead", "test":
			default:
				return fmt.Errorf("currency[%d]: status %q is invalid (expected 'live', 'dead', or 'test')", i, c.Status)
			}
		}
	}

	// 5. Validators validation
	for i, v := range doc.Validators {
		if v.PublicKey != "" && !strkey.IsValidEd25519PublicKey(v.PublicKey) {
			return fmt.Errorf("validator[%d]: invalid PUBLIC_KEY: %s", i, v.PublicKey)
		}
	}

	return nil
}

// ============================================================================
// Cryptographic Signature & DNS Verification
// ============================================================================

// SignTomlContent signs the TOML content using a Stellar private secret seed (S...).
// It computes the SHA-256 hash of the content and produces a hex-encoded Ed25519 signature.
func SignTomlContent(content []byte, secretSeed string) (string, error) {
	kp, err := keypair.Parse(secretSeed)
	if err != nil {
		return "", fmt.Errorf("parse secret seed: %w", err)
	}

	hash := sha256.Sum256(content)
	sigBytes, err := kp.Sign(hash[:])
	if err != nil {
		return "", fmt.Errorf("sign content hash: %w", err)
	}

	return hex.EncodeToString(sigBytes), nil
}

// VerifyTomlSignature validates that an Ed25519 signature is valid for the given TOML content
// and matching Stellar public key (G...).
func VerifyTomlSignature(content []byte, publicKey string, signatureHex string) (bool, error) {
	rawPubKey, err := strkey.Decode(strkey.VersionByteAccountID, publicKey)
	if err != nil {
		return false, fmt.Errorf("decode public key: %w", err)
	}

	sigBytes, err := hex.DecodeString(signatureHex)
	if err != nil {
		return false, fmt.Errorf("decode signature hex: %w", err)
	}

	if len(sigBytes) != ed25519.SignatureSize {
		return false, fmt.Errorf("invalid signature length: expected %d bytes, got %d", ed25519.SignatureSize, len(sigBytes))
	}

	hash := sha256.Sum256(content)
	valid := ed25519.Verify(rawPubKey, hash[:], sigBytes)
	return valid, nil
}

// DNSResolver is an abstraction for querying DNS TXT records.
type DNSResolver interface {
	LookupTXT(ctx context.Context, host string) ([]string, error)
}

type defaultDNSResolver struct{}

func (d *defaultDNSResolver) LookupTXT(ctx context.Context, host string) ([]string, error) {
	return net.DefaultResolver.LookupTXT(ctx, host)
}

// VerifyDNS validates that the domain publishes the expected Stellar SIGNING_KEY in its DNS TXT records.
// Checks: "_stellar.<domain>" and "stellar-signing-key.<domain>".
func VerifyDNS(ctx context.Context, domain string, expectedSigningKey string, resolver DNSResolver) error {
	domain = NormalizeHomeDomain(domain)
	if domain == "" {
		return errors.New("domain cannot be empty")
	}
	if !strkey.IsValidEd25519PublicKey(expectedSigningKey) {
		return fmt.Errorf("invalid expected signing key: %s", expectedSigningKey)
	}

	if resolver == nil {
		resolver = &defaultDNSResolver{}
	}

	checkHosts := []string{
		"_stellar." + domain,
		"stellar-signing-key." + domain,
		domain,
	}

	for _, host := range checkHosts {
		records, err := resolver.LookupTXT(ctx, host)
		if err != nil {
			continue
		}
		for _, txt := range records {
			txt = strings.TrimSpace(txt)
			if txt == expectedSigningKey ||
				strings.Contains(txt, "stellar-signing-key="+expectedSigningKey) ||
				strings.Contains(txt, "SIGNING_KEY="+expectedSigningKey) {
				return nil
			}
		}
	}

	return fmt.Errorf("dns verification failed: signing key %s not found in TXT records for %s", expectedSigningKey, domain)
}

// ============================================================================
// Multi-Tenant Redis Caching
// ============================================================================

// TomlCache defines the caching interface for generated stellar.toml files.
type TomlCache interface {
	Get(ctx context.Context, tenantID, domain string) ([]byte, bool, error)
	Set(ctx context.Context, tenantID, domain string, tomlData []byte, ttl time.Duration) error
	Invalidate(ctx context.Context, tenantID, domain string) error
	InvalidateAllForTenant(ctx context.Context, tenantID string) error
}

// RedisTomlCache implements TomlCache using Redis.
type RedisTomlCache struct {
	client redis.UniversalClient
	prefix string
}

// NewRedisTomlCache creates a new RedisTomlCache.
func NewRedisTomlCache(client redis.UniversalClient, prefix string) *RedisTomlCache {
	if prefix == "" {
		prefix = "stellar:toml"
	}
	return &RedisTomlCache{client: client, prefix: prefix}
}

func (c *RedisTomlCache) key(tenantID, domain string) string {
	return fmt.Sprintf("%s:%s:%s", c.prefix, tenantID, NormalizeHomeDomain(domain))
}

func (c *RedisTomlCache) Get(ctx context.Context, tenantID, domain string) ([]byte, bool, error) {
	if c.client == nil {
		return nil, false, nil
	}
	val, err := c.client.Get(ctx, c.key(tenantID, domain)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return val, true, nil
}

func (c *RedisTomlCache) Set(ctx context.Context, tenantID, domain string, tomlData []byte, ttl time.Duration) error {
	if c.client == nil {
		return nil
	}
	return c.client.Set(ctx, c.key(tenantID, domain), tomlData, ttl).Err()
}

func (c *RedisTomlCache) Invalidate(ctx context.Context, tenantID, domain string) error {
	if c.client == nil {
		return nil
	}
	return c.client.Del(ctx, c.key(tenantID, domain)).Err()
}

func (c *RedisTomlCache) InvalidateAllForTenant(ctx context.Context, tenantID string) error {
	if c.client == nil {
		return nil
	}
	pattern := fmt.Sprintf("%s:%s:*", c.prefix, tenantID)
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()
	for iter.Next(ctx) {
		_ = c.client.Del(ctx, iter.Val()).Err()
	}
	return iter.Err()
}

// MemoryTomlCache is an in-memory thread-safe implementation of TomlCache for testing or local use.
type MemoryTomlCache struct {
	mu    sync.RWMutex
	store map[string][]byte
}

// NewMemoryTomlCache creates a new MemoryTomlCache.
func NewMemoryTomlCache() *MemoryTomlCache {
	return &MemoryTomlCache{
		store: make(map[string][]byte),
	}
}

func (m *MemoryTomlCache) key(tenantID, domain string) string {
	return fmt.Sprintf("%s:%s", tenantID, NormalizeHomeDomain(domain))
}

func (m *MemoryTomlCache) Get(_ context.Context, tenantID, domain string) ([]byte, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.store[m.key(tenantID, domain)]
	if !ok {
		return nil, false, nil
	}
	copied := make([]byte, len(val))
	copy(copied, val)
	return copied, true, nil
}

func (m *MemoryTomlCache) Set(_ context.Context, tenantID, domain string, tomlData []byte, _ time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	copied := make([]byte, len(tomlData))
	copy(copied, tomlData)
	m.store[m.key(tenantID, domain)] = copied
	return nil
}

func (m *MemoryTomlCache) Invalidate(_ context.Context, tenantID, domain string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.store, m.key(tenantID, domain))
	return nil
}

func (m *MemoryTomlCache) InvalidateAllForTenant(_ context.Context, tenantID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	prefix := tenantID + ":"
	for k := range m.store {
		if strings.HasPrefix(k, prefix) {
			delete(m.store, k)
		}
	}
	return nil
}

// ============================================================================
// Multi-Tenant Toml Server & HTTP Handler
// ============================================================================

// TenantTomlProvider retrieves or builds the StellarToml document for a given tenant or host domain.
type TenantTomlProvider interface {
	GetTomlForDomain(ctx context.Context, domain string) (tenantID string, doc *StellarToml, err error)
}

// TomlServerConfig configures the TomlServer.
type TomlServerConfig struct {
	CacheTTL      time.Duration
	AllowInsecure bool // For local development testing
}

// TomlServer serves dynamic, multi-tenant RFC-compliant stellar.toml documents.
type TomlServer struct {
	provider TenantTomlProvider
	cache    TomlCache
	cfg      TomlServerConfig
}

// NewTomlServer creates a new TomlServer.
func NewTomlServer(provider TenantTomlProvider, cache TomlCache, cfg TomlServerConfig) *TomlServer {
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = 1 * time.Hour
	}
	return &TomlServer{
		provider: provider,
		cache:    cache,
		cfg:      cfg,
	}
}

// ServeHTTP handles requests for /.well-known/stellar.toml with CORS and security headers.
func (s *TomlServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Mandatory CORS headers per SEP-1
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, X-Requested-With")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// 2. Security & SSL headers
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", int(s.cfg.CacheTTL.Seconds())))
	if !s.cfg.AllowInsecure {
		w.Header().Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
	}

	// 3. Resolve tenant and domain
	host := r.Host
	if colon := strings.IndexByte(host, ':'); colon != -1 {
		host = host[:colon]
	}

	ctx := r.Context()

	// Check cache
	if s.cache != nil {
		if cached, ok, err := s.cache.Get(ctx, host, host); err == nil && ok && len(cached) > 0 {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(cached)
			return
		}
	}

	// Dynamic resolution from provider
	tenantID, doc, err := s.provider.GetTomlForDomain(ctx, host)
	if err != nil {
		http.Error(w, "stellar.toml not found for domain", http.StatusNotFound)
		return
	}

	// Encode and validate
	tomlBytes, err := GenerateToml(doc)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to generate stellar.toml: %v", err), http.StatusInternalServerError)
		return
	}

	// Populate cache
	if s.cache != nil {
		_ = s.cache.Set(ctx, tenantID, host, tomlBytes, s.cfg.CacheTTL)
		if tenantID != host {
			_ = s.cache.Set(ctx, host, host, tomlBytes, s.cfg.CacheTTL)
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(tomlBytes)
}
