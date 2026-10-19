package anchor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stellar/go/keypair"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTenantTomlProvider struct {
	tomls map[string]*StellarToml
}

func (m *mockTenantTomlProvider) GetTomlForDomain(_ context.Context, domain string) (string, *StellarToml, error) {
	doc, ok := m.tomls[domain]
	if !ok {
		return "", nil, errors.New("not found")
	}
	return "tenant-123", doc, nil
}

type mockDNSResolver struct {
	records map[string][]string
}

func (m *mockDNSResolver) LookupTXT(_ context.Context, host string) ([]string, error) {
	recs, ok := m.records[host]
	if !ok {
		return nil, errors.New("nxdomain")
	}
	return recs, nil
}

func TestGenerateAndParseToml_RFCCompliant(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	doc := &StellarToml{
		Version:             "2.0.0",
		NetworkPassphrase:   "Test SDF Network ; September 2015",
		TransferServer:      "https://anchor.example.com/sep6",
		TransferServerSep24: "https://anchor.example.com/sep24",
		KYCServer:           "https://anchor.example.com/sep12",
		WebAuthEndpoint:     "https://anchor.example.com/auth",
		SigningKey:          kp.Address(),
		HorizonURL:          "https://horizon-testnet.stellar.org",
		Accounts:            []string{kp.Address()},
		Documentation: &Documentation{
			OrgName:          "Nexora Global Payments",
			OrgURL:           "https://nexora.example.com",
			OrgOfficialEmail: "ops@nexora.example.com",
		},
		Principals: []Principal{
			{
				Name:  "Jane Doe",
				Email: "jane@nexora.example.com",
			},
		},
		Currencies: []Currency{
			{
				Code:            "USDC",
				Issuer:          issuerKp.Address(),
				Status:          "live",
				DisplayDecimals: 2,
				Name:            "USD Coin",
				IsAssetAnchored: true,
				AnchorAssetType: "fiat",
				AnchorAsset:     "USD",
			},
			{
				Code:            "XLM",
				Status:          "live",
				DisplayDecimals: 7,
				Name:            "Lumen",
			},
		},
		Validators: []Validator{
			{
				Alias:     "nexora-core-1",
				PublicKey: kp.Address(),
				Host:      "core-1.nexora.example.com:11625",
			},
		},
	}

	data, err := GenerateToml(doc)
	require.NoError(t, err)
	assert.NotEmpty(t, data)

	// Verify it contains expected TOML strings
	tomlStr := string(data)
	assert.Contains(t, tomlStr, "VERSION =")
	assert.Contains(t, tomlStr, "2.0.0")
	assert.Contains(t, tomlStr, "TRANSFER_SERVER =")
	assert.Contains(t, tomlStr, "https://anchor.example.com/sep6")
	assert.Contains(t, tomlStr, "code =")
	assert.Contains(t, tomlStr, "USDC")
	assert.Contains(t, tomlStr, kp.Address())

	// Parse back
	parsed, err := ParseToml(data)
	require.NoError(t, err)
	assert.Equal(t, "2.0.0", parsed.Version)
	assert.Equal(t, kp.Address(), parsed.SigningKey)
	assert.Len(t, parsed.Currencies, 2)
	assert.Equal(t, "USDC", parsed.Currencies[0].Code)
	assert.Equal(t, "XLM", parsed.Currencies[1].Code)
}

func TestValidateToml_StrictRules(t *testing.T) {
	t.Run("invalid signing key", func(t *testing.T) {
		doc := &StellarToml{
			Version:    "2.0.0",
			SigningKey: "not-a-stellar-key",
		}
		err := ValidateToml(doc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid SIGNING_KEY")
	})

	t.Run("invalid currency code", func(t *testing.T) {
		kp, err := keypair.Random()
		require.NoError(t, err)

		doc := &StellarToml{
			Version: "2.0.0",
			Currencies: []Currency{
				{
					Code:   "TOOLONGASSETCODE123", // > 12 chars
					Issuer: kp.Address(),
				},
			},
		}
		err = ValidateToml(doc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid asset code")
	})

	t.Run("invalid currency status", func(t *testing.T) {
		doc := &StellarToml{
			Version: "2.0.0",
			Currencies: []Currency{
				{
					Code:   "USD",
					Status: "invalid-status",
				},
			},
		}
		err := ValidateToml(doc)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "status \"invalid-status\" is invalid")
	})
}

func TestCryptographicSignature_SignAndVerify(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	content := []byte(`VERSION = "2.0.0"
NETWORK_PASSPHRASE = "Test SDF Network ; September 2015"
SIGNING_KEY = "` + kp.Address() + `"
`)

	sigHex, err := SignTomlContent(content, kp.Seed())
	require.NoError(t, err)
	assert.NotEmpty(t, sigHex)

	// Valid verification
	valid, err := VerifyTomlSignature(content, kp.Address(), sigHex)
	require.NoError(t, err)
	assert.True(t, valid)

	// Tampered content verification failure
	tampered := []byte(`VERSION = "2.0.0"
NETWORK_PASSPHRASE = "Tampered Network"
`)
	valid, err = VerifyTomlSignature(tampered, kp.Address(), sigHex)
	require.NoError(t, err)
	assert.False(t, valid)

	// Wrong public key
	otherKp, err := keypair.Random()
	require.NoError(t, err)
	valid, err = VerifyTomlSignature(content, otherKp.Address(), sigHex)
	require.NoError(t, err)
	assert.False(t, valid)
}

func TestDNSVerification(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	mockDNS := &mockDNSResolver{
		records: map[string][]string{
			"_stellar.example.com": {
				"stellar-signing-key=" + kp.Address(),
			},
		},
	}

	ctx := context.Background()

	// Successful verification
	err = VerifyDNS(ctx, "example.com", kp.Address(), mockDNS)
	assert.NoError(t, err)

	// Unknown domain or missing record
	err = VerifyDNS(ctx, "unknown.com", kp.Address(), mockDNS)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "dns verification failed")

	// Mismatched key
	otherKp, err := keypair.Random()
	require.NoError(t, err)
	err = VerifyDNS(ctx, "example.com", otherKp.Address(), mockDNS)
	assert.Error(t, err)
}

func TestTomlServer_CORS_SSL_And_Caching(t *testing.T) {
	kp, err := keypair.Random()
	require.NoError(t, err)

	doc := &StellarToml{
		Version:             "2.0.0",
		TransferServerSep24: "https://anchor.tenant.com/sep24",
		SigningKey:          kp.Address(),
	}

	provider := &mockTenantTomlProvider{
		tomls: map[string]*StellarToml{
			"anchor.tenant.com": doc,
		},
	}
	cache := NewMemoryTomlCache()

	server := NewTomlServer(provider, cache, TomlServerConfig{
		CacheTTL: 10 * time.Minute,
	})

	// 1. OPTIONS preflight
	reqOpts := httptest.NewRequest(http.MethodOptions, "/.well-known/stellar.toml", nil)
	reqOpts.Host = "anchor.tenant.com"
	recOpts := httptest.NewRecorder()
	server.ServeHTTP(recOpts, reqOpts)

	assert.Equal(t, http.StatusOK, recOpts.Code)
	assert.Equal(t, "*", recOpts.Header().Get("Access-Control-Allow-Origin"))
	assert.Contains(t, recOpts.Header().Get("Access-Control-Allow-Methods"), "GET")

	// 2. GET request
	reqGet := httptest.NewRequest(http.MethodGet, "/.well-known/stellar.toml", nil)
	reqGet.Host = "anchor.tenant.com"
	recGet := httptest.NewRecorder()
	server.ServeHTTP(recGet, reqGet)

	assert.Equal(t, http.StatusOK, recGet.Code)
	assert.Equal(t, "text/plain; charset=utf-8", recGet.Header().Get("Content-Type"))
	assert.Equal(t, "nosniff", recGet.Header().Get("X-Content-Type-Options"))
	assert.Equal(t, "DENY", recGet.Header().Get("X-Frame-Options"))
	assert.NotEmpty(t, recGet.Header().Get("Strict-Transport-Security"))
	assert.Contains(t, recGet.Body.String(), kp.Address())

	// 3. Verify Cache Hit
	cached, ok, err := cache.Get(context.Background(), "tenant-123", "anchor.tenant.com")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Contains(t, string(cached), kp.Address())

	// 4. Invalidation
	err = cache.Invalidate(context.Background(), "tenant-123", "anchor.tenant.com")
	require.NoError(t, err)
	_, ok, _ = cache.Get(context.Background(), "tenant-123", "anchor.tenant.com")
	assert.False(t, ok)
}
