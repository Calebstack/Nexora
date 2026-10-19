package stellar

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/stellar/go/keypair"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathFinder_StrictSendAndReceiveSelection(t *testing.T) {
	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	usdc := AssetDescriptor{Type: "credit_alphanum4", Code: "USDC", Issuer: issuerKp.Address()}
	xlm := AssetDescriptor{Type: "native", Code: "XLM"}
	eurc := AssetDescriptor{Type: "credit_alphanum4", Code: "EURC", Issuer: issuerKp.Address()}

	paths := []PaymentPath{
		{
			SourceAsset:   usdc,
			SourceAmount:  decimal.NewFromInt(100),
			DestAsset:     eurc,
			DestAmount:    decimal.NewFromFloat(91.50),
			Path:          []AssetDescriptor{xlm}, // 1-hop via XLM
			EffectiveRate: decimal.NewFromFloat(0.9150),
			HopCount:      1,
		},
		{
			SourceAsset:   usdc,
			SourceAmount:  decimal.NewFromInt(100),
			DestAsset:     eurc,
			DestAmount:    decimal.NewFromFloat(92.80),
			Path:          []AssetDescriptor{}, // direct 0-hop
			EffectiveRate: decimal.NewFromFloat(0.9280),
			HopCount:      0,
		},
		{
			SourceAsset:   usdc,
			SourceAmount:  decimal.NewFromInt(100),
			DestAsset:     eurc,
			DestAmount:    decimal.NewFromFloat(89.20),
			Path:          []AssetDescriptor{xlm, usdc}, // 2-hop
			EffectiveRate: decimal.NewFromFloat(0.8920),
			HopCount:      2,
		},
	}

	finder := &HorizonPathFinder{}

	// Strict Send should select highest destination amount (92.80)
	bestSend, err := finder.SelectBestStrictSendPath(paths)
	require.NoError(t, err)
	assert.Equal(t, decimal.NewFromFloat(92.80), bestSend.DestAmount)
	assert.Equal(t, 0, bestSend.HopCount)

	// Strict Receive paths with different source amounts
	receivePaths := []PaymentPath{
		{
			SourceAsset:  usdc,
			SourceAmount: decimal.NewFromFloat(108.50),
			DestAsset:    eurc,
			DestAmount:   decimal.NewFromInt(100),
			Path:         []AssetDescriptor{xlm},
			HopCount:     1,
		},
		{
			SourceAsset:  usdc,
			SourceAmount: decimal.NewFromFloat(107.20), // lowest source amount needed
			DestAsset:    eurc,
			DestAmount:   decimal.NewFromInt(100),
			Path:         []AssetDescriptor{},
			HopCount:     0,
		},
		{
			SourceAsset:  usdc,
			SourceAmount: decimal.NewFromFloat(112.00),
			DestAsset:    eurc,
			DestAmount:   decimal.NewFromInt(100),
			Path:         []AssetDescriptor{xlm, usdc},
			HopCount:     2,
		},
	}

	bestReceive, err := finder.SelectBestStrictReceivePath(receivePaths)
	require.NoError(t, err)
	assert.Equal(t, decimal.NewFromFloat(107.20), bestReceive.SourceAmount)
	assert.Equal(t, 0, bestReceive.HopCount)
}

func TestPathFinder_AssessPathDepth_DetectsCollapse(t *testing.T) {
	issuerKp, err := keypair.Random()
	require.NoError(t, err)

	usdc := AssetDescriptor{Type: "credit_alphanum4", Code: "USDC", Issuer: issuerKp.Address()}
	eurc := AssetDescriptor{Type: "credit_alphanum4", Code: "EURC", Issuer: issuerKp.Address()}

	path := PaymentPath{
		SourceAsset:  usdc,
		SourceAmount: decimal.NewFromInt(5000), // requesting $5000
		DestAsset:    eurc,
		DestAmount:   decimal.NewFromFloat(4600),
		Path:         []AssetDescriptor{},
		HopCount:     0,
	}

	t.Run("sufficient liquidity and low spread", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ob := horizon.OrderBookSummary{
				Bids: []horizon.PriceLevel{
					{Price: "0.9250", Amount: "10000.0"}, // $10,000 available
					{Price: "0.9245", Amount: "5000.0"},
				},
				Asks: []horizon.PriceLevel{
					{Price: "0.9255", Amount: "8000.0"},
				},
			}
			_ = json.NewEncoder(w).Encode(ob)
		}))
		defer ts.Close()

		finder := NewHorizonPathFinder(ts.URL, ts.Client())
		assessment, err := finder.AssessPathDepth(context.Background(), path, decimal.NewFromFloat(0.02))
		require.NoError(t, err)
		assert.True(t, assessment.SufficientDepth)
		assert.True(t, assessment.AvailableLiquidity.GreaterThan(path.SourceAmount))
	})

	t.Run("depth collapse: insufficient liquidity", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ob := horizon.OrderBookSummary{
				Bids: []horizon.PriceLevel{
					{Price: "0.9250", Amount: "500.0"}, // Only $500 available for a $5000 trade!
				},
			}
			_ = json.NewEncoder(w).Encode(ob)
		}))
		defer ts.Close()

		finder := NewHorizonPathFinder(ts.URL, ts.Client())
		assessment, err := finder.AssessPathDepth(context.Background(), path, decimal.NewFromFloat(0.02))
		assert.ErrorIs(t, err, ErrPathCollapsed)
		assert.False(t, assessment.SufficientDepth)
	})

	t.Run("depth collapse: empty orderbook", func(t *testing.T) {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ob := horizon.OrderBookSummary{
				Bids: []horizon.PriceLevel{},
			}
			_ = json.NewEncoder(w).Encode(ob)
		}))
		defer ts.Close()

		finder := NewHorizonPathFinder(ts.URL, ts.Client())
		_, err := finder.AssessPathDepth(context.Background(), path, decimal.NewFromFloat(0.02))
		assert.ErrorIs(t, err, ErrPathCollapsed)
	})
}

// Benchmark tests comparing direct swaps vs multi-hop path payment routing
func BenchmarkDirectSwapVsMultiHop(b *testing.B) {
	issuerKp, _ := keypair.Random()
	usdc := AssetDescriptor{Type: "credit_alphanum4", Code: "USDC", Issuer: issuerKp.Address()}
	xlm := AssetDescriptor{Type: "native", Code: "XLM"}
	btc := AssetDescriptor{Type: "credit_alphanum4", Code: "BTC", Issuer: issuerKp.Address()}
	eurc := AssetDescriptor{Type: "credit_alphanum4", Code: "EURC", Issuer: issuerKp.Address()}

	directPath := PaymentPath{
		SourceAsset:   usdc,
		SourceAmount:  decimal.NewFromInt(100),
		DestAsset:     eurc,
		DestAmount:    decimal.NewFromFloat(92.5),
		Path:          []AssetDescriptor{},
		EffectiveRate: decimal.NewFromFloat(0.925),
		HopCount:      0,
	}

	oneHopPath := PaymentPath{
		SourceAsset:   usdc,
		SourceAmount:  decimal.NewFromInt(100),
		DestAsset:     eurc,
		DestAmount:    decimal.NewFromFloat(92.1),
		Path:          []AssetDescriptor{xlm},
		EffectiveRate: decimal.NewFromFloat(0.921),
		HopCount:      1,
	}

	twoHopPath := PaymentPath{
		SourceAsset:   usdc,
		SourceAmount:  decimal.NewFromInt(100),
		DestAsset:     eurc,
		DestAmount:    decimal.NewFromFloat(91.8),
		Path:          []AssetDescriptor{xlm, btc},
		EffectiveRate: decimal.NewFromFloat(0.918),
		HopCount:      2,
	}

	paths := []PaymentPath{directPath, oneHopPath, twoHopPath}
	finder := &HorizonPathFinder{}

	b.Run("DirectSwapEvaluation", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = directPath.EffectiveRate.Mul(directPath.SourceAmount)
		}
	})

	b.Run("MultiHopEvaluation", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rate := twoHopPath.SourceAmount
			for j := 0; j < len(twoHopPath.Path); j++ {
				rate = rate.Mul(decimal.NewFromFloat(0.98))
			}
		}
	})

	b.Run("RouteSelectionBench", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _ = finder.SelectBestStrictSendPath(paths)
		}
	})
}
