package stellar

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon"
	"github.com/stellar/go/strkey"
)

// ============================================================================
// Domain Models for Path Finding & Orderbook Depth
// ============================================================================

// AssetDescriptor describes a Stellar asset for routing.
type AssetDescriptor struct {
	Type   string `json:"type"` // "native", "credit_alphanum4", "credit_alphanum12"
	Code   string `json:"code,omitempty"`
	Issuer string `json:"issuer,omitempty"`
}

// CanonicalString returns a formatted representation of the asset (e.g. "XLM" or "USDC:G...").
func (a AssetDescriptor) CanonicalString() string {
	if a.Type == "native" || a.Code == "" || a.Code == "XLM" {
		return "native"
	}
	return fmt.Sprintf("%s:%s", a.Code, a.Issuer)
}

// IsNative returns true if the asset is Stellar Lumens (XLM).
func (a AssetDescriptor) IsNative() bool {
	return a.Type == "native" || (a.Code == "XLM" && a.Issuer == "")
}

// PaymentPath represents an evaluated path option discovered from Horizon.
type PaymentPath struct {
	SourceAsset   AssetDescriptor   `json:"source_asset"`
	SourceAmount  decimal.Decimal   `json:"source_amount"`
	DestAsset     AssetDescriptor   `json:"destination_asset"`
	DestAmount    decimal.Decimal   `json:"destination_amount"`
	Path          []AssetDescriptor `json:"path"` // Intermediate hops
	EffectiveRate decimal.Decimal   `json:"effective_rate"`
	HopCount      int               `json:"hop_count"`
}

// StrictSendRequest parameters for querying strict-send paths.
type StrictSendRequest struct {
	SourceAccount      string            `json:"source_account,omitempty"`
	SourceAsset        AssetDescriptor   `json:"source_asset"`
	SourceAmount       decimal.Decimal   `json:"source_amount"`
	DestinationAccount string            `json:"destination_account,omitempty"`
	DestinationAssets  []AssetDescriptor `json:"destination_assets,omitempty"`
}

// StrictReceiveRequest parameters for querying strict-receive paths.
type StrictReceiveRequest struct {
	SourceAccount string            `json:"source_account,omitempty"`
	SourceAssets  []AssetDescriptor `json:"source_assets,omitempty"`
	DestAccount   string            `json:"destination_account,omitempty"`
	DestAsset     AssetDescriptor   `json:"destination_asset"`
	DestAmount    decimal.Decimal   `json:"dest_amount"`
}

// DepthAssessment details liquidity and slippage risks along a path.
type DepthAssessment struct {
	Path               PaymentPath     `json:"path"`
	AvailableLiquidity decimal.Decimal `json:"available_liquidity"`
	SlippageEstimate   decimal.Decimal `json:"slippage_estimate"`
	SufficientDepth    bool            `json:"sufficient_depth"`
	PriceImpactPercent decimal.Decimal `json:"price_impact_percent"`
	WarningMessage     string          `json:"warning_message,omitempty"`
}

var (
	ErrNoPathsFound    = errors.New("no liquidity path found between assets")
	ErrPathCollapsed   = errors.New("orderbook depth collapsed: liquidity insufficient for route")
	ErrInvalidAsset    = errors.New("invalid asset definition")
	ErrInvalidAmount   = errors.New("amount must be positive")
	ErrSlippageTooHigh = errors.New("price impact exceeds maximum slippage tolerance")
)

// ============================================================================
// PathFinder Interface & Implementation
// ============================================================================

// PathFinder discovers and assesses path payment routes on Stellar.
type PathFinder interface {
	FindStrictSendPaths(ctx context.Context, req StrictSendRequest) ([]PaymentPath, error)
	FindStrictReceivePaths(ctx context.Context, req StrictReceiveRequest) ([]PaymentPath, error)
	SelectBestStrictSendPath(paths []PaymentPath) (*PaymentPath, error)
	SelectBestStrictReceivePath(paths []PaymentPath) (*PaymentPath, error)
	AssessPathDepth(ctx context.Context, path PaymentPath, maxSlippageTolerance decimal.Decimal) (*DepthAssessment, error)
}

// HorizonPathFinder implements PathFinder by querying a Stellar Horizon instance.
type HorizonPathFinder struct {
	horizonURL string
	httpClient *http.Client
}

// NewHorizonPathFinder builds a new HorizonPathFinder.
func NewHorizonPathFinder(horizonURL string, httpClient *http.Client) *HorizonPathFinder {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &HorizonPathFinder{
		horizonURL: strings.TrimRight(horizonURL, "/"),
		httpClient: httpClient,
	}
}

// FindStrictSendPaths queries Horizon /paths/strict-send to discover routes where source amount is fixed.
func (p *HorizonPathFinder) FindStrictSendPaths(ctx context.Context, req StrictSendRequest) ([]PaymentPath, error) {
	if req.SourceAmount.LessThanOrEqual(decimal.Zero) {
		return nil, ErrInvalidAmount
	}

	endpoint := fmt.Sprintf("%s/paths/strict-send", p.horizonURL)
	query := url.Values{}

	if req.SourceAsset.IsNative() {
		query.Set("source_asset_type", "native")
	} else {
		if req.SourceAsset.Issuer == "" || !strkey.IsValidEd25519PublicKey(req.SourceAsset.Issuer) {
			return nil, fmt.Errorf("%w: invalid source issuer", ErrInvalidAsset)
		}
		if len(req.SourceAsset.Code) <= 4 {
			query.Set("source_asset_type", "credit_alphanum4")
		} else {
			query.Set("source_asset_type", "credit_alphanum12")
		}
		query.Set("source_asset_code", req.SourceAsset.Code)
		query.Set("source_asset_issuer", req.SourceAsset.Issuer)
	}

	query.Set("source_amount", req.SourceAmount.StringFixed(7))

	if req.DestinationAccount != "" {
		query.Set("destination_account", req.DestinationAccount)
	} else if len(req.DestinationAssets) > 0 {
		destStrs := make([]string, 0, len(req.DestinationAssets))
		for _, da := range req.DestinationAssets {
			destStrs = append(destStrs, da.CanonicalString())
		}
		query.Set("destination_assets", strings.Join(destStrs, ","))
	} else {
		return nil, errors.New("either destination_account or destination_assets must be provided")
	}

	fullURL := fmt.Sprintf("%s?%s", endpoint, query.Encode())
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute horizon request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("horizon error status %d: %s", resp.StatusCode, string(body))
	}

	var pathsResp struct {
		Embedded struct {
			Records []horizon.Path `json:"records"`
		} `json:"_embedded"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&pathsResp); err != nil {
		return nil, fmt.Errorf("decode horizon response: %w", err)
	}

	records := pathsResp.Embedded.Records
	if len(records) == 0 {
		return nil, ErrNoPathsFound
	}

	results := make([]PaymentPath, 0, len(records))
	for _, rec := range records {
		srcAmt, _ := decimal.NewFromString(rec.SourceAmount)
		destAmt, _ := decimal.NewFromString(rec.DestinationAmount)

		var effRate decimal.Decimal
		if !srcAmt.IsZero() {
			effRate = destAmt.Div(srcAmt)
		}

		hops := make([]AssetDescriptor, 0, len(rec.Path))
		for _, hop := range rec.Path {
			hops = append(hops, AssetDescriptor{
				Type:   hop.Type,
				Code:   hop.Code,
				Issuer: hop.Issuer,
			})
		}

		results = append(results, PaymentPath{
			SourceAsset: AssetDescriptor{
				Type:   rec.SourceAssetType,
				Code:   rec.SourceAssetCode,
				Issuer: rec.SourceAssetIssuer,
			},
			SourceAmount: srcAmt,
			DestAsset: AssetDescriptor{
				Type:   rec.DestinationAssetType,
				Code:   rec.DestinationAssetCode,
				Issuer: rec.DestinationAssetIssuer,
			},
			DestAmount:    destAmt,
			Path:          hops,
			EffectiveRate: effRate,
			HopCount:      len(hops),
		})
	}

	return results, nil
}

// FindStrictReceivePaths queries Horizon /paths/strict-receive where destination amount is fixed.
func (p *HorizonPathFinder) FindStrictReceivePaths(ctx context.Context, req StrictReceiveRequest) ([]PaymentPath, error) {
	if req.DestAmount.LessThanOrEqual(decimal.Zero) {
		return nil, ErrInvalidAmount
	}

	endpoint := fmt.Sprintf("%s/paths/strict-receive", p.horizonURL)
	query := url.Values{}

	if req.DestAsset.IsNative() {
		query.Set("destination_asset_type", "native")
	} else {
		if req.DestAsset.Issuer == "" || !strkey.IsValidEd25519PublicKey(req.DestAsset.Issuer) {
			return nil, fmt.Errorf("%w: invalid destination issuer", ErrInvalidAsset)
		}
		if len(req.DestAsset.Code) <= 4 {
			query.Set("destination_asset_type", "credit_alphanum4")
		} else {
			query.Set("destination_asset_type", "credit_alphanum12")
		}
		query.Set("destination_asset_code", req.DestAsset.Code)
		query.Set("destination_asset_issuer", req.DestAsset.Issuer)
	}

	query.Set("destination_amount", req.DestAmount.StringFixed(7))

	if req.SourceAccount != "" {
		query.Set("source_account", req.SourceAccount)
	} else if len(req.SourceAssets) > 0 {
		srcStrs := make([]string, 0, len(req.SourceAssets))
		for _, sa := range req.SourceAssets {
			srcStrs = append(srcStrs, sa.CanonicalString())
		}
		query.Set("source_assets", strings.Join(srcStrs, ","))
	} else {
		return nil, errors.New("either source_account or source_assets must be provided")
	}

	fullURL := fmt.Sprintf("%s?%s", endpoint, query.Encode())
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("execute horizon request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("horizon error status %d: %s", resp.StatusCode, string(body))
	}

	var pathsResp struct {
		Embedded struct {
			Records []horizon.Path `json:"records"`
		} `json:"_embedded"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&pathsResp); err != nil {
		return nil, fmt.Errorf("decode horizon response: %w", err)
	}

	records := pathsResp.Embedded.Records
	if len(records) == 0 {
		return nil, ErrNoPathsFound
	}

	results := make([]PaymentPath, 0, len(records))
	for _, rec := range records {
		srcAmt, _ := decimal.NewFromString(rec.SourceAmount)
		destAmt, _ := decimal.NewFromString(rec.DestinationAmount)

		var effRate decimal.Decimal
		if !srcAmt.IsZero() {
			effRate = destAmt.Div(srcAmt)
		}

		hops := make([]AssetDescriptor, 0, len(rec.Path))
		for _, hop := range rec.Path {
			hops = append(hops, AssetDescriptor{
				Type:   hop.Type,
				Code:   hop.Code,
				Issuer: hop.Issuer,
			})
		}

		results = append(results, PaymentPath{
			SourceAsset: AssetDescriptor{
				Type:   rec.SourceAssetType,
				Code:   rec.SourceAssetCode,
				Issuer: rec.SourceAssetIssuer,
			},
			SourceAmount: srcAmt,
			DestAsset: AssetDescriptor{
				Type:   rec.DestinationAssetType,
				Code:   rec.DestinationAssetCode,
				Issuer: rec.DestinationAssetIssuer,
			},
			DestAmount:    destAmt,
			Path:          hops,
			EffectiveRate: effRate,
			HopCount:      len(hops),
		})
	}

	return results, nil
}

// SelectBestStrictSendPath selects the route that maximizes destination amount for strict send.
func (p *HorizonPathFinder) SelectBestStrictSendPath(paths []PaymentPath) (*PaymentPath, error) {
	if len(paths) == 0 {
		return nil, ErrNoPathsFound
	}

	// Sort descending by destination amount; on tie, choose fewest hops
	sorted := make([]PaymentPath, len(paths))
	copy(sorted, paths)

	sort.Slice(sorted, func(i, j int) bool {
		cmp := sorted[i].DestAmount.Cmp(sorted[j].DestAmount)
		if cmp != 0 {
			return cmp > 0 // Higher dest amount wins
		}
		return sorted[i].HopCount < sorted[j].HopCount // Fewer hops wins on tie
	})

	best := sorted[0]
	return &best, nil
}

// SelectBestStrictReceivePath selects the route that minimizes source amount for strict receive.
func (p *HorizonPathFinder) SelectBestStrictReceivePath(paths []PaymentPath) (*PaymentPath, error) {
	if len(paths) == 0 {
		return nil, ErrNoPathsFound
	}

	// Sort ascending by source amount; on tie, choose fewest hops
	sorted := make([]PaymentPath, len(paths))
	copy(sorted, paths)

	sort.Slice(sorted, func(i, j int) bool {
		cmp := sorted[i].SourceAmount.Cmp(sorted[j].SourceAmount)
		if cmp != 0 {
			return cmp < 0 // Lower source amount needed wins
		}
		return sorted[i].HopCount < sorted[j].HopCount // Fewer hops wins on tie
	})

	best := sorted[0]
	return &best, nil
}

// AssessPathDepth queries the orderbooks along each hop to detect orderbook depth changes or path collapse.
func (p *HorizonPathFinder) AssessPathDepth(ctx context.Context, path PaymentPath, maxSlippageTolerance decimal.Decimal) (*DepthAssessment, error) {
	if maxSlippageTolerance.LessThanOrEqual(decimal.Zero) {
		maxSlippageTolerance = decimal.NewFromFloat(0.01) // default 1% slippage
	}

	// Construct list of pairs to check: Source -> Hop1 -> Hop2 -> ... -> Dest
	assets := make([]AssetDescriptor, 0, len(path.Path)+2)
	assets = append(assets, path.SourceAsset)
	assets = append(assets, path.Path...)
	assets = append(assets, path.DestAsset)

	var minLiquidity decimal.Decimal = decimal.NewFromInt(1_000_000_000)
	var maxPriceImpact decimal.Decimal = decimal.Zero

	for i := 0; i < len(assets)-1; i++ {
		selling := assets[i]
		buying := assets[i+1]

		ob, err := p.fetchOrderBook(ctx, selling, buying)
		if err != nil {
			return nil, fmt.Errorf("fetch orderbook for hop %d (%s -> %s): %w", i, selling.CanonicalString(), buying.CanonicalString(), err)
		}

		// Calculate available depth in bids
		var hopLiquidity decimal.Decimal
		for _, bid := range ob.Bids {
			amt, _ := decimal.NewFromString(bid.Amount)
			hopLiquidity = hopLiquidity.Add(amt)
		}

		if hopLiquidity.LessThan(minLiquidity) {
			minLiquidity = hopLiquidity
		}

		// Calculate price spread / impact from top bid to 5th bid
		if len(ob.Bids) > 1 {
			topBidPrice, _ := decimal.NewFromString(ob.Bids[0].Price)
			lastBidPrice, _ := decimal.NewFromString(ob.Bids[len(ob.Bids)-1].Price)
			if !topBidPrice.IsZero() {
				spread := topBidPrice.Sub(lastBidPrice).Abs().Div(topBidPrice)
				if spread.GreaterThan(maxPriceImpact) {
					maxPriceImpact = spread
				}
			}
		} else if len(ob.Bids) == 0 {
			return &DepthAssessment{
				Path:               path,
				AvailableLiquidity: decimal.Zero,
				SufficientDepth:    false,
				WarningMessage:     fmt.Sprintf("orderbook empty for %s -> %s", selling.CanonicalString(), buying.CanonicalString()),
			}, ErrPathCollapsed
		}
	}

	// Check if available liquidity covers the trade
	sufficient := minLiquidity.GreaterThanOrEqual(path.SourceAmount)
	if !sufficient {
		return &DepthAssessment{
			Path:               path,
			AvailableLiquidity: minLiquidity,
			SufficientDepth:    false,
			PriceImpactPercent: maxPriceImpact.Mul(decimal.NewFromInt(100)),
			WarningMessage:     "available orderbook depth is less than transaction size",
		}, ErrPathCollapsed
	}

	if maxPriceImpact.GreaterThan(maxSlippageTolerance) {
		return &DepthAssessment{
			Path:               path,
			AvailableLiquidity: minLiquidity,
			SufficientDepth:    false,
			PriceImpactPercent: maxPriceImpact.Mul(decimal.NewFromInt(100)),
			WarningMessage:     fmt.Sprintf("price impact %s%% exceeds max tolerance %s%%", maxPriceImpact.Mul(decimal.NewFromInt(100)), maxSlippageTolerance.Mul(decimal.NewFromInt(100))),
		}, ErrSlippageTooHigh
	}

	return &DepthAssessment{
		Path:               path,
		AvailableLiquidity: minLiquidity,
		SufficientDepth:    true,
		PriceImpactPercent: maxPriceImpact.Mul(decimal.NewFromInt(100)),
	}, nil
}

func (p *HorizonPathFinder) fetchOrderBook(ctx context.Context, selling, buying AssetDescriptor) (*horizon.OrderBookSummary, error) {
	endpoint := fmt.Sprintf("%s/order_book", p.horizonURL)
	query := url.Values{}

	setAssetQuery(query, "selling_", selling)
	setAssetQuery(query, "buying_", buying)
	query.Set("limit", "20")

	fullURL := fmt.Sprintf("%s?%s", endpoint, query.Encode())
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := p.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("orderbook error status: %d", resp.StatusCode)
	}

	var ob horizon.OrderBookSummary
	if err := json.NewDecoder(resp.Body).Decode(&ob); err != nil {
		return nil, err
	}

	return &ob, nil
}

func setAssetQuery(query url.Values, prefix string, asset AssetDescriptor) {
	if asset.IsNative() {
		query.Set(prefix+"asset_type", "native")
	} else {
		if len(asset.Code) <= 4 {
			query.Set(prefix+"asset_type", "credit_alphanum4")
		} else {
			query.Set(prefix+"asset_type", "credit_alphanum12")
		}
		query.Set(prefix+"asset_code", asset.Code)
		query.Set(prefix+"asset_issuer", asset.Issuer)
	}
}
