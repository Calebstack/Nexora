package api

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// Standard routing paths for foreign exchange operations.
const (
	RouteFXQuotesStream = "/v1/fx/quotes/stream"
	RouteFXQuote        = "/v1/fx/quote"
	RouteFXConvert      = "/v1/fx/convert"
	RouteFXRates        = "/v1/fx/rates"
	RouteFXCatalog      = "/v1/fx/catalog"
)

// Query parameters for the FX SSE streaming endpoint.
const (
	QueryParamFXPairs     = "pairs"
	QueryParamFXFrom      = "from"
	QueryParamFXTo        = "to"
	QueryParamFXInterval  = "interval"
	QueryParamFXHeartbeat = "heartbeat"
)

// Default timing configuration for quote streaming.
const (
	DefaultStreamInterval  = time.Second
	DefaultStreamHeartbeat = 15 * time.Second
	MinStreamInterval      = 100 * time.Millisecond
	MaxStreamInterval      = time.Hour
)

// StreamRequestOptions represents parsed parameters for a client SSE streaming session.
type StreamRequestOptions struct {
	Pairs     []string
	Interval  time.Duration
	Heartbeat time.Duration
}

// SetupSSEHeaders writes standard response headers for Server-Sent Events.
func SetupSSEHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
}

// ParseStreamDuration parses a duration query parameter within [MinStreamInterval, MaxStreamInterval].
func ParseStreamDuration(raw string, fallback time.Duration) (time.Duration, error) {
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < MinStreamInterval || d > MaxStreamInterval {
		return 0, fmt.Errorf("must be between %s and %s", MinStreamInterval, MaxStreamInterval)
	}
	return d, nil
}

// ParseStreamRequest extracts and validates SSE streaming parameters from an HTTP request.
func ParseStreamRequest(r *http.Request) (StreamRequestOptions, error) {
	rawPairs := r.URL.Query().Get(QueryParamFXPairs)
	if rawPairs == "" {
		from := r.URL.Query().Get(QueryParamFXFrom)
		to := r.URL.Query().Get(QueryParamFXTo)
		if from != "" && to != "" {
			rawPairs = from + ":" + to
		}
	}
	if rawPairs == "" {
		return StreamRequestOptions{}, fmt.Errorf("pairs (or from and to) query parameter is required")
	}

	pairList := strings.Split(rawPairs, ",")
	pairs := make([]string, 0, len(pairList))
	for _, p := range pairList {
		trimmed := strings.TrimSpace(p)
		if trimmed != "" {
			pairs = append(pairs, trimmed)
		}
	}

	interval, err := ParseStreamDuration(r.URL.Query().Get(QueryParamFXInterval), DefaultStreamInterval)
	if err != nil {
		return StreamRequestOptions{}, fmt.Errorf("invalid interval: %w", err)
	}

	heartbeat, err := ParseStreamDuration(r.URL.Query().Get(QueryParamFXHeartbeat), DefaultStreamHeartbeat)
	if err != nil {
		return StreamRequestOptions{}, fmt.Errorf("invalid heartbeat: %w", err)
	}

	return StreamRequestOptions{
		Pairs:     pairs,
		Interval:  interval,
		Heartbeat: heartbeat,
	}, nil
}
