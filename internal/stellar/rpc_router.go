package stellar

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"
)

type EndpointHealth struct { URL string; Latency time.Duration; LatestLedger uint32; Failures int; Healthy bool }
type RPCRouter struct { mu sync.RWMutex; endpoints []EndpointHealth; client *http.Client; maxLag uint32 }

func NewRPCRouter(urls []string, maxLag uint32) *RPCRouter { endpoints := make([]EndpointHealth, len(urls)); for i, url := range urls { endpoints[i] = EndpointHealth{URL: url} }; return &RPCRouter{endpoints: endpoints, client: &http.Client{Timeout: 3*time.Second}, maxLag: maxLag} }

func (r *RPCRouter) Probe(ctx context.Context, latestNetworkLedger uint32) { r.mu.Lock(); defer r.mu.Unlock(); for i := range r.endpoints { start := time.Now(); req, _ := http.NewRequestWithContext(ctx, http.MethodGet, r.endpoints[i].URL+"/root", nil); response, err := r.client.Do(req); if err != nil || response.StatusCode >= 500 { r.endpoints[i].Failures++; r.endpoints[i].Healthy = false; continue }; response.Body.Close(); r.endpoints[i].Latency = time.Since(start); r.endpoints[i].LatestLedger = latestNetworkLedger; r.endpoints[i].Healthy = true } }
func (r *RPCRouter) Select() (string, error) { r.mu.RLock(); defer r.mu.RUnlock(); candidates := append([]EndpointHealth(nil), r.endpoints...); sort.Slice(candidates, func(i,j int) bool { return candidates[i].Latency < candidates[j].Latency }); for _, endpoint := range candidates { if endpoint.Healthy && endpoint.Failures == 0 { return endpoint.URL, nil } }; return "", fmt.Errorf("no healthy Horizon endpoints") }
func (r *RPCRouter) Metrics() string { r.mu.RLock(); defer r.mu.RUnlock(); out := ""; for _, endpoint := range r.endpoints { out += fmt.Sprintf("nexora_horizon_endpoint_healthy{endpoint=%q} %d\n", endpoint.URL, boolInt(endpoint.Healthy)) }; return out }
func boolInt(value bool) int { if value { return 1 }; return 0 }
