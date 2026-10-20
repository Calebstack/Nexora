package health

import (
	"context"
	"net/http"
	"time"
)

type HorizonStatus struct { URL string; Healthy bool; Latency time.Duration; LatestLedger uint32 }

func CheckHorizon(ctx context.Context, client *http.Client, url string) HorizonStatus { start := time.Now(); result := HorizonStatus{URL: url}; request, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/root", nil); if err != nil { return result }; response, err := client.Do(request); if err != nil { return result }; response.Body.Close(); result.Latency = time.Since(start); result.Healthy = response.StatusCode >= 200 && response.StatusCode < 300; return result }
