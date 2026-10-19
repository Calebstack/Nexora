package settlement

import "testing"

func TestRiskEngineHoldsConflicts(t *testing.T) { result := (RiskEngine{MaxScore: .35, MaxAmount: 100}).Assess(RiskInput{SenderAgeDays: 365, HistoricalTransactions: 100, Amount: 10, ConflictingMempoolTransactions: true}); if result.ApproveInstantCredit || result.Score < .6 { t.Fatalf("unexpected assessment: %+v", result) } }
