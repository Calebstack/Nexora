package settlement

import "math"

type RiskInput struct { SenderAgeDays, HistoricalTransactions, RelationshipTransactions int; Amount float64; ConflictingMempoolTransactions bool }
type RiskAssessment struct { Score float64; ApproveInstantCredit bool; Reason string }
type RiskEngine struct { MaxScore, MaxAmount float64 }

func (e RiskEngine) Assess(in RiskInput) RiskAssessment {
	score := 0.0
	if in.SenderAgeDays < 7 { score += 0.30 } else if in.SenderAgeDays < 30 { score += 0.10 }
	if in.HistoricalTransactions == 0 { score += 0.30 } else if in.HistoricalTransactions < 5 { score += 0.10 }
	if in.RelationshipTransactions == 0 { score += 0.15 }
	if in.ConflictingMempoolTransactions { score += 0.60 }
	if in.Amount > e.MaxAmount { score += 0.20 }
	score = math.Min(1, score)
	approved := !in.ConflictingMempoolTransactions && score <= e.MaxScore && in.Amount <= e.MaxAmount
	reason := "within instant-credit thresholds"
	if !approved { reason = "held for confirmation or manual review" }
	return RiskAssessment{Score: score, ApproveInstantCredit: approved, Reason: reason}
}
