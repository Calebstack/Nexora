package anchor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/stellar"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/stellar/go/protocols/horizon/operations"
)

// ============================================================================
// Data Types for Automated Anchor Balance Reconciliation (Issue #35)
// ============================================================================

type DiscrepancyType string

const (
	DiscrepancyUnindexedDeposit    DiscrepancyType = "UNINDEXED_DEPOSIT"
	DiscrepancyUnauthorizedOutflow DiscrepancyType = "UNAUTHORIZED_OUTFLOW"
	DiscrepancyAmountMismatch      DiscrepancyType = "AMOUNT_MISMATCH"
	DiscrepancyMissingOnChain      DiscrepancyType = "MISSING_ON_CHAIN"
)

type DiscrepancySeverity string

const (
	SeverityCritical DiscrepancySeverity = "CRITICAL"
	SeverityHigh     DiscrepancySeverity = "HIGH"
	SeverityMedium   DiscrepancySeverity = "MEDIUM"
)

// AnchorTransactionRecord represents an internal ledger transaction record.
type AnchorTransactionRecord struct {
	ID        string          `json:"id"`
	TxHash    string          `json:"tx_hash,omitempty"`
	Type      string          `json:"type"` // "deposit", "withdrawal", "fee_collection"
	Account   string          `json:"account"`
	Asset     string          `json:"asset"`
	Amount    decimal.Decimal `json:"amount"`
	Status    string          `json:"status"` // "settled", "confirmed", "pending", "failed"
	Memo      string          `json:"memo,omitempty"`
	MemoType  string          `json:"memo_type,omitempty"`
	CreatedAt time.Time       `json:"created_at"`
}

// AnchorDiscrepancy represents a variance detected between on-chain ledger and internal records.
type AnchorDiscrepancy struct {
	ID             string              `json:"id"`
	Type           DiscrepancyType     `json:"type"`
	Severity       DiscrepancySeverity `json:"severity"`
	Account        string              `json:"account"`
	Asset          string              `json:"asset"`
	TxHash         string              `json:"tx_hash,omitempty"`
	Memo           string              `json:"memo,omitempty"`
	OnChainAmount  decimal.Decimal     `json:"on_chain_amount"`
	InternalAmount decimal.Decimal     `json:"internal_amount"`
	Variance       decimal.Decimal     `json:"variance"`
	Description    string              `json:"description"`
	DetectedAt     time.Time           `json:"detected_at"`
}

// ReconciliationVarianceReport details the 24-hour audit results for anchor accounts.
type ReconciliationVarianceReport struct {
	ID                   string              `json:"id"`
	AnchorAccount        string              `json:"anchor_account"`
	WindowStart          time.Time           `json:"window_start"`
	WindowEnd            time.Time           `json:"window_end"`
	TotalOnChainInflow   decimal.Decimal     `json:"total_on_chain_inflow"`
	TotalOnChainOutflow  decimal.Decimal     `json:"total_on_chain_outflow"`
	TotalInternalInflow  decimal.Decimal     `json:"total_internal_inflow"`
	TotalInternalOutflow decimal.Decimal     `json:"total_internal_outflow"`
	NetVariance          decimal.Decimal     `json:"net_variance"`
	TotalMatched         int                 `json:"total_matched"`
	Discrepancies        []AnchorDiscrepancy `json:"discrepancies"`
	Status               string              `json:"status"` // "BALANCED", "VARIANCE_DETECTED", "CRITICAL_ALERT"
	GeneratedAt          time.Time           `json:"generated_at"`
}

// ============================================================================
// Interfaces
// ============================================================================

// AnchorInternalTxRepo queries internal anchor transaction records.
type AnchorInternalTxRepo interface {
	GetAnchorTransactions(ctx context.Context, accounts []string, since time.Time) ([]AnchorTransactionRecord, error)
}

// AuditLogPublisher persists reconciliation variance reports to the append-only audit repository.
type AuditLogPublisher interface {
	PublishReconciliationReport(ctx context.Context, report *ReconciliationVarianceReport) error
}

// AlertEmitter dispatches alerts when critical discrepancies or variances exceed thresholds.
type AlertEmitter interface {
	EmitAlert(ctx context.Context, alertType string, message string, details map[string]interface{}) error
}

// OnChainOperationExtractor extracts payments from Horizon.
type OnChainOperationExtractor interface {
	Payments(accountID, cursor string, limit uint) ([]operations.Operation, error)
}

// ============================================================================
// AnchorReconciler Implementation
// ============================================================================

// AnchorReconcilerConfig holds configuration for the reconciliation engine.
type AnchorReconcilerConfig struct {
	WindowDuration       time.Duration   // e.g. 24h
	MaxVarianceThreshold decimal.Decimal // Alert threshold for net variance
}

// AnchorReconciler executes audits of on-chain Horizon ledger vs internal records.
type AnchorReconciler struct {
	extractor OnChainOperationExtractor
	repo      AnchorInternalTxRepo
	auditPub  AuditLogPublisher
	alerter   AlertEmitter
	cfg       AnchorReconcilerConfig
}

// NewAnchorReconciler builds a new AnchorReconciler.
func NewAnchorReconciler(
	extractor OnChainOperationExtractor,
	repo AnchorInternalTxRepo,
	auditPub AuditLogPublisher,
	alerter AlertEmitter,
	cfg AnchorReconcilerConfig,
) *AnchorReconciler {
	if cfg.WindowDuration <= 0 {
		cfg.WindowDuration = 24 * time.Hour
	}
	if cfg.MaxVarianceThreshold.LessThanOrEqual(decimal.Zero) {
		cfg.MaxVarianceThreshold = decimal.NewFromFloat(0.01) // default 0.01 variance threshold
	}
	return &AnchorReconciler{
		extractor: extractor,
		repo:      repo,
		auditPub:  auditPub,
		alerter:   alerter,
		cfg:       cfg,
	}
}

// OnChainPaymentItem is a normalized on-chain payment operation.
type OnChainPaymentItem struct {
	ID              string
	TransactionHash string
	From            string
	To              string
	Asset           string
	Amount          decimal.Decimal
	CreatedAt       time.Time
	Memo            string
}

// ReconcileAccount audits on-chain operations for account across the past window (default 24h).
func (r *AnchorReconciler) ReconcileAccount(ctx context.Context, account string) (*ReconciliationVarianceReport, error) {
	now := time.Now().UTC()
	windowStart := now.Add(-r.cfg.WindowDuration)

	// 1. Extract on-chain payments targeting or originating from account
	onChainPayments, err := r.extractOnChainPayments(ctx, account, windowStart)
	if err != nil {
		return nil, fmt.Errorf("extract on-chain operations for %s: %w", account, err)
	}

	// 2. Fetch internal transactions for account
	internalRecords, err := r.repo.GetAnchorTransactions(ctx, []string{account}, windowStart)
	if err != nil {
		return nil, fmt.Errorf("fetch internal records for %s: %w", account, err)
	}

	// 3. Multi-index internal records for fast matching
	byTxHash := make(map[string]*AnchorTransactionRecord)
	byMemo := make(map[string][]*AnchorTransactionRecord)
	internalMatched := make(map[string]bool)

	for i := range internalRecords {
		rec := &internalRecords[i]
		if rec.TxHash != "" {
			byTxHash[rec.TxHash] = rec
		}
		if rec.Memo != "" {
			byMemo[rec.Memo] = append(byMemo[rec.Memo], rec)
		}
	}

	report := &ReconciliationVarianceReport{
		ID:            uuid.New().String(),
		AnchorAccount: account,
		WindowStart:   windowStart,
		WindowEnd:     now,
		Discrepancies: make([]AnchorDiscrepancy, 0),
		GeneratedAt:   now,
	}

	// 4. Audit on-chain operations against internal records
	for _, op := range onChainPayments {
		isInflow := op.To == account
		isOutflow := op.From == account

		if isInflow {
			report.TotalOnChainInflow = report.TotalOnChainInflow.Add(op.Amount)
		}
		if isOutflow {
			report.TotalOnChainOutflow = report.TotalOnChainOutflow.Add(op.Amount)
		}

		// Try matching by TxHash first, then by Memo
		matchedRec := byTxHash[op.TransactionHash]
		if matchedRec == nil && op.Memo != "" {
			candidates := byMemo[op.Memo]
			for _, cand := range candidates {
				if !internalMatched[cand.ID] {
					matchedRec = cand
					break
				}
			}
		}

		if matchedRec != nil {
			internalMatched[matchedRec.ID] = true
			report.TotalMatched++

			if isInflow {
				report.TotalInternalInflow = report.TotalInternalInflow.Add(matchedRec.Amount)
			}
			if isOutflow {
				report.TotalInternalOutflow = report.TotalInternalOutflow.Add(matchedRec.Amount)
			}

			// Check amount discrepancy
			diff := op.Amount.Sub(matchedRec.Amount).Abs()
			if diff.GreaterThan(decimal.NewFromFloat(0.0000001)) {
				report.Discrepancies = append(report.Discrepancies, AnchorDiscrepancy{
					ID:             uuid.New().String(),
					Type:           DiscrepancyAmountMismatch,
					Severity:       SeverityHigh,
					Account:        account,
					Asset:          op.Asset,
					TxHash:         op.TransactionHash,
					Memo:           op.Memo,
					OnChainAmount:  op.Amount,
					InternalAmount: matchedRec.Amount,
					Variance:       diff,
					Description:    fmt.Sprintf("amount mismatch for tx %s: on-chain=%s, internal=%s", op.TransactionHash, op.Amount, matchedRec.Amount),
					DetectedAt:     now,
				})
			}
		} else {
			// Unmatched on-chain transaction!
			if isInflow {
				// Deposit received on-chain but not indexed internally
				report.Discrepancies = append(report.Discrepancies, AnchorDiscrepancy{
					ID:             uuid.New().String(),
					Type:           DiscrepancyUnindexedDeposit,
					Severity:       SeverityCritical,
					Account:        account,
					Asset:          op.Asset,
					TxHash:         op.TransactionHash,
					Memo:           op.Memo,
					OnChainAmount:  op.Amount,
					InternalAmount: decimal.Zero,
					Variance:       op.Amount,
					Description:    fmt.Sprintf("critical unindexed deposit received on-chain: tx=%s, amount=%s %s", op.TransactionHash, op.Amount, op.Asset),
					DetectedAt:     now,
				})
			} else if isOutflow {
				// Outflow sent from anchor account without internal withdrawal authorization!
				report.Discrepancies = append(report.Discrepancies, AnchorDiscrepancy{
					ID:             uuid.New().String(),
					Type:           DiscrepancyUnauthorizedOutflow,
					Severity:       SeverityCritical,
					Account:        account,
					Asset:          op.Asset,
					TxHash:         op.TransactionHash,
					Memo:           op.Memo,
					OnChainAmount:  op.Amount,
					InternalAmount: decimal.Zero,
					Variance:       op.Amount,
					Description:    fmt.Sprintf("critical unauthorized outflow detected from anchor account: tx=%s, amount=%s %s", op.TransactionHash, op.Amount, op.Asset),
					DetectedAt:     now,
				})
			}
		}
	}

	// 5. Check for settled internal records missing on-chain
	for _, rec := range internalRecords {
		if !internalMatched[rec.ID] && (rec.Status == "settled" || rec.Status == "confirmed") {
			report.Discrepancies = append(report.Discrepancies, AnchorDiscrepancy{
				ID:             uuid.New().String(),
				Type:           DiscrepancyMissingOnChain,
				Severity:       SeverityHigh,
				Account:        account,
				Asset:          rec.Asset,
				TxHash:         rec.TxHash,
				Memo:           rec.Memo,
				OnChainAmount:  decimal.Zero,
				InternalAmount: rec.Amount,
				Variance:       rec.Amount,
				Description:    fmt.Sprintf("internal record %s (tx=%s) marked %s but missing from on-chain horizon records", rec.ID, rec.TxHash, rec.Status),
				DetectedAt:     now,
			})
		}
	}

	// 6. Calculate net variance & status
	inflowVariance := report.TotalOnChainInflow.Sub(report.TotalInternalInflow).Abs()
	outflowVariance := report.TotalOnChainOutflow.Sub(report.TotalInternalOutflow).Abs()
	report.NetVariance = inflowVariance.Add(outflowVariance)

	hasCritical := false
	for _, d := range report.Discrepancies {
		if d.Severity == SeverityCritical {
			hasCritical = true
			break
		}
	}

	if hasCritical {
		report.Status = "CRITICAL_ALERT"
	} else if len(report.Discrepancies) > 0 || report.NetVariance.GreaterThan(r.cfg.MaxVarianceThreshold) {
		report.Status = "VARIANCE_DETECTED"
	} else {
		report.Status = "BALANCED"
	}

	// 7. Publish report to audit log
	if r.auditPub != nil {
		if err := r.auditPub.PublishReconciliationReport(ctx, report); err != nil {
			return nil, fmt.Errorf("publish reconciliation report to audit repo: %w", err)
		}
	}

	// 8. Emit alerts if critical or variance exceeded
	if r.alerter != nil && (hasCritical || report.NetVariance.GreaterThan(r.cfg.MaxVarianceThreshold)) {
		alertData := map[string]interface{}{
			"report_id":      report.ID,
			"anchor_account": report.AnchorAccount,
			"net_variance":   report.NetVariance.String(),
			"discrepancies":  len(report.Discrepancies),
			"status":         report.Status,
		}
		_ = r.alerter.EmitAlert(ctx, "anchor.reconciliation.variance_threshold_exceeded",
			fmt.Sprintf("Anchor %s reconciliation alert: status=%s, net_variance=%s", account, report.Status, report.NetVariance),
			alertData,
		)
	}

	return report, nil
}

// extractOnChainPayments queries Horizon operations for account up to 24 hours ago.
func (r *AnchorReconciler) extractOnChainPayments(ctx context.Context, account string, windowStart time.Time) ([]OnChainPaymentItem, error) {
	var payments []OnChainPaymentItem
	cursor := ""
	const pageSize = 100

	for {
		ops, err := r.extractor.Payments(account, cursor, pageSize)
		if err != nil {
			return nil, err
		}
		if len(ops) == 0 {
			break
		}

		reachedEndOfWindow := false
		for _, rawOp := range ops {
			cursor = rawOp.PagingToken()

			// Parse operation into payment item
			item, ok := parsePaymentOp(rawOp)
			if !ok {
				continue
			}

			if item.CreatedAt.Before(windowStart) {
				reachedEndOfWindow = true
				break
			}

			payments = append(payments, item)
		}

		if reachedEndOfWindow || len(ops) < pageSize {
			break
		}
	}

	return payments, nil
}

func parsePaymentOp(op operations.Operation) (OnChainPaymentItem, bool) {
	switch p := op.(type) {
	case operations.Payment:
		amt, _ := decimal.NewFromString(p.Amount)
		asset := p.Asset.Code
		if p.Asset.Type == "native" || asset == "" {
			asset = "XLM"
		}
		memo := ""
		if p.Transaction != nil {
			memo = p.Transaction.Memo
		}
		return OnChainPaymentItem{
			ID:              p.ID,
			TransactionHash: p.TransactionHash,
			From:            p.From,
			To:              p.To,
			Asset:           asset,
			Amount:          amt,
			CreatedAt:       p.LedgerCloseTime,
			Memo:            memo,
		}, true

	case operations.PathPayment:
		amt, _ := decimal.NewFromString(p.Amount)
		asset := p.Asset.Code
		if p.Asset.Type == "native" || asset == "" {
			asset = "XLM"
		}
		memo := ""
		if p.Transaction != nil {
			memo = p.Transaction.Memo
		}
		return OnChainPaymentItem{
			ID:              p.ID,
			TransactionHash: p.TransactionHash,
			From:            p.From,
			To:              p.To,
			Asset:           asset,
			Amount:          amt,
			CreatedAt:       p.LedgerCloseTime,
			Memo:            memo,
		}, true

	case operations.PathPaymentStrictSend:
		amt, _ := decimal.NewFromString(p.Amount)
		asset := p.Asset.Code
		if p.Asset.Type == "native" || asset == "" {
			asset = "XLM"
		}
		memo := ""
		if p.Transaction != nil {
			memo = p.Transaction.Memo
		}
		return OnChainPaymentItem{
			ID:              p.ID,
			TransactionHash: p.TransactionHash,
			From:            p.From,
			To:              p.To,
			Asset:           asset,
			Amount:          amt,
			CreatedAt:       p.LedgerCloseTime,
			Memo:            memo,
		}, true

	default:
		return OnChainPaymentItem{}, false
	}
}

// InMemAuditLogPublisher is an in-memory implementation of AuditLogPublisher for tests and default runs.
type InMemAuditLogPublisher struct {
	Reports []*ReconciliationVarianceReport
}

func NewInMemAuditLogPublisher() *InMemAuditLogPublisher {
	return &InMemAuditLogPublisher{Reports: make([]*ReconciliationVarianceReport, 0)}
}

func (p *InMemAuditLogPublisher) PublishReconciliationReport(_ context.Context, report *ReconciliationVarianceReport) error {
	p.Reports = append(p.Reports, report)
	return nil
}

// InMemAlertEmitter is an in-memory implementation of AlertEmitter for tests.
type InMemAlertEmitter struct {
	Alerts []map[string]interface{}
}

func NewInMemAlertEmitter() *InMemAlertEmitter {
	return &InMemAlertEmitter{Alerts: make([]map[string]interface{}, 0)}
}

func (a *InMemAlertEmitter) EmitAlert(_ context.Context, alertType string, message string, details map[string]interface{}) error {
	alert := map[string]interface{}{
		"type":    alertType,
		"message": message,
		"details": details,
	}
	a.Alerts = append(a.Alerts, alert)
	return nil
}

// Ensure unused json import is satisfied
var _ = json.Marshal
var _ = errors.New
var _ = stellar.ErrNoPathsFound
