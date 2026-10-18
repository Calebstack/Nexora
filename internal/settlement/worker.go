package settlement

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Lumen-Nexora/Nexora/internal/domain"
	"github.com/Lumen-Nexora/Nexora/internal/queue"
	"github.com/Lumen-Nexora/Nexora/internal/tenant"
	"github.com/Lumen-Nexora/Nexora/internal/tracing"
	"github.com/hibiken/asynq"
)

type Worker struct {
	engine *Engine
	poller *FallbackPoller
}

func NewWorker(engine *Engine) *Worker {
	return &Worker{engine: engine}
}

// WithPoller attaches a FallbackPoller for telemetry and fallback coordination.
func (w *Worker) WithPoller(poller *FallbackPoller) *Worker {
	w.poller = poller
	return w
}

func (w *Worker) HandleProcessTransfer(ctx context.Context, task *asynq.Task) error {
	ctx = queue.ContextFromTask(ctx, task)
	ctx, span := tracing.StartConsumer(ctx, task.Type())
	defer span.End()

	var payload queue.ProcessTransferPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("unmarshal payload: %w", err)
	}

	// Log through the context so every entry carries the inherited trace_id.
	logger := tracing.Logger(ctx)

	logger.Info().Str("tx_id", payload.TransactionID).Msg("processing transfer")

	tx, err := w.engine.txRepo.GetByID(ctx, payload.TransactionID)
	if err != nil {
		logger.Error().Err(err).Str("tx_id", payload.TransactionID).Msg("failed to fetch transaction for settlement check")
		return err
	}
	if tx.TenantID != nil {
		ctx = tenant.WithID(ctx, *tx.TenantID)
	}
	ctx = tenant.WithMode(ctx, tx.Mode)

	if tx.Status == domain.StatusFailed || tx.Status == domain.StatusReconciliationFailed {
		logger.Warn().Str("tx_id", payload.TransactionID).Str("status", string(tx.Status)).Msg("skipping already-failed transaction")
		_ = w.engine.txRepo.UpdateStatus(ctx, payload.TransactionID, domain.StatusFailed, tx.TxHash)
		return nil
	}

	if err := w.engine.SubmitTransfer(ctx, payload.TransactionID); err != nil {
		logger.Error().Err(err).Str("tx_id", payload.TransactionID).Msg("transfer submission failed")
		if w.poller != nil {
			w.poller.RecordSubmissionFailure(err)
		}
		if !strings.Contains(err.Error(), "awaiting reconciliation") {
			_ = w.engine.txRepo.UpdateStatus(ctx, payload.TransactionID, domain.StatusFailed, "")
		}
		return err
	}

	if w.poller != nil {
		w.poller.RecordSubmissionSuccess()
	}

	logger.Info().Str("tx_id", payload.TransactionID).Msg("transfer confirmed")
	return nil
}
