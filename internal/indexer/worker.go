package indexer

import (
	"context"
	"time"

	"github.com/Lumen-Nexora/Nexora/internal/config"
	"github.com/Lumen-Nexora/Nexora/internal/queue"
	"github.com/Lumen-Nexora/Nexora/internal/tracing"
	"github.com/hibiken/asynq"
)

// TypeLedgerGapScan schedules one GapDetector pass over every tenant.
const TypeLedgerGapScan = "indexer:ledger_gap_scan"

type Worker struct {
	indexer *Indexer
	config  *config.Config
	gaps    *GapDetector
}

// WithGapDetector enables HandleLedgerGapScan.
func (w *Worker) WithGapDetector(d *GapDetector) *Worker {
	w.gaps = d
	return w
}

// HandleLedgerGapScan ingests new ledgers and backfills gaps for every tenant.
func (w *Worker) HandleLedgerGapScan(ctx context.Context, task *asynq.Task) error {
	ctx = queue.ContextFromTask(ctx, task)
	ctx, span := tracing.StartConsumer(ctx, task.Type())
	defer span.End()
	if w.gaps == nil {
		return nil
	}
	if err := w.gaps.ScanAll(ctx); err != nil {
		tracing.Logger(ctx).Error().Err(err).Msg("ledger gap scan failed")
		return err
	}
	return nil
}

func NewWorker(indexer *Indexer, cfg *config.Config) *Worker {
	return &Worker{indexer: indexer, config: cfg}
}

func (w *Worker) HandleSyncLedger(ctx context.Context, task *asynq.Task) error {
	ctx = queue.ContextFromTask(ctx, task)
	ctx, span := tracing.StartConsumer(ctx, task.Type())
	defer span.End()

	// Log through the context so every entry carries the inherited trace_id.
	logger := tracing.Logger(ctx)

	logger.Info().Msg("running ledger sync")
	if err := w.indexer.SyncAll(ctx); err != nil {
		logger.Error().Err(err).Msg("ledger sync failed")
		return err
	}
	return nil
}

func (w *Worker) IndexerConfig() Config {
	return Config{
		PaymentsPageLimit: getEnvInt("INDEXER_PAYMENTS_PAGE_LIMIT", 50),
		StreamMinBackoff:  getEnvDuration("INDEXER_STREAM_MIN_BACKOFF", 1*time.Second),
		StreamMaxBackoff:  getEnvDuration("INDEXER_STREAM_MAX_BACKOFF", 30*time.Second),
		SyncPageSize:      getEnvInt("INDEXER_SYNC_PAGE_SIZE", 100),
	}
}

func getEnvInt(key string, defaultVal int) int {
	// This would typically use viper or os.Getenv, but we'll use config.Load() values
	// For now, return default; the config should be passed from main
	return defaultVal
}

func getEnvDuration(key string, defaultVal time.Duration) time.Duration {
	return defaultVal
}
