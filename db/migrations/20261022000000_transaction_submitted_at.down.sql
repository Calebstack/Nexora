DROP INDEX IF EXISTS idx_transactions_submitted_at;

ALTER TABLE transactions
    DROP COLUMN IF EXISTS submitted_at,
    DROP COLUMN IF EXISTS broadcast_outcome_unknown;
