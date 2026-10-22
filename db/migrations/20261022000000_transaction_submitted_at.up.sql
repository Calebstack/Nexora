ALTER TABLE transactions
    ADD COLUMN submitted_at TIMESTAMPTZ,
    ADD COLUMN broadcast_outcome_unknown BOOLEAN NOT NULL DEFAULT FALSE;

UPDATE transactions
SET submitted_at = COALESCE(submitted_at, created_at),
    broadcast_outcome_unknown = (status = 'submitted' OR tx_hash IS NOT NULL)
WHERE status = 'submitted'
   OR tx_hash IS NOT NULL;

CREATE INDEX idx_transactions_submitted_at
    ON transactions (submitted_at)
    WHERE status = 'submitted';
