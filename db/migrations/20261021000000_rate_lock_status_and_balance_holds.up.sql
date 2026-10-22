-- Add status and wallet_id to fx_rate_locks
ALTER TABLE fx_rate_locks ADD COLUMN IF NOT EXISTS status VARCHAR(20) NOT NULL DEFAULT 'active';
ALTER TABLE fx_rate_locks ADD CONSTRAINT chk_fx_rate_locks_status CHECK (status IN ('active', 'consumed', 'expired'));
ALTER TABLE fx_rate_locks ADD COLUMN IF NOT EXISTS wallet_id UUID REFERENCES wallets(id) ON DELETE SET NULL;

-- Backfill existing rows
UPDATE fx_rate_locks SET status = 'consumed' WHERE consumed_at IS NOT NULL;
UPDATE fx_rate_locks SET status = 'expired' WHERE consumed_at IS NULL AND expires_at < NOW();

-- Hot path index for asynchronous cleanup worker query:
-- SELECT ... WHERE expires_at < NOW() AND consumed_at IS NULL AND status = 'active'
CREATE INDEX IF NOT EXISTS idx_fx_rate_locks_expiration 
ON fx_rate_locks(expires_at) 
WHERE consumed_at IS NULL AND status = 'active';

CREATE INDEX IF NOT EXISTS idx_fx_rate_locks_status 
ON fx_rate_locks(status);

-- Custodial account balance holds for rate locks
CREATE TABLE IF NOT EXISTS fx_rate_lock_balance_holds (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    lock_id UUID NOT NULL REFERENCES fx_rate_locks(id) ON DELETE CASCADE,
    wallet_id UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    asset_code VARCHAR(12) NOT NULL,
    issuer TEXT NOT NULL DEFAULT '',
    amount DECIMAL(38, 18) NOT NULL CHECK (amount > 0),
    released BOOLEAN NOT NULL DEFAULT FALSE,
    released_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fx_rate_lock_holds_lock ON fx_rate_lock_balance_holds(lock_id);
CREATE INDEX IF NOT EXISTS idx_fx_rate_lock_holds_wallet_active ON fx_rate_lock_balance_holds(wallet_id, released) WHERE released = FALSE;
