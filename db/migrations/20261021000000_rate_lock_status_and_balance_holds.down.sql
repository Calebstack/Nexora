DROP TABLE IF EXISTS fx_rate_lock_balance_holds;
DROP INDEX IF EXISTS idx_fx_rate_locks_status;
DROP INDEX IF EXISTS idx_fx_rate_locks_expiration;
ALTER TABLE fx_rate_locks DROP CONSTRAINT IF EXISTS chk_fx_rate_locks_status;
ALTER TABLE fx_rate_locks DROP COLUMN IF EXISTS wallet_id;
ALTER TABLE fx_rate_locks DROP COLUMN IF EXISTS status;
