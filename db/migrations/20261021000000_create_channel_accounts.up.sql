-- Channel accounts: a pool of funded Stellar accounts dedicated to transaction
-- sequencing, so high-volume submission does not serialise on the primary
-- treasury account's sequence number (issue #41).
CREATE TABLE IF NOT EXISTS channel_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    public_key VARCHAR(56) NOT NULL UNIQUE,
    encrypted_secret TEXT NOT NULL,
    status VARCHAR(16) NOT NULL DEFAULT 'active'
        CHECK (status IN ('active', 'draining', 'disabled')),
    native_balance DECIMAL(38, 7) NOT NULL DEFAULT 0,
    last_used_at TIMESTAMPTZ,
    last_topped_up_at TIMESTAMPTZ,
    balance_check_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- The pool loads the active set on refresh; a partial index keeps that hot path
-- small as disabled/draining accounts accumulate.
CREATE INDEX IF NOT EXISTS idx_channel_accounts_active
    ON channel_accounts (status)
    WHERE status = 'active';

-- Balance monitor scans least-recently-checked accounts first.
CREATE INDEX IF NOT EXISTS idx_channel_accounts_balance_check
    ON channel_accounts (balance_check_at NULLS FIRST);
