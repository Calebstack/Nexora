-- Multi-currency anchor payout queue with balance reservation (issue #30).
CREATE TABLE anchor_payout_batches (
    id              UUID PRIMARY KEY,
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    wallet_id       UUID NOT NULL REFERENCES wallets(id),
    idempotency_key TEXT NOT NULL,
    request_hash    TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, idempotency_key)
);

CREATE TABLE anchor_payouts (
    id                     UUID PRIMARY KEY,
    batch_id               UUID NOT NULL REFERENCES anchor_payout_batches(id) ON DELETE CASCADE,
    tenant_id              UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    wallet_id              UUID NOT NULL REFERENCES wallets(id),
    reference              TEXT NOT NULL,
    partner                TEXT NOT NULL,
    destination_currency   TEXT NOT NULL CHECK (destination_currency IN ('NGN', 'KES', 'GHS', 'ZAR')),
    country                TEXT NOT NULL,
    destination_amount     NUMERIC(20, 2) NOT NULL CHECK (destination_amount > 0),
    source_asset           TEXT NOT NULL,
    source_issuer          TEXT NOT NULL,
    source_amount          NUMERIC(20, 7) NOT NULL CHECK (source_amount > 0),
    -- AES-256-GCM envelope of the beneficiary bank account.
    beneficiary_ciphertext BYTEA NOT NULL,
    status                 TEXT NOT NULL CHECK (status IN ('queued', 'submitting', 'submitted', 'completed', 'failed', 'reversed')),
    reservation_state      TEXT NOT NULL CHECK (reservation_state IN ('held', 'captured', 'released')),
    provider_ref           TEXT NOT NULL DEFAULT '',
    attempts               INT NOT NULL DEFAULT 0,
    next_attempt_at        TIMESTAMPTZ NOT NULL,
    lease_expires_at       TIMESTAMPTZ,
    last_error             TEXT NOT NULL DEFAULT '',
    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,
    completed_at           TIMESTAMPTZ,
    UNIQUE (batch_id, reference)
);

-- Per-partner claim scan over due queued work.
CREATE INDEX idx_anchor_payouts_queued
    ON anchor_payouts (partner, next_attempt_at)
    WHERE status = 'queued';

-- Per-partner in-flight count and stale-lease reclaim.
CREATE INDEX idx_anchor_payouts_submitting
    ON anchor_payouts (partner, lease_expires_at)
    WHERE status = 'submitting';

CREATE INDEX idx_anchor_payouts_batch ON anchor_payouts (batch_id);

-- Running total of stablecoin reserved per wallet balance. Available
-- balance = balances.balance - held.
CREATE TABLE anchor_payout_balance_holds (
    wallet_id  UUID NOT NULL REFERENCES wallets(id) ON DELETE CASCADE,
    asset_code TEXT NOT NULL,
    issuer     TEXT NOT NULL,
    held       NUMERIC(20, 7) NOT NULL DEFAULT 0 CHECK (held >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (wallet_id, asset_code, issuer)
);

-- Applied partner outcomes; the primary key makes callbacks idempotent.
CREATE TABLE anchor_payout_events (
    partner     TEXT NOT NULL,
    event_id    TEXT NOT NULL,
    payout_id   UUID NOT NULL REFERENCES anchor_payouts(id) ON DELETE CASCADE,
    outcome     TEXT NOT NULL CHECK (outcome IN ('success', 'failure', 'reversal')),
    reason      TEXT NOT NULL DEFAULT '',
    received_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (partner, event_id)
);
