-- Anchor SEP-6/24/31 status callback outbox with endpoint quarantine (issue #29).
CREATE TABLE anchor_callback_endpoints (
    id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    partner              TEXT NOT NULL,
    url                  TEXT NOT NULL,
    secret_ciphertext    BYTEA NOT NULL,
    consecutive_failures INT NOT NULL DEFAULT 0,
    quarantined_at       TIMESTAMPTZ,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (partner, url)
);

CREATE TABLE anchor_callback_deliveries (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint_id        UUID NOT NULL REFERENCES anchor_callback_endpoints(id) ON DELETE CASCADE,
    sep                SMALLINT NOT NULL CHECK (sep IN (6, 24, 31)),
    transaction_id     TEXT NOT NULL,
    transaction_status TEXT NOT NULL,
    -- Exact request body; kept as bytes so the signed payload never changes.
    payload            BYTEA NOT NULL,
    state              TEXT NOT NULL CHECK (state IN ('pending', 'delivered', 'quarantined', 'expired')),
    attempts           INT NOT NULL DEFAULT 0,
    next_attempt_at    TIMESTAMPTZ NOT NULL,
    expires_at         TIMESTAMPTZ NOT NULL,
    last_error         TEXT NOT NULL DEFAULT '',
    last_status_code   INT NOT NULL DEFAULT 0,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at       TIMESTAMPTZ
);

-- Claim scan: only pending rows, ordered by due time.
CREATE INDEX idx_anchor_callback_deliveries_due
    ON anchor_callback_deliveries (next_attempt_at)
    WHERE state = 'pending';

-- Quarantine listing and release per endpoint.
CREATE INDEX idx_anchor_callback_deliveries_quarantined
    ON anchor_callback_deliveries (endpoint_id, created_at)
    WHERE state = 'quarantined';

CREATE INDEX idx_anchor_callback_deliveries_pending_endpoint
    ON anchor_callback_deliveries (endpoint_id)
    WHERE state = 'pending';
