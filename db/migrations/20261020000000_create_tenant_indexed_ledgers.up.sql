-- Per-tenant ledger ingestion checkpoints for gap detection (issue #38).
-- One row per ledger applied for the tenant, including ledgers with no
-- payments for it, so a missing sequence is a gap.
CREATE TABLE tenant_indexed_ledgers (
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    mode       TEXT NOT NULL CHECK (mode IN ('live', 'test')),
    sequence   BIGINT NOT NULL CHECK (sequence > 0),
    indexed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, mode, sequence)
);

-- Highest sequence below which nothing is missing, so each gap scan reads
-- only rows above it.
CREATE TABLE tenant_ledger_watermarks (
    tenant_id          UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    mode               TEXT NOT NULL CHECK (mode IN ('live', 'test')),
    contiguous_through BIGINT NOT NULL CHECK (contiguous_through >= 0),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, mode)
);
