-- Multi-tenant FX custom markup and spread margin configurations
CREATE TABLE IF NOT EXISTS tenant_fx_margins (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    from_asset VARCHAR(12) NOT NULL,
    to_asset VARCHAR(12) NOT NULL,
    tier_min_amount DECIMAL(38, 18) NOT NULL DEFAULT 0 CHECK (tier_min_amount >= 0),
    tier_max_amount DECIMAL(38, 18) CHECK (tier_max_amount IS NULL OR tier_max_amount > tier_min_amount),
    markup_bps INT NOT NULL,
    min_margin_bps INT NOT NULL DEFAULT 0,
    max_margin_bps INT NOT NULL DEFAULT 5000,
    created_by TEXT NOT NULL DEFAULT 'admin',
    updated_by TEXT NOT NULL DEFAULT 'admin',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_corridor_different_assets CHECK (from_asset <> to_asset),
    CONSTRAINT chk_margin_ceilings CHECK (max_margin_bps >= min_margin_bps),
    CONSTRAINT chk_markup_within_ceilings CHECK (markup_bps >= min_margin_bps AND markup_bps <= max_margin_bps),
    CONSTRAINT uq_tenant_corridor_tier UNIQUE (tenant_id, from_asset, to_asset, tier_min_amount)
);

CREATE INDEX IF NOT EXISTS idx_tenant_fx_margins_lookup
    ON tenant_fx_margins (tenant_id, from_asset, to_asset, tier_min_amount DESC);

CREATE INDEX IF NOT EXISTS idx_tenant_fx_margins_tenant
    ON tenant_fx_margins (tenant_id);

-- Audit log for all margin modifications with acting administrative principal
CREATE TABLE IF NOT EXISTS tenant_fx_margin_audit (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    margin_id UUID NOT NULL,
    tenant_id UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    action VARCHAR(20) NOT NULL CHECK (action IN ('create', 'update', 'delete')),
    old_values JSONB,
    new_values JSONB,
    actor_principal TEXT NOT NULL,
    actor_ip TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tenant_fx_margin_audit_margin ON tenant_fx_margin_audit(margin_id);
CREATE INDEX IF NOT EXISTS idx_tenant_fx_margin_audit_tenant ON tenant_fx_margin_audit(tenant_id);
