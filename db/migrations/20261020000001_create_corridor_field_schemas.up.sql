-- Versioned SEP-31 corridor customer field schemas (issue #34). Versions are
-- immutable; exactly one version per (tenant, corridor, customer type) is
-- active.
CREATE TABLE corridor_field_schemas (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    corridor      TEXT NOT NULL,
    customer_type TEXT NOT NULL,
    version       INT NOT NULL CHECK (version > 0),
    schema        JSONB NOT NULL,
    active        BOOLEAN NOT NULL DEFAULT FALSE,
    created_by    TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, corridor, customer_type, version)
);

CREATE UNIQUE INDEX uq_corridor_field_schemas_active
    ON corridor_field_schemas (tenant_id, corridor, customer_type)
    WHERE active;
