-- Corridor fee schedules for anchor fee disclosure (issue #33). A row with
-- tenant_id NULL is the platform default; a tenant row overrides it for the
-- same corridor and speed tier.
CREATE TABLE anchor_corridor_fee_schedules (
    id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id        UUID REFERENCES tenants(id) ON DELETE CASCADE,
    corridor         TEXT NOT NULL,
    speed_tier       TEXT NOT NULL CHECK (speed_tier IN ('standard', 'express', 'instant')),
    rail             TEXT NOT NULL DEFAULT '',
    processing_flat  NUMERIC(20, 7) NOT NULL DEFAULT 0 CHECK (processing_flat >= 0),
    processing_bps   NUMERIC(9, 4)  NOT NULL DEFAULT 0 CHECK (processing_bps >= 0),
    processing_min   NUMERIC(20, 7) NOT NULL DEFAULT 0 CHECK (processing_min >= 0),
    processing_max   NUMERIC(20, 7) CHECK (processing_max >= processing_min),
    rail_flat        NUMERIC(20, 7) NOT NULL DEFAULT 0 CHECK (rail_flat >= 0),
    rail_bps         NUMERIC(9, 4)  NOT NULL DEFAULT 0 CHECK (rail_bps >= 0),
    rail_min         NUMERIC(20, 7) NOT NULL DEFAULT 0 CHECK (rail_min >= 0),
    rail_max         NUMERIC(20, 7) CHECK (rail_max >= rail_min),
    time_zone        TEXT NOT NULL DEFAULT 'UTC',
    -- [{"weekdays":[1,2,3,4,5],"start_minute":480,"end_minute":600,"multiplier":"1.5"}]
    surge_windows    JSONB NOT NULL DEFAULT '[]',
    operations       INT NOT NULL DEFAULT 1 CHECK (operations BETWEEN 1 AND 100),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE NULLS NOT DISTINCT (tenant_id, corridor, speed_tier)
);

CREATE INDEX idx_anchor_corridor_fee_schedules_lookup
    ON anchor_corridor_fee_schedules (corridor, speed_tier);
