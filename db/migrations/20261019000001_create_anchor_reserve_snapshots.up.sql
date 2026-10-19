-- Hourly anchor reserve snapshots for compliance audits (issue #31).
-- Rows are append-only: UPDATE, DELETE and TRUNCATE are rejected.
CREATE TABLE anchor_reserve_snapshots (
    id                 UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    asset_code         TEXT NOT NULL,
    issuer             TEXT NOT NULL,
    reserve_currency   TEXT NOT NULL,
    on_chain_supply    NUMERIC(30, 7) NOT NULL,
    off_chain_reserves NUMERIC(30, 7) NOT NULL,
    reserve_ratio      NUMERIC(38, 18),
    deviation          NUMERIC(38, 18),
    breached           BOOLEAN NOT NULL,
    snapshot_hour      TIMESTAMPTZ NOT NULL,
    captured_at        TIMESTAMPTZ NOT NULL,
    UNIQUE (asset_code, issuer, snapshot_hour)
);

CREATE INDEX idx_anchor_reserve_snapshots_asset_time
    ON anchor_reserve_snapshots (asset_code, issuer, captured_at);

CREATE FUNCTION anchor_reserve_snapshots_immutable() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'anchor_reserve_snapshots is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER anchor_reserve_snapshots_no_update_delete
    BEFORE UPDATE OR DELETE ON anchor_reserve_snapshots
    FOR EACH ROW EXECUTE FUNCTION anchor_reserve_snapshots_immutable();

CREATE TRIGGER anchor_reserve_snapshots_no_truncate
    BEFORE TRUNCATE ON anchor_reserve_snapshots
    FOR EACH STATEMENT EXECUTE FUNCTION anchor_reserve_snapshots_immutable();
