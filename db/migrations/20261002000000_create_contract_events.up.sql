CREATE TABLE IF NOT EXISTS contract_events (
    id VARCHAR(64) PRIMARY KEY,
    contract_id VARCHAR(56) NOT NULL,
    event_type VARCHAR(20) NOT NULL,
    topic TEXT NOT NULL DEFAULT '',
    topics JSONB NOT NULL DEFAULT '[]'::jsonb,
    data JSONB NOT NULL DEFAULT 'null'::jsonb,
    ledger BIGINT NOT NULL,
    tx_hash VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_contract_events_contract_id ON contract_events(contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_events_topic ON contract_events(topic);
CREATE INDEX IF NOT EXISTS idx_contract_events_ledger ON contract_events(ledger);

-- One row per ledger the indexer has processed, including ledgers that emitted
-- no events. The stored hash lets the indexer detect re-orgs and the sequence
-- lets it detect gaps.
CREATE TABLE IF NOT EXISTS contract_event_ledgers (
    sequence BIGINT PRIMARY KEY,
    hash VARCHAR(64) NOT NULL,
    indexed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
