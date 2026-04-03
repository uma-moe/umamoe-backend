-- Add total request counter to api_keys
ALTER TABLE api_keys ADD COLUMN total_requests BIGINT NOT NULL DEFAULT 0;

-- Widen key_prefix to fit "uma_k_" + 8 random chars
ALTER TABLE api_keys ALTER COLUMN key_prefix TYPE VARCHAR(16);

-- Daily per-endpoint usage summary for each API key
CREATE TABLE IF NOT EXISTS api_key_usage (
    api_key_id  UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    endpoint    VARCHAR(255) NOT NULL,
    date        DATE NOT NULL DEFAULT CURRENT_DATE,
    requests    BIGINT NOT NULL DEFAULT 1,
    PRIMARY KEY (api_key_id, endpoint, date)
);

CREATE INDEX idx_api_key_usage_date ON api_key_usage (date);
