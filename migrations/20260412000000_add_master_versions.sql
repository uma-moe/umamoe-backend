CREATE TABLE IF NOT EXISTS master_versions (
    id SERIAL PRIMARY KEY,
    app_version VARCHAR(50) NOT NULL,
    resource_version VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);
