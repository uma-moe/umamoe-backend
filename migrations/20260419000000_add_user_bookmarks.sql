CREATE TABLE IF NOT EXISTS user_bookmarks (
    id             SERIAL PRIMARY KEY,
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    inheritance_id INTEGER NOT NULL,
    created_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, inheritance_id)
);

CREATE INDEX idx_user_bookmarks_user_id ON user_bookmarks (user_id);
