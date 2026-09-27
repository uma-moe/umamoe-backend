-- OAuth providers can return avatar URLs longer than 512 characters.
-- PostgreSQL TEXT keeps the URL intact without imposing an arbitrary limit.
ALTER TABLE users
    ALTER COLUMN avatar_url TYPE TEXT;

ALTER TABLE user_identities
    ALTER COLUMN avatar_url TYPE TEXT;
