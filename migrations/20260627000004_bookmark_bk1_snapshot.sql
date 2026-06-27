-- Bookmarks now snapshot the same stable combo key used for borrow tracking,
-- but keep the legacy content-hash snapshot intact for rollback safety.
--
-- Existing bookmarks keep their current content-hash snapshot in
-- user_bookmarks.bookmarked_hash and are accepted by the application as legacy
-- matches. They are lazily given bookmarked_borrow_key when a user loads their
-- bookmarks, which avoids reimplementing the fast bk1 hash in SQL and keeps a
-- clean rollback path for old code.

ALTER TABLE user_bookmarks
    ADD COLUMN IF NOT EXISTS bookmarked_borrow_key TEXT,
    ADD COLUMN IF NOT EXISTS support_card_id INTEGER,
    ADD COLUMN IF NOT EXISTS support_card_limit_break INTEGER,
    ADD COLUMN IF NOT EXISTS support_card_experience INTEGER;
