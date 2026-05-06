-- Add trainer name to partner_inheritance so the frontend can display who
-- owns the partner without a separate lookup. Populated from
-- practice_partner_owner_info.owner_name in the bot's API response.

ALTER TABLE partner_inheritance
    ADD COLUMN IF NOT EXISTS trainer_name TEXT;
