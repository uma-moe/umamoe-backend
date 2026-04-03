-- Fix rows where the bot wrote the latest cumulative value into index [0],
-- creating a 32-element array with a bogus leading value.
-- After the bounds fix migration re-indexed [0:31] → [1:32], the bogus value
-- sits at [1].
--
-- Two cases:
--   Non-top-100 (daily_fans[32] = 0): move [1] to position 31 (before trailing 0)
--   Top-100 (daily_fans[32] != 0):    move [1] to end as tally, result is 32 elements
--
-- Only affects March 2026 rows where daily_fans[1] > daily_fans[2] and data starts day 1

-- Case 1: Non-top-100, data starts day 1
UPDATE circle_member_fans_monthly
SET daily_fans = daily_fans[2:31] || ARRAY[daily_fans[1], 0::bigint]
WHERE year = 2026
  AND month = 3
  AND array_length(daily_fans, 1) = 32
  AND daily_fans[1] > daily_fans[2]
  AND daily_fans[2] > 0
  AND daily_fans[32] = 0;

-- Case 2: Top-100, data starts day 1
UPDATE circle_member_fans_monthly
SET daily_fans = daily_fans[2:] || ARRAY[daily_fans[1]]
WHERE year = 2026
  AND month = 3
  AND array_length(daily_fans, 1) = 32
  AND daily_fans[1] > daily_fans[2]
  AND daily_fans[2] > 0
  AND daily_fans[32] != 0;
