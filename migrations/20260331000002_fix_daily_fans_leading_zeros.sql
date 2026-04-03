-- Fix rows where the circle was joined mid-month (leading zeros in daily_fans).
-- The bogus first element was not caught by migration 000001 because daily_fans[2] = 0.
--
-- Two cases:
--   Non-top-100 (daily_fans[32] = 0): move [1] to position 31 (before trailing 0)
--   Top-100 (daily_fans[32] != 0):    move [1] to end as tally, result is 32 elements

-- Case 1: Non-top-100, leading zeros (joined mid-month)
UPDATE circle_member_fans_monthly
SET daily_fans = daily_fans[2:31] || ARRAY[daily_fans[1], 0::bigint]
WHERE year = 2026
  AND month = 3
  AND array_length(daily_fans, 1) = 32
  AND daily_fans[1] > 0
  AND daily_fans[2] = 0
  AND daily_fans[32] = 0;

-- Case 2: Top-100, leading zeros (joined mid-month)
UPDATE circle_member_fans_monthly
SET daily_fans = daily_fans[2:] || ARRAY[daily_fans[1]]
WHERE year = 2026
  AND month = 3
  AND array_length(daily_fans, 1) = 32
  AND daily_fans[1] > 0
  AND daily_fans[2] = 0
  AND daily_fans[32] != 0;
