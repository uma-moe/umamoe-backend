-- Migration: Fix one-slot-off zero in daily_fans for March 2026
-- Date: 2026-03-06
-- Cause: Bot disruption caused the day-6 reading to land on day-7 instead,
--        leaving daily_fans[6] = 0 and daily_fans[7] = the actual value.
-- Fix:   Swap positions 6 and 7: put daily_fans[7] into [6] and 0 into [7].
-- Only rows where daily_fans[6] = 0 AND daily_fans[7] != 0 are touched.

UPDATE circle_member_fans_monthly
SET daily_fans =
    daily_fans[:5]                     
    || ARRAY[daily_fans[7]]            
    || ARRAY[0::bigint]                
    || daily_fans[8:]                  
WHERE year  = 2026
  AND month = 3
  AND daily_fans[6] = 0
  AND daily_fans[7] != 0;

