-- ============================================================
-- THE CASE OF THE GOLDEN FIGURINE - SOLUTION
-- ============================================================
-- 
-- CLUES:
-- 1. Figurine stolen during GOLDEN WALRUS (S3) session: 13:00-13:15
-- 2. Thief "slipped away" - meaning they weren't present during the session
-- 3. ~2 hours later (~15:00), volunteer saw someone jittery at cafeteria
-- 4. That person was buying coffee repeatedly (3+ times till end of day)
--
-- KEY INSIGHT: Swiping into S3 at 13:13:32 means being ABSENT for 
-- the first 13 minutes - plenty of time to steal the figurine!
-- The thief swiped in late to create a fake alibi.
-- ============================================================

WITH 
-- People who were NOT present during most of the S3 session
-- Either: didn't attend at all, OR arrived very late (after 13:10)
suspects_without_alibi AS (
    SELECT DISTINCT p.badge_uid, p.full_name, s.ts as arrival_time
    FROM people p
    LEFT JOIN swipes s ON p.badge_uid = s.badge_uid 
        AND s.session_code = 'S3'
        AND s.ts BETWEEN '2025-09-01 13:00:00' AND '2025-09-01 13:15:00'
    WHERE s.ts IS NULL                          -- Didn't attend at all
       OR s.ts > '2025-09-01 13:10:00'          -- Arrived suspiciously late
),

-- People who bought coffee 3+ times starting around 15:00 (2 hours later)
-- "jittery and buying coffee repeatedly"
jittery_coffee_buyers AS (
    SELECT badge_uid, 
           COUNT(*) as coffee_count,
           MIN(ts) as first_coffee_time
    FROM purchases
    WHERE product = 'Coffee'
      AND ts >= '2025-09-01 15:00:00'
    GROUP BY badge_uid
    HAVING COUNT(*) >= 3
)

-- THE THIEF: intersection of both conditions
SELECT 
    s.full_name AS thief_name,
    s.badge_uid,
    s.arrival_time AS late_arrival_to_session,
    c.coffee_count AS coffees_bought_after_15h,
    c.first_coffee_time,
    md5(lower(trim(s.full_name))) AS verification_hash
FROM suspects_without_alibi s
INNER JOIN jittery_coffee_buyers c ON s.badge_uid = c.badge_uid
WHERE s.arrival_time IS NOT NULL  -- Specifically: late arrivals (fake alibi attempt)
ORDER BY c.first_coffee_time;
