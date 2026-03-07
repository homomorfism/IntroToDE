WITH 
suspects_without_alibi AS (
    SELECT DISTINCT p.badge_uid, p.full_name, s.ts as arrival_time
    FROM people p
    LEFT JOIN swipes s ON p.badge_uid = s.badge_uid 
        AND s.session_code = 'S3'
        AND s.ts BETWEEN '2025-09-01 13:00:00' AND '2025-09-01 13:15:00'
    WHERE s.ts IS NULL                         
       OR s.ts > '2025-09-01 13:10:00'        
),
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
SELECT 
    s.full_name AS thief_name,
    s.badge_uid,
    s.arrival_time AS late_arrival_to_session,
    c.coffee_count AS coffees_bought_after_15h,
    c.first_coffee_time,
    md5(lower(trim(s.full_name))) AS verification_hash
FROM suspects_without_alibi s
INNER JOIN jittery_coffee_buyers c ON s.badge_uid = c.badge_uid
WHERE s.arrival_time IS NOT NULL  
ORDER BY c.first_coffee_time;
