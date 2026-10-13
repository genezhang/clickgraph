WITH with_w1 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      count(v2.user_id) AS "v4"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.user_id
)
SELECT 
      minOrNull(w1.v4) AS "min(fc)", 
      maxOrNull(w1.v4) AS "max(fc)", 
      coalesce(sum(w1.v4), 0) AS "sum(fc)", 
      stddevSamp(w1.v4) AS "stDev(fc)"
FROM with_w1 AS w1
