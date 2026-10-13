WITH with_w1 AS (
SELECT 
      v0.user_id AS "v3__user_id", 
      count(v2.user_id) AS "v4"
FROM test_integration.users AS v0
JOIN test_integration.follows AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users AS v2 ON v1.followed_id = v2.user_id
GROUP BY v0.user_id
)
SELECT 
      avg(w1.v4) AS "avg_follows_per_user"
FROM with_w1 AS w1
