WITH with_w1 AS (
SELECT 
      v2.full_name AS "v3"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      w1.v3 AS "followedName"
FROM with_w1 AS w1
LIMIT 3