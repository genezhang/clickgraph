WITH with_w1 AS (
SELECT 
      v0.user_id AS "v1__user_id", 
      v0.full_name AS "p2_v1_name"
FROM test_integration.users_test AS v0
)
SELECT 
      w1.p2_v1_name AS "p.name", 
      v5.full_name AS "c.name"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v2 ON v2.follower_id = w1.v1__user_id
JOIN test_integration.users_test AS v3 ON v2.followed_id = v3.user_id
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = v3.user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
WHERE v2.follow_id <> v4.follow_id
