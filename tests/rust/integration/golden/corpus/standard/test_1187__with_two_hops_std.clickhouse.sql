WITH with_w1 AS (
SELECT 
      v2.user_id AS "v3__user_id", 
      v2.user_id AS "p2_v3_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
)
SELECT 
      w1.p2_v3_user_id AS "c.user_id", 
      v7.user_id AS "a.user_id"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
JOIN test_integration.user_follows_test AS v6 ON v6.follower_id = v5.user_id
JOIN test_integration.users_test AS v7 ON v6.followed_id = v7.user_id
WHERE v4.follow_id <> v6.follow_id

-- column c.user_id: Value [("c.user_id", "c.user_id")]
-- column a.user_id: Value [("a.user_id", "a.user_id")]