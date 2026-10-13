WITH with_w1 AS (
SELECT 
      v2.user_id AS "v3__user_id", 
      v2.user_id AS "p2_v3_user_id"
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
), 
optional_o2 AS (
SELECT 
      v6.followed_id AS "v5__user_id", 
      v7.user_id AS "v7__user_id", 
      v7.user_id AS "p2_v7_user_id"
FROM test_integration.user_follows_test AS v6
JOIN test_integration.users_test AS v7 ON v6.follower_id = v7.user_id
)
SELECT 
      w1.p2_v3_user_id AS "c.user_id", 
      o2.p2_v7_user_id AS "n2.user_id"
FROM with_w1 AS w1
JOIN test_integration.user_follows_test AS v4 ON v4.follower_id = w1.v3__user_id
JOIN test_integration.users_test AS v5 ON v4.followed_id = v5.user_id
LEFT JOIN optional_o2 AS o2 ON v5.user_id = o2.v5__user_id
