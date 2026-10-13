WITH v1_both AS (
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.follower_id AS "__cg_start_0", 
      e.followed_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS "follow_date", 
      e.follow_id AS "follow_id", 
      e.followed_id AS "followed_id", 
      e.follower_id AS "follower_id", 
      e.followed_id AS "__cg_start_0", 
      e.follower_id AS "__cg_end_0"
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
optional_o1 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.user_id AS "p2_v2_user_id"
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
), 
optional_o3 AS (
SELECT 
      v3.follower_id AS "v2__user_id", 
      v4.user_id AS "v4__user_id", 
      v4.user_id AS "p2_v4_user_id"
FROM test_integration.user_follows_test AS v3
JOIN test_integration.users_test AS v4 ON v3.followed_id = v4.user_id
WHERE v3.follower_id IS NOT NULL
)
SELECT 
      v0.user_id AS "a.user_id", 
      o1.p2_v2_user_id AS "b.user_id", 
      o3.p2_v4_user_id AS "c.user_id"
FROM test_integration.users_test AS v0
LEFT JOIN optional_o1 AS o1 ON v0.user_id = o1.v0__user_id
LEFT JOIN optional_o3 AS o3 ON o1.v2__user_id = o3.v2__user_id
