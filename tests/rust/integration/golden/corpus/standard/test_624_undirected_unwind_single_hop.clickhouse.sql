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
with_w2 AS (
SELECT 
      v0.user_id AS "v0__user_id", 
      v2.user_id AS "v2__user_id", 
      v2.full_name AS "p2_v2_name", 
      __cg_element AS "v3"
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
ARRAY JOIN [1, 2] AS __cg_element
)
SELECT 
      w2.p2_v2_name AS "b.name", 
      w2.v3 AS "n"
FROM with_w2 AS w2

-- column b.name: Value [("b.name", "b.name")]
-- column n: Value [("n", "n")]