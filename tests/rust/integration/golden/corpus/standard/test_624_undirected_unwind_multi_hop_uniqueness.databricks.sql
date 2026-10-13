WITH v1_both AS (
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
v3_both AS (
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.follower_id AS `__cg_start_0`, 
      e.followed_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e

UNION ALL
SELECT 
      e.follow_date AS `follow_date`, 
      e.follow_id AS `follow_id`, 
      e.followed_id AS `followed_id`, 
      e.follower_id AS `follower_id`, 
      e.followed_id AS `__cg_start_0`, 
      e.follower_id AS `__cg_end_0`
FROM test_integration.user_follows_test AS e
WHERE e.follower_id <> e.followed_id
), 
with_w3 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v4.user_id AS `v4__user_id`, 
      v4.full_name AS `p2_v4_name`, 
      __cg_element AS `v5`
FROM test_integration.users_test AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.__cg_end_0 = v2.user_id
JOIN v3_both AS v3 ON v3.__cg_start_0 = v2.user_id
JOIN test_integration.users_test AS v4 ON v3.__cg_end_0 = v4.user_id
LATERAL VIEW explode(array(1, 2)) AS __cg_element
WHERE v1.follow_id <> v3.follow_id
)
SELECT 
      w3.p2_v4_name AS `c.name`, 
      w3.v5 AS `n`
FROM with_w3 AS w3

-- column c.name: Value [("c.name", "c.name")]
-- column n: Value [("n", "n")]