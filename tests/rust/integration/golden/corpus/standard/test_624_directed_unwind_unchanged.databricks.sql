WITH with_w1 AS (
SELECT 
      v0.user_id AS `v0__user_id`, 
      v2.user_id AS `v2__user_id`, 
      v2.full_name AS `p2_v2_name`, 
      __cg_element AS `v3`
FROM test_integration.users_test AS v0
JOIN test_integration.user_follows_test AS v1 ON v1.follower_id = v0.user_id
JOIN test_integration.users_test AS v2 ON v1.followed_id = v2.user_id
LATERAL VIEW explode(array(1, 2)) AS __cg_element
)
SELECT 
      w1.p2_v2_name AS `b.name`, 
      w1.v3 AS `n`
FROM with_w1 AS w1
