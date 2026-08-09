WITH with_a_friends_cte_0 AS (SELECT 
      a.user_id AS `p1_a_user_id`, 
      collect_list(t0.followed_id) AS `friends`
FROM test_integration.users_test AS a
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = a.user_id
GROUP BY a.user_id
), 
with_a_c_cte_1 AS (SELECT 
      a_friends.p1_a_user_id AS `p1_a_user_id`, 
      (SELECT count(*) FROM (SELECT explode(a_friends.`friends`) AS x) WHERE x IN (SELECT __r0.follower_id FROM test_integration.user_follows_test AS __r0 INNER JOIN test_integration.user_follows_test AS __r1 ON __r0.followed_id = __r1.follower_id)) AS `c`
FROM with_a_friends_cte_0 AS a_friends
)
SELECT 
      a_c.p1_a_user_id AS `id`, 
      a_c.c AS `c`
FROM with_a_c_cte_1 AS a_c
ORDER BY id ASC NULLS LAST
