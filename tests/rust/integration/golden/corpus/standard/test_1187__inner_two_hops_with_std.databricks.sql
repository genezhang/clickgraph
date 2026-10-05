WITH with_c_cte_0 AS (SELECT 
      t0.followed_id AS `p1_c_user_id`
FROM test_integration.users_test AS x
INNER JOIN test_integration.user_follows_test AS t1 ON t1.follower_id = x.user_id
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = t1.followed_id
WHERE t0.follow_id <> t1.follow_id
)
SELECT 
      c.p1_c_user_id AS `c.user_id`
FROM with_c_cte_0 AS c
