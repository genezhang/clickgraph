WITH with_c_cte_0 AS (SELECT 
      t0.followed_id AS "p1_c_user_id"
FROM test_integration.users_test AS z
INNER JOIN test_integration.user_follows_test AS t0 ON t0.follower_id = z.user_id
)
SELECT 
      c.p1_c_user_id AS "c.user_id", 
      t1.follower_id AS "n2.user_id"
FROM with_c_cte_0 AS c
INNER JOIN test_integration.user_follows_test AS t2 ON t2.followed_id = t2.followed_id
LEFT JOIN test_integration.user_follows_test AS t1 ON t1.followed_id = t2.followed_id
