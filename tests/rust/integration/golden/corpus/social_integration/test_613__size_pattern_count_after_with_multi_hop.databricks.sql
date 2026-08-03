WITH with_a_cte_0 AS (SELECT 
      a.user_id AS `p1_a_user_id`
FROM test_integration.users_test AS a
)
SELECT 
      a.p1_a_user_id AS `a.user_id`, 
      coalesce((SELECT COUNT(*) FROM test_integration.user_follows_test AS r1 INNER JOIN test_integration.user_follows_test AS r2 ON r1.followed_id = r2.follower_id WHERE r1.follower_id = a.p1_a_user_id), 0) AS `c`
FROM with_a_cte_0 AS a
