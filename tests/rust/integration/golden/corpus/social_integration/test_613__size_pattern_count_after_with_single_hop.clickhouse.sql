WITH with_a_cte_0 AS (SELECT 
      a.user_id AS "p1_a_user_id"
FROM test_integration.users_test AS a
)
SELECT 
      a.p1_a_user_id AS "a.user_id", 
      coalesce((SELECT COUNT(*) FROM test_integration.user_follows_test WHERE user_follows_test.follower_id = a.p1_a_user_id), 0) AS "c"
FROM with_a_cte_0 AS a
