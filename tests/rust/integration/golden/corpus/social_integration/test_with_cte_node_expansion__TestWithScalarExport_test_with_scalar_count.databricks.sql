WITH with_user_count_cte_0 AS (SELECT 
      count(a.user_id) AS `user_count`
FROM test_integration.users_test AS a
)
SELECT 
      user_count.user_count AS `user_count`
FROM with_user_count_cte_0 AS user_count
