WITH with_u_cte_1 AS (SELECT 
      u.country AS `p1_u_country`
FROM test_integration.users_test AS u
)
SELECT 
      u.p1_u_country AS `user.country`, 
      count(*) AS `user_count`
FROM with_u_cte_1 AS u
GROUP BY u.p1_u_country
ORDER BY user_count DESC
LIMIT 3