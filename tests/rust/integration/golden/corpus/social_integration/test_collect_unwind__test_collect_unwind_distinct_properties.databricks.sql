WITH with_u_cte_1 AS (SELECT 
      u.city AS `p1_u_city`
FROM test_integration.users_test AS u
)
SELECT DISTINCT 
      u.p1_u_city AS `user.city`
FROM with_u_cte_1 AS u
ORDER BY `user.city` ASC
