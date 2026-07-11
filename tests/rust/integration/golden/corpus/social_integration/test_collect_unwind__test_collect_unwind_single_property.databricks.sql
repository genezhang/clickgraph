WITH with_u_cte_1 AS (SELECT 
      u.full_name AS `p1_u_name`
FROM test_integration.users_test AS u
)
SELECT 
      u.p1_u_name AS `user.name`
FROM with_u_cte_1 AS u
LIMIT 3