WITH with_u_cte_1 AS (SELECT *
FROM test_integration.users_test AS u
)
SELECT 
      u.full_name AS `user.name`
FROM with_u_cte_1 AS u
LIMIT 3