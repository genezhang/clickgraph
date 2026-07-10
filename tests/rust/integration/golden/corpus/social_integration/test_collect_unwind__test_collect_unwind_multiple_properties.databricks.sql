WITH with_u_cte_1 AS (SELECT 
      u.city AS `p1_u_city`
FROM test_integration.users_test AS u
)
SELECT 
      u.full_name AS `user.name`, 
      u.email_address AS `user.email`, 
      u.p1_u_city AS `user.city`
FROM with_u_cte_1 AS u
LIMIT 3