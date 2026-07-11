WITH with_u_cte_1 AS (SELECT 
      u.city AS "p1_u_city", 
      u.full_name AS "p1_u_name"
FROM test_integration.users_test AS u
)
SELECT 
      u.p1_u_name AS "user.name", 
      u.p1_u_city AS "user.city"
FROM with_u_cte_1 AS u
ORDER BY u.p1_u_city ASC
LIMIT 3