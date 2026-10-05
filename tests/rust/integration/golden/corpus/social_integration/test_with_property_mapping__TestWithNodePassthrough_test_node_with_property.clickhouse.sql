WITH with_u_userName_cte_0 AS (SELECT 
      u.full_name AS "userName"
FROM test_integration.users_test AS u
)
SELECT 
      u_userName.userName AS "userName"
FROM with_u_userName_cte_0 AS u_userName
LIMIT 1