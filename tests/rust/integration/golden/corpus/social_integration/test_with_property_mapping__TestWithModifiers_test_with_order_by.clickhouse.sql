WITH with_userName_cte_0 AS (SELECT 
      u.full_name AS "userName"
FROM test_integration.users_test AS u
ORDER BY userName ASC
)
SELECT 
      userName.userName AS "userName"
FROM with_userName_cte_0 AS userName
LIMIT 5