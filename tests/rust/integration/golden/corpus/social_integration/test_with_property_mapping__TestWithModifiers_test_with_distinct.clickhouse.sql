WITH with_userName_cte_0 AS (SELECT DISTINCT 
      u.full_name AS "userName"
FROM test_integration.users_test AS u
)
SELECT 
      userName.userName AS "userName"
FROM with_userName_cte_0 AS userName
LIMIT 5