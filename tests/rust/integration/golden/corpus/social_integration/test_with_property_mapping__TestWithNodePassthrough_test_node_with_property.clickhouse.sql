WITH with_u_userName_cte_0 AS (SELECT 
      u.full_name AS "p1_u_name", 
      u.user_id AS "p1_u_user_id"
FROM test_integration.users_test AS u
)
SELECT 
      u_userName.p1_u_name AS "userName.full_name", 
      u_userName.p1_u_name AS "userName.name", 
      u_userName.p1_u_user_id AS "userName.user_id"
FROM with_u_userName_cte_0 AS u_userName
LIMIT 1