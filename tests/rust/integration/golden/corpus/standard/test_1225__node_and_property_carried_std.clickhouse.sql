WITH with_n_u_cte_0 AS (SELECT 
      u.full_name AS "n"
FROM test_integration.users_test AS u
)
SELECT 
      n_u.n AS "n"
FROM with_n_u_cte_0 AS n_u
