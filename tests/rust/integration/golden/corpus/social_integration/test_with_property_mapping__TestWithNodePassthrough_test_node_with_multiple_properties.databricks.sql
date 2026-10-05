WITH with_e_n_u_cte_0 AS (SELECT 
      u.full_name AS `n`, 
      u.email_address AS `e`
FROM test_integration.users_test AS u
)
SELECT 
      e_n_u.n AS `n`, 
      e_n_u.e AS `e`
FROM with_e_n_u_cte_0 AS e_n_u
LIMIT 1