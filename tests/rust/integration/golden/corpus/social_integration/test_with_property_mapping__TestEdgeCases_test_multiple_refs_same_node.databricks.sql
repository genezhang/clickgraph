WITH with_lower_n_n1_n2_cte_0 AS (SELECT 
      u.full_name AS `n1`, 
      u.full_name AS `n2`, 
      lower(u.full_name) AS `lower_n`
FROM test_integration.users_test AS u
)
SELECT 
      lower_n_n1_n2.n1 AS `n1`, 
      lower_n_n1_n2.n2 AS `n2`, 
      lower_n_n1_n2.lower_n AS `lower_n`
FROM with_lower_n_n1_n2_cte_0 AS lower_n_n1_n2
LIMIT 1