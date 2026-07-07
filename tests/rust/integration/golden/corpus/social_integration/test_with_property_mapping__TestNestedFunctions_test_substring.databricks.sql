WITH with_prefix_cte_0 AS (SELECT 
      substring(u.full_name, (1) + 1, 5) AS `prefix`
FROM test_integration.users_test AS u
)
SELECT 
      prefix.prefix AS `prefix`
FROM with_prefix_cte_0 AS prefix
LIMIT 1