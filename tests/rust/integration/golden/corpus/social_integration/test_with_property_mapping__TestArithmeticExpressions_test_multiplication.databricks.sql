WITH with_doubled_cte_0 AS (SELECT 
      u.user_id * 2 AS `doubled`
FROM test_integration.users_test AS u
)
SELECT 
      doubled.doubled AS `doubled`
FROM with_doubled_cte_0 AS doubled
LIMIT 1