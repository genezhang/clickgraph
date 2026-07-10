WITH with_nameCount_cte_0 AS (SELECT 
      count(u.full_name) AS `nameCount`
FROM test_integration.users_test AS u
)
SELECT 
      nameCount.nameCount AS `nameCount`
FROM with_nameCount_cte_0 AS nameCount
