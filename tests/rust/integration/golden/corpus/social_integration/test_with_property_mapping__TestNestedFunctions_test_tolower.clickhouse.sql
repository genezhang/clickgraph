WITH with_lowerName_cte_0 AS (SELECT 
      lower(u.full_name) AS "lowerName"
FROM test_integration.users_test AS u
)
SELECT 
      lowerName.lowerName AS "lowerName"
FROM with_lowerName_cte_0 AS lowerName
LIMIT 1