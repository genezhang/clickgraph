WITH with_offsetId_cte_0 AS (SELECT 
      u.user_id + 100 AS "offsetId"
FROM test_integration.users_test AS u
)
SELECT 
      offsetId.offsetId AS "offsetId"
FROM with_offsetId_cte_0 AS offsetId
LIMIT 1