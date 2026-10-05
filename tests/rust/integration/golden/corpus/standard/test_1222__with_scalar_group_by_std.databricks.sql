WITH with_ag_cte_0 AS (SELECT 
      a.age AS `ag`
FROM test_integration.users_test AS a
)
SELECT 
      ag.ag AS `ag`, 
      count(*) AS `n`
FROM with_ag_cte_0 AS ag
GROUP BY ag.ag
