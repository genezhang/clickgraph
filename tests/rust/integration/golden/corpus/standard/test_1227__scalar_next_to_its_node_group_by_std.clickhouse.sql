WITH with_a_ag_cte_0 AS (SELECT 
      a.age AS "ag"
FROM test_integration.users_test AS a
)
SELECT 
      anyLast(a_ag.ag) AS "ag", 
      count(*) AS "n"
FROM with_a_ag_cte_0 AS a_ag
GROUP BY a_ag.ag
