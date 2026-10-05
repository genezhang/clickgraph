WITH with_ag_cte_0 AS (SELECT 
      a.age AS "ag"
FROM test_integration.users_test AS a
), 
with_ag_c_cte_1 AS (SELECT 
      anyLast(ag.ag) AS "ag", 
      count(*) AS "c"
FROM with_ag_cte_0 AS ag
GROUP BY ag.ag
)
SELECT 
      ag_c.ag AS "ag", 
      ag_c.c AS "c"
FROM with_ag_c_cte_1 AS ag_c
ORDER BY ag_c.ag ASC
