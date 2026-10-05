WITH with_ag_cte_0 AS (SELECT 
      a.age AS `ag`
FROM brahmand.users_bench AS a
)
SELECT 
      any_value(ag.ag) AS `ag`, 
      count(*) AS `n`
FROM with_ag_cte_0 AS ag
GROUP BY ag.ag
