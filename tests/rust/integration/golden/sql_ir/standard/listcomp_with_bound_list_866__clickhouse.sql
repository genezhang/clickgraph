WITH with_lst_cte_0 AS (SELECT 
      [1, 2, 3, 4] AS "lst"
)
SELECT 
      arrayFilter(x -> x > 2, lst.lst) AS "c"
FROM with_lst_cte_0 AS lst
