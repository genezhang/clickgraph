WITH with_lst_cte_0 AS (SELECT 
      array(1, 2, 3, 4) AS `lst`
)
SELECT 
      filter(lst.lst, x -> x > 2) AS `c`
FROM with_lst_cte_0 AS lst
