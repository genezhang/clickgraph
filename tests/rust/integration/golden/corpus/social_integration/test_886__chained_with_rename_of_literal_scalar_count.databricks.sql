WITH with_one_cte_0 AS (SELECT 
      1 AS `one`
), 
with_two_cte_1 AS (SELECT 
      one.one AS `two`
FROM with_one_cte_0 AS one
)
SELECT 
      count(two.two) AS `c`
FROM with_two_cte_1 AS two
