WITH with_n_cte_0 AS (SELECT 
      u.full_name AS `n`
FROM brahmand.users_bench AS u
ORDER BY n ASC
LIMIT 2 OFFSET 1
)
SELECT 
      n.n AS `n`
FROM with_n_cte_0 AS n
