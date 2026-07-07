WITH with_b_cte_0 AS (SELECT 
      a.balance AS "b"
FROM db_composite_id.accounts AS a
ORDER BY b ASC
LIMIT 1, 2
)
SELECT 
      b.b AS "b"
FROM with_b_cte_0 AS b
