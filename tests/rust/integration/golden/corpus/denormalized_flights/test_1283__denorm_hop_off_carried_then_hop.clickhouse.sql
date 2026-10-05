WITH with_c_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code"
FROM test_integration.flights AS t0
), 
with_a_cte_1 AS (SELECT 
      t1.Dest AS "p1_a_code"
FROM with_c_cte_0 AS c
INNER JOIN test_integration.flights AS t1 ON t1.Origin = c.p1_c_code
)
SELECT 
      a.p1_a_code AS "x", 
      t2.Dest AS "y"
FROM test_integration.flights AS t2
INNER JOIN with_a_cte_1 AS a ON t2.Origin = a.p1_a_code
