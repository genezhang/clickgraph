WITH with_c_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code"
FROM test_integration.flights AS t0
), 
with_a_c_cte_1 AS (SELECT 
      c.p1_c_code AS "p1_c_code", 
      t1.Dest AS "p1_a_code"
FROM test_integration.flights AS t1
INNER JOIN with_c_cte_0 AS c ON t1.Origin = c.p1_c_code
)
SELECT 
      a_c.p1_c_code AS "x", 
      a_c.p1_a_code AS "y"
FROM with_a_c_cte_1 AS a_c
