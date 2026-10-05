WITH with_c_cte_0 AS (SELECT 
      t0.Dest AS `p1_c_code`
FROM test_integration.flights AS t0
)
SELECT 
      c.p1_c_code AS `c.code`, 
      t1.Dest AS `a.code`
FROM test_integration.flights AS t2
INNER JOIN with_c_cte_0 AS c ON t2.Origin = c.p1_c_code
INNER JOIN test_integration.flights AS t1 ON t1.Origin = t2.Dest
