WITH with_c_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code"
FROM test_integration.flights AS t0
)
SELECT 
      c.p1_c_code AS "c.code", 
      t1.Dest AS "n1.code", 
      t1.Origin AS "n2.code"
FROM test_integration.flights AS t2
INNER JOIN with_c_cte_0 AS c ON c.p1_c_code = t2.Origin
INNER JOIN test_integration.flights AS t1 ON t1.Dest = t2.Dest
WHERE (t1.flight_id <> t2.flight_id OR t1.flight_number <> t2.flight_number)
