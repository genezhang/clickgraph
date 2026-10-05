WITH with_c_cte_0 AS (SELECT 
      t0.Dest AS "p1_c_code"
FROM test_integration.flights AS t1
INNER JOIN test_integration.flights AS t0 ON t0.Origin = t1.Dest
WHERE (t0.flight_id <> t1.flight_id OR t0.flight_number <> t1.flight_number)
)
SELECT 
      c.p1_c_code AS "c.code"
FROM with_c_cte_0 AS c
