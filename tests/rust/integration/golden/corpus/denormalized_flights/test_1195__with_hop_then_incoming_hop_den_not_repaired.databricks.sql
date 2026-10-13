WITH with_w1 AS (
SELECT 
      v1.Dest AS `v3__code`, 
      v1.Dest AS `p2_v3_code`
FROM test_integration.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
)
SELECT 
      w1.p2_v3_code AS `c.code`, 
      v4.Dest AS `n1.code`, 
      v6.Origin AS `n2.code`
FROM with_w1 AS w1
JOIN test_integration.flights AS v4 ON v4.Origin = w1.v3__code
JOIN test_integration.flights AS v6 ON v6.Dest = v4.Dest
WHERE (v4.Dest IS NOT NULL AND v6.Origin IS NOT NULL AND (v4.flight_id <> v6.flight_id OR v4.flight_number <> v6.flight_number))
