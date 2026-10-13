WITH with_w1 AS (
SELECT 
      v3.Dest AS `v5__code`, 
      v3.Dest AS `p2_v5_code`
FROM test_integration.flights AS v1
JOIN test_integration.flights AS v3 ON v3.Origin = v1.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Dest IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number))
)
SELECT 
      w1.p2_v5_code AS `c.code`
FROM with_w1 AS w1
