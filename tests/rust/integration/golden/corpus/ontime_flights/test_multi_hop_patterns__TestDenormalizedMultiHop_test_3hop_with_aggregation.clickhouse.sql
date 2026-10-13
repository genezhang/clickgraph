WITH with_w1 AS (
SELECT 
      v1.Dest AS "v7", 
      count(*) AS "v8"
FROM default.flights AS v1
JOIN default.flights AS v3 ON v3.Origin = v1.Dest
JOIN default.flights AS v5 ON v5.Origin = v3.Dest
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL AND v3.Dest IS NOT NULL AND v5.Dest IS NOT NULL AND (v1.flight_id <> v3.flight_id OR v1.flight_number <> v3.flight_number) AND (v1.flight_id <> v5.flight_id OR v1.flight_number <> v5.flight_number) AND (v3.flight_id <> v5.flight_id OR v3.flight_number <> v5.flight_number))
GROUP BY v1.Dest
)
SELECT 
      w1.v7 AS "hub", 
      w1.v8 AS "connections"
FROM with_w1 AS w1
ORDER BY w1.v8 DESC NULLS FIRST
LIMIT 5