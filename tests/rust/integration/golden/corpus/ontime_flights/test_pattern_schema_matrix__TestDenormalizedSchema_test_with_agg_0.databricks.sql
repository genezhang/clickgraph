WITH with_w1 AS (
SELECT 
      v1.OriginCityName AS `v3`, 
      count(v1.flight_id) AS `v4`
FROM default.flights AS v1
WHERE (v1.Origin IS NOT NULL AND v1.Dest IS NOT NULL)
GROUP BY v1.OriginCityName
)
SELECT 
      w1.v3 AS `prop`, 
      w1.v4 AS `cnt`
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST
LIMIT 10