WITH with_w1 AS (
SELECT 
      v1.origin_city AS "v3", 
      count(*) AS "v4"
FROM db_denormalized.flights_denorm AS v1
WHERE (v1.origin_code IS NOT NULL AND v1.dest_code IS NOT NULL)
GROUP BY v1.origin_city
)
SELECT 
      w1.v3 AS "origin", 
      w1.v4 AS "flights"
FROM with_w1 AS w1
ORDER BY w1.v4 DESC NULLS FIRST

-- column origin: Value [("origin", "origin")]
-- column flights: Value [("flights", "flights")]