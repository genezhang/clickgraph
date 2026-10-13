SELECT 
      v1.origin_city AS "a.city", 
      v1.dest_city AS "b.city"
FROM db_denormalized.flights_denorm AS v1
WHERE (v1.origin_code IS NOT NULL AND v1.dest_code IS NOT NULL)

-- column a.city: Value [("a.city", "a.city")]
-- column b.city: Value [("b.city", "b.city")]