SELECT 
      v1.origin_city AS "origin.city", 
      v1.dest_city AS "dest.city"
FROM db_denormalized.flights_denorm AS v1
WHERE (v1.origin_code IS NOT NULL AND v1.dest_code IS NOT NULL)

-- column origin.city: Value [("origin.city", "origin.city")]
-- column dest.city: Value [("dest.city", "dest.city")]