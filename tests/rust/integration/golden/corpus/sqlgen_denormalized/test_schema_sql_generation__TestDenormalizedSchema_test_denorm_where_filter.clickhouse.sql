SELECT 
      count(*) AS "flights"
FROM db_denormalized.flights_denorm AS v1
WHERE (v1.origin_code IS NOT NULL AND v1.dest_code IS NOT NULL AND (v1.origin_city = 'Seattle' AND v1.dest_state = 'CA'))

-- column flights: Value [("flights", "flights")]