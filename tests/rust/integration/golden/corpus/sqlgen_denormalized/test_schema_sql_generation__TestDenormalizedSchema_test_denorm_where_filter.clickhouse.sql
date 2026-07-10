SELECT 
      count(*) AS "flights"
FROM db_denormalized.flights_denorm AS t0
WHERE (t0.origin_city = 'Seattle' AND t0.dest_state = 'CA')
