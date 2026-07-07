SELECT 
      count(*) AS `flights`
FROM db_denormalized.flights_denorm AS f
WHERE (f.origin_city = 'Seattle' AND f.dest_state = 'CA')
