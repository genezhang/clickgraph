SELECT 
      count(*) AS "flight_count"
FROM db_denormalized.flights_denorm AS f
WHERE f.dest_state = 'CA'
