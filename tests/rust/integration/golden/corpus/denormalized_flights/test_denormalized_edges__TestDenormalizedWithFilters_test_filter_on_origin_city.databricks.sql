SELECT 
      f.flight_number AS `f.flight_num`, 
      f.dest_city AS `dest.city`
FROM db_denormalized.flights_denorm AS f
WHERE f.origin_city = 'Los Angeles'
ORDER BY f.flight_number ASC
