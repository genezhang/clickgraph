SELECT 
      f.flight_number AS `f.flight_num`, 
      f.carrier AS `f.carrier`
FROM db_denormalized.flights_denorm AS f
