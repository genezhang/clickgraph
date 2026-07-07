SELECT 
      r.origin_code AS `r.from_id`, 
      r.dest_code AS `r.to_id`, 
      r.arrival_time AS `r.arrival_time`, 
      r.carrier AS `r.carrier`, 
      r.departure_time AS `r.departure_time`, 
      r.distance AS `r.distance`, 
      r.flight_id AS `r.flight_id`, 
      r.flight_number AS `r.flight_num`
FROM db_denormalized.flights_denorm AS r
