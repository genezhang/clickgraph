SELECT 
      f.origin_city AS `o.city`, 
      f.dest_state AS `d.state`, 
      count(*) AS `flight_count`
FROM db_denormalized.flights_denorm AS f
GROUP BY f.origin_city, f.dest_state
