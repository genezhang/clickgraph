SELECT 
      f.dest_state AS `dest.state`, 
      count(*) AS `flights`
FROM db_denormalized.flights_denorm AS f
GROUP BY f.dest_state
