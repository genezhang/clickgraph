SELECT 
      t0.origin_city AS `a.city`, 
      t0.origin_state AS `a.state`, 
      t0.dest_city AS `b.city`, 
      t0.dest_state AS `b.state`
FROM db_denormalized.flights_denorm AS t0
