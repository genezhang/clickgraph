SELECT 
      t0.origin_state AS "a.state", 
      t0.dest_state AS "b.state", 
      count(*) AS "n"
FROM db_denormalized.flights_denorm AS t0
GROUP BY t0.origin_state, t0.dest_state
