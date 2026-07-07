SELECT 
      f.terminal AS "o.terminal", 
      f.gate AS "d.gate"
FROM db_denormalized.flights_denorm AS f
