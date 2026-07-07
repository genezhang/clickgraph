SELECT DISTINCT 
      a.origin_state AS "a.state"
FROM db_denormalized.flights_denorm AS a
UNION DISTINCT 
SELECT DISTINCT 
      a.dest_state AS "a.state"
FROM db_denormalized.flights_denorm AS a
