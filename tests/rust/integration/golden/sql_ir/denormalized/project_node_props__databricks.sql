SELECT 
      a.origin_code AS `a.code`, 
      a.origin_city AS `a.city`, 
      a.origin_state AS `a.state`
FROM db_denormalized.flights_denorm AS a
UNION DISTINCT 
SELECT 
      a.dest_code AS `a.code`, 
      a.dest_city AS `a.city`, 
      a.dest_state AS `a.state`
FROM db_denormalized.flights_denorm AS a
