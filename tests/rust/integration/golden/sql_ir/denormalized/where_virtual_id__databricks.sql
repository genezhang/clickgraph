SELECT 
      a.origin_city AS `a.city`
FROM db_denormalized.flights_denorm AS a
WHERE a.origin_code = 'LAX'
UNION DISTINCT 
SELECT 
      a.dest_city AS `a.city`
FROM db_denormalized.flights_denorm AS a
WHERE a.dest_code = 'LAX'
