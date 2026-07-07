SELECT 
      a.origin_code AS "a.code"
FROM db_denormalized.flights_denorm AS a
WHERE a.origin_state = 'CA'
UNION DISTINCT 
SELECT 
      a.dest_code AS "a.code"
FROM db_denormalized.flights_denorm AS a
WHERE a.dest_state = 'CA'
