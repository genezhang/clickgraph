SELECT count(`a.code`) AS "c" FROM (
SELECT 
      a.origin_code AS "a.code"
FROM db_denormalized.flights_denorm AS a
UNION DISTINCT 
SELECT 
      a.dest_code AS "a.code"
FROM db_denormalized.flights_denorm AS a
) AS __union
UNION ALL 
SELECT 
      count(f.flight_id) AS "c"
FROM db_denormalized.flights_denorm AS f
