SELECT count(`a.code`) AS `count(a)` FROM (
SELECT 
      a.origin_code AS `a.code`
FROM db_denormalized.flights_denorm AS a
UNION DISTINCT 
SELECT 
      a.dest_code AS `a.code`
FROM db_denormalized.flights_denorm AS a
) AS __union
