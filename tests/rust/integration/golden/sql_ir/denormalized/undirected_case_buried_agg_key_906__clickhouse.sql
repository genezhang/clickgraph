SELECT CASE WHEN count(`r.flight_id`) > 5 THEN `r.origin_code` ELSE 'x' END AS "m" FROM (
SELECT 
      r.flight_id AS "r.flight_id",
      r.origin_code AS "r.origin_code"
FROM db_denormalized.flights_denorm AS r
UNION ALL 
SELECT 
      r.flight_id AS "r.flight_id",
      r.dest_code AS "r.origin_code"
FROM db_denormalized.flights_denorm AS r
) AS __union
GROUP BY `r.origin_code`
