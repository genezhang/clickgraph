SELECT `a.code` AS `a.code`, count(`r.flight_id`) AS `count(r)` FROM (
SELECT 
      r.origin_code AS `a.code`,
      r.flight_id AS `r.flight_id`,
      r.origin_code AS `r.origin_code`
FROM db_denormalized.flights_denorm AS r
UNION ALL 
SELECT 
      r.dest_code AS `a.code`,
      r.flight_id AS `r.flight_id`,
      r.origin_code AS `r.origin_code`
FROM db_denormalized.flights_denorm AS r
) AS __union
GROUP BY `a.code`
