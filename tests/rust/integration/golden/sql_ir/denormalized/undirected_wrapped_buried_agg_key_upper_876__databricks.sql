SELECT concat(upper(`r.origin_code`), string(count(`r.flight_id`))) AS `m` FROM (
SELECT 
      r.flight_id AS `r.flight_id`,
      r.origin_code AS `r.origin_code`
FROM db_denormalized.flights_denorm AS r
UNION ALL 
SELECT 
      r.flight_id AS `r.flight_id`,
      r.dest_code AS `r.origin_code`
FROM db_denormalized.flights_denorm AS r
) AS __union
GROUP BY upper(r.origin_code)
