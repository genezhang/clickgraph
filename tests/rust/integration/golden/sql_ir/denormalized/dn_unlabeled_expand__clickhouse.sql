SELECT `a.city` AS `a.city`, `a.code` AS `a.code`, `a.state` AS `a.state`, `r.from_id` AS `r.from_id`, `r.to_id` AS `r.to_id`, `r.arrival_time` AS `r.arrival_time`, `r.carrier` AS `r.carrier`, `r.departure_time` AS `r.departure_time`, `r.distance` AS `r.distance`, `r.flight_id` AS `r.flight_id`, `r.flight_num` AS `r.flight_num`, `b.city` AS `b.city`, `b.code` AS `b.code`, `b.state` AS `b.state` FROM (
SELECT 
      r.origin_city AS "a.city", 
      r.origin_code AS "a.code", 
      r.origin_state AS "a.state", 
      r.origin_code AS "r.from_id", 
      r.dest_code AS "r.to_id", 
      r.arrival_time AS "r.arrival_time", 
      r.carrier AS "r.carrier", 
      r.departure_time AS "r.departure_time", 
      r.distance AS "r.distance", 
      r.flight_id AS "r.flight_id", 
      r.flight_number AS "r.flight_num", 
      r.dest_city AS "b.city", 
      r.dest_code AS "b.code", 
      r.dest_state AS "b.state"
FROM db_denormalized.flights_denorm AS r
UNION ALL 
SELECT 
      r.dest_city AS "a.city", 
      r.dest_code AS "a.code", 
      r.dest_state AS "a.state", 
      r.origin_code AS "r.from_id", 
      r.dest_code AS "r.to_id", 
      r.arrival_time AS "r.arrival_time", 
      r.carrier AS "r.carrier", 
      r.departure_time AS "r.departure_time", 
      r.distance AS "r.distance", 
      r.flight_id AS "r.flight_id", 
      r.flight_number AS "r.flight_num", 
      r.origin_city AS "b.city", 
      r.origin_code AS "b.code", 
      r.origin_state AS "b.state"
FROM db_denormalized.flights_denorm AS r
) AS __union
LIMIT 25