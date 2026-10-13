WITH v1_both AS (
SELECT 
      e.arrival_time AS `arrival_time`, 
      e.carrier AS `carrier`, 
      e.departure_time AS `departure_time`, 
      e.dest_code AS `dest_code`, 
      e.distance AS `distance`, 
      e.flight_id AS `flight_id`, 
      e.flight_number AS `flight_number`, 
      e.origin_code AS `origin_code`, 
      e.origin_code AS `__cg_start_0`, 
      e.dest_code AS `__cg_end_0`
FROM db_denormalized.flights_denorm AS e

UNION ALL
SELECT 
      e.arrival_time AS `arrival_time`, 
      e.carrier AS `carrier`, 
      e.departure_time AS `departure_time`, 
      e.dest_code AS `dest_code`, 
      e.distance AS `distance`, 
      e.flight_id AS `flight_id`, 
      e.flight_number AS `flight_number`, 
      e.origin_code AS `origin_code`, 
      e.dest_code AS `__cg_start_0`, 
      e.origin_code AS `__cg_end_0`
FROM db_denormalized.flights_denorm AS e
WHERE e.origin_code <> e.dest_code
), 
__cg_nodes_Airport__cg_rows AS (
SELECT 
      e.origin_code AS `code`, 
      e.origin_city AS `city__cg0`, 
      NULL AS `city__cg1`, 
      e.origin_state AS `state__cg0`, 
      NULL AS `state__cg1`
FROM db_denormalized.flights_denorm AS e

UNION ALL
SELECT 
      e.dest_code AS `code`, 
      NULL AS `city__cg0`, 
      e.dest_city AS `city__cg1`, 
      NULL AS `state__cg0`, 
      e.dest_state AS `state__cg1`
FROM db_denormalized.flights_denorm AS e
), 
__cg_nodes_Airport AS (
SELECT 
      e.code AS `code`, 
      any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.city__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.city__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.city__cg0, e.city__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) AS `city`, 
      any_value(CASE WHEN regexp_replace(regexp_replace(typeof(e.state__cg0), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') = regexp_replace(regexp_replace(typeof(e.state__cg1), '^(tinyint|smallint|int|bigint)$', 'int'), '^((var)?char\\(\\d+\\)|string( collate .*)?)$', 'string') THEN coalesce(e.state__cg0, e.state__cg1) ELSE raise_error('ClickGraph: a property has different types on different labels or types') END, true) AS `state`
FROM __cg_nodes_Airport__cg_rows AS e
WHERE NOT isnull(e.code)
GROUP BY e.code
)
SELECT 
      v2.city AS `b.city`, 
      count(*) AS `flights`
FROM __cg_nodes_Airport AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.code
JOIN __cg_nodes_Airport AS v2 ON v1.__cg_end_0 = v2.code
WHERE v0.city = 'Seattle'
GROUP BY v2.city

-- column b.city: Value [("b.city", "b.city")]
-- column flights: Value [("flights", "flights")]