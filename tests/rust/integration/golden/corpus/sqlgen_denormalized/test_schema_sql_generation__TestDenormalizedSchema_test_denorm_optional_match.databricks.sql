WITH __cg_nodes_Airport__cg_rows AS (
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
), 
optional_o3 AS (
SELECT 
      v1.origin_code AS `v0__code`, 
      v1.flight_id AS `v1__flight_id`, 
      v1.flight_number AS `v1__flight_number`, 
      v1.origin_code AS `v1__origin_code`, 
      v1.dest_code AS `v1__dest_code`, 
      v1.dest_code AS `v2__code`
FROM db_denormalized.flights_denorm AS v1
WHERE v1.dest_code IS NOT NULL
)
SELECT 
      v0.code AS `a.code`, 
      count(o3.v1__flight_id) AS `outgoing`
FROM __cg_nodes_Airport AS v0
LEFT JOIN optional_o3 AS o3 ON v0.code = o3.v0__code
GROUP BY v0.code

-- column a.code: Value [("a.code", "a.code")]
-- column outgoing: Value [("outgoing", "outgoing")]