WITH v1_both AS (
SELECT 
      e.Dest AS `Dest`, 
      e.Origin AS `Origin`, 
      e.airline AS `airline`, 
      e.arr_time AS `arr_time`, 
      e.dep_time AS `dep_time`, 
      e.distance_miles AS `distance_miles`, 
      e.flight_id AS `flight_id`, 
      e.flight_number AS `flight_number`, 
      e.Origin AS `__cg_start_0`, 
      e.Dest AS `__cg_end_0`
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS `Dest`, 
      e.Origin AS `Origin`, 
      e.airline AS `airline`, 
      e.arr_time AS `arr_time`, 
      e.dep_time AS `dep_time`, 
      e.distance_miles AS `distance_miles`, 
      e.flight_id AS `flight_id`, 
      e.flight_number AS `flight_number`, 
      e.Dest AS `__cg_start_0`, 
      e.Origin AS `__cg_end_0`
FROM test_integration.flights AS e
WHERE e.Origin <> e.Dest
), 
__cg_nodes_Airport__cg_rows AS (
SELECT 
      e.Origin AS `code`, 
      e.OriginCityName AS `city__cg0`, 
      NULL AS `city__cg1`, 
      e.OriginState AS `state__cg0`, 
      NULL AS `state__cg1`
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS `code`, 
      NULL AS `city__cg0`, 
      e.DestCityName AS `city__cg1`, 
      NULL AS `state__cg0`, 
      e.DestState AS `state__cg1`
FROM test_integration.flights AS e
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
      count(*) AS `flights`
FROM __cg_nodes_Airport AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.code
JOIN __cg_nodes_Airport AS v2 ON v1.__cg_end_0 = v2.code
WHERE v0.city = 'Seattle'

-- column flights: Value [("flights", "flights")]