WITH __cg_nodes_Airport__cg_rows AS (
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
), 
optional_o3 AS (
SELECT 
      v1.Origin AS `v0__code`, 
      v1.flight_id AS `v1__flight_id`, 
      v1.flight_number AS `v1__flight_number`, 
      v1.Origin AS `v1__Origin`, 
      v1.Dest AS `v1__Dest`, 
      v1.Dest AS `v2__code`, 
      v1.Dest AS `p2_v2_code`
FROM test_integration.flights AS v1
WHERE v1.Dest IS NOT NULL
), 
with_w4 AS (
SELECT 
      v0.code AS `v0__code`, 
      v0.code AS `p2_v0_code`, 
      o3.v1__flight_id AS `v1__flight_id`, 
      o3.v1__flight_number AS `v1__flight_number`, 
      o3.v1__Origin AS `v1__Origin`, 
      o3.v1__Dest AS `v1__Dest`, 
      o3.v2__code AS `v2__code`, 
      o3.p2_v2_code AS `p2_v2_code`
FROM __cg_nodes_Airport AS v0
LEFT JOIN optional_o3 AS o3 ON v0.code = o3.v0__code
), 
optional_d5 AS (
SELECT DISTINCT 
      w4.p2_v0_code AS `p2_v0_code`, 
      w4.v0__code AS `v0__code`, 
      w4.v2__code AS `v2__code`
FROM with_w4 AS w4
), 
optional_o6 AS (
SELECT 
      d5.v2__code AS `v2__code`, 
      d5.v0__code AS `v0__code`, 
      v3.flight_id AS `v3__flight_id`, 
      v3.flight_number AS `v3__flight_number`, 
      v3.Origin AS `v3__Origin`, 
      v3.Dest AS `v3__Dest`, 
      v3.Dest AS `v4__code`, 
      v3.Dest AS `p2_v4_code`
FROM optional_d5 AS d5
JOIN test_integration.flights AS v3 ON v3.Origin = d5.v2__code
WHERE (v3.Dest IS NOT NULL AND d5.v2__code IS NOT NULL AND d5.p2_v0_code = 'JFK')
)
SELECT 
      w4.p2_v0_code AS `a.code`, 
      w4.p2_v2_code AS `b.code`, 
      o6.p2_v4_code AS `c.code`
FROM with_w4 AS w4
LEFT JOIN optional_o6 AS o6 ON w4.v2__code = o6.v2__code AND w4.v0__code = o6.v0__code
ORDER BY w4.p2_v0_code ASC NULLS LAST, w4.p2_v2_code ASC NULLS LAST, o6.p2_v4_code ASC NULLS LAST
