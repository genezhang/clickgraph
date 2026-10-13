WITH __cg_nodes_Airport__cg_rows AS (
SELECT 
      e.Origin AS "code", 
      e.OriginCityName AS "city__cg0", 
      NULL AS "city__cg1", 
      e.OriginState AS "state__cg0", 
      NULL AS "state__cg1"
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS "code", 
      NULL AS "city__cg0", 
      e.DestCityName AS "city__cg1", 
      NULL AS "state__cg0", 
      e.DestState AS "state__cg1"
FROM test_integration.flights AS e
), 
__cg_nodes_Airport AS (
SELECT 
      e.code AS "code", 
      anyIfOrNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.city__cg0, e.city__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.city__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.city__cg0, e.city__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.city__cg0, e.city__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) AS "city", 
      anyIfOrNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')), NOT isNull(CAST(coalesce(e.state__cg0, e.state__cg1), if(replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg0), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') = replaceRegexpOne(replaceRegexpOne(replaceRegexpAll(toTypeName(e.state__cg1), 'Nullable\\(|LowCardinality\\(|\\)', ''), '^U?Int\\d+$', 'Int'), '^(String|FixedString\\(\\d+)$', 'String') AND toTypeName(coalesce(e.state__cg0, e.state__cg1)) NOT LIKE 'Variant(%', toTypeName(coalesce(e.state__cg0, e.state__cg1)), 'ClickGraph_property_has_different_types_on_different_labels_or_types')))) AS "state"
FROM __cg_nodes_Airport__cg_rows AS e
WHERE NOT isNull(e.code)
GROUP BY e.code
), 
v1_both AS (
SELECT 
      e.Dest AS "Dest", 
      e.Origin AS "Origin", 
      e.airline AS "airline", 
      e.arr_time AS "arr_time", 
      e.dep_time AS "dep_time", 
      e.distance_miles AS "distance_miles", 
      e.flight_id AS "flight_id", 
      e.flight_number AS "flight_number", 
      e.Origin AS "__cg_start_0", 
      e.Dest AS "__cg_end_0"
FROM test_integration.flights AS e

UNION ALL
SELECT 
      e.Dest AS "Dest", 
      e.Origin AS "Origin", 
      e.airline AS "airline", 
      e.arr_time AS "arr_time", 
      e.dep_time AS "dep_time", 
      e.distance_miles AS "distance_miles", 
      e.flight_id AS "flight_id", 
      e.flight_number AS "flight_number", 
      e.Dest AS "__cg_start_0", 
      e.Origin AS "__cg_end_0"
FROM test_integration.flights AS e
WHERE e.Origin <> e.Dest
), 
optional_o3 AS (
SELECT 
      v0.code AS "v0__code", 
      v1.flight_id AS "v1__flight_id", 
      v1.flight_number AS "v1__flight_number", 
      v1.Origin AS "v1__Origin", 
      v1.Dest AS "v1__Dest", 
      v2.code AS "v2__code"
FROM __cg_nodes_Airport AS v0
JOIN v1_both AS v1 ON v1.__cg_start_0 = v0.code
JOIN __cg_nodes_Airport AS v2 ON v1.__cg_end_0 = v2.code
)
SELECT 
      v0.code AS "a.code", 
      count(o3.v1__flight_id) AS "c"
FROM __cg_nodes_Airport AS v0
LEFT JOIN optional_o3 AS o3 ON v0.code = o3.v0__code
GROUP BY v0.code
ORDER BY v0.code ASC

-- column a.code: Value [("a.code", "a.code")]
-- column c: Value [("c", "c")]