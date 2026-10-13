WITH __cg_nodes_Airport__cg_rows AS (
SELECT 
      e.Origin AS `code`, 
      e.OriginCityName AS `city__cg0`, 
      NULL AS `city__cg1`, 
      e.OriginState AS `state__cg0`, 
      NULL AS `state__cg1`
FROM default.flights AS e

UNION ALL
SELECT 
      e.Dest AS `code`, 
      NULL AS `city__cg0`, 
      e.DestCityName AS `city__cg1`, 
      NULL AS `state__cg0`, 
      e.DestState AS `state__cg1`
FROM default.flights AS e
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
      v0.state AS `n.state`
FROM __cg_nodes_Airport AS v0
WHERE v0.state IS NOT NULL
LIMIT 10
-- column n.state: Value [("n.state", "n.state")]