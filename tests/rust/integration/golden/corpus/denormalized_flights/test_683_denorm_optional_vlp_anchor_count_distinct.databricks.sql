WITH RECURSIVE __denorm_scan_a AS (
SELECT 
      "Origin" AS "Origin",
      min("OriginCityName") AS "OriginCityName",
      min("OriginState") AS "OriginState"
FROM (
SELECT 
      s.Origin AS "Origin",
      s.OriginCityName AS "OriginCityName",
      s.OriginState AS "OriginState"
FROM test_integration.flights AS s
UNION DISTINCT 
SELECT 
      s.Dest AS "Origin",
      s.DestCityName AS "OriginCityName",
      s.DestState AS "OriginState"
FROM test_integration.flights AS s
)
GROUP BY "Origin"
), 
vlp_a_b_inner AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships
    FROM test_integration.flights AS t0
    WHERE 1 <= 3
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships
    FROM vlp_a_b_inner vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT array_contains(vp.path_nodes, next.Dest)
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      count(DISTINCT a.Origin) AS `c`
FROM __denorm_scan_a AS a
LEFT JOIN vlp_a_b AS vt0 ON a.Origin = vt0.start_id
