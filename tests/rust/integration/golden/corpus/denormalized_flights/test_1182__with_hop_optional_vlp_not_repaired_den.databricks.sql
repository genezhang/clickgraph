WITH RECURSIVE with_c_cte_0 AS (SELECT 
      t0.Dest AS `p1_c_code`
FROM test_integration.flights AS t0
), 
__denorm_scan_a AS (
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
vlp_a_b AS (
    SELECT
        t1.Origin as start_id,
        t1.Dest as end_id,
        1 as hop_count,
        array(struct(t1.flight_id, t1.flight_number)) as path_edges,
        array(t1.Origin, t1.Dest) as path_nodes,
        array() as path_relationships,
        t1.`Dest` as `end_Dest`
    FROM test_integration.flights AS t1
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        next.`Dest` as `end_Dest`
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      c.p1_c_code AS `c.code`, 
      vt0.end_Dest AS `b.code`
FROM __denorm_scan_a AS a
INNER JOIN with_c_cte_0 AS c ON 1 = 1
JOIN test_integration.flights AS t2 ON 1 = 1
LEFT JOIN vlp_a_b AS vt0 ON a.Origin = vt0.start_id
