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
vlp_a_a AS (
    SELECT
        node_universe.__node_id as start_id,
        node_universe.__node_id as end_id,
        0 as hop_count,
        (
            SELECT arraySlice([tuple(__seed_edge.flight_id, __seed_edge.flight_number)], 1, 0)
            FROM test_integration.flights AS __seed_edge
            LIMIT 1
        ) as path_edges,
        [node_universe.__node_id] as path_nodes,
        CAST([] AS Array(String)) as path_relationships,
        node_universe.__prop_0 as start_Origin
    FROM (
            SELECT DISTINCT Origin AS __node_id, Origin AS __prop_0
            FROM test_integration.flights
            UNION DISTINCT
            SELECT DISTINCT Dest AS __node_id, Dest AS __prop_0
            FROM test_integration.flights
        ) AS node_universe
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [tuple(next.flight_id, next.flight_number)]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        vp."start_Origin" as "start_Origin"
    FROM vlp_a_a vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 10 AND NOT has(vp.path_edges, tuple(next.flight_id, next.flight_number))
)
SELECT 
      a.Origin AS "a.code", 
      count(*) AS "count(*)"
FROM __denorm_scan_a AS a
LEFT JOIN vlp_a_a AS vt0 ON a.Origin = vt0.start_id AND a.Origin = vt0.end_id
GROUP BY a.Origin
