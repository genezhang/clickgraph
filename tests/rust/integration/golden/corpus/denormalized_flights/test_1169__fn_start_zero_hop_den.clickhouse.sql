WITH RECURSIVE vlp_a_b AS (
    SELECT
        node_universe.__node_id as start_id,
        node_universe.__node_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_edges,
        [node_universe.__node_id] as path_nodes,
        CAST([] AS Array(String)) as path_relationships,
        node_universe.__prop_1 as end_Dest
    FROM (
            SELECT DISTINCT Origin AS __node_id, OriginCityName AS __prop_0, Origin AS __prop_1
            FROM test_integration.flights AS t0 WHERE lowerUTF8(t0.OriginCityName) = 'atlanta'
            UNION DISTINCT
            SELECT DISTINCT Dest AS __node_id, DestCityName AS __prop_0, Dest AS __prop_1
            FROM test_integration.flights AS t0 WHERE lowerUTF8(t0.DestCityName) = 'atlanta'
        ) AS node_universe
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        [] as path_relationships,
        next."Dest" as "end_Dest"
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.Dest)
)
SELECT 
      t.end_Dest AS "b.code"
FROM vlp_a_b AS t
