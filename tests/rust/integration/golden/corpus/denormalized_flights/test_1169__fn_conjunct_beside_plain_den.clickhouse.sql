WITH RECURSIVE vlp_a_b AS (
    SELECT
        node_universe.__node_id as start_id,
        node_universe.__node_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_edges,
        [node_universe.__node_id] as path_nodes,
        CAST([] AS Array(String)) as path_relationships,
        node_universe.__prop_2 as end_Dest
    FROM (
            SELECT DISTINCT Origin AS __node_id, OriginCityName AS __prop_0, OriginState AS __prop_1, Origin AS __prop_2
            FROM test_integration.flights AS t0 WHERE (t0.OriginCityName = 'Atlanta' AND lowerUTF8(t0.OriginState) = 'xx')
            UNION DISTINCT
            SELECT DISTINCT Dest AS __node_id, DestCityName AS __prop_0, DestState AS __prop_1, Dest AS __prop_2
            FROM test_integration.flights AS t0 WHERE (t0.DestCityName = 'Atlanta' AND lowerUTF8(t0.DestState) = 'xx')
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
