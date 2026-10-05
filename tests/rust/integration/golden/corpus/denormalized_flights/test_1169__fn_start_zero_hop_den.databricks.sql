WITH RECURSIVE vlp_a_b AS (
    SELECT
        node_universe.__node_id as start_id,
        node_universe.__node_id as end_id,
        0 as hop_count,
        (
            SELECT slice(array(struct(__seed_edge.flight_id, __seed_edge.flight_number)), 1, 0)
            FROM test_integration.flights AS __seed_edge
            LIMIT 1
        ) as path_edges,
        array(node_universe.__node_id) as path_nodes,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        node_universe.__prop_0 as start_OriginCityName,
        node_universe.__prop_1 as end_Dest
    FROM (
            SELECT DISTINCT Origin AS __node_id, OriginCityName AS __prop_0, Origin AS __prop_1
            FROM test_integration.flights AS t0 WHERE lower(t0.OriginCityName) = 'atlanta'
            UNION DISTINCT
            SELECT DISTINCT Dest AS __node_id, DestCityName AS __prop_0, Dest AS __prop_1
            FROM test_integration.flights AS t0 WHERE lower(t0.DestCityName) = 'atlanta'
        ) AS node_universe
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        vp.`start_OriginCityName` as `start_OriginCityName`,
        next.`Dest` as `end_Dest`
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      t.end_Dest AS `b.code`
FROM vlp_a_b AS t
