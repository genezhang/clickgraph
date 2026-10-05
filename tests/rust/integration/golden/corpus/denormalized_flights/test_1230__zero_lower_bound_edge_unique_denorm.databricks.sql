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
        node_universe.__prop_1 as start_Origin,
        node_universe.__prop_2 as start_OriginState,
        node_universe.__prop_3 as end_DestCityName,
        node_universe.__prop_4 as end_Dest,
        node_universe.__prop_5 as end_DestState
    FROM (
            SELECT DISTINCT Origin AS __node_id, OriginCityName AS __prop_0, Origin AS __prop_1, OriginState AS __prop_2, OriginCityName AS __prop_3, Origin AS __prop_4, OriginState AS __prop_5
            FROM test_integration.flights
            UNION DISTINCT
            SELECT DISTINCT Dest AS __node_id, DestCityName AS __prop_0, Dest AS __prop_1, DestState AS __prop_2, DestCityName AS __prop_3, Dest AS __prop_4, DestState AS __prop_5
            FROM test_integration.flights
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
        vp.`start_Origin` as `start_Origin`,
        vp.`start_OriginState` as `start_OriginState`,
        next.`DestCityName` as `end_DestCityName`,
        next.`Dest` as `end_Dest`,
        next.`DestState` as `end_DestState`
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 3 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_a_b AS t
