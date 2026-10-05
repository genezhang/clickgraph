WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(struct(t0.flight_id, t0.flight_number)) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array() as path_relationships,
        t0.`Origin` as `start_Origin`,
        t0.`Dest` as `end_Dest`
    FROM test_integration.flights AS t0
    WHERE 1 <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(struct(next.flight_id, next.flight_number))),
        concat(vp.path_nodes, array(next.Dest)),
        array() as path_relationships,
        vp.`start_Origin` as `start_Origin`,
        next.`Dest` as `end_Dest`
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
), 
with_a_b_cte_0 AS (SELECT 
      start_code AS `p1_a_code`, 
      end_code AS `p1_b_code`
FROM vlp_a_b AS t
), 
vlp_d_e AS (
    SELECT
        t1.Origin as start_id,
        t1.Dest as end_id,
        1 as hop_count,
        array(struct(t1.flight_id, t1.flight_number)) as path_edges,
        array(t1.Origin, t1.Dest) as path_nodes,
        array() as path_relationships,
        t1.`Origin` as `start_Origin`,
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
        vp.`start_Origin` as `start_Origin`,
        next.`Dest` as `end_Dest`
    FROM vlp_d_e vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_edges, struct(next.flight_id, next.flight_number))
)
SELECT 
      a_b.p1_a_code AS `a.code`, 
      t.end_Dest AS `e.code`
FROM vlp_d_e AS t
INNER JOIN with_a_b_cte_0 AS a_b ON 1 = 1
INNER JOIN test_integration.flights AS t2 ON t2.Dest = t.start_id AND t2.Origin = a_b.p1_b_code
