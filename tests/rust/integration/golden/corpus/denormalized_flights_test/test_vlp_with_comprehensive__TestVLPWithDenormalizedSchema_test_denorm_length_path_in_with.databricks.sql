WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        array(t0.Origin) as path_edges,
        array(t0.Origin, t0.Dest) as path_nodes,
        array('FLIGHT') as path_relationships
    FROM test_integration.flights AS t0
    WHERE t0.Origin = 'LAX' AND hop_count <= 2
    UNION ALL
    SELECT
        next.Origin as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        concat(vp.path_edges, array(next.Origin)),
        concat(vp.path_nodes, array(next.Dest)),
        concat(vp.path_relationships, array('FLIGHT')) as path_relationships
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT array_contains(vp.path_nodes, next.Dest)
), 
with_a_b_hops_cte_0 AS (SELECT 
      start_city AS `p1_a_city`, 
      end_city AS `p1_b_city`, 
      hop_count AS `hops`
FROM vlp_a_b AS t
WHERE hop_count = 2
)
SELECT 
      a_b_hops.p1_a_city AS `a.city`, 
      a_b_hops.p1_b_city AS `b.city`, 
      a_b_hops.hops AS `hops`
FROM with_a_b_hops_cte_0 AS a_b_hops
ORDER BY a_b_hops.p1_b_city ASC
LIMIT 5