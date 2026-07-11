WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        ['FLIGHT'] as path_relationships
    FROM test_integration.flights AS t0
    WHERE t0.Origin = 'LAX' AND hop_count <= 2
    UNION ALL
    SELECT
        vp.start_id as start_id,
        next.Dest as end_id,
        vp.hop_count + 1,
        arrayConcat(vp.path_edges, [next.Origin]),
        arrayConcat(vp.path_nodes, [next.Dest]),
        arrayConcat(vp.path_relationships, ['FLIGHT']) as path_relationships
    FROM vlp_a_b vp
    JOIN test_integration.flights next ON next.Origin = vp.end_id
    WHERE vp.hop_count < 2 AND NOT has(vp.path_nodes, next.Dest)
), 
with_a_b_hops_path_nodes_cte_0 AS (SELECT 
      start_city AS "p1_a_city", 
      end_city AS "p1_b_city", 
      path_nodes AS "path_nodes", 
      hop_count AS "hops"
FROM vlp_a_b AS t
WHERE hops = 2
)
SELECT 
      a_b_hops_path_nodes.p1_a_city AS "a.city", 
      a_b_hops_path_nodes.p1_b_city AS "b.city", 
      length(a_b_hops_path_nodes.path_nodes) AS "node_count"
FROM with_a_b_hops_path_nodes_cte_0 AS a_b_hops_path_nodes
ORDER BY a_b_hops_path_nodes.p1_b_city ASC
LIMIT 5