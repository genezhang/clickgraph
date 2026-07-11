WITH RECURSIVE vlp_a_b AS (
    SELECT
        t0.Origin as start_id,
        t0.Dest as end_id,
        1 as hop_count,
        [t0.Origin] as path_edges,
        [t0.Origin, t0.Dest] as path_nodes,
        ['FLIGHT'] as path_relationships
    FROM test_integration.flights AS t0
    WHERE t0.Origin = 'LAX' AND 1 <= 2
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
with_dest_dest_state_hops_origin_cte_0 AS (SELECT 
      t.start_city AS "origin", 
      t.end_city AS "dest", 
      t.end_state AS "dest_state", 
      hop_count AS "hops"
FROM vlp_a_b AS t
WHERE hops = 2
)
SELECT 
      dest_dest_state_hops_origin.origin AS "origin", 
      dest_dest_state_hops_origin.dest AS "dest", 
      dest_dest_state_hops_origin.dest_state AS "dest_state", 
      dest_dest_state_hops_origin.hops AS "hops"
FROM with_dest_dest_state_hops_origin_cte_0 AS dest_dest_state_hops_origin
ORDER BY dest_dest_state_hops_origin.dest ASC
LIMIT 5