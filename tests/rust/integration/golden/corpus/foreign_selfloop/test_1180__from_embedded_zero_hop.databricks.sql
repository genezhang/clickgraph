WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.pid as start_id,
        start_node.pid as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.pid) as path_nodes,
        (
            SELECT slice(array(struct(__seed_edge.mgr_id, __seed_edge.emp_id)), 1, 0)
            FROM testdb.reports AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM testdb.people AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.pid)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.mgr_id, rel.emp_id))) as path_edges
    FROM vlp_v1_path vp
    JOIN testdb.reports AS rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people AS end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.mgr_id, rel.emp_id))
)
SELECT 
      v0.pid AS `a.pid`, 
      v2.pid AS `b.pid`
FROM vlp_v1_path AS v1
JOIN testdb.people AS v0 ON v1.start_id = v0.pid
JOIN testdb.people AS v2 ON v1.end_id = v2.pid
