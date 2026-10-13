WITH RECURSIVE vlp_v1_path_inner AS (
    SELECT 
        start_node.pid as start_id,
        end_node.pid as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.pid, end_node.pid) as path_nodes,
        array(struct(rel.mgr_id, rel.emp_id)) as path_edges
    FROM testdb.people AS start_node
    JOIN testdb.reports AS rel ON start_node.pid = rel.mgr_id
    JOIN testdb.people AS end_node ON rel.emp_id = end_node.pid
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.pid)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.mgr_id, rel.emp_id))) as path_edges
    FROM vlp_v1_path_inner vp
    JOIN testdb.reports AS rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people AS end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.mgr_id, rel.emp_id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
)
SELECT 
      v2.name AS `b.name`
FROM vlp_v1_path AS v1
JOIN testdb.people AS v0 ON v1.start_id = v0.pid
JOIN testdb.people AS v2 ON v1.end_id = v2.pid
