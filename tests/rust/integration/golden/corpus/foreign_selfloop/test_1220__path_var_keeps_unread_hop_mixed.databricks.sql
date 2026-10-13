WITH RECURSIVE vlp_v3_path AS (
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
    WHERE (start_node.pid IN (SELECT DISTINCT 
      v2.pid AS `id`
FROM testdb.people AS v0
JOIN testdb.reports AS v1 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v2 ON v1.emp_id = v2.pid))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.pid)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.mgr_id, rel.emp_id))) as path_edges
    FROM vlp_v3_path vp
    JOIN testdb.reports AS rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people AS end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.mgr_id, rel.emp_id))
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_v3_path AS v3
JOIN testdb.people AS v2 ON v3.start_id = v2.pid
JOIN testdb.reports AS v1 ON v1.emp_id = v2.pid
JOIN testdb.people AS v0 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v4 ON v3.end_id = v4.pid
WHERE NOT array_contains(v3.path_edges, struct(v1.mgr_id, v1.emp_id))

-- column count(*): Value [("count(*)", "count(*)")]