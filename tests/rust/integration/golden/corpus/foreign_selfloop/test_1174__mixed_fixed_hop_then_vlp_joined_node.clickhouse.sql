WITH RECURSIVE vlp_v3_path AS (
    SELECT 
        start_node.pid as start_id,
        end_node.pid as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.pid, end_node.pid] as path_nodes,
        [tuple(rel.mgr_id, rel.emp_id)] as path_edges
    FROM testdb.people AS start_node
    JOIN testdb.reports AS rel ON start_node.pid = rel.mgr_id
    JOIN testdb.people AS end_node ON rel.emp_id = end_node.pid
    WHERE (start_node.pid IN (SELECT DISTINCT 
      v2.pid AS "id"
FROM testdb.people AS v0
JOIN testdb.reports AS v1 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v2 ON v1.emp_id = v2.pid))
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.pid]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.mgr_id, rel.emp_id)]) as path_edges
    FROM vlp_v3_path vp
    JOIN testdb.reports AS rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people AS end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.mgr_id, rel.emp_id))
)
SELECT 
      v0.pid AS "c.pid", 
      v2.pid AS "a.pid", 
      v4.pid AS "b.pid"
FROM vlp_v3_path AS v3
JOIN testdb.people AS v2 ON v3.start_id = v2.pid
JOIN testdb.reports AS v1 ON v1.emp_id = v2.pid
JOIN testdb.people AS v0 ON v1.mgr_id = v0.pid
JOIN testdb.people AS v4 ON v3.end_id = v4.pid
WHERE NOT has(v3.path_edges, tuple(v1.mgr_id, v1.emp_id))
