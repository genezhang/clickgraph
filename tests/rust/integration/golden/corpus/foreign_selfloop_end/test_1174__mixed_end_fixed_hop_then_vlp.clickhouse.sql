WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.pid as start_id,
        rel.emp_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.pid, rel.emp_id] as path_nodes,
        [tuple(rel.mgr_id, rel.emp_id)] as path_edges
    FROM testdb.people start_node
    JOIN testdb.reports rel ON start_node.pid = rel.mgr_id
    LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) end_own ON end_own.pid = rel.emp_id
    UNION ALL
    SELECT
        vp.start_id,
        rel.emp_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [rel.emp_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.mgr_id, rel.emp_id)]) as path_edges
    FROM vlp_a_b vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) end_own ON end_own.pid = rel.emp_id
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.mgr_id, rel.emp_id))
)
SELECT 
      t0.mgr_id AS "c.pid", 
      t0.emp_id AS "a.pid"
FROM vlp_a_b AS t
INNER JOIN testdb.reports AS t0 ON t0.emp_id = t.start_id
