WITH RECURSIVE vlp_a_b AS (
    SELECT 
        rel.mgr_id as start_id,
        end_node.pid as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [rel.mgr_id, end_node.pid] as path_nodes,
        [tuple(rel.mgr_id, rel.emp_id)] as path_edges
    FROM testdb.reports rel
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.pid]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.mgr_id, rel.emp_id)]) as path_edges
    FROM vlp_a_b vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.mgr_id, rel.emp_id))
)
SELECT 
      t0.mgr_id AS "c.pid", 
      t.start_id AS "a.pid", 
      t.end_id AS "b.pid"
FROM vlp_a_b AS t
INNER JOIN testdb.reports AS t0 ON t0.emp_id = t.start_id
