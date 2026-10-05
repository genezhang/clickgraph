WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.pid as start_id,
        start_node.pid as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.pid] as path_nodes,
        (
            SELECT arraySlice([tuple(__seed_edge.mgr_id, __seed_edge.emp_id)], 1, 0)
            FROM testdb.reports AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM testdb.people AS start_node
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
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_edges, tuple(rel.mgr_id, rel.emp_id))
)
SELECT 
      t.start_id AS "a.pid", 
      t.end_emp_id AS "b.pid"
FROM vlp_a_b AS t
