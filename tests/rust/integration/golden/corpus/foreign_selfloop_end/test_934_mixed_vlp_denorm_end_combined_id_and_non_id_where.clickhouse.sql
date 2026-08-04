WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.pid as start_id,
        rel.emp_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.pid, rel.emp_id] as path_nodes,
        [tuple(rel.mgr_id, rel.emp_id)] as path_edges,
        start_node.name as start_name,
        end_own.name as end_name
    FROM testdb.people start_node
    JOIN testdb.reports rel ON start_node.pid = rel.mgr_id
    LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) end_own ON end_own.pid = rel.emp_id
    WHERE (rel.emp_id = 2 AND end_own.name = 'Bob')
    UNION ALL
    SELECT
        vp.start_id,
        rel.emp_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [rel.emp_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.mgr_id, rel.emp_id)]) as path_edges,
        vp.start_name as start_name,
        end_own.name as end_name
    FROM vlp_a_b_inner vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    LEFT JOIN (SELECT pid, any(name) as name FROM testdb.people GROUP BY pid) end_own ON end_own.pid = rel.emp_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.mgr_id, rel.emp_id))
      AND (rel.emp_id = 2 AND end_own.name = 'Bob')
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE ((end_id = 2 AND end_name = 'Bob')) AND hop_count >= 2
)
SELECT 
      t.start_name AS "a.name"
FROM vlp_a_b AS t
