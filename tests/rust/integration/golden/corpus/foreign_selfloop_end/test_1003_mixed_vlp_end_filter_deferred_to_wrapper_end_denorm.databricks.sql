WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        start_node.pid as start_id,
        rel.emp_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.pid, rel.emp_id) as path_nodes,
        array(struct(rel.mgr_id, rel.emp_id)) as path_edges,
        start_node.name as start_name,
        end_own.name as end_name
    FROM testdb.people start_node
    JOIN testdb.reports rel ON start_node.pid = rel.mgr_id
    LEFT JOIN (SELECT pid, any_value(name) as name FROM testdb.people GROUP BY pid) end_own ON end_own.pid = rel.emp_id
    UNION ALL
    SELECT
        vp.start_id,
        rel.emp_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(rel.emp_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.mgr_id, rel.emp_id))) as path_edges,
        vp.start_name as start_name,
        end_own.name as end_name
    FROM vlp_a_b_inner vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    LEFT JOIN (SELECT pid, any_value(name) as name FROM testdb.people GROUP BY pid) end_own ON end_own.pid = rel.emp_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.mgr_id, rel.emp_id))
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE (end_name = 'Alice') AND hop_count >= 2
)
SELECT 
      t.start_name AS `a.name`
FROM vlp_a_b AS t
