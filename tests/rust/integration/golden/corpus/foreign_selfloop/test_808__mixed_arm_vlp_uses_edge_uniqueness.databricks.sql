WITH RECURSIVE vlp_a_b_inner AS (
    SELECT 
        rel.mgr_id as start_id,
        end_node.pid as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(rel.mgr_id, end_node.pid) as path_nodes,
        array(struct(rel.mgr_id, rel.emp_id)) as path_edges,
        end_node.name as end_name
    FROM testdb.reports rel
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    LEFT JOIN (SELECT pid, any_value(name) as name FROM testdb.people GROUP BY pid) start_own ON start_own.pid = rel.mgr_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.pid)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.mgr_id, rel.emp_id))) as path_edges,
        end_node.name as end_name
    FROM vlp_a_b_inner vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.mgr_id, rel.emp_id))
),
vlp_a_b AS (
    SELECT * FROM vlp_a_b_inner WHERE hop_count >= 2
)
SELECT 
      t.end_name AS `b.name`
FROM vlp_a_b AS t
