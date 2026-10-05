WITH RECURSIVE vlp_a_b AS (
    SELECT 
        rel.mgr_id as start_id,
        end_node.pid as end_id,
        1 as hop_count,
        array('REPORTS_TO') as path_relationships,
        array(rel.mgr_id, end_node.pid) as path_nodes,
        array(struct(rel.mgr_id, rel.emp_id)) as path_edges
    FROM testdb.reports rel
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    LEFT JOIN (SELECT pid, any_value(name) as name FROM testdb.people GROUP BY pid) start_own ON start_own.pid = rel.mgr_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        concat(vp.path_relationships, array('REPORTS_TO')) as path_relationships,
        concat(vp.path_nodes, array(end_node.pid)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.mgr_id, rel.emp_id))) as path_edges
    FROM vlp_a_b vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.mgr_id, rel.emp_id))
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_a_b AS t
INNER JOIN testdb.reports AS t0 ON t0.emp_id = t.start_id
