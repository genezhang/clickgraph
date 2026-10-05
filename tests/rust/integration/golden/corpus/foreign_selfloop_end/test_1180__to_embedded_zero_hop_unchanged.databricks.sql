WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.pid as start_id,
        start_node.pid as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.pid) as path_nodes
    FROM testdb.people AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        rel.emp_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(rel.emp_id)) as path_nodes
    FROM vlp_a_b vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_nodes, rel.emp_id)
)
SELECT 
      t.start_id AS `a.pid`, 
      t.end_emp_id AS `b.pid`
FROM vlp_a_b AS t
