WITH RECURSIVE vlp_a_b AS (
    SELECT DISTINCT 
        start_node.mgr_id as start_id,
        start_node.mgr_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.mgr_id] as path_nodes
    FROM testdb.reports AS start_node
    WHERE start_node.mgr_id = 2
    UNION ALL
    SELECT
        vp.start_id,
        end_node.pid as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.pid]) as path_nodes
    FROM vlp_a_b vp
    JOIN testdb.reports rel ON vp.end_id = rel.mgr_id
    JOIN testdb.people end_node ON rel.emp_id = end_node.pid
    WHERE vp.hop_count < 2
      AND NOT has(vp.path_nodes, end_node.pid)
)
SELECT 
      t.end_id AS "b.pid"
FROM vlp_a_b AS t
