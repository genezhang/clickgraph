WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.user_id as start_id,
        start_node.user_id as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.user_id) as path_nodes,
        start_node.name as start_name,
        start_node.name as end_name
    FROM test_integration.users AS start_node
    WHERE start_node.name = 'Alice'
)
SELECT 
      t.start_name AS `a.name`, 
      t.end_name AS `b.name`
FROM vlp_a_b AS t
