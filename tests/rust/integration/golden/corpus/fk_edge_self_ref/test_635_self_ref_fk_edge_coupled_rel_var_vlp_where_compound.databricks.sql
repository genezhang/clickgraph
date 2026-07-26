WITH RECURSIVE vlp_c_p_inner AS (
    SELECT 
        start_node.object_id as start_id,
        end_node.object_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.object_id, end_node.object_id) as path_nodes,
        end_node.name as end_name
    FROM test_integration.fs_objects_single start_node
    JOIN test_integration.fs_objects_single end_node ON start_node.parent_id = end_node.object_id
    WHERE end_node.name = 'x' AND start_node.parent_id > 0
    UNION ALL
    SELECT
        new_start.object_id as start_id,
        vp.end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(array(new_start.object_id), vp.path_nodes) as path_nodes,
        vp.end_name as end_name
    FROM vlp_c_p_inner vp
    JOIN test_integration.fs_objects_single current_node ON vp.start_id = current_node.object_id
    JOIN test_integration.fs_objects_single new_start ON new_start.parent_id = current_node.object_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_nodes, new_start.object_id)
      AND new_start.parent_id > 0
),
vlp_c_p AS (
    SELECT * FROM vlp_c_p_inner WHERE (end_name = 'x')
)
SELECT 
      t.end_name AS `p.name`
FROM vlp_c_p AS t
