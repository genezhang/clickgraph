WITH RECURSIVE vlp_p_c AS (
    SELECT 
        start_node.object_id as start_id,
        end_node.object_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.object_id, end_node.object_id) as path_nodes,
        array(struct(start_node.object_id, end_node.object_id)) as path_edges,
        start_node.name as start_name
    FROM test_integration.fs_objects_single start_node
    JOIN test_integration.fs_objects_single end_node ON start_node.parent_id = end_node.object_id
    WHERE start_node.parent_id > 0
    UNION ALL
    SELECT
        new_start.object_id as start_id,
        vp.end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(array(new_start.object_id), vp.path_nodes) as path_nodes,
        concat(array(struct(new_start.object_id, current_node.object_id)), vp.path_edges) as path_edges,
        new_start.name as start_name
    FROM vlp_p_c vp
    JOIN test_integration.fs_objects_single current_node ON vp.start_id = current_node.object_id
    JOIN test_integration.fs_objects_single new_start ON new_start.parent_id = current_node.object_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(new_start.object_id, current_node.object_id))
      AND new_start.parent_id > 0
)
SELECT 
      t.start_name AS `p.name`
FROM vlp_p_c AS t
