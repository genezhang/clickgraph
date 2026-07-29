WITH RECURSIVE vlp_a_b AS (
    SELECT 
        concat(string(start_node.region), '|', string(start_node.object_id)) as start_id,
        concat(string(end_node.region), '|', string(end_node.object_id)) as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(concat(string(start_node.region), '|', string(start_node.object_id)), concat(string(end_node.region), '|', string(end_node.object_id))) as path_nodes,
        array(struct(start_node.region, start_node.object_id, end_node.region, end_node.object_id)) as path_edges,
        end_node.name as end_name
    FROM test_integration.fs_objects_composite start_node
    JOIN test_integration.fs_objects_composite end_node ON start_node.parent_region = end_node.region AND start_node.parent_id = end_node.object_id
    WHERE start_node.name = 'doc'
    UNION ALL
    SELECT
        vp.start_id,
        concat(string(new_end.region), '|', string(new_end.object_id)) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(concat(string(new_end.region), '|', string(new_end.object_id)))) as path_nodes,
        concat(vp.path_edges, array(struct(current_node.region, current_node.object_id, new_end.region, new_end.object_id))) as path_edges,
        new_end.name as end_name
    FROM vlp_a_b vp
    JOIN test_integration.fs_objects_composite current_node ON vp.end_id = concat(string(current_node.region), '|', string(current_node.object_id))
    JOIN test_integration.fs_objects_composite new_end ON current_node.parent_region = new_end.region AND current_node.parent_id = new_end.object_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(current_node.region, current_node.object_id, new_end.region, new_end.object_id))
)
SELECT 
      t.end_name AS `b.name`
FROM vlp_a_b AS t
