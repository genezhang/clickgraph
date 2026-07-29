WITH RECURSIVE vlp_a_b AS (
    SELECT 
        concat(string(start_node.region), '|', string(start_node.object_id)) as start_id,
        concat(string(end_node.region), '|', string(end_node.object_id)) as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(concat(string(start_node.region), '|', string(start_node.object_id)), concat(string(end_node.region), '|', string(end_node.object_id))) as path_nodes,
        array(struct(start_node.region, start_node.object_id, end_node.region, end_node.object_id)) as path_edges,
        start_node.name as start_name,
        end_node.name as end_name
    FROM test_integration.fs_objects_composite start_node
    JOIN test_integration.fs_objects_composite end_node ON start_node.parent_region = end_node.region AND start_node.parent_id = end_node.object_id
    UNION ALL
    SELECT
        concat(string(new_start.region), '|', string(new_start.object_id)) as start_id,
        vp.end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(array(concat(string(new_start.region), '|', string(new_start.object_id))), vp.path_nodes) as path_nodes,
        concat(array(struct(new_start.region, new_start.object_id, current_node.region, current_node.object_id)), vp.path_edges) as path_edges,
        new_start.name as start_name,
        vp.end_name as end_name
    FROM vlp_a_b vp
    JOIN test_integration.fs_objects_composite current_node ON vp.start_id = concat(string(current_node.region), '|', string(current_node.object_id))
    JOIN test_integration.fs_objects_composite new_start ON new_start.parent_region = current_node.region AND new_start.parent_id = current_node.object_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(new_start.region, new_start.object_id, current_node.region, current_node.object_id))
)
SELECT 
      t.start_name AS `a.name`, 
      t.end_name AS `b.name`
FROM vlp_a_b AS t
