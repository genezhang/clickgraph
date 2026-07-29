WITH RECURSIVE vlp_a_b AS (
    SELECT 
        concat(toString(start_node.region), '|', toString(start_node.object_id)) as start_id,
        concat(toString(end_node.region), '|', toString(end_node.object_id)) as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [concat(toString(start_node.region), '|', toString(start_node.object_id)), concat(toString(end_node.region), '|', toString(end_node.object_id))] as path_nodes,
        [tuple(start_node.region, start_node.object_id, end_node.region, end_node.object_id)] as path_edges,
        start_node.name as start_name,
        end_node.name as end_name
    FROM test_integration.fs_objects_composite start_node
    JOIN test_integration.fs_objects_composite end_node ON start_node.parent_region = end_node.region AND start_node.parent_id = end_node.object_id
    UNION ALL
    SELECT
        concat(toString(new_start.region), '|', toString(new_start.object_id)) as start_id,
        vp.end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat([concat(toString(new_start.region), '|', toString(new_start.object_id))], vp.path_nodes) as path_nodes,
        arrayConcat([tuple(new_start.region, new_start.object_id, current_node.region, current_node.object_id)], vp.path_edges) as path_edges,
        new_start.name as start_name,
        vp.end_name as end_name
    FROM vlp_a_b vp
    JOIN test_integration.fs_objects_composite current_node ON vp.start_id = concat(toString(current_node.region), '|', toString(current_node.object_id))
    JOIN test_integration.fs_objects_composite new_start ON new_start.parent_region = current_node.region AND new_start.parent_id = current_node.object_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(new_start.region, new_start.object_id, current_node.region, current_node.object_id))
)
SELECT 
      t.start_name AS "a.name", 
      t.end_name AS "b.name"
FROM vlp_a_b AS t
