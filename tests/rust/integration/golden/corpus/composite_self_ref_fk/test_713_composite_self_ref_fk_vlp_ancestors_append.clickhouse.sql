WITH RECURSIVE vlp_a_b AS (
    SELECT 
        concat(toString(start_node.region), '|', toString(start_node.object_id)) as start_id,
        concat(toString(end_node.region), '|', toString(end_node.object_id)) as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [concat(toString(start_node.region), '|', toString(start_node.object_id)), concat(toString(end_node.region), '|', toString(end_node.object_id))] as path_nodes,
        [tuple(start_node.region, start_node.object_id, end_node.region, end_node.object_id)] as path_edges,
        end_node.name as end_name
    FROM test_integration.fs_objects_composite start_node
    JOIN test_integration.fs_objects_composite end_node ON start_node.parent_region = end_node.region AND start_node.parent_id = end_node.object_id
    WHERE start_node.name = 'doc'
    UNION ALL
    SELECT
        vp.start_id,
        concat(toString(new_end.region), '|', toString(new_end.object_id)) as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [concat(toString(new_end.region), '|', toString(new_end.object_id))]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(current_node.region, current_node.object_id, new_end.region, new_end.object_id)]) as path_edges,
        new_end.name as end_name
    FROM vlp_a_b vp
    JOIN test_integration.fs_objects_composite current_node ON vp.end_id = concat(toString(current_node.region), '|', toString(current_node.object_id))
    JOIN test_integration.fs_objects_composite new_end ON current_node.parent_region = new_end.region AND current_node.parent_id = new_end.object_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(current_node.region, current_node.object_id, new_end.region, new_end.object_id))
)
SELECT 
      t.end_name AS "b.name"
FROM vlp_a_b AS t
