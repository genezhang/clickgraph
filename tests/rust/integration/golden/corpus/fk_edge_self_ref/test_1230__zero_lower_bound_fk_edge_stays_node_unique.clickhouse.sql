WITH RECURSIVE vlp_a_b AS (
    SELECT 
        start_node.object_id as start_id,
        start_node.object_id as end_id,
        0 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.object_id] as path_nodes
    FROM test_integration.fs_objects_single AS start_node
    UNION ALL
    SELECT
        new_start.object_id as start_id,
        vp.end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat([new_start.object_id], vp.path_nodes) as path_nodes
    FROM vlp_a_b vp
    JOIN test_integration.fs_objects_single current_node ON vp.start_id = current_node.object_id
    JOIN test_integration.fs_objects_single new_start ON new_start.parent_id = current_node.object_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_nodes, new_start.object_id)
)
SELECT 
      count(*) AS "count(*)"
FROM vlp_a_b AS t
