WITH RECURSIVE vlp_v1_path_inner AS (
    SELECT 
        start_node.object_id as start_id,
        end_node.object_id as end_id,
        1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        [start_node.object_id, end_node.object_id] as path_nodes,
        [tuple(rel.object_id, rel.parent_id)] as path_edges
    FROM test_integration.fs_objects_single AS start_node
    JOIN test_integration.fs_objects_single AS rel ON start_node.object_id = rel.object_id
    JOIN test_integration.fs_objects_single AS end_node ON rel.parent_id = end_node.object_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.object_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST([] AS Array(String)) as path_relationships,
        arrayConcat(vp.path_nodes, [end_node.object_id]) as path_nodes,
        arrayConcat(vp.path_edges, [tuple(rel.object_id, rel.parent_id)]) as path_edges
    FROM vlp_v1_path_inner vp
    JOIN test_integration.fs_objects_single AS rel ON vp.end_id = rel.object_id
    JOIN test_integration.fs_objects_single AS end_node ON rel.parent_id = end_node.object_id
    WHERE vp.hop_count < 3
      AND NOT has(vp.path_edges, tuple(rel.object_id, rel.parent_id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 3
)
SELECT 
      v0.name AS "a.name", 
      v2.name AS "b.name"
FROM vlp_v1_path AS v1
JOIN test_integration.fs_objects_single AS v0 ON v1.start_id = v0.object_id
JOIN test_integration.fs_objects_single AS v2 ON v1.end_id = v2.object_id

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]