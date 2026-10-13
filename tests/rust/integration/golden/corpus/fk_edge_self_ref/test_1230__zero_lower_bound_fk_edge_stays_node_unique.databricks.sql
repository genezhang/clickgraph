WITH RECURSIVE vlp_v1_path AS (
    SELECT 
        start_node.object_id as start_id,
        start_node.object_id as end_id,
        0 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.object_id) as path_nodes,
        (
            SELECT slice(array(struct(__seed_edge.object_id, __seed_edge.parent_id)), 1, 0)
            FROM test_integration.fs_objects_single AS __seed_edge
            LIMIT 1
        ) as path_edges
    FROM test_integration.fs_objects_single AS start_node
    UNION ALL
    SELECT
        vp.start_id,
        end_node.object_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.object_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.object_id, rel.parent_id))) as path_edges
    FROM vlp_v1_path vp
    JOIN test_integration.fs_objects_single AS rel ON vp.end_id = rel.object_id
    JOIN test_integration.fs_objects_single AS end_node ON rel.parent_id = end_node.object_id
    WHERE vp.hop_count < 3
      AND NOT array_contains(vp.path_edges, struct(rel.object_id, rel.parent_id))
)
SELECT 
      count(*) AS `count(*)`
FROM vlp_v1_path AS v1
JOIN test_integration.fs_objects_single AS v0 ON v1.start_id = v0.object_id
JOIN test_integration.fs_objects_single AS v2 ON v1.end_id = v2.object_id
