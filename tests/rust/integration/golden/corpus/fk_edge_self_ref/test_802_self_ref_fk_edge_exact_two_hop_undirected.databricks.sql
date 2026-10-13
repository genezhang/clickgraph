WITH RECURSIVE v1_both AS (
SELECT 
      e.object_id AS `object_id`, 
      e.parent_id AS `parent_id`, 
      e.object_id AS `__cg_start_0`, 
      e.parent_id AS `__cg_end_0`
FROM test_integration.fs_objects_single AS e

UNION ALL
SELECT 
      e.object_id AS `object_id`, 
      e.parent_id AS `parent_id`, 
      e.parent_id AS `__cg_start_0`, 
      e.object_id AS `__cg_end_0`
FROM test_integration.fs_objects_single AS e
WHERE e.object_id <> e.parent_id
), 
vlp_v1_path_inner AS (
    SELECT 
        start_node.object_id as start_id,
        end_node.object_id as end_id,
        1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        array(start_node.object_id, end_node.object_id) as path_nodes,
        array(struct(rel.object_id, rel.parent_id)) as path_edges
    FROM test_integration.fs_objects_single AS start_node
    JOIN v1_both AS rel ON start_node.object_id = rel.__cg_start_0
    JOIN test_integration.fs_objects_single AS end_node ON rel.__cg_end_0 = end_node.object_id
    UNION ALL
    SELECT
        vp.start_id,
        end_node.object_id as end_id,
        vp.hop_count + 1 as hop_count,
        CAST(array() AS ARRAY<STRING>) as path_relationships,
        concat(vp.path_nodes, array(end_node.object_id)) as path_nodes,
        concat(vp.path_edges, array(struct(rel.object_id, rel.parent_id))) as path_edges
    FROM vlp_v1_path_inner vp
    JOIN v1_both AS rel ON vp.end_id = rel.__cg_start_0
    JOIN test_integration.fs_objects_single AS end_node ON rel.__cg_end_0 = end_node.object_id
    WHERE vp.hop_count < 2
      AND NOT array_contains(vp.path_edges, struct(rel.object_id, rel.parent_id))
),
vlp_v1_path AS (
    SELECT * FROM vlp_v1_path_inner WHERE hop_count >= 2
)
SELECT 
      v0.name AS `a.name`, 
      v2.name AS `b.name`
FROM vlp_v1_path AS v1
JOIN test_integration.fs_objects_single AS v0 ON v1.start_id = v0.object_id
JOIN test_integration.fs_objects_single AS v2 ON v1.end_id = v2.object_id

-- column a.name: Value [("a.name", "a.name")]
-- column b.name: Value [("b.name", "b.name")]